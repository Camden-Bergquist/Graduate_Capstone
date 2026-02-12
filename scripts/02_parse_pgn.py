import os
import sys
import io
import re
import yaml
import time
import math
import hashlib
import argparse
import datetime as dt
from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple

import zstandard as zstd
import chess.pgn
import xxhash
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp



# Config/utilities

def load_cfg():
    with open("config/pipeline.yml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def parse_timecontrol(tc_str: str) -> Tuple[Optional[int], Optional[int]]:
    if not tc_str or tc_str == "-" or "+" not in tc_str:
        return None, None
    a, b = tc_str.split("+", 1)
    try:
        return int(a), int(b)
    except Exception:
        return None, None

def tc_bucket(initial: int, inc: int) -> str:
    # Lichess convention: base + 40*increment
    total = initial + 40 * inc
    if total < 180:
        return "bullet"
    if total < 600:
        return "blitz"
    if total < 1800:
        return "rapid"
    return "classical"

def h64_int(s: str) -> int:
    return xxhash.xxh64(s).intdigest()

def keep_by_hash_str(value: str, mod: int, keep_set: set[int]) -> bool:
    return (h64_int(value) % mod) in keep_set

def keep_by_game_id(game_id: str, mod: int, keep_set: set[int]) -> bool:
    return keep_by_hash_str(game_id, mod, keep_set)

def cohort_member(username: Optional[str], mod: int, keep_set: set[int]) -> bool:
    if username is None:
        return False
    return keep_by_hash_str(username, mod, keep_set)

def format_eta(seconds: float) -> str:
    if seconds is None or not math.isfinite(seconds) or seconds < 0:
        return "?"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m:02d}m"
    if m > 0:
        return f"{m}m {s:02d}s"
    return f"{s}s"

def write_run_info(run_info_dir: str):
    ensure_dir(run_info_dir)
    import platform
    lines = []
    lines.append(f"timestamp_utc: {dt.datetime.utcnow().isoformat()}Z")
    lines.append(f"python: {sys.version.replace(os.linesep,' ')}")
    lines.append(f"platform: {platform.platform()}")
    try:
        import chess as _chess, pyarrow as _pa, zstandard as _zstd, xxhash as _xx
        lines.append(f"python-chess: {_chess.__version__}")
        lines.append(f"pyarrow: {_pa.__version__}")
        lines.append(f"zstandard: {_zstd.__version__}")
        lines.append(f"xxhash: {_xx.__version__}")
    except Exception as e:
        lines.append(f"version_check_error: {e}")
    with open(os.path.join(run_info_dir, "python_versions_parse.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# Deterministic anonymization

@dataclass(frozen=True)
class AnonCfg:
    enabled: bool
    salt: str
    truncate: int
    salt_source: str  # "env" | "config" | "none"

def load_anon_cfg(cfg: dict) -> AnonCfg:
    a = cfg.get("anonymization", {}) or {}
    enabled = bool(a.get("enabled", False))
    trunc = int(a.get("truncate", 16))
    salt_env = os.environ.get("LICHESS_ANON_SALT", "") or ""
    salt_cfg = str(a.get("salt", "") or "")
    if salt_env:
        salt = salt_env
        src = "env"
    elif salt_cfg:
        salt = salt_cfg
        src = "config"
    else:
        salt = ""
        src = "none"
    if enabled and not salt:
        raise RuntimeError(
            "Anonymization enabled but no salt set. "
            "Set env var LICHESS_ANON_SALT or config anonymization.salt."
        )
    return AnonCfg(enabled=enabled, salt=salt, truncate=trunc, salt_source=src)

def anon_user(username: Optional[str], anon: AnonCfg) -> Optional[str]:
    if username is None:
        return None
    if not anon.enabled:
        return username
    h = hashlib.sha256((anon.salt + username).encode("utf-8")).hexdigest()
    return "u_" + h[:anon.truncate]


# Header-first PGN scanning

TAG_RE = re.compile(r'^\[(\w+)\s+"(.*)"\]\s*$')

def parse_headers_from_lines(header_lines: List[str]) -> Dict[str, str]:
    out = {}
    for ln in header_lines:
        m = TAG_RE.match(ln)
        if not m:
            continue
        out[m.group(1)] = m.group(2)
    return out

class LineReader:
    """Line-by-line reader with 1-line pushback."""
    def __init__(self, text_io):
        self._t = text_io
        self._buf = None

    def readline(self) -> str:
        if self._buf is not None:
            ln = self._buf
            self._buf = None
            return ln
        return self._t.readline()

    def pushback(self, ln: str):
        self._buf = ln

def read_one_game_header(reader: LineReader) -> Optional[List[str]]:
    """Read a [Tag "..."] header block; return list of header lines or None at EOF."""
    header_lines = []
    while True:
        ln = reader.readline()
        if not ln:
            return None
        s = ln.strip()
        if not s:
            continue
        if s.startswith("["):
            header_lines.append(ln.rstrip("\n"))
            break
    while True:
        ln = reader.readline()
        if not ln:
            break
        s = ln.strip()
        if not s:
            break
        if s.startswith("["):
            header_lines.append(ln.rstrip("\n"))
        else:
            reader.pushback(ln)
            break
    return header_lines

def skip_or_collect_movetext(reader: LineReader, keep_text: bool) -> Tuple[Optional[str], int]:
    """Read movetext until blank line or next header; optionally return text."""
    lines = [] if keep_text else None
    n_lines = 0
    while True:
        ln = reader.readline()
        if not ln:
            break
        s = ln.strip()
        if not s:
            break
        if s.startswith("["):
            reader.pushback(ln)
            break
        n_lines += 1
        if keep_text:
            lines.append(ln.rstrip("\n"))
    if keep_text:
        return "\n".join(lines), n_lines
    return None, n_lines


# ---------------------------
# Output buffering
# ---------------------------

def make_game_buffer(is_cohort: bool) -> Dict[str, List]:
    cols = {
        "game_id": [],
        "month": [],
        "utc_date": [],
        "utc_time": [],
        "white": [],
        "black": [],
        "white_rating": [],
        "black_rating": [],
        "rating_diff": [],
        "abs_rating_diff": [],
        "result": [],
        "winner": [],
        "score_white": [],
        "is_upset": [],
        "termination": [],
        "time_control": [],
        "initial": [],
        "increment": [],
        "tc_bucket": [],
        "eco": [],
        "opening": [],
        "plies": [],
        "moves": [],
    }
    if is_cohort:
        cols["white_in_cohort"] = []
        cols["black_in_cohort"] = []
    return cols

def make_moves_buffer() -> Dict[str, List]:
    return {
        "game_id": [],
        "month": [],
        "ply": [],
        "move_number": [],
        "side": [],
        "san": [],
        "uci": [],
        "clock_after": [],
        "time_spent": [],

        "tc_bucket": [],
        "initial": [],
        "increment": [],
        "white": [],
        "black": [],
        "white_rating": [],
        "black_rating": [],
        "result": [],
        "opening": [],
        "eco": [],
    }

def buffer_append(buf: Dict[str, List], row: Dict):
    for k in buf.keys():
        buf[k].append(row.get(k))

def flush_buffer(buf: Dict[str, List], out_path: str, compression: str):
    any_key = next(iter(buf.keys()))
    if len(buf[any_key]) == 0:
        return
    table = pa.Table.from_pydict(buf)
    pq.write_table(table, out_path, compression=compression)
    for k in buf.keys():
        buf[k].clear()

def seconds_from_clock(node) -> Optional[float]:
    """Return seconds remaining after move from [%clk ...] annotation if present."""
    try:
        return node.clock()
    except Exception:
        return None


# Month worker

@dataclass
class MonthResult:
    month: str
    ok: bool
    msg: str
    games_seen: int
    games_kept_meta: int
    games_kept_cohort: int
    games_kept_moves: int
    moves_rows: int
    seconds: float

def _dir_has_parquet(path: str) -> bool:
    if not os.path.isdir(path):
        return False
    try:
        for n in os.listdir(path):
            if n.endswith(".parquet"):
                return True
        return False
    except Exception:
        return False

def process_month(month: str, cfg: dict, compression: str, anon: AnonCfg,
                  skip_existing: bool, progress_every_sec: int) -> MonthResult:
    t0 = time.time()

    raw_dir = cfg["paths"]["raw_dir"]
    out_meta_root = cfg["paths"]["games_meta_dir"]
    out_cohort_root = cfg["paths"]["games_cohort_dir"]
    out_moves_root = cfg["paths"].get("moves_meta_dir", None)

    filters = cfg.get("filters", {}) or {}
    tc_allowed = set(filters.get("tc_buckets", []) or [])
    min_rating = filters.get("min_rating", None)
    max_rating = filters.get("max_rating", None)
    drop_terms = set(filters.get("drop_terminations", []) or [])

    samp = cfg.get("sampling", {}) or {}

    meta_mod = int(samp["meta"]["hash_mod"])
    meta_keep = set(int(x) for x in samp["meta"]["hash_keep"])

    cohort_mod = int(samp["cohort"]["player_hash_mod"])
    cohort_keep = set(int(x) for x in samp["cohort"]["player_hash_keep"])

    moves_enabled = bool(out_moves_root) and ("moves" in samp)
    if moves_enabled:
        moves_mod = int(samp["moves"]["hash_mod"])
        moves_keep = set(int(x) for x in samp["moves"]["hash_keep"])
    else:
        moves_mod, moves_keep = 1, {0}

    perf = cfg.get("performance", {}) or {}
    # Games: allow either performance.chunk_size or performance.games_chunk_size
    chunk_size = int(perf.get("chunk_size", perf.get("games_chunk_size", 25000)))
    moves_chunk_size = int(perf.get("moves_chunk_size", 250000))

    fname = f"lichess_db_standard_rated_{month}.pgn.zst"
    path = os.path.join(raw_dir, fname)
    if not os.path.exists(path):
        return MonthResult(month, False, f"Missing file: {path}", 0, 0, 0, 0, 0, time.time() - t0)

    meta_month_dir = os.path.join(out_meta_root, f"month={month}")
    cohort_month_dir = os.path.join(out_cohort_root, f"month={month}")
    ensure_dir(meta_month_dir)
    ensure_dir(cohort_month_dir)

    moves_month_dir = None
    if moves_enabled:
        moves_month_dir = os.path.join(out_moves_root, f"month={month}")
        ensure_dir(moves_month_dir)

    # Skip logic: skip if all enabled outputs already have parquet files
    if skip_existing:
        meta_done = _dir_has_parquet(meta_month_dir)
        cohort_done = _dir_has_parquet(cohort_month_dir)
        moves_done = True if not moves_enabled else _dir_has_parquet(moves_month_dir)
        if meta_done and cohort_done and moves_done:
            return MonthResult(month, True, "Skipped (outputs exist)", 0, 0, 0, 0, 0, time.time() - t0)

    meta_buf = make_game_buffer(is_cohort=False)
    cohort_buf = make_game_buffer(is_cohort=True)
    moves_buf = make_moves_buffer() if moves_enabled else None

    meta_part = 0
    cohort_part = 0
    moves_part = 0

    games_seen = 0
    kept_meta = 0
    kept_cohort = 0
    kept_moves = 0
    moves_rows = 0

    total_comp_bytes = os.path.getsize(path)
    last_print = time.time()
    start_read = time.time()

    def maybe_print_progress(fobj):
        nonlocal last_print
        now = time.time()
        if now - last_print < progress_every_sec:
            return
        last_print = now
        try:
            comp_read = fobj.tell()
        except Exception:
            comp_read = 0
        frac = comp_read / total_comp_bytes if total_comp_bytes > 0 else 0.0
        elapsed = now - start_read
        rate = comp_read / elapsed if elapsed > 0 else 0.0
        eta = (total_comp_bytes - comp_read) / rate if rate > 0 else float("inf")
        print(
            f"[{month}] {frac*100:5.1f}%  "
            f"seen={games_seen:,} meta={kept_meta:,} cohort={kept_cohort:,} moves_games={kept_moves:,} moves_rows={moves_rows:,}  "
            f"read={comp_read/1e9:5.2f}GB/{total_comp_bytes/1e9:5.2f}GB  ETA={format_eta(eta)}",
            flush=True
        )

    try:
        with open(path, "rb") as f:
            dctx = zstd.ZstdDecompressor()
            stream = dctx.stream_reader(f)
            text = io.TextIOWrapper(stream, encoding="utf-8", errors="replace", newline="\n")
            reader = LineReader(text)

            while True:
                header_lines = read_one_game_header(reader)
                if header_lines is None:
                    break

                headers = parse_headers_from_lines(header_lines)
                games_seen += 1

                site = headers.get("Site", "")
                game_id = site.rsplit("/", 1)[-1] if site else None
                if not game_id:
                    skip_or_collect_movetext(reader, keep_text=False)
                    maybe_print_progress(f)
                    continue

                white_real = headers.get("White")
                black_real = headers.get("Black")
                result = headers.get("Result")
                term = headers.get("Termination", "")

                if term in drop_terms:
                    skip_or_collect_movetext(reader, keep_text=False)
                    maybe_print_progress(f)
                    continue

                try:
                    wE = int(headers.get("WhiteElo"))
                    bE = int(headers.get("BlackElo"))
                except Exception:
                    skip_or_collect_movetext(reader, keep_text=False)
                    maybe_print_progress(f)
                    continue

                if min_rating is not None and (wE < min_rating or bE < min_rating):
                    skip_or_collect_movetext(reader, keep_text=False)
                    maybe_print_progress(f)
                    continue
                if max_rating is not None and (wE > max_rating or bE > max_rating):
                    skip_or_collect_movetext(reader, keep_text=False)
                    maybe_print_progress(f)
                    continue

                tc = headers.get("TimeControl", "-")
                initial, inc = parse_timecontrol(tc)
                if initial is None or inc is None:
                    skip_or_collect_movetext(reader, keep_text=False)
                    maybe_print_progress(f)
                    continue

                bucket = tc_bucket(initial, inc)
                if tc_allowed and bucket not in tc_allowed:
                    skip_or_collect_movetext(reader, keep_text=False)
                    maybe_print_progress(f)
                    continue

                keep_meta = keep_by_game_id(game_id, meta_mod, meta_keep)

                w_in_cohort = cohort_member(white_real, cohort_mod, cohort_keep)
                b_in_cohort = cohort_member(black_real, cohort_mod, cohort_keep)
                keep_cohort = (w_in_cohort or b_in_cohort)

                keep_moves = False
                if moves_enabled:
                    keep_moves = keep_by_game_id(game_id, moves_mod, moves_keep)

                if not keep_meta and not keep_cohort and not keep_moves:
                    skip_or_collect_movetext(reader, keep_text=False)
                    maybe_print_progress(f)
                    continue

                movetext, _ = skip_or_collect_movetext(reader, keep_text=True)

                pgn_text = "\n".join(header_lines) + "\n\n" + (movetext or "") + "\n\n"
                game = chess.pgn.read_game(io.StringIO(pgn_text))
                if game is None:
                    maybe_print_progress(f)
                    continue

                # Determine winner / score
                if result == "1-0":
                    winner = "white"
                    score_white = 1.0
                elif result == "0-1":
                    winner = "black"
                    score_white = 0.0
                elif result == "1/2-1/2":
                    winner = "draw"
                    score_white = 0.5
                else:
                    maybe_print_progress(f)
                    continue

                rating_diff = wE - bE
                abs_rating_diff = abs(rating_diff)

                is_upset = 0
                if winner == "white" and rating_diff < 0:
                    is_upset = 1
                elif winner == "black" and rating_diff > 0:
                    is_upset = 1

                eco = headers.get("ECO", None)
                opening = headers.get("Opening", None)
                utc_date = headers.get("UTCDate", None)
                utc_time = headers.get("UTCTime", None)

                white = anon_user(white_real, anon)
                black = anon_user(black_real, anon)

                # Compute plies/moves cheaply (mainline)
                plies = 0
                node = game
                while node.variations:
                    node = node.variation(0)
                    plies += 1
                moves = (plies + 1) // 2

                base_row = {
                    "game_id": game_id,
                    "month": month,
                    "utc_date": utc_date,
                    "utc_time": utc_time,
                    "white": white,
                    "black": black,
                    "white_rating": wE,
                    "black_rating": bE,
                    "rating_diff": rating_diff,
                    "abs_rating_diff": abs_rating_diff,
                    "result": result,
                    "winner": winner,
                    "score_white": score_white,
                    "is_upset": is_upset,
                    "termination": term,
                    "time_control": tc,
                    "initial": initial,
                    "increment": inc,
                    "tc_bucket": bucket,
                    "eco": eco,
                    "opening": opening,
                    "plies": plies,
                    "moves": moves,
                }

                if keep_meta:
                    kept_meta += 1
                    buffer_append(meta_buf, base_row)
                    if len(meta_buf["game_id"]) >= chunk_size:
                        out_path = os.path.join(meta_month_dir, f"part-{meta_part:04d}.parquet")
                        flush_buffer(meta_buf, out_path, compression=compression)
                        meta_part += 1

                if keep_cohort:
                    kept_cohort += 1
                    row2 = dict(base_row)
                    row2["white_in_cohort"] = int(w_in_cohort)
                    row2["black_in_cohort"] = int(b_in_cohort)
                    buffer_append(cohort_buf, row2)
                    if len(cohort_buf["game_id"]) >= chunk_size:
                        out_path = os.path.join(cohort_month_dir, f"part-{cohort_part:04d}.parquet")
                        flush_buffer(cohort_buf, out_path, compression=compression)
                        cohort_part += 1

                # Move/clock extraction
                if keep_moves and moves_enabled and moves_month_dir and moves_buf is not None:
                    kept_moves += 1

                    board = game.board()
                    prev_white = float(initial)
                    prev_black = float(initial)

                    ply = 0
                    node = game
                    while node.variations:
                        next_node = node.variation(0)
                        ply += 1

                        move = next_node.move
                        uci = move.uci()
                        san = board.san(move)
                        board.push(move)

                        side = "white" if (ply % 2 == 1) else "black"
                        clock_after = seconds_from_clock(next_node)

                        time_spent = None
                        if clock_after is not None:
                            try:
                                ca = float(clock_after)
                                if side == "white":
                                    prev = prev_white
                                else:
                                    prev = prev_black
                                # Estimate spent time using increment
                                time_spent = prev - (ca - float(inc))
                                if time_spent < 0:
                                    time_spent = 0.0
                                if side == "white":
                                    prev_white = ca
                                else:
                                    prev_black = ca
                                clock_after = ca
                            except Exception:
                                clock_after = None
                                time_spent = None

                        moves_buf["game_id"].append(game_id)
                        moves_buf["month"].append(month)
                        moves_buf["ply"].append(ply)
                        moves_buf["move_number"].append((ply + 1) // 2)
                        moves_buf["side"].append(side)
                        moves_buf["san"].append(san)
                        moves_buf["uci"].append(uci)
                        moves_buf["clock_after"].append(clock_after)
                        moves_buf["time_spent"].append(time_spent)

                        moves_buf["tc_bucket"].append(bucket)
                        moves_buf["initial"].append(initial)
                        moves_buf["increment"].append(inc)
                        moves_buf["white"].append(white)
                        moves_buf["black"].append(black)
                        moves_buf["white_rating"].append(wE)
                        moves_buf["black_rating"].append(bE)
                        moves_buf["result"].append(result)
                        moves_buf["opening"].append(opening)
                        moves_buf["eco"].append(eco)

                        moves_rows += 1
                        node = next_node

                        if len(moves_buf["game_id"]) >= moves_chunk_size:
                            out_path = os.path.join(moves_month_dir, f"part-{moves_part:04d}.parquet")
                            flush_buffer(moves_buf, out_path, compression=compression)
                            moves_part += 1

                maybe_print_progress(f)

        # Flush remaining buffers
        if len(meta_buf["game_id"]) > 0:
            out_path = os.path.join(meta_month_dir, f"part-{meta_part:04d}.parquet")
            flush_buffer(meta_buf, out_path, compression=compression)

        if len(cohort_buf["game_id"]) > 0:
            out_path = os.path.join(cohort_month_dir, f"part-{cohort_part:04d}.parquet")
            flush_buffer(cohort_buf, out_path, compression=compression)

        if moves_enabled and moves_month_dir and moves_buf is not None and len(moves_buf["game_id"]) > 0:
            out_path = os.path.join(moves_month_dir, f"part-{moves_part:04d}.parquet")
            flush_buffer(moves_buf, out_path, compression=compression)

        sec = time.time() - t0
        msg = f"Wrote meta -> {meta_month_dir} ; cohort -> {cohort_month_dir}"
        if moves_enabled and moves_month_dir:
            msg += f" ; moves -> {moves_month_dir}"
        return MonthResult(
            month=month,
            ok=True,
            msg=msg,
            games_seen=games_seen,
            games_kept_meta=kept_meta,
            games_kept_cohort=kept_cohort,
            games_kept_moves=kept_moves,
            moves_rows=moves_rows,
            seconds=sec
        )

    except Exception as e:
        sec = time.time() - t0
        return MonthResult(
            month=month,
            ok=False,
            msg=f"Error: {e}",
            games_seen=games_seen,
            games_kept_meta=kept_meta,
            games_kept_cohort=kept_cohort,
            games_kept_moves=kept_moves,
            moves_rows=moves_rows,
            seconds=sec
        )


# Main (parallel orchestrator)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", default=None, help="Comma-separated list like 2024-01,2024-02 (overrides YAML months)")
    ap.add_argument("--workers", type=int, default=0, help="Parallel month workers (0 = auto)")
    ap.add_argument("--no-parallel", action="store_true", help="Force single-process parsing")
    ap.add_argument("--skip-existing", action="store_true", help="Skip months whose outputs already exist")
    ap.add_argument("--progress-every", type=int, default=30, help="Seconds between progress prints (per month)")
    args = ap.parse_args()

    cfg = load_cfg()

    run_info_dir = cfg["paths"]["run_info_dir"]
    write_run_info(run_info_dir)

    ensure_dir(cfg["paths"]["games_meta_dir"])
    ensure_dir(cfg["paths"]["games_cohort_dir"])
    if cfg["paths"].get("moves_meta_dir", None):
        ensure_dir(cfg["paths"]["moves_meta_dir"])

    parquet_cfg = cfg.get("parquet", {}) or {}
    compression = str(parquet_cfg.get("compression", "zstd")).lower()
    if compression not in ("zstd", "snappy", "gzip", "brotli", "lz4", "none"):
        raise RuntimeError(f"Unsupported parquet.compression: {compression}")

    anon = load_anon_cfg(cfg)

    months = cfg.get("months", []) or []
    if args.months:
        months = [m.strip() for m in args.months.split(",") if m.strip()]
    if not months:
        print("No months specified.", file=sys.stderr)
        sys.exit(2)

    if args.no_parallel:
        workers = 1
    else:
        if args.workers and args.workers > 0:
            workers = args.workers
        else:
            cpu = os.cpu_count() or 4
            workers = max(1, min(8, cpu // 2))

    moves_enabled = bool(cfg["paths"].get("moves_meta_dir", None)) and ("moves" in (cfg.get("sampling", {}) or {}))

    print(
        f"[INFO] months={len(months)} workers={workers} compression={compression} "
        f"anon_enabled={anon.enabled} salt_source={anon.salt_source} moves_enabled={moves_enabled}",
        flush=True
    )

    t_all = time.time()

    if workers == 1:
        for m in months:
            res = process_month(m, cfg, compression, anon, args.skip_existing, args.progress_every)
            status = "OK" if res.ok else "FAIL"
            print(
                f"[{status}] {res.month} | seen={res.games_seen:,} meta={res.games_kept_meta:,} "
                f"cohort={res.games_kept_cohort:,} moves_games={res.games_kept_moves:,} moves_rows={res.moves_rows:,} "
                f"| {res.msg} | {format_eta(res.seconds)}",
                flush=True
            )
        print(f"[DONE] total elapsed: {format_eta(time.time() - t_all)}", flush=True)
        return

    futures = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for m in months:
            futures.append(ex.submit(process_month, m, cfg, compression, anon, args.skip_existing, args.progress_every))

        done = 0
        ok = 0
        for fut in as_completed(futures):
            res = fut.result()
            done += 1
            ok += 1 if res.ok else 0
            status = "OK" if res.ok else "FAIL"
            print(
                f"[{status}] {res.month} ({done}/{len(months)}) | seen={res.games_seen:,} meta={res.games_kept_meta:,} "
                f"cohort={res.games_kept_cohort:,} moves_games={res.games_kept_moves:,} moves_rows={res.moves_rows:,} "
                f"| {res.msg} | {format_eta(res.seconds)}",
                flush=True
            )

    print(f"[DONE] months_ok={ok}/{len(months)} total elapsed: {format_eta(time.time() - t_all)}", flush=True)


if __name__ == "__main__":
    mp.freeze_support()
    main()
