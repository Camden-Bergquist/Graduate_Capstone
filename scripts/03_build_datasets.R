suppressPackageStartupMessages({
  library(yaml)
  library(duckdb)
  library(DBI)
})

# Helpers
`%||%` <- function(a, b) if (!is.null(a)) a else b
to_fwd <- function(p) gsub("\\\\", "/", normalizePath(p, mustWork = FALSE))
sql_q  <- function(p) paste0("'", gsub("'", "''", to_fwd(p)), "'")

has_any_parquet <- function(dir_path) {
  if (is.null(dir_path) || !dir.exists(dir_path)) return(FALSE)
  length(list.files(dir_path, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)) > 0
}

cfg <- yaml::read_yaml("config/pipeline.yml")
paths <- cfg$paths %||% list()

run_info_dir <- paths$run_info_dir %||% "data/derived/run_info"
dir.create(run_info_dir, recursive = TRUE, showWarnings = FALSE)

# Inputs
games_cohort_dir <- paths$games_cohort_dir
games_meta_dir   <- paths$games_meta_dir
moves_meta_dir   <- paths$moves_meta_dir %||% NULL

# Outputs (defaults if not provided)
out_player_month         <- paths$player_month_path %||% "data/derived/full_datasets/player_month.parquet"
out_player_month_tc      <- paths$player_month_tc_path %||% "data/derived/full_datasets/player_month_tc.parquet"
out_meta_month_tc        <- paths$meta_month_tc_path %||% "data/derived/full_datasets/meta_month_tc.parquet"
out_meta_opening_month   <- paths$meta_opening_month_tc_path %||% "data/derived/full_datasets/meta_opening_month_tc.parquet"
out_player_opening_tc    <- paths$player_opening_month_tc_path %||% "data/derived/full_datasets/player_opening_month_tc.parquet"

out_moves_month_tc       <- paths$moves_month_tc_path %||% "data/derived/moves_month_tc.parquet"
out_player_time_month_tc <- paths$player_time_month_tc_path %||% "data/derived/player_time_month_tc.parquet"

# Ensure output dirs exist
dir.create(dirname(out_player_month), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_player_month_tc), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_meta_month_tc), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_meta_opening_month), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_player_opening_tc), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_moves_month_tc), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_player_time_month_tc), recursive = TRUE, showWarnings = FALSE)

# In-memory DuckDB
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
on.exit({
  try(dbDisconnect(con, shutdown = TRUE), silent = TRUE)
}, add = TRUE)

# Prefer to respect compression choice if present
parq_comp <- tolower((cfg$parquet$compression %||% "zstd"))
if (!parq_comp %in% c("zstd","snappy","gzip","brotli","lz4","none","uncompressed")) parq_comp <- "zstd"
message("[INFO] DuckDB parquet compression = ", parq_comp)

# Player datasets from games_cohort
if (has_any_parquet(games_cohort_dir)) {
  
  parquet_glob <- file.path(games_cohort_dir, "**", "*.parquet")
  
  # Create a reusable VIEW with two rows per game (white+black perspectives), cohort-only
  base_sql <- sprintf("
  CREATE OR REPLACE VIEW subjects AS
  WITH games AS (
    SELECT
      month,
      game_id,
      white,
      black,
      white_rating,
      black_rating,
      rating_diff,
      result,
      tc_bucket,
      eco,
      opening,
      moves,
      white_in_cohort,
      black_in_cohort
    FROM read_parquet(%s)
  ),
  expanded AS (
    SELECT
      white AS player,
      month,
      tc_bucket,
      game_id,
      white_rating AS rating,
      black_rating AS opp_rating,
      CASE WHEN result = '1-0' THEN 1 ELSE 0 END AS win,
      CASE WHEN result = '0-1' THEN 1 ELSE 0 END AS loss,
      CASE WHEN result = '1/2-1/2' THEN 1 ELSE 0 END AS draw,
      eco,
      opening,
      moves,
      rating_diff AS rating_diff,
      CAST(white_in_cohort AS INTEGER) AS in_cohort
    FROM games
    WHERE white IS NOT NULL

    UNION ALL

    SELECT
      black AS player,
      month,
      tc_bucket,
      game_id,
      black_rating AS rating,
      white_rating AS opp_rating,
      CASE WHEN result = '0-1' THEN 1 ELSE 0 END AS win,
      CASE WHEN result = '1-0' THEN 1 ELSE 0 END AS loss,
      CASE WHEN result = '1/2-1/2' THEN 1 ELSE 0 END AS draw,
      eco,
      opening,
      moves,
      -rating_diff AS rating_diff,
      CAST(black_in_cohort AS INTEGER) AS in_cohort
    FROM games
    WHERE black IS NOT NULL
  )
  SELECT *
  FROM expanded
  WHERE in_cohort = 1 AND player IS NOT NULL
  ", sql_q(parquet_glob))
  
  dbExecute(con, base_sql)
  
  # player_month
  player_month_sql <- sprintf("
  COPY (
    WITH opening_counts AS (
      SELECT player, month, opening, COUNT(*) AS opening_n
      FROM subjects
      WHERE opening IS NOT NULL
      GROUP BY player, month, opening
    ),
    top_opening AS (
      SELECT player, month, opening AS top_opening
      FROM (
        SELECT
          player, month, opening, opening_n,
          ROW_NUMBER() OVER (
            PARTITION BY player, month
            ORDER BY opening_n DESC, opening ASC
          ) AS rn
        FROM opening_counts
      )
      WHERE rn = 1
    ),
    summary AS (
      SELECT
        player,
        month,
        COUNT(*) AS games,
        SUM(win) AS wins,
        SUM(loss) AS losses,
        SUM(draw) AS draws,
        CAST(SUM(win) AS DOUBLE) / COUNT(*) AS win_rate,
        CAST(SUM(draw) AS DOUBLE) / COUNT(*) AS draw_rate,
        AVG(rating) AS avg_rating,
        AVG(opp_rating) AS avg_opp_rating,
        AVG(rating_diff) AS avg_rating_diff,
        AVG(moves) AS avg_moves,
        COUNT(DISTINCT opening) AS opening_diversity
      FROM subjects
      GROUP BY player, month
    )
    SELECT s.*, t.top_opening
    FROM summary s
    LEFT JOIN top_opening t
    USING (player, month)
    ORDER BY player, month
  ) TO %s (FORMAT PARQUET, COMPRESSION %s);
  ", sql_q(out_player_month), toupper(parq_comp))
  
  dbExecute(con, player_month_sql)
  message("[OK] wrote: ", out_player_month)
  
  # player_month_tc
  player_month_tc_sql <- sprintf("
  COPY (
    WITH opening_counts AS (
      SELECT player, month, tc_bucket, opening, COUNT(*) AS opening_n
      FROM subjects
      WHERE opening IS NOT NULL
      GROUP BY player, month, tc_bucket, opening
    ),
    top_opening AS (
      SELECT player, month, tc_bucket, opening AS top_opening
      FROM (
        SELECT
          player, month, tc_bucket, opening, opening_n,
          ROW_NUMBER() OVER (
            PARTITION BY player, month, tc_bucket
            ORDER BY opening_n DESC, opening ASC
          ) AS rn
        FROM opening_counts
      )
      WHERE rn = 1
    ),
    summary AS (
      SELECT
        player,
        month,
        tc_bucket,
        COUNT(*) AS games,
        SUM(win) AS wins,
        SUM(loss) AS losses,
        SUM(draw) AS draws,
        CAST(SUM(win) AS DOUBLE) / COUNT(*) AS win_rate,
        CAST(SUM(draw) AS DOUBLE) / COUNT(*) AS draw_rate,
        AVG(rating) AS avg_rating,
        AVG(opp_rating) AS avg_opp_rating,
        AVG(rating_diff) AS avg_rating_diff,
        AVG(moves) AS avg_moves,
        COUNT(DISTINCT opening) AS opening_diversity
      FROM subjects
      GROUP BY player, month, tc_bucket
    )
    SELECT s.*, t.top_opening
    FROM summary s
    LEFT JOIN top_opening t
    USING (player, month, tc_bucket)
    ORDER BY player, month, tc_bucket
  ) TO %s (FORMAT PARQUET, COMPRESSION %s);
  ", sql_q(out_player_month_tc), toupper(parq_comp))
  
  dbExecute(con, player_month_tc_sql)
  message("[OK] wrote: ", out_player_month_tc)
  
  # player_opening_month_tc
  player_opening_sql <- sprintf("
  COPY (
    SELECT
      player,
      month,
      tc_bucket,
      eco,
      opening,
      COUNT(*) AS games,
      CAST(SUM(win) AS DOUBLE) / COUNT(*) AS win_rate,
      CAST(SUM(draw) AS DOUBLE) / COUNT(*) AS draw_rate,
      AVG(rating) AS avg_rating,
      AVG(rating_diff) AS avg_rating_diff
    FROM subjects
    GROUP BY player, month, tc_bucket, eco, opening
    ORDER BY player, month, tc_bucket, games DESC
  ) TO %s (FORMAT PARQUET, COMPRESSION %s);
  ", sql_q(out_player_opening_tc), toupper(parq_comp))
  
  dbExecute(con, player_opening_sql)
  message("[OK] wrote: ", out_player_opening_tc)
  
} else {
  message("[SKIP] games_cohort_dir missing or has no parquet: ", games_cohort_dir)
}

# Metagame datasets from games_meta
if (has_any_parquet(games_meta_dir)) {
  
  meta_glob <- file.path(games_meta_dir, "**", "*.parquet")
  
  dbExecute(con, sprintf("
    CREATE OR REPLACE VIEW meta_games AS
    SELECT
      month,
      game_id,
      white_rating,
      black_rating,
      rating_diff,
      abs_rating_diff,
      result,
      winner,
      score_white,
      is_upset,
      termination,
      time_control,
      initial,
      increment,
      tc_bucket,
      eco,
      opening,
      moves
    FROM read_parquet(%s)
  ", sql_q(meta_glob)))
  
  # meta_month_tc (month x tc_bucket summaries)
  meta_month_tc_sql <- sprintf("
  COPY (
    SELECT
      month,
      tc_bucket,
      COUNT(*) AS games,
      AVG(white_rating) AS avg_white_rating,
      AVG(black_rating) AS avg_black_rating,
      AVG(abs_rating_diff) AS avg_abs_rating_diff,
      AVG(moves) AS avg_moves,
      CAST(SUM(CASE WHEN result='1-0' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS white_win_rate,
      CAST(SUM(CASE WHEN result='0-1' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS black_win_rate,
      CAST(SUM(CASE WHEN result='1/2-1/2' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS draw_rate,
      CAST(SUM(is_upset) AS DOUBLE) / COUNT(*) AS upset_rate,
      COUNT(DISTINCT opening) AS opening_distinct
    FROM meta_games
    GROUP BY month, tc_bucket
    ORDER BY month, tc_bucket
  ) TO %s (FORMAT PARQUET, COMPRESSION %s);
  ", sql_q(out_meta_month_tc), toupper(parq_comp))
  
  dbExecute(con, meta_month_tc_sql)
  message("[OK] wrote: ", out_meta_month_tc)
  
  # meta_opening_month_tc (month x tc_bucket x opening summaries)
  meta_opening_sql <- sprintf("
  COPY (
    SELECT
      month,
      tc_bucket,
      eco,
      opening,
      COUNT(*) AS games,
      CAST(COUNT(*) AS DOUBLE) / SUM(COUNT(*)) OVER (PARTITION BY month, tc_bucket) AS share,
      CAST(SUM(CASE WHEN result='1-0' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS white_win_rate,
      CAST(SUM(CASE WHEN result='0-1' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS black_win_rate,
      CAST(SUM(CASE WHEN result='1/2-1/2' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS draw_rate,
      CAST(SUM(is_upset) AS DOUBLE) / COUNT(*) AS upset_rate,
      AVG(abs_rating_diff) AS avg_abs_rating_diff,
      AVG(moves) AS avg_moves
    FROM meta_games
    WHERE opening IS NOT NULL
    GROUP BY month, tc_bucket, eco, opening
    ORDER BY month, tc_bucket, games DESC
  ) TO %s (FORMAT PARQUET, COMPRESSION %s);
  ", sql_q(out_meta_opening_month), toupper(parq_comp))
  
  dbExecute(con, meta_opening_sql)
  message("[OK] wrote: ", out_meta_opening_month)
  
} else {
  message("[SKIP] games_meta_dir missing or has no parquet: ", games_meta_dir)
}

# Time-usage datasets from moves_meta

if (has_any_parquet(moves_meta_dir)) {
  
  moves_glob <- file.path(moves_meta_dir, "**", "*.parquet")
  
  # Base view, filter clearly-bad rows
  dbExecute(con, sprintf("
    CREATE OR REPLACE VIEW moves_raw AS
    SELECT
      month,
      tc_bucket,
      game_id,
      ply,
      move_number,
      side,
      time_spent,
      clock_after,
      initial,
      increment,
      white,
      black,
      white_rating,
      black_rating,
      result,
      opening,
      eco
    FROM read_parquet(%s)
  ", sql_q(moves_glob)))
  
  dbExecute(con, "
    CREATE OR REPLACE VIEW moves AS
    SELECT *
    FROM moves_raw
    WHERE side IN ('white','black')
      AND time_spent IS NOT NULL
      AND time_spent >= 0
      AND time_spent <= 3600
      AND clock_after IS NOT NULL
      AND clock_after >= 0
      AND clock_after <= 24*3600
  ")
  
  # Enrich with mover/player + ratings + derived features
  dbExecute(con, "
    CREATE OR REPLACE VIEW moves_enriched AS
    SELECT
      month,
      tc_bucket,
      game_id,
      ply,
      move_number,
      side,
      CASE WHEN side='white' THEN white ELSE black END AS player,
      CASE WHEN side='white' THEN white_rating ELSE black_rating END AS player_rating,
      CASE WHEN side='white' THEN black_rating ELSE white_rating END AS opp_rating,
      CASE WHEN side='white' THEN (white_rating - black_rating) ELSE (black_rating - white_rating) END AS rating_diff,
      result,
      opening,
      eco,
      initial,
      increment,
      time_spent,
      clock_after,
      CASE
        WHEN clock_after <= GREATEST(10, 2*increment) THEN 1 ELSE 0
      END AS is_low_time,
      CASE
        WHEN move_number <= 10 THEN 'opening'
        WHEN move_number <= 25 THEN 'middlegame'
        ELSE 'endgame'
      END AS phase
    FROM moves
    WHERE (CASE WHEN side='white' THEN white ELSE black END) IS NOT NULL
      AND (CASE WHEN side='white' THEN white_rating ELSE black_rating END) IS NOT NULL
  ")
  
  # Summary: month x tc_bucket (population time usage)
  moves_month_tc_sql <- sprintf("
  COPY (
    SELECT
      month,
      tc_bucket,
      COUNT(*) AS move_rows,
      COUNT(DISTINCT game_id) AS games,
      AVG(time_spent) AS avg_time_spent,
      QUANTILE_CONT(time_spent, 0.5) AS median_time_spent,
      QUANTILE_CONT(time_spent, 0.9) AS p90_time_spent,
      AVG(clock_after) AS avg_clock_after,
      CAST(SUM(CASE WHEN is_low_time=1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS low_time_rate
    FROM moves_enriched
    GROUP BY month, tc_bucket
    ORDER BY month, tc_bucket
  ) TO %s (FORMAT PARQUET, COMPRESSION %s);
  ", sql_q(out_moves_month_tc), toupper(parq_comp))
  
  dbExecute(con, moves_month_tc_sql)
  message("[OK] wrote: ", out_moves_month_tc)
  
  # Player-level time usage: player x month x tc_bucket
  # (HAVING clause avoids tiny samples
  player_time_sql <- sprintf("
  COPY (
    SELECT
      player,
      month,
      tc_bucket,
      COUNT(*) AS move_rows,
      COUNT(DISTINCT game_id) AS games,
      AVG(time_spent) AS avg_time_spent,
      QUANTILE_CONT(time_spent, 0.5) AS median_time_spent,
      QUANTILE_CONT(time_spent, 0.9) AS p90_time_spent,
      AVG(clock_after) AS avg_clock_after,
      CAST(SUM(CASE WHEN is_low_time=1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS low_time_rate,
      AVG(player_rating) AS avg_rating,
      AVG(rating_diff) AS avg_rating_diff,
      AVG(CASE WHEN phase='opening' THEN time_spent END) AS avg_time_opening,
      AVG(CASE WHEN phase='middlegame' THEN time_spent END) AS avg_time_middlegame,
      AVG(CASE WHEN phase='endgame' THEN time_spent END) AS avg_time_endgame
    FROM moves_enriched
    GROUP BY player, month, tc_bucket
    HAVING COUNT(*) >= 20
    ORDER BY player, month, tc_bucket
  ) TO %s (FORMAT PARQUET, COMPRESSION %s);
  ", sql_q(out_player_time_month_tc), toupper(parq_comp))
  
  dbExecute(con, player_time_sql)
  message("[OK] wrote: ", out_player_time_month_tc)
  
} else {
  message("[SKIP] moves_meta_dir missing or has no parquet (expected until you re-parse): ",
          moves_meta_dir %||% "<NULL>")
}

# Run-info logging
writeLines(capture.output(sessionInfo()),
           file.path(run_info_dir, "R_sessionInfo_build_datasets_duckdb.txt"))

message("[DONE] dataset build complete.")
``
::contentReference[oaicite:0]{index=0}
