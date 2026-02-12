suppressPackageStartupMessages({
  library(yaml)
  library(duckdb)
})

cfg <- yaml::read_yaml("config/pipeline.yml")
games_cohort_dir <- cfg$paths$games_cohort_dir
out_path <- cfg$paths$player_month_path
out_path_tc <- if (!is.null(cfg$paths$player_month_tc_path)) cfg$paths$player_month_tc_path else "data/derived/player_month_tc.parquet"
run_info_dir <- cfg$paths$run_info_dir

dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_path_tc), recursive = TRUE, showWarnings = FALSE)
dir.create(run_info_dir, recursive = TRUE, showWarnings = FALSE)

# In-memory DuckDB
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

on.exit({
  try(dbDisconnect(con, shutdown = TRUE), silent = TRUE)
}, add = TRUE)

# DuckDB prefers forward slashes on Windows
to_fwd <- function(p) gsub("\\\\", "/", normalizePath(p, mustWork = FALSE))

# SQL-quote helper (single-quote escaping)
sql_q <- function(p) paste0("'", gsub("'", "''", to_fwd(p)), "'")

parquet_glob <- file.path(games_cohort_dir, "**", "*.parquet")

# Build everything once as a VIEW so we can reuse it for both outputs
# (This view is "player-game" rows: two rows per game, white+black perspective)
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
    game_id,
    white_rating AS rating,
    black_rating AS opp_rating,
    CASE WHEN result = '1-0' THEN 1 ELSE 0 END AS win,
    CASE WHEN result = '0-1' THEN 1 ELSE 0 END AS loss,
    CASE WHEN result = '1/2-1/2' THEN 1 ELSE 0 END AS draw,
    tc_bucket,
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
    game_id,
    black_rating AS rating,
    white_rating AS opp_rating,
    CASE WHEN result = '0-1' THEN 1 ELSE 0 END AS win,
    CASE WHEN result = '1-0' THEN 1 ELSE 0 END AS loss,
    CASE WHEN result = '1/2-1/2' THEN 1 ELSE 0 END AS draw,
    tc_bucket,
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
WHERE in_cohort = 1
", sql_q(parquet_glob))

DBI::dbExecute(con, base_sql)

# Output 1: player_month
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
  SELECT
    s.*,
    t.top_opening
  FROM summary s
  LEFT JOIN top_opening t
  USING (player, month)
  ORDER BY player, month
) TO %s (FORMAT PARQUET, COMPRESSION ZSTD);
", sql_q(out_path))

DBI::dbExecute(con, player_month_sql)

# Output 2: player_month_tc
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
  SELECT
    s.*,
    t.top_opening
  FROM summary s
  LEFT JOIN top_opening t
  USING (player, month, tc_bucket)
  ORDER BY player, month, tc_bucket
) TO %s (FORMAT PARQUET, COMPRESSION ZSTD);
", sql_q(out_path_tc))

DBI::dbExecute(con, player_month_tc_sql)

# Run-info logging
writeLines(capture.output(sessionInfo()),
           file.path(run_info_dir, "R_sessionInfo_player_month_duckdb.txt"))

message("Wrote: ", out_path)
message("Wrote: ", out_path_tc)
