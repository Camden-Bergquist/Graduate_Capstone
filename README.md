# Lichess Behavioral Modeling

Graduate capstone for the M.S. in Data Science and Analytics at Graceland University, completed in 2026. This project uses public Lichess game archives to examine how playing behavior, opening choices, and time use relate to player ratings and subsequent rating changes.

## Start Here

[Read the final research paper](Paper_Final_Draft.pdf) for the research questions, methodology, results, visualizations, and limitations.

## Project Highlights

- Built an R/Python workflow covering 24 months of archives, from January 2024 through December 2025.
- Streamed and sampled game records, stored intermediate data in partitioned Parquet files, and used DuckDB SQL to build seven linked analytical datasets.
- Constructed longitudinal behavioral features and fit player fixed-effects models, predictive regression benchmarks, and behavioral clustering models.
- Evaluated results with later-month holdouts and sensitivity analyses, documenting uncertainty and observational limitations.

The largest derived table contains approximately 7.2 million player-opening-month-time-control observations. These are aggregated records, not a count of unique players or games.

## Selected Finding

In within-player models, broader opening repertoires and changes in a player's most-played opening were more consistently associated with later rating gains than raw game volume. These are observational associations; they do not establish that changing openings causes improvement.

## Repository Structure

| Location | Purpose |
|---|---|
| `scripts/01_download.R` | Download source archives |
| `scripts/02_parse_pgn.py` | Parse and sample game records |
| `scripts/03_build_datasets.R` | Build derived analytical datasets |
| `scripts/03_build_player_month.R` | Aggregate longitudinal player records |
| `quarto/` | Pipeline, exploratory analysis, and modeling documents |

## Tools and Reproduction Notes

R, Python, SQL, DuckDB, Arrow/Parquet, and Quarto.

This repository contains research code rather than a packaged application. A full rerun requires downloading substantial archive data, installing the dependencies used by the scripts, and adjusting local paths and worker settings. Review `config/pipeline.yml` and the pipeline document before running the workflow. The paper is the most direct way to review the completed analysis without rebuilding the datasets.
