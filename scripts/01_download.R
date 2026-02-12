suppressPackageStartupMessages({
  library(yaml)
  library(digest)
})

cfg <- yaml::read_yaml("config/pipeline.yml")
raw_dir <- cfg$paths$raw_dir
checksums_csv <- cfg$paths$checksums_csv
run_info_dir <- cfg$paths$run_info_dir

dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(checksums_csv), recursive = TRUE, showWarnings = FALSE)
dir.create(run_info_dir, recursive = TRUE, showWarnings = FALSE)

month_to_url <- function(m) {
  paste0("https://database.lichess.org/standard/lichess_db_standard_rated_", m, ".pgn.zst")
}

sha256_file <- function(path) {
  digest::digest(file = path, algo = "sha256", serialize = FALSE)
}

have_aria2 <- nzchar(Sys.which("aria2c"))

download_one <- function(url, dest, raw_dir, filename) {
  have_aria2 <- nzchar(Sys.which("aria2c"))
  aria2_sidecar <- paste0(dest, ".aria2")
  
  # If aria2 sidecar exists, download is incomplete -> we must resume
  if (file.exists(aria2_sidecar)) {
    message("Found .aria2 sidecar; will resume: ", filename)
  } else if (file.exists(dest)) {
    # File exists and no sidecar -> assume complete
    message("File exists and no .aria2 sidecar; assuming complete: ", filename)
    return(invisible(TRUE))
  }
  
  if (have_aria2) {
    message("Using aria2c: ", filename)
    
    # -c = continue/resume
    # --disable-ipv6=true = avoid Windows IPv6 routing issues ("unreachable network")
    # --file-allocation=none = avoid preallocating full 30GB (prevents false 'complete' checks)
    # -x/-s = connections (start conservative; increase later if stable)
    cmd <- sprintf(
      'aria2c -c --disable-ipv6=true --file-allocation=none -x 1 -s 1 --timeout=60 --connect-timeout=60 -d "%s" -o "%s" "%s"',
      normalizePath(raw_dir, winslash = "/"),
      filename,
      url
    )
    
    status <- system(cmd, intern = FALSE, ignore.stdout = FALSE, ignore.stderr = FALSE)
    if (status != 0) stop("aria2c download failed for: ", url)
    
    # If aria2 completes successfully, it should remove the .aria2 file automatically.
    if (file.exists(aria2_sidecar)) {
      stop("aria2c finished but .aria2 sidecar still exists (download likely incomplete): ", filename)
    }
    
    return(invisible(TRUE))
  }
  
  # Fallback: base R download (less reliable for huge files)
  message("Using base R download.file (less reliable for huge files). Consider installing aria2c.")
  options(timeout = 60 * 60 * 6) # 6 hours
  
  # If a partial exists from a previous base-R attempt, remove it (base R doesn't resume well)
  if (file.exists(dest)) {
    message("Removing existing partial file from previous attempt: ", filename)
    file.remove(dest)
  }
  
  status <- try(
    utils::download.file(url, destfile = dest, mode = "wb", method = "curl", quiet = FALSE),
    silent = TRUE
  )
  if (inherits(status, "try-error")) stop("download.file failed for: ", url)
  
  invisible(TRUE)
}


rows <- list()

for (m in cfg$months) {
  url <- month_to_url(m)
  filename <- basename(url)
  dest <- file.path(raw_dir, filename)
  
  message("Downloading (if needed): ", filename)
  download_one(url, dest, raw_dir, filename)
  
  message("Hashing: ", filename)
  h <- sha256_file(dest)
  info <- file.info(dest)
  
  rows[[length(rows) + 1]] <- data.frame(
    month = m,
    filename = filename,
    url = url,
    size_bytes = info$size,
    sha256 = h,
    stringsAsFactors = FALSE
  )
}

df_new <- do.call(rbind, rows)

if (file.exists(checksums_csv)) {
  df_old <- read.csv(checksums_csv, stringsAsFactors = FALSE)
  df <- rbind(df_old[!df_old$filename %in% df_new$filename, ], df_new)
} else {
  df <- df_new
}

df <- df[order(df$month), ]
write.csv(df, checksums_csv, row.names = FALSE)
message("Wrote checksums to: ", checksums_csv)

writeLines(capture.output(sessionInfo()),
           file.path(run_info_dir, "R_sessionInfo_download.txt"))
