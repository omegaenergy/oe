cat("Starting transform and render pipeline.")
st <- Sys.time()

library(here)
library(fs)
library(tidyverse)
library(lubridate)
library(sf)
library(leaflet)
library(leafpop)
library(leafem)
library(htmltools)
library(htmlwidgets)
library(plotly)

# helper functions
source(here("src/gwl/functions/f_gwl_helpers.R"))
source(here("src/gwl/functions/f_calculate_water_year.R"))
source(here("src/gwl/functions/f_gwl_preprocess.R"))

# download data
source(here("src/gwl/01_download.R"))

# build all dashboards
# ids_select <- unique(gwl$web_name)
ids_select <- c("sasb") 

cat("  Building", length(ids_select), "id(s):", paste0("\n      ", ids_select))

for(i in seq_along(ids_select)){
  
  # i=1
  id <- ids_select[i]
  
  cat("Starting pipeline for B118 basin:", id, "[", i, "/", length(ids_select), "].\n")
  
  cat("  Preprocessing data...")
  preprocessed <- suppressWarnings(f_gwl_preprocess(id))
  cat("done.\n")
  
  cat("  Zipping data...")
  file_data   <- here(glue::glue("content/{id}/{id}.csv"))
  file_zip    <- str_replace(file_data, ".csv", ".zip")
  file_html   <- here(glue::glue("content/{id}/index.html"))
  file_s3     <- glue::glue("s3://wg-gwl/{id}/{id}.zip")
  file_signed <- here("presigned_url.txt")
  
  # write csv, zip it up, and rm csv
  if(!dir_exists(fs::path_dir(file_data))){
    dir_create(fs::path_dir(file_data))
  }
  preprocessed$maoi %>% write_csv(file_data)
  zip(zipfile = file_zip, files = file_data, extras = "-j")
  
  # move to s3 and generate presigned URL
  cat("\n  Pushing to S3 and generating presigned URL...")
  f_s3_copy(file_zip, file_s3)
  presigned_url <- f_s3_sign(file_s3, file_signed) 
  
  cat("  Writing dashboard...")
  f_write_dashboard(id)
  
  cat("  Encrypting dashboard...")
  f_encrypt_file(id)
  cat("done.\n")
  
  cat("  Cleaning up...")
  unlink(file_data); unlink(file_zip)
  cat("done.\n\n")
}

total_time <- Sys.time() - st
cat("  Finished download pipline after:", total_time, "minutes.\n\n\n")
rm(total_time, st)
