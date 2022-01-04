cat("Starting download pipeline.")
st <- Sys.time()

# increase timeout to download larger data
options(timeout = 1000)

# read Bulletin 118 subbasins
b118_names <- read_csv(here("src/gwl/data_input/b118/b118_names.csv")) %>% 
  select(-Basin_Su_1) 
b118 <- st_read(here("src/gwl/data_input/b118/shp/B118_SGMA_2019_Basin_Prioritization.shp")) %>% 
  st_transform(3310) %>% 
  left_join(b118_names) %>% 
  rmapshaper::ms_simplify(keep_shapes = TRUE)

# tempfiles to hold downloaded data
tf1 <- tempfile()
tf2 <- tempfile()
tf3 <- tempfile()

# urls for GSA polygons from SGMA viewer, and periodic groundwater level measurement database
url_gsa <- "https://sgma.water.ca.gov/portal/service/gsadocument/exclusivegsa"
url_gwl <- "https://data.cnra.ca.gov/dataset/dd9b15f5-6d08-4d8c-bace-37dc761a9c08/resource/c51e0af9-5980-4aa3-8965-e9ea494ad468/download/periodic_gwl_bulkdatadownload.zip"
url_wyt <- "https://data.cnra.ca.gov/dataset/806ce291-645b-4646-8e15-9295b7740f5a/resource/b8fae043-4458-40f1-935c-4748157cbf92/download/sgma_wyt_dataset.csv"

# download files
cat("  Downloading GSA and GWL urls...")
walk2(c(url_gsa, url_gwl), c(tf1, tf2), ~download.file(.x, .y))
cat("done.\n")

# read gsa polygons
cat("  Reading GSA polygons...")
unzip(tf1)
gsa <- st_read("GSA_Master.shp") %>% 
  st_transform(3310) %>% 
  rmapshaper::ms_simplify(keep_shapes = TRUE) %>% 
  separate(Basin, into = c("Basin_Subb","rm"), sep = " ", extra = "drop") %>% 
  select(-rm) %>% 
  left_join(b118_names)
cat("done.\n")

# read gwl data and make spatial
cat("  Reading groundwater level measurements, stations, perforations...")
files_meas <- c("measurements.csv", "stations.csv", "perforations.csv")

gwl <- files_meas %>% 
  map(~read_csv(unzip(tf2, .x))) %>% 
  reduce(left_join, "SITE_CODE") %>% 
  select(-MONITORING_PROGRAM.x, MONITORING_PROGRAM = MONITORING_PROGRAM.y) %>% 
  # remove old measurements and nonsense above land surface measurements
  filter(MSMT_DATE >= lubridate::ymd("1980-01-01") & GSE_GWE >= 0) %>%
  st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4269, remove = FALSE) %>% 
  st_transform(3310) %>% 
  # add GSA data 
  st_join(gsa) %>% 
  # overwrite B118 boundaries because some are missing and others are wrong
  select(-BASIN_CODE) %>% 
  st_join(select(b118, BASIN_CODE = Basin_Subb)) %>% 
  # only retain points in B118 basins
  filter(!is.na(BASIN_CODE)) %>%
  st_drop_geometry() %>% # drop geometry for faster in-memory processing
  # only retain sites with at least 3 measurements
  dtplyr::lazy_dt() %>% 
  group_by(SITE_CODE) %>% 
  mutate(n = n()) %>% 
  ungroup() %>% 
  filter(n >= 3) %>% 
  # summarize groundwater levels to monthly vals and take most recent observation
  mutate(
    MSMT_DATE_SUMMARY = ymd(paste(year(MSMT_DATE), month(MSMT_DATE), "15", sep = "-"))
  ) %>% 
  group_by(SITE_CODE, MSMT_DATE_SUMMARY) %>% 
  arrange(MSMT_DATE_SUMMARY) %>% 
  mutate(GSE_GWE_SUMMARY = mean(GSE_GWE, na.rm = TRUE)) %>% 
  slice(1) %>% 
  ungroup() %>% 
  collect()

cat("done.\n")


# TODO: download and join HUC8 boundaries for WYT to filter in the next step

# water year types from SGMA portal
cat("  Downloading water year types...")
wyt <- read_csv(url_wyt)
cat("done.\n")

# clean up
cat("  Cleaning up...")
files_rm <- c(dir_ls(here(), regexp = "GSA_Master"), 
              here(files_meas),
              c(tf1, tf2, tf3))
walk(files_rm, ~unlink(.x))
cat("done.\n")

total_time <- Sys.time() - st
cat("  Finished download pipline after:", total_time, "minutes. \n\n\n")
rm(total_time, st)
