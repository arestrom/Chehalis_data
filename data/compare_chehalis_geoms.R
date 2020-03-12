#===========================================================================
# Identify differences in geoms between DB and official LLID layer
#
# Notes:
#  1. To get the latest LLID data...open geolib LLID_ROUTES in qgis. Then
#     export as a geopackage. Set CRS to 2927 first. Default settings will
#     work. Takes a few minutes to download. Then use code below to trim
#     to WRIAs 22 and 23.
#
#  Completed: 2020-03-11
#
# AS 2020-03-10
#===========================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Load libraries
library(DBI)
library(RPostgres)
library(dplyr)
library(remisc)
library(tidyr)
library(sf)
library(stringi)
library(lubridate)
library(glue)
library(odbc)
library(units)

# Set options
options(digits=14)

# Keep connections pane from opening
options("connectionObserver" = NULL)

#=====================================================================================
# Function to get user for database
pg_user <- function(user_label) {
  Sys.getenv(user_label)
}

# Function to get pw for database
pg_pw <- function(pwd_label) {
  Sys.getenv(pwd_label)
}

# Function to get pw for database
pg_host <- function(host_label) {
  Sys.getenv(host_label)
}

# Function to connect to postgres
pg_con_local = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

# Get table of valid units
units_table = valid_udunits(quiet = TRUE)

#=========================================================================
# Import all stream data from sg where two or more segments exist per llid
#=========================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "st.stream_id, st.gid, st.geom as geometry ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where stream_id is not null and wria_code in ('22', '23') ",
           "order by waterbody_name")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
streams_st = wria_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Pull out cases where LLID is duplicated
dup_streams = streams_st %>%
  st_drop_geometry() %>%
  group_by(llid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(llid) %>%
  distinct() %>%
  inner_join(streams_st, by = "llid")

# # Query to get wria data
# qry = glue("select wria_code, wria_description as wria_name, geom as geometry ",
#            "from wria_lut")
#
# # Run the query
# db_con = pg_con_local(dbname = "spawning_ground")
# wria_st = st_read(db_con, query = qry)
# dbDisconnect(db_con)
#
# # Check epsg
# st_crs(wria_st)$epsg
#
# # Get Arleta's latest llid data....see notes above.
# llid_st = read_sf("data/LLID_ROUTES.gpkg", layer = "LLID_ROUTES", crs = 2927)
# # Write chehalis subset to local
# llid_chehalis = llid_st %>%
#   st_join(wria_st) %>%
#   filter(wria_code %in% c("22", "23"))
# # Process for subsetting
# llid_chehalis = llid_chehalis %>%
#   rename(geometry = geom) %>%
#   select(objectid = OBJECTID, llid = LLID, gnis_name = GNIS_STREAMNAME, llid_name = LLID_STREAMNAME,
#          wria_code, wria_name, geometry)
# #unique(llid_chehalis$wria_code)
# write_sf(llid_chehalis, "data/llid_chehalis_2020-02-11.gpkg")
# Get Arleta's latest llid data
llid_chehalis = read_sf("data/llid_chehalis_2020-02-11.gpkg", layer = "llid_chehalis_2020-02-11", crs = 2927)

# Trim to only LLIDs currently in DB
ll_id = unique(streams_st$llid)
llid_chehalis = llid_chehalis %>%
  filter(llid %in% ll_id) %>%
  rename(geometry = geom) %>%
  arrange(llid_name)

# Pull out cases where LLID is duplicated...only Chehalis...not a problem....just one in streams_st
dup_llid = llid_chehalis %>%
  st_drop_geometry() %>%
  select(objectid, llid) %>%
  group_by(llid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(llid) %>%
  distinct() %>%
  inner_join(llid_chehalis, by = "llid")

# Pull out attributes from streams to add to llid
add_cols = streams_st %>%
  select(waterbody_id, waterbody_name, waterbody_display_name,
         llid, cat_code, stream_id, gid) %>%
  st_drop_geometry()

# Add columns to llid
llid_chehalis = llid_chehalis %>%
  left_join(add_cols, by = "llid")

# Trim to match streams
llid_chehalis = llid_chehalis %>%
  mutate(src = "latest_llid") %>%
  select(src, waterbody_id, waterbody_name, waterbody_display_name,
         llid, cat_code, stream_id, gid, geometry)

# Add source to streams_st
streams_st = streams_st %>%
  mutate(src = "stream_table") %>%
  select(src, waterbody_id, waterbody_name, waterbody_display_name,
         llid, cat_code, stream_id, gid, geometry)

# Combine
all_streams = rbind(streams_st, llid_chehalis)

# Flatten using distinct
strt = Sys.time()
all_streams = all_streams %>%
  distinct(., .keep_all = TRUE) %>%
  arrange(waterbody_display_name)
nd = Sys.time(); nd - strt

# Arrange
changed_streams = all_streams %>%
  st_drop_geometry() %>%
  group_by(waterbody_id) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1L) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# Pull out streams for checking
check_streams = all_streams %>%
  filter(waterbody_id %in% changed_streams) %>%
  arrange(waterbody_display_name)

#===========================================================================================
# As I get lastest from Arleta...update with latest geometry
#===========================================================================================

# Run rest of code if any streams have changed
if (nrow(check_streams) > 0 ) {
  # Add line length info to identify streams that should be checked closer
  check_streams = check_streams %>%
    mutate(stream_feet = as.numeric(st_length(geometry))) %>%
    #mutate(stream_miles = round(as.numeric(units::set_units(stream_feet, "US_survey_mile")), 5)) %>%
    select(src, waterbody_id, waterbody_name, waterbody_display_name,
           llid, cat_code, stream_id, stream_feet, gid, geometry)

  # Compare differences
  chk_length = check_streams %>%
    select(src, waterbody_id, waterbody_display_name, stream_feet) %>%
    st_drop_geometry()

  # Pivot wider for comparison
  chk_length_p = chk_length %>%
    pivot_wider(., names_from = src, values_from = stream_feet, names_prefix = "source_") %>%
    mutate(dif_from_latest_llid = round(source_latest_llid - source_stream_table)) %>%
    filter(abs(dif_from_latest_llid) > 100)

  # Pull out new stream geometry
  updated_geo = check_streams %>%
    filter(src == "latest_llid") %>%
    rename(geom = geometry) %>%
    mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
    mutate(created_by = Sys.getenv("USERNAME")) %>%
    mutate(modified_datetime = as.POSIXct(NA)) %>%
    mutate(modified_by = NA_character_) %>%
    select(stream_id, waterbody_id, gid, geom, created_datetime,
           created_by, modified_datetime, modified_by)

  # Check CRS
  st_crs(updated_geo)$epsg

  # Pull out waterbody_id
  wb_id = updated_geo %>%
    select(waterbody_id) %>%
    st_drop_geometry() %>%
    distinct() %>%
    pull(waterbody_id)

  # Print out length
  length(wb_id)

  # Combine to string
  wb_id = paste0(paste0("'", wb_id, "'"), collapse = ", ")

  # Delete query
  qry = glue("delete from stream where waterbody_id in ({wb_id})")
  # Run query
  db_con = pg_con_local(dbname = "spawning_ground")
  dbExecute(db_con, qry)
  dbDisconnect(db_con)

  # Write temp table
  db_con = pg_con_local(dbname = "spawning_ground")
  st_write(obj = updated_geo, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
  dbDisconnect(db_con)

  # Use select into query to get data into point_location
  qry = glue::glue("INSERT INTO stream ",
                   "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                   "gid, geom, ",
                   "CAST(created_datetime AS timestamptz), created_by, ",
                   "CAST(modified_datetime AS timestamptz), modified_by ",
                   "FROM stream_temp")

  # Insert select to DB
  db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
  DBI::dbExecute(db_con, qry)
  DBI::dbDisconnect(db_con)

  # Drop temp
  db_con = pg_con_local(dbname = "spawning_ground")
  DBI::dbExecute(db_con, "DROP TABLE stream_temp")
  DBI::dbDisconnect(db_con)
}

# Run rest of code if any streams have changed
if (nrow(check_streams) > 0 ) {

  # Delete query
  qry = glue("delete from stream where waterbody_id in ({wb_id})")
  # Run query
  db_con = pg_con_local(dbname = "spawning_ground_archive")
  dbExecute(db_con, qry)
  dbDisconnect(db_con)

  # Write temp table
  db_con = pg_con_local(dbname = "spawning_ground_archive")
  st_write(obj = updated_geo, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
  dbDisconnect(db_con)

  # Use select into query to get data into point_location
  qry = glue::glue("INSERT INTO stream ",
                   "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                   "gid, geom, ",
                   "CAST(created_datetime AS timestamptz), created_by, ",
                   "CAST(modified_datetime AS timestamptz), modified_by ",
                   "FROM stream_temp")

  # Insert select to DB
  db_con = dbConnect(odbc::odbc(), dsn = "local_spawn_archive", timezone = "UTC")
  DBI::dbExecute(db_con, qry)
  DBI::dbDisconnect(db_con)

  # Drop temp
  db_con = pg_con_local(dbname = "spawning_ground_archive")
  DBI::dbExecute(db_con, "DROP TABLE stream_temp")
  DBI::dbDisconnect(db_con)
}
































