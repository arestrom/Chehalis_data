#===========================================================================
# Replace cases where LLIDs have been split....replace with single from Arleta
#
# Notes:
#  1. To get the latest LLID data from geolib...open geolib LLID_ROUTES in qgis. Then
#     export as a geopackage. Set CRS to 2927 first. Default settings will
#     work. Takes a few minutes to download. Then use code below to trim
#     to WRIAs 22 and 23.
#  2. Some substantial changes occurred on 2020-03-12. One of the most dramatic was
#     Sherwood Creek, which was essentially split into Hensen Creek for the lower
#     portion and Sherwood Creek for part of the upper portion. So RMs will no
#     longer be meaningful and should be replaced by coordinates and point descriptors.
#
#  Completed: 2020-04-01
#
# AS 2020-04-01
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
library(openxlsx)

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

# # Get table of valid units
# units_table = valid_udunits(quiet = TRUE)

#=========================================================================
# Get stream data from SG
#=========================================================================

# Define query to get needed data.
# Need to join wria_lut table twice...both by geom and through location table to get all streams
# wria info will not be present in location if no RMs were previously entered but stream is present
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, wr.wria_id, st.stream_id, st.gid, st.geom as geometry ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "left join location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join wria_lut as wrl on loc.wria_id = wrl.wria_id ",
           "where wr.wria_code in ('22', '23') ",
           "or wrl.wria_code in ('22', '23') ",
           "order by wb.waterbody_name")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
streams_st = st_read(db_con, query = qry)
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
  inner_join(streams_st, by = "llid") %>%
  select(-geometry)

# Pull out cases where llid is duplicated
dup_llid = dup_streams %>%
  filter(!is.na(llid)) %>%
  filter(!waterbody_name == "Chehalis River")

# #=========================================================================
# # Get WRIA data
# #=========================================================================
#
# # Query to get wria data
# qry = glue("select wria_code, wria_description as wria_name, geom as geometry ",
#            "from wria_lut")
# # Run the query
# db_con = pg_con_local(dbname = "spawning_ground")
# wria_st = st_read(db_con, query = qry)
# dbDisconnect(db_con)
# st_crs(wria_st)$epsg

#=========================================================================
# Get stream data from Arleta's latest
#=========================================================================

# # Get Arleta's latest llid data....see notes above....but this time from a gdb
# llid_st = read_sf("data/LLID20200311.gdb", layer = "LLID_routes", crs = 2927)
# # Write chehalis subset to local
# llid_chehalis = llid_st %>%
#   st_zm() %>%
#   st_join(wria_st) %>%
#   filter(wria_code %in% c("22", "23"))
# # Process for subsetting
# llid_chehalis = llid_chehalis %>%
#   rename(geometry = Shape) %>%
#   select(llid = LLID, gnis_name = GNIS_STREAMNAME, llid_name = LLID_STREAMNAME,
#          wria_code, wria_name, geometry)
# #unique(llid_chehalis$wria_code)
# write_sf(llid_chehalis, "data/llid_chehalis_2020-03-12.gpkg")
# Get Arleta's latest llid data
llid_chehalis = read_sf("data/llid_chehalis_2020-03-12.gpkg",
                        layer = "llid_chehalis_2020-03-12", crs = 2927)
st_crs(llid_chehalis)$epsg

#===========================================================================================
# Correct one stream at a time...Replace multiple segments with one contiguous.
#===========================================================================================

#======== Tributary 0882H =============================

# Set stream llid
ll_id = '1225445466679'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_llid %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# Verify only one pulled
if (length(wb_id) > 1L ) {
  cat("\nWarning: There should only be one value. Investigate!\n\n")
}

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# Verify only one pulled
if (length(gid) > 1L ) {
  cat("\nWarning: There should only be one value. Investigate!\n\n")
}

# gid = c(?L)

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = wb_id) %>%
  mutate(gid = gid) %>%
  mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = as.POSIXct(NA)) %>%
  mutate(modified_by = NA_character_) %>%
  select(stream_id, waterbody_id, gid, geom, created_datetime,
         created_by, modified_datetime, modified_by)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp")
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

# Dump from dup_llid
dup_llid = dup_llid %>%
  filter(!llid == ll_id)

#======== Tributary 0894 =============================

# Set stream llid
ll_id = '1227326466522'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_llid %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# Verify only one pulled
if (length(wb_id) > 1L ) {
  cat("\nWarning: There should only be one value. Investigate!\n\n")
}

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# Verify only one pulled
if (length(gid) > 1L ) {
  cat("\nWarning: There should only be one value. Investigate!\n\n")
}

# gid = c(?L)

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = wb_id) %>%
  mutate(gid = gid) %>%
  mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = as.POSIXct(NA)) %>%
  mutate(modified_by = NA_character_) %>%
  select(stream_id, waterbody_id, gid, geom, created_datetime,
         created_by, modified_datetime, modified_by)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp")
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

# Dump from dup_llid
dup_llid = dup_llid %>%
  filter(!llid == ll_id)
































