#===========================================================================
# Identify differences in geoms between DB and official LLID layer
# Look for cases where stream segments intersect and LLID differs
#
# Notes:
#  1. Can get the latest LLID data from geolib...open geolib LLID_ROUTES in qgis. Then
#     export as a geopackage. Set CRS to 2927 first. Default settings will
#     work. Takes a few minutes to download. Then use code below to trim
#     to WRIAs 22 and 23. For now am using LLID20200311.gdb in current folder.
#  2. Some substantial changes occurred on 2020-03-12. One of the most dramatic was
#     Sherwood Creek, which was essentially split into Hensen Creek for the lower
#     portion and Sherwood Creek for part of the upper portion. So RMs will no
#     longer be meaningful and should be replaced by coordinates and point descriptors.
#
#  Completed: 2020-04-27
#
# ToDo: Update name of file to update_llid_geo.R
#
# AS 2020-04-27
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

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "st.stream_id, st.gid, st.geom ",
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

#=========================================================================
# Get WRIA data
#=========================================================================

# Query to get wria data
qry = glue("select wria_code, wria_description as wria_name, geom as geometry ",
           "from wria_lut")
# Run the query
db_con = pg_con_local(dbname = "spawning_ground")
wria_st = st_read(db_con, query = qry)
dbDisconnect(db_con)
st_crs(wria_st)$epsg

# #=========================================================================
# # Get stream data from Leslie's latest
# #=========================================================================
#
# # # Get Leslies latest gdb...as of 2020-03-12
# # cat_llid_st = read_sf("data/wdfwStreamCatalogGeometry.gdb", layer = "StreamCatalogLine", crs = 2927)
# # st_crs(cat_llid_st)$epsg
# # # Write chehalis subset to local
# # cat_llid_chehalis = cat_llid_st %>%
# #   st_zm() %>%
# #   st_join(wria_st) %>%
# #   filter(wria_code %in% c("22", "23"))
# # # Pull out what I need
# # cat_llid_chehalis = cat_llid_chehalis %>%
# #   rename(geometry = Shape) %>%
# #   select(cat_code = StreamCatalogID, cat_name = StreamCatalogName, llid = LLID,
# #          llid_name = LLIDName, comment = Comment, orientation = OrientID)
# # #unique(llid_chehalis$wria_code)
# # write_sf(cat_llid_chehalis, "data/cat_llid_chehalis_2020-03-12.gpkg")
# # Get Lestie's latest llid data
# cat_llid_chehalis = read_sf("data/cat_llid_chehalis_2020-03-12.gpkg",
#                             layer = "cat_llid_chehalis_2020-03-12", crs = 2927)

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

# # Trim to only LLIDs currently in DB......THIS FILTERED OUT 0024A
# ll_id = unique(streams_st$llid)
# llid_chehalis = llid_chehalis %>%
#   filter(llid %in% ll_id) %>%
#   arrange(llid_name)

# Pull out cases where LLID is duplicated...only Chehalis...not a problem....just one in streams_st
dup_llid = llid_chehalis %>%
  st_drop_geometry() %>%
  select(llid) %>%
  group_by(llid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(llid) %>%
  distinct() %>%
  inner_join(llid_chehalis, by = "llid")

#=========================================================================
# Check for geometry overlaps where the LLIDs do not agree
#=========================================================================

# Join by geometry
streams_llid = streams_st %>%
  rename(sg_llid = llid) %>%
  st_join(llid_chehalis, largest = TRUE)

# Check for missing LLIDs
any(is.na(streams_llid$sg_llid))
any(is.na(streams_llid$llid))

# Pull out cases where llids dont match
streams_llid = streams_llid %>%
  filter(!sg_llid == llid)

#=========================================================================
# Manually update 23.0898. Geometry is the same but LLID differs
# Turned out sg is correct. Problem is in llid_chehalis...somewhere?
#=========================================================================

# Pull out values for different geometries to verify: Looks like all is ok
# The problem is in the llid_chehalis layer...two LLIDs for same geometry
sg_llid = "1226874466448"
rt_llid = "1226874466448"
lc_llid = "1226874466448"

# Query to update LLID
qry = glue("???????????")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#=========================================================================
# Manually update 22.0824A. Need update both LLID and geometry
#=========================================================================

# Query to update LLID
qry = glue("update waterbody_lut ",
           "set latitude_longitude_id = '1240151471207' ",
           "where waterbody_id = 'c5e392e3-3707-441d-a046-e77b7e1c35a0'")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# gid = 853

# Delete stream entry (geometry)
qry = glue("delete from stream ",
           "where waterbody_id = 'c5e392e3-3707-441d-a046-e77b7e1c35a0'")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Create stream entry
updated_geo = llid_chehalis %>%
  filter(llid == '1240151471207') %>%
  mutate(gid = 853) %>%
  mutate(waterbody_id = 'c5e392e3-3707-441d-a046-e77b7e1c35a0') %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = as.POSIXct(NA)) %>%
  mutate(modified_by = NA_character_) %>%
  select(stream_id, waterbody_id, gid, geom, created_datetime,
         created_by, modified_datetime, modified_by)

  # Write to temp table
  db_con = pg_con_local(dbname = "spawning_ground")
  st_write(obj = updated_geo, dsn = db_con, layer = "stream_temp")
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

# Run rest of code if any streams have changed
if (nrow(updated_geo) > 0 ) {

  # Set waterbody_id
  wb_id = 'c5e392e3-3707-441d-a046-e77b7e1c35a0'

  # Delete query
  qry = glue("delete from stream where waterbody_id in ('{wb_id}')")
  # Run query
  db_con = pg_con_local(dbname = "spawning_ground_archive")
  dbExecute(db_con, qry)
  dbDisconnect(db_con)

  # Write temp table
  db_con = pg_con_local(dbname = "spawning_ground_archive")
  st_write(obj = updated_geo, dsn = db_con, layer = "stream_temp")
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
































