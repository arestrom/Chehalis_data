#===========================================================================
# Add missing reach points provided by Lea
#
# Notes:
#  1.
#
#  Completed: 2020-03-31
#
# AS 2020-03-31
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
#library(iformr)
#library(odbc)
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

#=========================================================================
# Import all stream data from sg
#=========================================================================

# Define query to get needed data.
# Need to join wria_lut table twice...both by geom and through location table to get all streams
# wria info will not be present in location if no RMs were previously entered but stream is present
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "st.stream_id, st.gid, st.geom as geometry ",
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

# Pull LLIDs without geometry
stream = streams_st %>%
  st_drop_geometry() %>%
  distinct

# Get waterbody_ids
wb_ids = stream %>%
  select(waterbody_id, stream_id, waterbody_name, llid)

#=========================================================================
# Import all reach data from sg
#=========================================================================

# Get existing rm_data
qry = glue("select distinct loc.location_id, wb.latitude_longitude_id as llid, ",
           "loc.river_mile_measure as river_mile, st.stream_id ",
           "from location as loc ",
           "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where wria_code in ('22', '23') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point')")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
reach_points = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

#=========================================================================
# Import all new reach points from Leas spreadsheet
#=========================================================================

# Pull in xlsx
new_points = read.xlsx("data/2020-03-31_NoEndPointToLea.xlsx", sheet = 2)

#=========================================================================
# Add streams to Leas data to verify none are missing
#=========================================================================

# Add waterbody IDs
new_points = new_points %>%
  left_join(wb_ids, by = c("waterbody_id", "llid"))

# Pull out missing streams
missing_streams = new_points %>%
  filter(is.na(waterbody_name))

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

# # # Get Arleta's latest llid data....see notes above....but this time from a gdb
# # llid_st = read_sf("data/LLID20200311.gdb", layer = "LLID_routes", crs = 2927)
# # # Write chehalis subset to local
# # llid_chehalis = llid_st %>%
# #   st_zm() %>%
# #   st_join(wria_st) %>%
# #   filter(wria_code %in% c("22", "23"))
# # # Process for subsetting
# # llid_chehalis = llid_chehalis %>%
# #   rename(geometry = Shape) %>%
# #   select(llid = LLID, gnis_name = GNIS_STREAMNAME, llid_name = LLID_STREAMNAME,
# #          wria_code, wria_name, geometry)
# # #unique(llid_chehalis$wria_code)
# # write_sf(llid_chehalis, "data/llid_chehalis_2020-03-12.gpkg")
# # Get Arleta's latest llid data
# llid_chehalis = read_sf("data/llid_chehalis_2020-03-12.gpkg",
#                         layer = "llid_chehalis_2020-03-12", crs = 2927)

#=========================================================================
# Correct upper portion of NF Newaukum Creek. Upper end branch correction.
#=========================================================================

# # Set gid of stream to delete
# gid = 3638L
#
# # Delete query
# qry = glue("delete from stream where gid = '{gid}'")
# # Run query
# db_con = pg_con_local(dbname = "spawning_ground")
# dbExecute(db_con, qry)
# dbDisconnect(db_con)

#===========================================================================================
# Verify new points do not already exist in location...esp. after stream adjustments
#===========================================================================================

# Dump llid from reach_points to avoid possible source of mismatch
reach_pts = reach_points %>%
  select(location_id, river_mile, stream_id)

# Add existing RMs to new_points
new_pts = new_points %>%
  left_join(reach_pts, by = c("stream_id", "river_mile")) %>%
  filter(is.na(location_id))

#===========================================================================================
# Add points
#===========================================================================================

# Pull out data for location table
new_loc = new_pts %>%
  select(waterbody_id, river_mile_measure = river_mile, location_description = description,
         latitude = Latitude, longitude = Longitude) %>%
  distinct()

# Add needed columns
new_loc = new_loc %>%
  mutate(location_id = remisc::get_uuid(nrow(new_loc))) %>%
  mutate(location_type_id = "0caa52b7-dfd7-4bf6-bd99-effb17099fd3") %>%               # Reach boundary point
  mutate(stream_channel_type_id = "540d2361-7598-46b0-88b0-895558760c52") %>%         # Not applicable...Reach end point
  mutate(location_orientation_type_id = "ffdeeb40-11c8-4268-a2a5-bd73dedd8c25") %>%   # Not applicable...Reach end point
  mutate(location_code = NA_character_) %>%
  mutate(location_name = NA_character_) %>%
  mutate(waloc_id = NA_integer_) %>%
  mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), tzone = "UTC")) %>%
  mutate(modified_by = NA_character_)

# Pull out location portion
loc_tab = new_loc %>%
  select(location_id, waterbody_id, location_type_id,
         stream_channel_type_id, location_orientation_type_id,
         river_mile_measure, location_code, location_name,
         location_description, waloc_id, created_datetime,
         created_by, modified_datetime, modified_by)

# Get subset of data with coordinates
loc_coords = new_loc %>%
  filter(!is.na(latitude) & !is.na(longitude)) %>%
  select(location_id, latitude, longitude, created_datetime,
         created_by, modified_datetime, modified_by)

# Create geometry in 4326
loc_coords_st = st_as_sf(loc_coords, coords = c("longitude", "latitude"), crs = 4326)

# Convert to state-plane
loc_coords_st = st_transform(loc_coords_st, 2927)

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from location_coordinates"
db_con = pg_con_local(dbname = "spawning_ground")
max_gid = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)
next_gid = max_gid$max + 1

# Add remaining fields
loc_coords_tab = loc_coords_st %>%
  mutate(location_coordinates_id = remisc::get_uuid(nrow(loc_coords))) %>%
  mutate(gid = seq(next_gid, nrow(loc_coords_st) + next_gid - 1)) %>%
  mutate(horizontal_accuracy = NA_real_) %>%
  mutate(comment_text = NA_character_) %>%
  select(location_coordinates_id, location_id, horizontal_accuracy,
         comment_text, gid, geometry, created_datetime, created_by,
         modified_datetime, modified_by)

#=================================================================
# Identify wria_id and add to loc_tab
#=================================================================

# Query to get wria data
qry = glue("select wria_id, wria_code, wria_description as wria_name, geom as geometry ",
           "from wria_lut")

# Run the query
db_con = pg_con_local(dbname = "spawning_ground")
wria_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Check epsg
st_crs(wria_st)$epsg

# Join wria to loc_coords_st
loc_coords = st_join(loc_coords_st, wria_st)
unique(loc_coords$wria_code)
table(loc_coords$wria_code, useNA = "ifany")

# Pull out just the columns needed
wria_codes = loc_coords %>%
  st_drop_geometry() %>%
  select(location_id, wria_id)

# Add WRIA IDs
loc_tab = loc_tab %>%
  left_join(wria_codes, by = "location_id")

# Pull out needed data
loc_tab = loc_tab %>%
  select(location_id, waterbody_id, wria_id, location_type_id,
         stream_channel_type_id, location_orientation_type_id,
         river_mile_measure, location_code, location_name,
         location_description, waloc_id, created_datetime,
         created_by, modified_datetime, modified_by)

#=========================================================================================
# Verify not nulls and datatypes
#=========================================================================================

# location
location_dt = loc_tab
any(is.na(location_dt$location_id))
any(is.na(location_dt$waterbody_id))
any(is.na(location_dt$wria_id))
any(is.na(location_dt$location_type_id))
any(is.na(location_dt$created_datetime))
any(is.na(location_dt$created_by))

# location_coordinates
location_coordinates_dt = loc_coords_tab
any(is.na(location_coordinates_dt$location_coordinates_id))
any(is.na(location_coordinates_dt$location_id))
any(is.na(location_coordinates_dt$gid))
any(is.na(location_coordinates_dt$geometry))
any(is.na(location_coordinates_dt$created_datetime))
any(is.na(location_coordinates_dt$created_by))

#=========================================================================================
# Write all data tables to local spawning_ground
#=========================================================================================

#=======================================================
# Location data...local
#=======================================================

# Write locations
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbWriteTable(db_con, "location", location_dt, row.names = FALSE, append = TRUE)
DBI::dbDisconnect(db_con)

# Write location_coordinates
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = location_coordinates_dt, dsn = db_con, layer = "location_coordinates_temp")
DBI::dbDisconnect(db_con)

# Use select into query to get data into location_coordinates
qry = glue::glue("INSERT INTO location_coordinates ",
                 "SELECT CAST(location_coordinates_id AS UUID), CAST(location_id AS UUID), ",
                 "horizontal_accuracy, comment_text, gid, geometry AS geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM location_coordinates_temp")

# Insert select to spawning_ground
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, "DROP TABLE location_coordinates_temp")
DBI::dbDisconnect(db_con)

#============================================================
# Reset gid_sequence

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from location_coordinates"
db_con = pg_con_local(dbname = "spawning_ground")
max_gid = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)

# Code to reset sequence
qry = glue("SELECT setval('location_coordinates_gid_seq', {max_gid}, true)")
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)


