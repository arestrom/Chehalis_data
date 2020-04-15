#===========================================================================
# Correct edited WRIA 22 and 23 reach_points from Lea. Some LLIDs and
# waterbodies have been added or changed. So now need to make sure
# points are assigned to the correct waterbody_id
#
# Notes:
#  1.
#
#  Completed: 2020-04-
#
# AS 2020-04-14
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

#============================================================================
# Import all stream data from sg that are in WRIAs 22 or 23
#============================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, st.stream_id, st.gid, st.geom as geometry ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "where stream_id is not null and wr.wria_code in ('22', '23') ",
           "order by waterbody_name")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
sg_streams = st_read(db_con, query = qry)
dbDisconnect(db_con)

#============================================================================
# Import all reach data from points file corrected by Lea
#============================================================================

# Get Lea's corrected point data
reach_edits = read_sf("data/SG_Lea_Point_Edits.gdb", layer = "sg_points_lea_edits")

# Pull out coordinates then rename to the default geometry...also takes care of the bogus z value
reach_edits = reach_edits %>%
  mutate(lon = as.numeric(st_coordinates(Shape)[,1])) %>%
  mutate(lat = as.numeric(st_coordinates(Shape)[,2])) %>%
  st_drop_geometry() %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
  arrange(waterbody_name, river_mile) %>%
  mutate(seq_id = as.integer(seq(1, nrow(reach_edits)))) %>%
  select(seq_id, fid, rc_wbid = waterbody_id, rc_wbname = waterbody_name, rc_wbdisplay_name = waterbody_display_name,
         rc_llid = llid, rc_cat_code = cat_code, rc_stid = stream_id, location_id, river_mile, location_name,
         location_description, comments) %>%
  st_transform(2927)

#==========================================================================
# Join points to streams by nearest_feature, i.e., stream nearest to point
#==========================================================================

# Identify the nearest stream to the reach_edit points
nearest_stream = reach_edits %>%
  mutate(nst = try(st_nearest_feature(reach_edits, sg_streams))) %>%
  st_drop_geometry()

# Create nst for join in streams
stream = sg_streams %>%
  st_drop_geometry() %>%
  mutate(nst = seq(1, nrow(sg_streams)))

# Join stream to nearest_stream by nst
nearest_stream = nearest_stream %>%
  inner_join(stream, by = "nst") %>%
  select(seq_id, fid, rc_wbid, waterbody_id, rc_wbname, waterbody_name, rc_wbdisplay_name,
         waterbody_display_name, rc_llid, llid, rc_cat_code, cat_code,
         rc_stid, stream_id, gid, location_id, river_mile, location_name,
         location_description, comments, nst)

# Pull out the cases where wbid match and check more carefully
waterbody_matches = nearest_stream %>%
  filter(rc_wbid == waterbody_id)

# Pull out cases where wbid does not match
waterbody_no_match = nearest_stream %>%
  filter(!rc_wbid == waterbody_id) %>%
  arrange(rc_wbname)

#==========================================================================
# Inspect cases where waterbody_ids agree. See if any updates needed
# Result: Looks good...no obvious updates needed
#==========================================================================

# Check if all llids agree....These are now Ok
chk_llid = waterbody_matches %>%
  filter(!rc_llid == llid)

# Pull out data where new points need to be added to location table
new_points_one_seq_id = waterbody_matches %>%
  mutate(location_id = trimws(location_id)) %>%
  filter(is.na(location_id) | location_id == "") %>%
  pull(seq_id)

# Verify all are unique
any(duplicated(new_points_one_seq_id))

# Get new locations from reach_edits....will have geometry
new_points_one = reach_edits %>%
  filter(seq_id %in% new_points_one_seq_id) %>%
  select(seq_id, waterbody_id = rc_wbid, waterbody_name = rc_wbname,
         river_mile_measure = river_mile, location_name, location_description,
         comments)

# NOTE: Remember to check river_miles for missing values and descriptions
#       for @ characters....change to At. Do this after combining datasets
#       one and two

#==========================================================================
# Inspect cases where waterbody_ids do not agree. Updates wiil be needed.
#==========================================================================

#========= Step 1, inspect new end_points ==============

# First pull out data where new points need to be added to location table
new_points_two_seq_id = waterbody_no_match %>%
  mutate(location_id = trimws(location_id)) %>%
  filter(is.na(location_id) | location_id == "") %>%
  pull(seq_id)

# Verify all are unique
any(duplicated(new_points_two_seq_id))

# Get new locations from reach_edits....will have geometry
new_points_two = reach_edits %>%
  filter(seq_id %in% new_points_two_seq_id) %>%
  select(seq_id, rc_wbid, rc_wbname, rc_llid, river_mile_measure = river_mile,
         location_name, location_description, comments)

# Get stream identifiers from no-match
stream_no_match = waterbody_no_match %>%
  filter(seq_id %in% new_points_two_seq_id) %>%
  select(seq_id, waterbody_id, waterbody_name, llid)

# Join to new_points_two
new_points_two = new_points_two %>%
  inner_join(stream_no_match, by = "seq_id") %>%
  st_transform(4326) %>%
  select(seq_id, rc_wbid, waterbody_id, rc_wbname, waterbody_name,
         rc_llid, llid, river_mile_measure, location_name,
         location_description, comments) %>%
  arrange(waterbody_name)

# Manually update comments...will use indicated wbid
# new_points_two = new_points_two %>%
  # mutate(use_dat = case_when(                        # No need to use. All are correct on Lea's side
  #   seq_id %in% c(718, 719, 652, 32, 658, 725, 731
  #                 722, 713, 715, 260, 724, 729, 438,
  #                 439, 27, 744, 240) ~ "rc",
  #   is.na(seq_id) ~ "sg"))

# Comments to Lea
# 1. Trib 0056, RM 0.7 displays at mouth, 0.0
#               RM 0.0 dislays off the stream line, may be incorrect GPS coordinates.

# Pull out needed fields from two
new_points_two = new_points_two %>%
  st_transform(2927) %>%
  select(seq_id, waterbody_id = rc_wbid, waterbody_name = rc_wbname,
         river_mile_measure, location_name, location_description,
         comments)

# Create location table entries with new end_points
new_points = rbind(new_points_one, new_points_two)

# Pull out wria table for wria join
qry = glue("select wr.wria_id, wr.wria_code, wr.geom as geometry ",
           "from wria_lut as wr ",
           "where wr.wria_code in ('22', '23')")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
wria_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Create new location_id and correct values in rm, desc
new_points = new_points %>%
  mutate(location_id = remisc::get_uuid(nrow(new_points))) %>%
  st_join(wria_st) %>%
  mutate(location_type_id = "0caa52b7-dfd7-4bf6-bd99-effb17099fd3") %>%               # Reach boundary point
  mutate(stream_channel_type_id = "540d2361-7598-46b0-88b0-895558760c52") %>%         # Not applicable
  mutate(location_orientation_type_id = "ffdeeb40-11c8-4268-a2a5-bd73dedd8c25") %>%   # Not applicable
  mutate(location_code = NA_character_) %>%
  mutate(waloc_id = NA_integer_) %>%
  mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = as.POSIXct(NA)) %>%
  mutate(modified_by = NA_character_) %>%
  select(seq_id, location_id, waterbody_id, wria_id, location_type_id,
         stream_channel_type_id, location_orientation_type_id,
         river_mile_measure, location_code, location_name, location_description,
         waloc_id, created_datetime, created_by, modified_datetime,
         modified_by)

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from location_coordinates"
db_con = pg_con_local(dbname = "spawning_ground")
max_gid = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)
next_gid = max_gid$max + 1

# Pull out data for location_coordinates table
new_coords = new_points %>%
  mutate(location_coordinates_id = remisc::get_uuid(nrow(new_points))) %>%
  mutate(horizontal_accuracy = NA_real_) %>%
  mutate(comment_text = NA_character_) %>%
  mutate(gid = seq(next_gid, nrow(new_points) + next_gid - 1)) %>%
  select(location_coordinates_id, location_id, horizontal_accuracy,
         comment_text, gid, created_datetime, created_by,
         modified_datetime, modified_by)

#=========================================================================================
# Get rid of new points from match and no-match
# Then check comments to look for any updates needed in location table
#=========================================================================================

# Pull out the cases where wbid match and check more carefully
waterbody_matches = waterbody_matches %>%
  mutate(location_id = trimws(location_id)) %>%
  filter(!is.na(location_id) & !location_id == "")

# Check on range of tasks needed...manually checked..get rid of "c", and "new".
unique(waterbody_matches$comments)

# Pull out data where updates are needed
updates_match = waterbody_matches %>%
  filter(!is.na(comments)) %>%
  filter(!comments %in% c("c", "New"))

















# Pull out cases where wbid does not match
waterbody_no_match = waterbody_no_match %>%
  filter(!is.na(location_id))







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












