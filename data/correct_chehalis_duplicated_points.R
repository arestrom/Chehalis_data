#===========================================================================
# Correct WRIA 22 and 23 reach_points from Lea. After adding and editing
# noticed that some points, especially on SF Newaukum were duplicated.
# Checking that only one point per stream is present
#
# Notes:
#  1.
#
#  Completed: 2020-04-
#
# AS 2020-04-29
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

#=========================================================
# Function to update location data
#=========================================================

# Update location descriptions
update_point_desc = function(x) {
  db_con = pg_con_local(dbname = "spawning_ground")
  if( !is.data.frame(x) & !ncol(x) == 3 ) {
    stop("The input data must be a three column dataframe")
  }
  for( i in 1:nrow(x) ) {
    loc_id = x$location_id[i]
    loc_desc = x$location_description[i]
    loc_desc = gsub("'", "''", loc_desc)
    qry = glue::glue("update location ",
                     "set location_description = '{loc_desc}' ",
                     "where location_id = '{loc_id}'")
    DBI::dbExecute(db_con, qry)
  }
  dbDisconnect(db_con)
}

# Update points that have been moved to new locations in imported data
update_point = function(x) {
  db_con = pg_con_local(dbname = "spawning_ground")
  if( !is.data.frame(x) & !ncol(x) == 7 ) {
    stop("The input data must be a three column dataframe")
  }
  for( i in 1:nrow(x) ) {
    loc_id = x$location_id[i]
    loc_geom = st_as_binary(x$geometry[i][[1]], hex = TRUE)
    qry = glue::glue("update location_coordinates ",
                     "set geom = ST_SetSRID('{loc_geom}'::geometry, 2927) ",
                     "where location_id = '{loc_id}'")
    DBI::dbExecute(db_con, qry)
  }
  dbDisconnect(db_con)
}

#============================================================================
# Import WRIA 22 and 23 points that are withing 100 meters and with same RM
#============================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "loc.river_mile_measure as river_mile, loc.location_id, ",
           "loc.location_name, loc.location_description, loc.created_datetime, ",
           "loc.created_by, st.stream_id, lc.gid, lc.geom as geometry ",
           "from location as loc ",
           "left join location_coordinates as lc on loc.location_id = lc.location_id ",
           "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "where wr.wria_code in ('22', '23') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by wb.waterbody_name, loc.river_mile_measure")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
sg_points = st_read(db_con, query = qry)
dbDisconnect(db_con)

#============================================================================
# Identify cases where RM is duplicated...by waterbody_id and RM
#============================================================================

# Pull out duplicated RMs
dup_rc = sg_points %>%
  group_by(waterbody_id, river_mile) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(waterbody_id, dup_location_id = location_id, river_mile) %>%
  st_drop_geometry() %>%
  distinct()

# Join back to get both copies
dup_rc = dup_rc %>%
  inner_join(sg_points, by = c("waterbody_id", "river_mile")) %>%
  st_as_sf() %>%
  st_transform(4326)

# Result: Only four cases...all ok, when wria_code...Chehalis issue---is omitted.

#============================================================================
# Identify cases where RM is duplicated...by polygon intersect
#============================================================================

# Just keep points with coordinates...no reason for next step otherwise
rc_points = sg_points %>%
  filter(!is.na(gid))

# Pull out duplicated RMs, then buffer by 100 ft. Units will be in units of CRS (survey feet)
rc_poly = rc_points %>%
  st_buffer(dist = 100)

# # Verify in QGIS
# write_sf(rc_poly, "data/rc_poly.gpkg")

# Rename fields in rc_poly to allow better names in join
rc_poly = rc_poly %>%
  select(poly_wb_id = waterbody_id, poly_wb_name = waterbody_name,
         poly_rm = river_mile, poly_loc_id = location_id,
         poly_loc_name = location_name, poly_loc_desc = location_description,
         poly_gid = gid, poly_create_dt = created_datetime)

# Join
pt_poly = rc_poly %>%
  st_join(rc_points) %>%
  arrange(poly_gid) %>%
  group_by(poly_gid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup()

# Pull out multiple joins
mult_pt_gid = pt_poly %>%
  filter(n_seq > 1) %>%
  pull(poly_gid)

# Get full set of points with multiple intersects
mult_point = pt_poly %>%
  filter(poly_gid %in% mult_pt_gid)





















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

#===============================================================================================
# STAGE 1. Pull out cases where new points need to be added.
# Step 1. Process where waterbodies agree,
#      2. Manually verify that the waterbody_id provided in gdb should be the one used.
#      3. Generate tables for new points
#===============================================================================================

#=====================================================================
# Inspect cases where waterbody_ids agree.
#=====================================================================

#== Step 1, Pull out new_points data, inspect comments =======

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

# Check comments
unique(new_points_one$comments)

# NOTE: Remember to check river_miles for missing values. Do this after
# combining datasets one and two

#==========================================================================
# Inspect cases where waterbody_ids do not agree.
#==========================================================================

#== Step 2. Verify WBIDs for new end_points: IDs do not match  ====

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

#========= Step 3. Generate tables for new points that need to be added   ==============

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

# # THIS TIME ONLY.....Forgot to update RM to zero
# new_points$river_mile_measure[new_points$seq_id == 654] = 0.0
# sort(unique(new_points$river_mile_measure))

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
# STAGE 2. Make any needed adjustments on existing points
# Step 1. Get rid of points that were newly added from data that matched.
#      2. Check comments to look for any updates needed to location table
#         Possible updates include moving points or edits to descriptions.
#      3. Get rid of points that were newly added from data that did not match.
#      4. Add point geoms to no-match data so streams can be verified.
#      5. For no-match data, verify that rc_wbid should be used. Spot check.
#      6. Add wbid from location table to check if wbid needs to be updated.
#=========================================================================================

#=== 1. Get rid of points for matching data without location_id ===============
#===    these point will be added at the end

# Pull out the cases where wbid match and check more carefully
waterbody_matches = waterbody_matches %>%
  mutate(location_id = trimws(location_id)) %>%
  filter(!is.na(location_id) & !location_id == "")

#=== 2. Check comments for any needed updates to existing points ==========

# Check on range of tasks needed...manually checked..get rid of "c", and "new".
unique(waterbody_matches$comments)

# Pull out data where updates are needed
updates_match = waterbody_matches %>%
  filter(!is.na(comments)) %>%
  filter(!comments %in% c("c", "New"))

# Get cases where location description needs to be updated
location_desc = updates_match %>%
  filter(comments == "updated location descripton") %>%
  select(location_id, location_description, comments)

# # Run the update function: DONE !!!!!!!!!!!!!!
# update_point_desc(location_desc)

# Get cases where point was moved
point_moved = updates_match %>%
  filter(comments %in% c("Moved Point", "Moved point to stream layer.")) %>%
  select(seq_id, location_id, stream_id, river_mile,
         location_description, comments)

# Pull out seq_ids
point_moved_id = point_moved %>%
  pull(seq_id)

# Add geometry
new_geo = reach_edits %>%
  filter(seq_id %in% point_moved_id) %>%
  select(seq_id)

# Join to point_moved
point_moved = point_moved %>%
  left_join(new_geo, by = "seq_id")

# # Run the update function: DONE !!!!!!!!!!!!
# update_point(point_moved)

# Check again for comments
unique(updates_match$comments)

#=== 3. Get rid of points for no-match data without location_id ===============
#===    these point will be added at the end

# Pull out cases where wbid does not match
waterbody_no_match = waterbody_no_match %>%
  mutate(location_id = trimws(location_id)) %>%
  filter(!is.na(location_id) & !location_id == "")

#=== 5. Add point_geoms from reach_edit to no-match data ====================================

# Get the no-match seq id
no_match_seq_id = unique(waterbody_no_match$seq_id)

# Pull out geometry data points in Lea's edited data
edited_points = reach_edits %>%
  filter(seq_id %in% no_match_seq_id) %>%
  select(seq_id)

# Join to new_points_two
waterbody_no_match_geo = waterbody_no_match %>%
  left_join(edited_points, by = "seq_id") %>%
  st_as_sf() %>%
  st_transform(4326) %>%
  select(seq_id, rc_wbid, waterbody_id, rc_wbname, waterbody_name,
         rc_llid, llid, rc_cat_code, cat_code, river_mile,
         location_id, stream_id, location_name, location_description,
         comments) %>%
  arrange(waterbody_name)

#============================================================================================
# Sent waterbody_no_match_geo to Lea for review...too many unknowns
#============================================================================================

# # Output sgs_streams
# st_crs(sg_streams)$epsg
# st_write(sg_streams, "data/sg_streams.shp")
#
# # Output waterbody_mismatch
# waterbody_mismatch = waterbody_no_match_geo %>%
#   st_transform(2927)
#
# # Output sgs_streams
# st_crs(waterbody_mismatch)$epsg
# st_write(waterbody_mismatch, "data/waterbody_mismatch.shp")

#============================================================================================
# 6. After Lea's review....make the following changes
#============================================================================================

# A. Delete one point...this is duplicated
waterbody_no_match_geo = waterbody_no_match_geo %>%
  filter(!seq_id == 218)

# # B. Only one survey has been entered for this end_point to update to other and delete
# qry = glue("update survey ",
#            "set upper_end_point_id = '32daafd4-affe-4d83-8762-8b5a9cb74de9', ",
#            "lower_end_point_id = '6170e2d3-139f-43ac-8594-90da3fae6150' ",
#            "where survey_id = '10d0987a-e5a3-4e0e-8c4e-3daed459e0a4'")
#
# # Insert select to spawning_ground
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # C. Delete the point geometry
# qry = glue("delete from location_coordinates ",
#            "where location_id = 'ea6e3d85-f60e-4df7-8280-bccc2cfaeebe'")
#
# # Insert select to spawning_ground
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # D. Delete the point
# qry = glue("delete from location ",
#            "where location_id = 'ea6e3d85-f60e-4df7-8280-bccc2cfaeebe'")
#
# # Insert select to spawning_ground
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
#===========================================================
# Need to delete two points on Elwood that are duplicates
#===========================================================

# # --- First point --------------------
#
# # Get all surveys where upper_end_point entered as duplicate point....manully verified only upper involved
# qry = glue("select survey_id from survey ",
#            "where upper_end_point_id = 'c714008e-f293-41d2-b5a5-1d57d3b92f83'")
#
# # Insert select to spawning_ground
# db_con = pg_con_local(dbname = "spawning_ground")
# s_id = DBI::dbGetQuery(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # Format s_id
# s_id = paste0(paste0("'", unique(s_id$survey_id), "'"), collapse = ", ")
#
# # Multiple surveys entered for this end_point....update to other and delete
# qry = glue("update survey ",
#            "set upper_end_point_id = 'aecab56c-f09a-4777-9890-558514c46684' ",
#            "where survey_id in ({s_id})")
#
# # Update
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # C. Delete the point geometry
# qry = glue("delete from location_coordinates ",
#            "where location_id = 'c714008e-f293-41d2-b5a5-1d57d3b92f83'")
#
# # Insert select to spawning_ground
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # D. Delete the point
# qry = glue("delete from location ",
#            "where location_id = 'c714008e-f293-41d2-b5a5-1d57d3b92f83'")
#
# # Insert select to spawning_ground
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # --- Second point --------------------
#
# # Get all surveys where upper_end_point entered as duplicate point....manully verified only upper involved
# qry = glue("select survey_id from survey ",
#            "where upper_end_point_id = '5e649ff1-a49f-489b-9567-a16ef951f721'")
#
# # Insert select to spawning_ground
# db_con = pg_con_local(dbname = "spawning_ground")
# s_id = DBI::dbGetQuery(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # Format s_id
# s_id = paste0(paste0("'", unique(s_id$survey_id), "'"), collapse = ", ")
#
# # Multiple surveys entered for this end_point....update to other and delete
# qry = glue("update survey ",
#            "set upper_end_point_id = 'e5a02e9d-1d58-4206-8cda-0df7a815b870' ",
#            "where survey_id in ({s_id})")
#
# # Update
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # C. Delete the point geometry
# qry = glue("delete from location_coordinates ",
#            "where location_id = '5e649ff1-a49f-489b-9567-a16ef951f721'")
#
# # Insert select to spawning_ground
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # D. Delete the point
# qry = glue("delete from location ",
#            "where location_id = '5e649ff1-a49f-489b-9567-a16ef951f721'")
#
# # Insert select to spawning_ground
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)

# Update no_match_geo to corrected wb_ids
# E. Only two needed to be corrected....Info from Lea
use_wbid = c(60, 62)

# Shift info
waterbody_fixed_geo = waterbody_no_match_geo %>%
  mutate(rc_wbid = if_else(seq_id %in% use_wbid, waterbody_id, rc_wbid)) %>%
  mutate(rc_wbname = if_else(seq_id %in% use_wbid, waterbody_name, rc_wbname)) %>%
  mutate(rc_llid = if_else(seq_id %in% use_wbid, llid, rc_llid)) %>%
  mutate(rc_cat_code = if_else(seq_id %in% use_wbid, cat_code, rc_cat_code)) %>%
  select( - c(waterbody_id, waterbody_name, llid, cat_code))

# Pull out needed variables and rows...just need comments now
waterbody_fixed_geo = waterbody_fixed_geo %>%
  select(seq_id, waterbody_id = rc_wbid, waterbody_name = rc_wbname, llid = rc_llid,
         cat_code = rc_cat_code, river_mile, location_id, stream_id, location_name,
         location_description, comments) %>%
  filter(!is.na(comments))

#============================================================================================
# Move points where indicated
#============================================================================================

# Check comments
unique(waterbody_fixed_geo$comments)

# Get cases where points were moved
points_moved = waterbody_fixed_geo %>%
  filter(comments %in% c("Moved Point", "Moved Point, updated location descripton")) %>%
  st_transform(2927) %>%
  select(seq_id, location_id, stream_id, river_mile,
         location_description, comments)

# # Run the update function: DONE !!!!!!!!!!!!
# update_point(points_moved)

# Check again for comments
unique(waterbody_fixed_geo$comments)

# Get cases where location description needs to be updated
location_desc_two = waterbody_fixed_geo %>%
  filter(comments %in% c("updated location descripton", "Moved Point, updated location descripton")) %>%
  select(location_id, location_description, comments) %>%
  st_drop_geometry()

# # Run the update function: DONE !!!!!!!!!!!!!!
# update_point_desc(location_desc_two)

#============================================================================================
# Correct a few items
#============================================================================================

# Fill in some empty stuff....
new_points_dt = new_points %>%
  mutate(location_code = trimws(location_code)) %>%
  mutate(location_code = if_else(location_code == "", NA_character_, location_code)) %>%
  mutate(location_name = trimws(location_name)) %>%
  mutate(location_name = if_else(location_name == "", NA_character_, location_name)) %>%
  mutate(location_description = trimws(location_description)) %>%
  mutate(location_description = if_else(location_description == "", NA_character_, location_description)) %>%
  mutate(river_mile_measure = if_else(is.na(river_mile_measure) & location_description == "Mouth",
                                      0.0, river_mile_measure))

#============================================================================================
# Look for cases where new points may create duplicate RMs
#============================================================================================

# Import reach data...some may have been updated
# Get existing rm_data
qry = glue("select distinct wb.waterbody_id, loc.location_id, wb.latitude_longitude_id as llid, ",
           "loc.river_mile_measure as river_mile, st.stream_id, lc.location_coordinates_id, ",
           "lc.gid, loc.location_name, loc.location_description ",
           "from location as loc ",
           "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "left join location_coordinates as lc on loc.location_id = lc.location_id ",
           "where wria_code in ('22', '23') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point')")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
reach_points = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out new_points columns to check if dups will be created
np_trim = new_points_dt %>%
  st_drop_geometry() %>%
  mutate(new_geo = "yes") %>%
  select(waterbody_id, new_loc_id = location_id, new_rm = river_mile_measure,
         new_desc = location_description, new_geo)

# Join to new_points
chk_np = reach_points %>%
  left_join(np_trim, by = "waterbody_id") %>%
  filter(!is.na(new_geo)) %>%
  filter(river_mile == new_rm)

# Pull out location_ids in chk_np...these cases need special attention below
dup_loc_ids = unique(chk_np$new_loc_id)

# 1. Pull out truly new points...no duplicated RMs
no_dup_new_points = new_points_dt %>%
  filter(!location_id %in% dup_loc_ids)

# Pull out cases where only new coordinates and location descriptions are needed
new_coords_descs_id = chk_np %>%
  filter(is.na(location_coordinates_id)) %>%
  pull(new_loc_id)

# 2. Get the data for new_coords_descs
new_coords_descs = new_points_dt %>%
  filter(location_id %in% new_coords_descs_id)

# Pull out cases where coordinates are duplicated
new_dup_coords_id = chk_np %>%
  filter(!is.na(location_coordinates_id)) %>%
  pull(new_loc_id)

# 3. Get the data for new_coords_descs
new_dup_coords = new_points_dt %>%
  filter(location_id %in% new_dup_coords_id) %>%
  rename(river_mile = river_mile_measure)

# Add gid
sub_np = chk_np %>%
  select(waterbody_id, river_mile, gid)

# Join to new_dup_coords
new_dup_coords = new_dup_coords %>%
  left_join(sub_np, by = c("waterbody_id", "river_mile"))

#=========================================================================
# Start with number 3 first
#=========================================================================

# Investigate the 7 cases where coords are duplicated. Choose the best
chk_dup_coords = new_dup_coords %>%
  st_transform(4326) %>%
  mutate(lat = st_coordinates(geometry)[, 2]) %>%
  mutate(lon = st_coordinates(geometry)[, 1]) %>%
  mutate(coords = paste0(lat, ", ", lon))

# # Seq_ids to dump
# dump_ids = c(8, 564, 654, 240, 27)
#
# # Output coords I am not entering to the database to Lea
# not_entering = chk_dup_coords %>%
#   filter(seq_id %in% dump_ids) %>%
#   st_drop_geometry() %>%
#   select(seq_id, location_id, waterbody_id, river_mile,
#          location_description, gid, coords)
#
# # Output with styling
# num_cols = ncol(not_entering)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "NotEntering.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NotEntering", gridLines = TRUE)
# writeData(wb, sheet = 1, not_entering, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Pull out cases that I can add and combine with no_dup_new_points
ok_dup_coords = new_dup_coords %>%
  filter(!seq_id %in% dump_ids) %>%
  select(seq_id, location_id, waterbody_id, wria_id, location_type_id,
         stream_channel_type_id, location_orientation_type_id,
         river_mile_measure = river_mile, location_code, location_name,
         location_description, waloc_id, created_datetime, created_by,
         modified_datetime, modified_by)

# Combine
no_dup_new_points = rbind(no_dup_new_points, ok_dup_coords)

#=========================================================================
# Number 2
#=========================================================================

# Investigate the 35 cases where only coords and descriptions supposedly needed
# Spot check indicates I can update descripitons, then add geometry
chk_coords_descs = new_coords_descs %>%
  st_transform(4326) %>%
  mutate(lat = st_coordinates(geometry)[, 2]) %>%
  mutate(lon = st_coordinates(geometry)[, 1]) %>%
  mutate(coords = paste0(lat, ", ", lon)) %>%
  select(-c(wria_id, stream_channel_type_id,
            location_type_id, location_orientation_type_id))

# Pull out just the data needed for location_description updates
coords_desc_updates = new_coords_descs %>%
  select(waterbody_id, river_mile = river_mile_measure,
         new_location_description = location_description) %>%
  mutate(comments = NA_character_) %>%
  left_join(reach_points, by = c("waterbody_id", "river_mile")) %>%
  select(location_id, location_description, new_location_description, comments)

# Can use all new descriptions, so trim to needed
location_desc = coords_desc_updates %>%
  mutate(location_description = new_location_description) %>%
  st_drop_geometry() %>%
  select(location_id, location_description, comments)

# # Run the update function: DONE !!!!!!!!!!!!!!
# update_point_desc(location_desc)

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from location_coordinates"
db_con = pg_con_local(dbname = "spawning_ground")
max_gid = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)
next_gid = max_gid$max + 1

# Pull out data for location_coordinates table
coords_only = coords_desc_updates %>%
  mutate(location_coordinates_id = remisc::get_uuid(nrow(coords_desc_updates))) %>%
  mutate(horizontal_accuracy = NA_real_) %>%
  mutate(comment_text = NA_character_) %>%
  mutate(gid = seq(next_gid, nrow(coords_desc_updates) + next_gid - 1)) %>%
  mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = as.POSIXct(NA)) %>%
  mutate(modified_by = NA_character_) %>%
  select(location_coordinates_id, location_id, horizontal_accuracy,
         comment_text, gid, created_datetime, created_by,
         modified_datetime, modified_by)

# Write coords_only data
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = coords_only, dsn = db_con, layer = "location_coordinates_temp")
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

#=========================================================================
# Number 3 ... Add final set of new locations and points
#=========================================================================

# Write final set of data to database
no_dup_new_points = rbind(no_dup_new_points, ok_dup_coords)

# Pull out location_dt data
location_dt = no_dup_new_points %>%
  st_drop_geometry() %>%
  mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = as.POSIXct(NA)) %>%
  mutate(modified_by = NA_character_) %>%
  select(location_id, waterbody_id, wria_id, location_type_id,
         stream_channel_type_id, location_orientation_type_id,
         river_mile_measure, location_code, location_name,
         location_description, waloc_id, created_datetime,
         created_by, modified_datetime, modified_by)

# Write locations
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbWriteTable(db_con, "location", location_dt, row.names = FALSE, append = TRUE)
DBI::dbDisconnect(db_con)

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from location_coordinates"
db_con = pg_con_local(dbname = "spawning_ground")
max_gid = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)
next_gid = max_gid$max + 1

# Pull out data for location_coordinates table
location_coordinates_dt = no_dup_new_points %>%
  mutate(location_coordinates_id = remisc::get_uuid(nrow(no_dup_new_points))) %>%
  mutate(horizontal_accuracy = NA_real_) %>%
  mutate(comment_text = NA_character_) %>%
  mutate(gid = seq(next_gid, nrow(no_dup_new_points) + next_gid - 1)) %>%
  select(location_coordinates_id, location_id, horizontal_accuracy,
         comment_text, gid, created_datetime, created_by,
         modified_datetime, modified_by)

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

#============================================================================================
# Dump the newly uploaded points...created duplicates in many cases
#============================================================================================

# # Get the lc_ids
# lc_id = location_coordinates_dt$location_coordinates_id
# lc_id = paste0(paste0("'", lc_id, "'"), collapse = ", ")
#
# # Delete
# qry = glue("delete from location_coordinates ",
#            "where location_coordinates_id in ({lc_id})")
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # Get the lc_ids
# loc_id = location_dt$location_id
# loc_id = paste0(paste0("'", loc_id, "'"), collapse = ", ")
#
# # Delete
# qry = glue("delete from location ",
#            "where location_id in ({loc_id})")
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)

#============================================================================================
# Should Shaeffer Slough be updated ????? Verify
#============================================================================================

#============================================================
# Reset gid_sequence
#============================================================

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



