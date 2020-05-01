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

#===========================================================================
# NF Newaukum Issues:
#===========================================================================

# 1. Trib 0910 RM 0.0 deleted. Use NF Newaukum 17.3
# 2. NF Newaukum, 18.10 updated to Tributary 0910 (23.0910), RM 0.8
# 3. Trib 0910, 0.9, updated to NF Newaukum 18.2
# 4. Switched locations for Trib 0890 RMs 2.70 and 4.10. Clear mixup in entry.
# 5. Two sets of points for SF Newaukum 11.0, 11.2, both at N Fork Bridge. Kept 11.0
# 6. All points on SF Newaukum using old RMs as for Mainstem. Not restarting at origin.
# 7. Reassigned all RMs along old Newaukum that were actually SF to SF.


# Set location_id
loc_id = "6d750339-7fcb-465c-9db3-78b578bdc4e3"

# Set queries
up_qry = glue::glue("select count(survey_id) as upper_count from survey ",
                    "where upper_end_point_id = '{loc_id}'")
lo_qry = glue::glue("select count(survey_id) as lower_count from survey ",
                    "where lower_end_point_id = '{loc_id}'")
# Check number of surveys tied to location_id
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbGetQuery(db_con, up_qry)
DBI::dbGetQuery(db_con, lo_qry)
dbDisconnect(db_con)

# Update point
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("update location set ",
           "waterbody_id = 'edcacb6b-3859-4e39-9304-3dd6dd5dfc98', ",
           "river_mile_measure = 16.3, ",
           "location_description = 'Pigeon Springs Pullout (upper)', ",
           "modified_datetime = now(), ",
           "modified_by = 'stromas' ",
           "where location_id = 'cae3c4a6-9ec1-4e6e-87a1-5a6e8732cd95'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

# Update survey
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("update survey set ",
           "lower_end_point_id = '5ab2b2d0-9af1-4cc4-9da5-1fed6ac2be29' ",
           "where lower_end_point_id = '6d750339-7fcb-465c-9db3-78b578bdc4e3'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

# Delete location
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("delete from location ",
           "where location_id = '6d750339-7fcb-465c-9db3-78b578bdc4e3'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

#======================================================================
# Add new stream
#======================================================================

# llid
ll_id = '1232785469628'

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Unnamed Tributary (LB)",
            waterbody_display_name = "Unnamed Trib",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "23",
            tributary_to_name = "Porter Creek",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to sink
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Get last gid
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid = max_gid$gid + 1L
# gid = 3639L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Get geometry
llid_chehalis = read_sf("data/llid_chehalis_2020-03-12.gpkg",
                        layer = "llid_chehalis_2020-03-12", crs = 2927)

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid) %>%
  mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = as.POSIXct(NA)) %>%
  mutate(modified_by = NA_character_) %>%
  select(stream_id, waterbody_id, gid, geom, created_datetime,
         created_by, modified_datetime, modified_by)

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



