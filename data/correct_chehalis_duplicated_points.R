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
# Add new unnamed stream
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

# Update reach point to associate with new stream
# new_wb_id = "480e352c-bcb1-4ae0-ac92-e1f45a4ee83e"
# loc_id = "82f87a85-b268-46f0-94f7-6bb9552a320b"
qry = glue::glue("UPDATE location ",
                 "SET waterbody_id = '{new_wb_id}' ",
                 "WHERE location_id = '{loc_id}'")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Update reach point to associate with new stream
# new_wb_id = "480e352c-bcb1-4ae0-ac92-e1f45a4ee83e"
# loc_id = "a5936011-0631-4349-886f-f54410cb1532"
qry = glue::glue("UPDATE location ",
                 "SET waterbody_id = '{new_wb_id}' ",
                 "WHERE location_id = '{loc_id}'")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#======================================================================
# Add new unnamed stream
#======================================================================

# llid
ll_id = '1230774469526'
#wb_id =

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Unnamed Tributary (LB)",
            waterbody_display_name = "Unnamed Trib",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "23.0677A",
            tributary_to_name = "Waddell Creek",
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

# Update reach point to associate with new stream
# new_wb_id = ""
# loc_id = "45f473ab-ab42-43b1-b4a9-afd29c1f5860"
qry = glue::glue("UPDATE location ",
                 "SET waterbody_id = '{new_wb_id}' ",
                 "WHERE location_id = '{loc_id}'")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Update reach point to associate with new stream
# new_wb_id = ""
# loc_id = "b275132d-4a79-4237-9d0e-b537f1ad5984"
qry = glue::glue("UPDATE location ",
                 "SET waterbody_id = '{new_wb_id}' ",
                 "WHERE location_id = '{loc_id}'")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#======================================================================
# Add new unnamed stream
#======================================================================

# llid
ll_id = '1230815469563'
#wb_id =

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Unnamed Tributary",
            waterbody_display_name = "Unnamed Trib",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "23",
            tributary_to_name = "Waddell Creek",
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

# Update reach point to associate with new stream
# new_wb_id = ""
# loc_id = "91c491fd-245d-4f92-a8df-29f965769aea"
qry = glue::glue("UPDATE location ",
                 "SET waterbody_id = '{new_wb_id}' ",
                 "WHERE location_id = '{loc_id}'")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#======================================================================
# Add McDonald Creek 22.0480 geometry
#======================================================================

# llid and waterbody_id
ll_id = '1234097469950'
wb_id = 'aed10dcf-fd0a-49d0-acd4-4aaf1160050a'

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
#new_wb_id = wb$waterbody_id
new_wb_id = wb_id

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

# Update reach point to associate with new stream
# new_wb_id = ""
# loc_id = "91c491fd-245d-4f92-a8df-29f965769aea"
qry = glue::glue("UPDATE location ",
                 "SET waterbody_id = '{new_wb_id}' ",
                 "WHERE location_id = '{loc_id}'")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#======================================================================
# Add Hensen Creek, formerly Sherwood lower portion
#======================================================================

# llid and waterbody_id
ll_id = '1234888470075'
#wb_id = ''

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Hensen Creek",
            waterbody_display_name = "Hensen Cr (22.0362)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0362",
            tributary_to_name = "Satsop River",
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

#======================================================================
# Add Newman Creek
#======================================================================

# llid and waterbody_id
ll_id = '1234654469839'
wb_id = '17557462-e4d9-4497-ba26-b54ad9831413'

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
new_wb_id = wb_id

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

#======================================================================
# Add East Branch Newman Creek...no apparent previous surveys
#======================================================================

# llid and waterbody_id
ll_id = '1234436470320'
#wb_id = ''

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "East Branch Newman Creek",
            waterbody_display_name = "E Branch Newman Cr (22.0484)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0484",
            tributary_to_name = "Newman Creek",
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

#======================================================================
# Add King Creek geo
#======================================================================

# llid and waterbody_id
ll_id = '1235100470551'
wb_id = '55780ca0-684d-4024-8e47-9c03aa6d5efa'

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
new_wb_id = wb_id

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

#======================================================================
# Add King Creek Tributary A...no apparent previous surveys
#======================================================================

# llid and waterbody_id
ll_id = '1235064470599'
#wb_id = ''

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "King Creek Tributary A",
            waterbody_display_name = "King Cr Trib A (22.-004)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.-004",
            tributary_to_name = "King Creek",
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

# Made a mistake...already had a waterbody_id. So:
# 1. Update pre_existing waterbody
qry = glue("update waterbody_lut set ",
           "waterbody_name = 'King Creek Tributary A', ",
           "waterbody_display_name = 'King Cr Trib A (22.-004)', ",
           "latitude_longitude_id = '1235064470599', ",
           "stream_catalog_code = '22.-004', ",
           "tributary_to_name = 'King Creek' ",
           "where waterbody_id = '4ef40a78-55b6-4991-8dd0-54a3c13c189a'")
# Run update
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# 2. Update waterbody in newly added stream
qry = glue("update stream set ",
           "waterbody_id = '4ef40a78-55b6-4991-8dd0-54a3c13c189a' ",
           "where stream_id = 'e3c3a410-4a5a-45da-8648-3b290ea96956'")

# Run update
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# 3. Delete newly created waterbody
qry = glue("delete from waterbody_lut ",
           "where waterbody_id = '{new_wb_id}'")
# Run update
db_con = dbConnect(odbc::odbc(), dsn = "local_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#======================================================================
# Add Phillips Creek geo
#======================================================================

# llid and waterbody_id
ll_id = '1232954472044'
wb_id = 'b8d5a068-fe90-4801-b4ed-0b68a7a5c0c1'

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
new_wb_id = wb_id

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

#===========================================================================
# NF Satsop Issues:
#===========================================================================

# Set location_id
loc_id = "88be4b53-1187-484e-90db-a88ec406ae5c"

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

# Update point to correct waterbody
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("update location set ",
           "waterbody_id = 'fd8d2966-c6e0-4dd9-9977-7f0799e6caaf', ",
           # "river_mile_measure = 16.3, ",
           # "location_description = 'Pigeon Springs Pullout (upper)', ",
           "modified_datetime = now(), ",
           "modified_by = 'stromas' ",
           "where location_id = '88be4b53-1187-484e-90db-a88ec406ae5c'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

# # Update survey
# db_con = pg_con_local(dbname = "spawning_ground")
# qry = glue("update survey set ",
#            "lower_end_point_id = '5ab2b2d0-9af1-4cc4-9da5-1fed6ac2be29' ",
#            "where lower_end_point_id = '6d750339-7fcb-465c-9db3-78b578bdc4e3'")
# DBI::dbExecute(db_con, qry)
# dbDisconnect(db_con)
#
# # Delete location
# db_con = pg_con_local(dbname = "spawning_ground")
# qry = glue("delete from location ",
#            "where location_id = '6d750339-7fcb-465c-9db3-78b578bdc4e3'")
# DBI::dbExecute(db_con, qry)
# dbDisconnect(db_con)

#======================================================================
# Add Baker Creek geo
#======================================================================

# llid and waterbody_id
ll_id = '1234524473124'
wb_id = '0795fe9a-5f76-4726-aa6e-f5add12b258e'

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
new_wb_id = wb_id

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

#======================================================================
# Add Unnamed Creek geo
#======================================================================

# llid and waterbody_id
ll_id = '1234595472731'
wb_id = '0cbd0418-dd36-4757-9be7-6f0b863c73a3'

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
new_wb_id = wb_id

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

#======================================================================
# Add Unnamed Creek geo
#======================================================================

# llid and waterbody_id
ll_id = '1235203472654'
wb_id = '291ba365-bfdf-41bb-9b2a-9f35fcabfcb4'

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
new_wb_id = wb_id

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

#===========================================================================
# WF Humptulips Issues:
#===========================================================================

# Set location_id
loc_id = "7bf9ff34-c01a-4945-a4e0-22111ef8f9df"

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

# Update point to correct waterbody
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("update location set ",
           "waterbody_id = '109b64f4-274d-4666-96a5-c249fcc0d801', ",
           # "river_mile_measure = 16.3, ",
           "location_description = 'Polson Camp bridge', ",
           "modified_datetime = now(), ",
           "modified_by = 'stromas' ",
           "where location_id = 'cfef2011-826c-47da-b13c-e70e85b48a6b'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

# Update stream geometry to correct waterbody
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("update stream set ",
           "waterbody_id = '109b64f4-274d-4666-96a5-c249fcc0d801' ",
           "where stream_id = 'ce997fe5-2edc-41a6-b08b-36b10bd3978e'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

# Update location_coordinates
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("update location_coordinates ",
           "set location_id = 'c09697a1-7b43-4ec7-ab49-52575ad75879' ",
           "where location_coordinates_id = '417bed44-f8df-4f7d-a537-d54d0ad278db'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

# Update stock_lut to correct waterbody
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("update stock_lut set ",
           "waterbody_id = '109b64f4-274d-4666-96a5-c249fcc0d801' ",
           "where waterbody_id = 'b8a2079f-101b-415d-8fde-bf8158de0473'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

# Update survey
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("update survey set ",
           "lower_end_point_id = '8f322e5a-b418-4502-b9ad-58b286ecf661' ",
           "where lower_end_point_id = '3d8d8604-c986-480c-84d7-b6b6c268b051'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)
#
# Update survey
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("update survey set ",
           "upper_end_point_id = 'a8499d7a-3a33-47e6-a8b3-87ef65bab6ab' ",
           "where survey_id = '368e6e15-64c0-436e-9ae7-d82d0029c311'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)
#
# Delete location_coordinates
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("delete from location_coordinates ",
           "where location_id = '30627880-8a1a-4b5a-9e2d-55b947be253c'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

# Delete location
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("delete from location ",
           "where location_id = '30627880-8a1a-4b5a-9e2d-55b947be253c'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

# Delete waterbody
db_con = pg_con_local(dbname = "spawning_ground")
qry = glue("delete from waterbody_lut ",
           "where waterbody_id = 'b8a2079f-101b-415d-8fde-bf8158de0473'")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)


#============================================================================================
# Should Shaeffer Slough be updated? Verify
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



