#===========================================================================
# Identify differences in geoms between DB and official LLID layer
#
# Notes:
#  1.
#
#  Completed: 2020-03-10
#
# AS 2020-03-10
#===========================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Load libraries
library(DBI)
library(RPostgres)
#library(tidyverse)
library(dplyr)
library(remisc)
library(tidyr)
library(sf)
library(stringi)
library(lubridate)
library(glue)
library(odbc)

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

# # Get Arleta's latest llid data
# llid_st = read_sf("C:/data/RStudio/spawn_survey_data/data/llid_st.gpkg", layer = "llid_st", crs = 2927)
# # Write chehalis subset to local
# llid_chehalis = llid_st %>%
#   filter(wria_code %in% c("22", "23"))
# #unique(llid_chehalis$wria_code)
# write_sf(llid_chehalis, "data/llid_chehalis.gpkg")
# Get Arleta's latest llid data
llid_chehalis = read_sf("data/llid_chehalis.gpkg", layer = "llid_chehalis", crs = 2927)

# Process for subsetting
llid_chehalis = llid_chehalis %>%
  rename(geometry = geom) %>%
  select(llid = LLID, gnis_name = GNIS_STREAMNAME, llid_name = LLID_STREAMNAME, geometry)

# Trim to only LLIDs currently in DB
ll_id = unique(streams_st$llid)
llid_chehalis = llid_chehalis %>%
  filter(llid %in% ll_id)

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
  select(waterbody_id, waterbody_name, waterbody_display_name,
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

check_streams = all_streams %>%
  filter(waterbody_id %in% changed_streams) %>%
  arrange(waterbody_display_name)


#===========================================================================================
# Correct one stream at a time...Replace multiple segments with one contiguous.
#===========================================================================================

#======== Bear Creek =============================

# Set stream llid
ll_id = '1227706466618'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Beaver Creek =============================

# Set stream llid
ll_id = '1226096466309'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Bloomquist Creek =============================

# Set stream llid
ll_id = '1233284468064'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Capps Creek =============================

# Set stream llid
ll_id = '1232821466419'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Cedar Creek =============================

# Set stream llid
ll_id = '1232868468842'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Chehalis R =============================

# Set stream llid
ll_id = '1238225469619'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Door Creek =============================

# Set stream llid
ll_id = '1226035466086'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== EF Humptulips R =============================

# Set stream llid
ll_id = '1238879472474'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Fall Creek =============================

# Set stream llid
ll_id = '1231264469391'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Hanaford Creek =============================

# Set stream llid
ll_id = '1229391467447'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Lucas Creek =============================

# Set stream llid
ll_id = '1227798466363'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Mill Creek =============================

# Set stream llid
ll_id = '1230163466414'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== NF Newaukum =============================

# Set stream llid
ll_id = '1228368466045'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Noski Creek =============================

# Set stream llid
ll_id = '1230860469689'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== O'Brian Creek =============================

# Set stream llid
ll_id = '1238518472844'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Pants Creek =============================

# Set stream llid
ll_id = '1230357469643'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Smith Creek =============================

# Set stream llid
ll_id = '1233995466470'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== SF Garrard Creek =============================

# Set stream llid
ll_id = '1233028468057'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== SF Newaukum Creek =============================

# Set stream llid
ll_id = '1228549466053'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Stevens Creek =============================

# Set stream llid
ll_id = '1239844472308'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Tributary 0017 =============================

# Set stream llid
ll_id = '1240435471019'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Tributary 0054 =============================

# Set stream llid
ll_id = '1239308471644'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Tributary 0056 =============================

# Set stream llid
ll_id = '1239130471702'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Tributary 0059 =============================

# Set stream llid
ll_id = '1238804471676'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Tributary 0072 =============================

# Set stream llid
ll_id = '1239112472713'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Tributary 0074 =============================

# Set stream llid
ll_id = '1238943473226'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Tributary 0107 =============================

# Set stream llid
ll_id = '1238377473085'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Unnamed Trib (23.1197) =============================

# Set stream llid
ll_id = '1233500464508'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Unnamed Trib (23.1189) =============================

# Set stream llid
ll_id = '1233502464789'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Unnamed Trib (23.1190) =============================

# Set stream llid
ll_id = '1233538464790'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Unnamed Trib (23.1196) =============================

# Set stream llid
ll_id = '1233491464508'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Unnamed Trib (23.1073) =============================

# Set stream llid
ll_id = '1231531463696'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Unnamed Trib (23.1213) =============================

# Set stream llid
ll_id = '1232770464131'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Unnamed Trib (23.1182) =============================

# Set stream llid
ll_id = '1232815464978'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

#======== Unnamed Trib (23.1194) =============================

# Set stream llid
ll_id = '1233083464462'

# Get stream...verify names first
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get corresponding waterbody_id
wb_id = dup_streams %>%
  filter(llid == ll_id) %>%
  select(waterbody_id) %>%
  distinct() %>%
  pull(waterbody_id)

# wb_id = "bf5d304e-06e8-416f-a3c9-46c2f7fc299c"

# Get first corresponding gid
gid = dup_streams %>%
  filter(llid == ll_id) %>%
  select(gid) %>%
  head(1L) %>%
  pull(gid)

# gid = 769L

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

#stream_st = head(stream_st, 1)

# Delete query
qry = glue("delete from stream where waterbody_id = '{wb_id}'")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Write temp table
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = stream_st, dsn = db_con, layer = "stream_temp", overwrite = TRUE)
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

# Dump from dup_streams
dup_streams = dup_streams %>%
  filter(!llid == ll_id)

