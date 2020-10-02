#===========================================================================
# Add new streams to SG...Several indicated where points exist but no stream
# geometry...either for point or for downstream portion. I want all the
# streams to link up to saltwater in some way. No orphan segments.
#
# Notes:
#  1.
#
#  Completed: 2020-09-
#
# AS 2020-09-30
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
library(iformr)
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

# Function to connect to postgres on local
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

# Function to connect to postgres on prod
pg_con_prod = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = pg_host("pg_host_prod"),
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_prod"),
    port = port)
  con
}

#=========================================================================
# Import all stream data from sg for WRIAs 22 and 23
#=========================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "st.stream_id, st.gid, st.geom as geometry ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where wria_code in ('22', '23') ",
           "order by waterbody_name")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
streams_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Pull out cases where LLID is duplicated....None anymore, only streams with missing LLIDs
dup_stream_id = streams_st %>%
  st_drop_geometry() %>%
  group_by(llid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(llid) %>%
  distinct()

# Pull out cases with missing LLIDs
no_llid = streams_st %>%
  st_drop_geometry() %>%
  filter(is.na(llid)) %>%
  distinct()

# Identify any streams where LLID needs to be updated
update_llid = no_llid %>%
  filter(!is.na(stream_id))

# Arrange streams
streams_st = streams_st %>%
  arrange(waterbody_name)

#=========================================================================
# Update missing LLID to all DBs....inspect in QGIS to find LLID.
#=========================================================================

# # Define query
# qry = glue("update waterbody_lut ",
#            "set latitude_longitude_id = '1234654469839' ",
#            "where waterbody_id = '17557462-e4d9-4497-ba26-b54ad9831413'")
#
# # Update local
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# dbDisconnect(db_con)
#
# # Define query
# qry = glue("update spawning_ground.waterbody_lut ",
#            "set latitude_longitude_id = '1234654469839' ",
#            "where waterbody_id = '17557462-e4d9-4497-ba26-b54ad9831413'")
#
# # Update FISH prod
# db_con = pg_con_prod(dbname = "FISH")
# DBI::dbExecute(db_con, qry)
# dbDisconnect(db_con)

#=========================================================================
# Get stream data from Leslie's latest
#=========================================================================

# # Get Leslies latest gdb...as of 2020-03-12
# cat_llid_st = read_sf("data/wdfwStreamCatalogGeometry.gdb", layer = "StreamCatalogLine", crs = 2927)
# st_crs(cat_llid_st)$epsg
# # Write chehalis subset to local
# cat_llid_chehalis = cat_llid_st %>%
#   st_zm() %>%
#   st_join(wria_st) %>%
#   filter(wria_code %in% c("22", "23"))
# # Pull out what I need
# cat_llid_chehalis = cat_llid_chehalis %>%
#   rename(geometry = Shape) %>%
#   select(cat_code = StreamCatalogID, cat_name = StreamCatalogName, llid = LLID,
#          llid_name = LLIDName, comment = Comment, orientation = OrientID)
# #unique(llid_chehalis$wria_code)
# write_sf(cat_llid_chehalis, "data/cat_llid_chehalis_2020-03-12.gpkg")
# Get Lestie's latest llid data
cat_llid_chehalis = read_sf("data/cat_llid_chehalis_2020-03-12.gpkg",
                            layer = "cat_llid_chehalis_2020-03-12", crs = 2927)

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

#===========================================================================================
# Add streams one at a time....First add orphan streams. No downstream portion or stream at pt
#===========================================================================================

#======== Little Hoquiam River =============================

# Don't have any cleary assigned surveys there, but it should be in there for reference
# and the possibility surveys were not properly assigned.

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1238927469951'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe ==================

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Hoquiam", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Hoquiam", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here =====================

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Little Hoquiam River",
            waterbody_display_name = "Little Hoquiam R (22.0155)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0155",
            tributary_to_name = "Hoquiam River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# # Correct mistake on display name
# wb = wb %>%
#   mutate(waterbody_display_name = "Little Hoquiam R (22.0155)")
#
# # Correct in DB
# qry = glue("update waterbody_lut ",
#            "set waterbody_display_name = 'Little Hoquiam R (22.0155)' ",
#            "where waterbody_id = '1d1cf340-ff02-4752-8789-834bf1d4e5e9'")
# # Update
# db_con = pg_con_local(dbname = "spawning_ground")
# DBI::dbExecute(db_con, qry)
# dbDisconnect(db_con)

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== North Fork Little Hoquiam =============================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1239205469898'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe ==================

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Hoquiam", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Hoquiam", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "23.0157", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here =====================

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "North Fork Little Hoquiam River",
            waterbody_display_name = "NF Little Hoquiam R (22.0157)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0157",
            tributary_to_name = "Little Hoquiam River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Lytle Creek ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1238527470522'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Lytle", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Lytle", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0144", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Lytle Creek",
            waterbody_display_name = "Lytle Cr (22.0144)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0144",
            tributary_to_name = "EF Hoquiam River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Barnum Creek ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1239303470593'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Barnum", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Barnum", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0178", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Barnum Creek",
            waterbody_display_name = "Barnum Cr (22.0178)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0178",
            tributary_to_name = "WF Hoquiam River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Davis Creek ============================================================================

# Geometry is already entered correctly. Just need to update LLID, cat code, and name
# Old data was entered for same stream. It just extends up further now. Single LLID
# now takes in both 22.0180 and 22.0184.
# Old info:
# Name: Unnamed Tributary
# LLID: 1239314470676
# catcode: 22.0184
# trib-to: blank

# Update local =================================

# Update stream info
qry = glue("update waterbody_lut set ",
           "waterbody_name = 'Davis Creek', ",
           "waterbody_display_name = 'Davis Cr (22.0180)', ",
           "latitude_longitude_id = '1239314470676', ",
           "stream_catalog_code = '22.0180', ",
           "tributary_to_name = 'Hoquiam River' ",
           "where waterbody_id = '0bebee05-d34e-49ac-8fb8-0f2e632ae106'")

# Run
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Update prod =================================

# Update stream info
qry = glue("update spawning_ground.waterbody_lut set ",
           "waterbody_name = 'Davis Creek', ",
           "waterbody_display_name = 'Davis Cr (22.0180)', ",
           "latitude_longitude_id = '1239314470676', ",
           "stream_catalog_code = '22.0180', ",
           "tributary_to_name = 'Hoquiam River' ",
           "where waterbody_id = '0bebee05-d34e-49ac-8fb8-0f2e632ae106'")

# Run
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#======== Trib 0140 ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1238687470218'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# # Get info from cat_llid
# st_cat = cat_llid_chehalis %>%
#   filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Barnum", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Tributary 0140", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0140", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Tributary 0140",
            waterbody_display_name = "Tributary (22.0140)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0140",
            tributary_to_name = "EF Hoquiam River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Trib 0141 ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1238703470237'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# # Get info from cat_llid
# st_cat = cat_llid_chehalis %>%
#   filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Barnum", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Tributary 0141", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0141", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Tributary 0141",
            waterbody_display_name = "Tributary (22.0141)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0141",
            tributary_to_name = "EF Hoquiam River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Trib 0142 ============================================================================

# Cat code indicates a different LLID that what Amy provided. I'm assuming the cat code indicates
# the requested stream. This is a larger stream. The LLID Amy provided is right below, LB. It is
# does not have an assigned cat code.

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1238750470336'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# # Get info from cat_llid
# st_cat = cat_llid_chehalis %>%
#   filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Barnum", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Tributary 0142", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0142", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Tributary 0142",
            waterbody_display_name = "Tributary (22.0142)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0142",
            tributary_to_name = "EF Hoquiam River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Trib 0143 ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1238647470465'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# # Get info from cat_llid
# st_cat = cat_llid_chehalis %>%
#   filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Barnum", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Tributary 0143", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0143", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Tributary 0143",
            waterbody_display_name = "Tributary (22.0143)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0143",
            tributary_to_name = "EF Hoquiam River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Tributary 22.0201 ============================================================================

# Geometry is already entered correctly. Just need to update name
# Old info:
# Name: Unnamed Tributary
# LLID: 1237411470888
# catcode: 22.0201
# trib-to: blank

# Update local =================================

# Update stream info
qry = glue("update waterbody_lut set ",
           "waterbody_name = 'Tributary 0201', ",
           "waterbody_display_name = 'Tributary (22.0201)', ",
           "latitude_longitude_id = '1237411470888', ",
           "stream_catalog_code = '22.0201', ",
           "tributary_to_name = 'East Fork Wishkah R' ",
           "where waterbody_id = 'cd3d673e-3652-4208-9357-5d917aa965f1'")

# Run
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Update prod =================================

# Update stream info
qry = glue("update spawning_ground.waterbody_lut set ",
           "waterbody_name = 'Tributary 0201', ",
           "waterbody_display_name = 'Tributary (22.0201)', ",
           "latitude_longitude_id = '1237411470888', ",
           "stream_catalog_code = '22.0201', ",
           "tributary_to_name = 'East Fork Wishkah R' ",
           "where waterbody_id = 'cd3d673e-3652-4208-9357-5d917aa965f1'")

# Run
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#======== Tributary 22.0202 ============================================================================

# Geometry entered incorrectly. Look for RMs on 22.0202 and 22.0203
qry = glue("select loc.location_id, wb.waterbody_name, wb.stream_catalog_code as cat_code, ",
           "wb.latitude_longitude_id as llid, loc.river_mile_measure ",
           "from location as loc ",
           "inner join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "where wb.waterbody_id = '59737d0c-0568-4fa5-b8b0-5d66e1137a69'")
# Run
db_con = pg_con_local(dbname = "spawning_ground")
old_points = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)

# Pull out ids for RMs
rm_ids = old_points %>%
  filter(!is.na(river_mile_measure)) %>%
  pull(location_id)

# Create string
rm_ids = paste0(paste0("'", rm_ids, "'"), collapse = ", ")

# Get all surveys for these points
qry = glue("select survey_datetime, observer_last_name, data_submitter_last_name ",
           "from survey ",
           "where upper_end_point_id in ({rm_ids}) ",
           "or lower_end_point_id in ({rm_ids})")

# Run
db_con = pg_con_local(dbname = "spawning_ground")
old_surveys = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)

# Result....only two old surveys
# All were for 22.0202 which is the lower section. None were more than RM = 0.5.
# So safe to update stream to rerouted LLID.

# Update local =================================

# First update waterbody_lut as needed
qry = glue("update waterbody_lut set ",
           "waterbody_name = 'Tributary 0204', ",
           "waterbody_display_name = 'Tributary (22.0202)', ",
           "latitude_longitude_id = '1237347470942', ",
           "stream_catalog_code = '22.0202', ",
           "tributary_to_name = 'East Fork Wishkah R' ",
           "where waterbody_id = '59737d0c-0568-4fa5-b8b0-5d66e1137a69'")
# Run
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# First update waterbody_lut as needed
qry = glue("update spawning_ground.waterbody_lut set ",
           "waterbody_name = 'Tributary 0202', ",
           "waterbody_display_name = 'Tributary (22.0202)', ",
           "latitude_longitude_id = '1237347470942', ",
           "stream_catalog_code = '22.0202', ",
           "tributary_to_name = 'East Fork Wishkah R' ",
           "where waterbody_id = '59737d0c-0568-4fa5-b8b0-5d66e1137a69'")
# Run
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Then get rid of existing geometry....wrong LLID
qry = glue("delete from stream ",
           "where stream_id = '10bc362e-9023-427f-9853-6f66ccd062d0'")
# Run
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Then get rid of existing geometry....wrong LLID
qry = glue("delete from spawning_ground.stream ",
           "where stream_id = '10bc362e-9023-427f-9853-6f66ccd062d0'")
# Run
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Then add geometry ===========================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1237347470942'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = "59737d0c-0568-4fa5-b8b0-5d66e1137a69"

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Tributary 22.0204 ============================================================================

# Geometry entered incorrectly. Look for RMs on 22.0204 and 22.0205
qry = glue("select loc.location_id, wb.waterbody_name, wb.stream_catalog_code as cat_code, ",
           "wb.latitude_longitude_id as llid, loc.river_mile_measure ",
           "from location as loc ",
           "inner join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "where wb.waterbody_id = '59bfc1fb-8596-405c-92a2-cdf23e7fef9e'")
# Run
db_con = pg_con_local(dbname = "spawning_ground")
old_points = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)

# Pull out ids for RMs
rm_ids = old_points %>%
  filter(!is.na(river_mile_measure)) %>%
  pull(location_id)

# Create string
rm_ids = paste0(paste0("'", rm_ids, "'"), collapse = ", ")

# Get all surveys for these points
qry = glue("select survey_datetime, observer_last_name, data_submitter_last_name ",
           "from survey ",
           "where upper_end_point_id in ({rm_ids}) ",
           "or lower_end_point_id in ({rm_ids})")

# Run
db_con = pg_con_local(dbname = "spawning_ground")
old_surveys = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)

# Result....only 123 old surveys...none after 1998.
# All were for 22.0204 which is the lower section. None were more than RM = 0.4.
# So safe to update stream to rerouted LLID.

# Update local =================================    STOPPED HERE !!!!!!!!!!!!!!!

# STILL NEED TO RUN THIS NEXT SECTION !!!!!!!!!!!!!!!!!!!!!!!!!

# First update waterbody_lut as needed
qry = glue("update waterbody_lut set ",
           "waterbody_name = 'Tributary 0204', ",
           "waterbody_display_name = 'Tributary (22.0204)', ",
           "latitude_longitude_id = '1237218471356', ",
           "stream_catalog_code = '22.0204', ",
           "tributary_to_name = 'East Fork Wishkah R' ",
           "where waterbody_id = '59bfc1fb-8596-405c-92a2-cdf23e7fef9e'")
# Run
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# First update waterbody_lut as needed
qry = glue("update spawning_ground.waterbody_lut set ",
           "waterbody_name = 'Tributary 0204', ",
           "waterbody_display_name = 'Tributary (22.0204)', ",
           "latitude_longitude_id = '1237218471356', ",
           "stream_catalog_code = '22.0204', ",
           "tributary_to_name = 'East Fork Wishkah R' ",
           "where waterbody_id = '59bfc1fb-8596-405c-92a2-cdf23e7fef9e'")
# Run
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Then get rid of existing geometry....wrong LLID
qry = glue("delete from stream ",
           "where stream_id = '6f044281-c7c9-4145-877f-ee30a4f049e5'")
# Run
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Then get rid of existing geometry....wrong LLID
qry = glue("delete from spawning_ground.stream ",
           "where stream_id = '6f044281-c7c9-4145-877f-ee30a4f049e5'")
# Run
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Then add geometry ===========================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1237218471356'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = "59bfc1fb-8596-405c-92a2-cdf23e7fef9e"

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Ramey Creek ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1237666471855'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Ramey", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Ramey", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0213", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# No geometry entered for existing waterbody

# First update waterbody_lut as needed
qry = glue("update waterbody_lut set ",
           "waterbody_name = 'Ramey Creek', ",
           "waterbody_display_name = 'Ramey Cr (22.0213)', ",
           "latitude_longitude_id = '1237666471855', ",
           "stream_catalog_code = '22.0213', ",
           "tributary_to_name = 'West Fork Wishkah R' ",
           "where waterbody_id = 'e95b3bad-c859-428b-8b95-66eb414107de'")
# Run
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# First update waterbody_lut as needed
qry = glue("update spawning_ground.waterbody_lut set ",
           "waterbody_name = 'Ramey Creek', ",
           "waterbody_display_name = 'Ramey Cr (22.0213)', ",
           "latitude_longitude_id = '1237666471855', ",
           "stream_catalog_code = '22.0213', ",
           "tributary_to_name = 'West Fork Wishkah R' ",
           "where waterbody_id = 'e95b3bad-c859-428b-8b95-66eb414107de'")
# Run
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = "e95b3bad-c859-428b-8b95-66eb414107de"

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== West Fork Andrews Creek ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1240221468233'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "West Fork Andrews", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "West Fork Andrews", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.1365", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "West Fork Andrews Creek",
            waterbody_display_name = "WF Andrews Cr (22.1365)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.1365",
            tributary_to_name = "Andrews Creek",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Elliot Slough ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1237808469766'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Elliot", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Elliot", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0238", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Elliot Slough",
            waterbody_display_name = "Elliot Slough (22.0238)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0238",
            tributary_to_name = "Chehalis River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Mox Chuck Slough ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1237399469574'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Mox", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Mox", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0251", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Mox Chuck Slough",
            waterbody_display_name = "Mox Chuck Slough (22.0251)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0251",
            tributary_to_name = "Chehalis River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Blue Slough ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1237218469493'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Blue", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Blue", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0254", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Blue Slough",
            waterbody_display_name = "Blue Slough (22.0254)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0254",
            tributary_to_name = "Chehalis River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Preachers Slough ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1237165469494'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Preachers", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Preachers", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0255", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# Create new entry in waterbody_lut
wb = tibble(waterbody_id = remisc::get_uuid(1L),
            waterbody_name = "Preachers Slough",
            waterbody_display_name = "Preachers Slough (22.0255)",
            latitude_longitude_id = ll_id,
            stream_catalog_code = "22.0255",
            tributary_to_name = "Chehalis River",
            obsolete_flag = 0L,
            obsolete_datetime = as.POSIXct(NA))

# Write to local
db_con = pg_con_local(dbname = "spawning_ground")
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to prod
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_lut")
DBI::dbWriteTable(db_con, tbl, wb, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = wb$waterbody_id

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

#======== Higgins Slough ============================================================================

# Set stream llid....from QGIS...Arleta's latest
ll_id = '1236814469582'

# Get stream geometry from Arleta's layer
st_llid = llid_chehalis %>%
  filter(llid == ll_id)

# Check crs
st_crs(st_llid)$epsg

#===== Verify stream is not already in SG based on stream_name or cat_coe =========

# Get info from cat_llid
st_cat = cat_llid_chehalis %>%
  filter(llid == ll_id | stringi::stri_detect_fixed(cat_name, "Higgins", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id = streams_st %>%
  filter(llid == ll_id)

# Check if the waterbody_id already exists in stream table
wb_id_2 = streams_st %>%
  filter(stringi::stri_detect_fixed(waterbody_display_name, "Higgins", max_count = 1))

# Check if the waterbody_id already exists in stream table
wb_id_3 = streams_st %>%
  filter(stringi::stri_detect_fixed(cat_code, "22.0257", max_count = 1))

#============ Manually inspect in QGIS, then enter new stream data here ===========

# No geometry entered for existing waterbody

# First update waterbody_lut as needed
qry = glue("update waterbody_lut set ",
           "waterbody_name = 'Higgins Slough (RB)', ",
           "waterbody_display_name = 'Higgins Slough (22.0257)', ",
           "latitude_longitude_id = '1236814469582', ",
           "stream_catalog_code = '22.0257', ",
           "tributary_to_name = 'Chehalis River' ",
           "where waterbody_id = '68abaeeb-846f-4c7a-93cd-7f5c51487377'")
# Run
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# First update waterbody_lut as needed
qry = glue("update spawning_ground.waterbody_lut set ",
           "waterbody_name = 'Higgins Slough (RB)', ",
           "waterbody_display_name = 'Higgins Slough (22.0257)', ",
           "latitude_longitude_id = '1236814469582', ",
           "stream_catalog_code = '22.0257', ",
           "tributary_to_name = 'Chehalis River' ",
           "where waterbody_id = '68abaeeb-846f-4c7a-93cd-7f5c51487377'")
# Run
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Write stream to local =======================

# Get last gid local
qry = glue("select max(gid) as gid from stream")
# Run query
db_con = pg_con_local(dbname = "spawning_ground")
max_gid_local = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_local = max_gid_local$gid + 1L
# gid = 3666L

# Pull out wb_id
new_wb_id = "68abaeeb-846f-4c7a-93cd-7f5c51487377"

# Create stream
stream_st = st_llid %>%
  mutate(stream_id = remisc::get_uuid(1L)) %>%
  mutate(waterbody_id = new_wb_id) %>%
  mutate(gid = new_gid_local) %>%
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

# Use select into query to get data into stream
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

# Write stream to prod =======================

# Get last gid prod
qry = glue("select max(gid) as gid from spawning_ground.stream")
# Run query
db_con = pg_con_prod(dbname = "FISH")
max_gid_prod = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Generate new gid
new_gid_prod = max_gid_prod$gid + 1L
# gid = 3666L

# Verify gids match
if (!new_gid_local == new_gid_prod) {
  cat("\nWARNING: Adjust gid below before writing!!!\n\n")
} else {
  cat("\nGIDs match. Ok to proceed.\n\n")
}

# Write prod temp table
db_con = pg_con_prod(dbname = "FISH")
tbl = Id(schema = "spawning_ground", table = "stream_temp")
st_write(obj = stream_st, dsn = db_con, layer = tbl)
dbDisconnect(db_con)

# Use select into query to get data into stream
qry = glue::glue("INSERT INTO spawning_ground.stream ",
                 "SELECT CAST(stream_id AS UUID), CAST(waterbody_id AS UUID), ",
                 "gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.stream_temp")

# Insert select to DB
db_con = dbConnect(odbc::odbc(), dsn = "prod_fish_spawn", timezone = "UTC")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.stream_temp")
DBI::dbDisconnect(db_con)

# STOPPED HERE...START ON Peels Slough next.......................

