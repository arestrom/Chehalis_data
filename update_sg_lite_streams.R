#===============================================================================
# Reload streams to sg_lite as Arleta's and Leslie's layers are updated
#
#
# Notes
#
# ToDo:
#  1.
#
# AS 2020-03-12
#===============================================================================

# Clear workspace
rm(list=ls(all.names=TRUE))

# Load libraries
library(dplyr)
library(DBI)
library(RPostgres)
library(RSQLite)
library(tibble)
library(lubridate)
library(sf)
library(glue)
library(wkb)
library(rmapshaper)

# Keep connections pane from opening
options("connectionObserver" = NULL)

#=======================================================================================
# Define some needed functions. Put in package if useful later ----
#=======================================================================================

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

#=================================================================================================
# waterbody and stream
#=================================================================================================

# Get waterbody and stream data relevant to WRIAs 22 and 23...only pulls streams with geometry
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id, wb.stream_catalog_code, wb.tributary_to_name, ",
           "wb.obsolete_flag, wb.obsolete_datetime, st.stream_id, st.geom as geometry, ",
           "st.created_datetime, st.created_by, st.modified_datetime, st.modified_by ",
           "from waterbody_lut as wb ",
           "inner join stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "where wr.wria_code in ('22', '23') ",
           "order by wb.waterbody_name")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
wb = dat %>%
  select(waterbody_id, waterbody_name, waterbody_display_name,
         latitude_longitude_id, stream_catalog_code, tributary_to_name,
         obsolete_flag, obsolete_datetime) %>%
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC"))) %>%
  distinct()

# Delete all existing from stream
qry = glue("delete from stream where stream_id is not null")
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Pull out stream data
st = dat %>%
  select(stream_id, waterbody_id, geom = geometry, created_datetime,
         created_by, modified_datetime, modified_by) %>%
  filter(!is.na(stream_id)) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'stream', st, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat", "wb", "st"))

# # Test reading in as binary....WORKS PERFECT !!!!!  ========================
#
# # Query to get stream data
# qry = glue("select wb.waterbody_name as stream_name, st.geom as geometry ",
#            "from waterbody_lut as wb ",
#            "inner join stream as st on wb.waterbody_id = st.waterbody_id ",
#            "where wb.waterbody_name = 'Chehalis River'")
#
# # Run the query
# db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
# ch_st = st_read(db_con, query = qry, crs = 2927)
# dbDisconnect(db_con)
#
# # Check size of object and crs
# object.size(ch_st)
# st_crs(ch_st)$epsg
#
# # Verify with plot
# plot(ch_st)
#
# # Clean up
# rm(list = c("ch_st"))









