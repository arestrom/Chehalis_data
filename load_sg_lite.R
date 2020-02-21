#===============================================================================
# Load WRIA 22 and 23 data, 2018-2020 to sg_lite
#
#
# Notes on sqlite, spatialite:
#  1. To load spatialite:
#     https://www.gaia-gis.it/fossil/libspatialite/wiki?name=mod_spatialite
#     https://github.com/sqlitebrowser/sqlitebrowser/wiki/SpatiaLite-on-Windows   Issues for Windows 10
#     https://gdal.org/index.html   Fabulous resource
#     https://www.datacamp.com/community/tutorials/sqlite-in-r   Automate building parametrized queries
#     https://keen-swartz-3146c4.netlify.com/intro.html    New book...looks great.
#  2. The mod_spatialite dlls must be placed in the root directory of the project
#  3. In DB Browser, can load spatialite module as below by specifying path to module
#     Use -- Load extension
#  4. May want to convert all geometries to binary using: x = st_as_binary(x) before
#     writing. May then be able to write directly to field without sf_write.
#     See: https://r-spatial.github.io/sf/articles/sf2.html
#  5. See also: https://github.com/poissonconsulting/readwritesqlite
#  6. Joe Thorley used BLOB for geometry...check how to read, write, manipulate, and display using sf.
#     I should probably always just write as binary object...avoids select into queries.
#     He also stored dates as REAL not TEXT...what are benefits of real vs text? Faster?
#     Any chances of rounding? Truncation.
#  7. See: https://www.sqlitetutorial.net/sqlite-foreign-key/
#     Need to use: PRAGMA foreign_keys = ON; to make sure constraints are used.
#     I tested to verify foreign key constraints are possible for current DB
#
#
# Notes on R procedure:
#  1.
#
#
# ToDo:
#  1.
#
# AS 2020-02-19
#===============================================================================

# Clear workspace
rm(list=ls(all=TRUE))

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

#===============================================================================
# Create database
#===============================================================================

# Create database connection
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')

# Load a table...you can drop manually in spatilite gui later and it will still open without error
# There just needs to be a table in the DB to ensure the magic number stuff works out correctly.
qry = glue("CREATE TABLE adipose_clip_status_lut ( ",
	         "adipose_clip_status_id text DEFAULT (CreateUUID()) PRIMARY KEY, ",
           "adipose_clip_status_code text NOT NULL, ",
           "adipose_clip_status_description text NOT NULL, ",
           "obsolete_flag text NOT NULL, ",
           "obsolete_datetime text DEFAULT (datetime('now')) ",
           ") WITHOUT ROWID;")

# Write new table
dbExecute(con, qry)

# Check
dbListTables(con)
dbListFields(con, name = 'adipose_clip_status_lut')

# Disconnect
dbDisconnect(con)

# # Load the spatialite extension....spatialite dlls must be in the root directory
# qry = "SELECT load_extension('mod_spatialite')"
# rs = dbGetQuery(con, qry)
#
# # Test....works !!
# dbGetQuery(con, "SELECT CreateUUID() as uuid")
#
# # Disconnect
# dbDisconnect(con)

# #===============================================================================
# # Playing with hex and binary
# #===============================================================================
#
# # Create point as wkt
# (stpt = st_point(c(-122.1234,47.3487)))
#
# # See wkt printed
# st_as_text(stpt)
#
# # Convert to binary
# (st_bin = st_as_binary(stpt))
#
# # Convert to hex
# (st_hex = rawToHex(st_bin))
#
# # Convert back to binary
# (st_bin_two = wkb::hex2raw(st_hex))
#
# # Convert back to sfc
# (x = st_as_sfc(st_bin_two))

#======================================================================================================
# Copy LUTs
#======================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "adipose_clip_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'adipose_clip_status_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "age_code_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'age_code_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "area_surveyed_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'area_surveyed_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "barrier_measurement_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'barrier_measurement_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "barrier_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'barrier_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "count_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'count_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "cwt_detection_method_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'cwt_detection_method_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "cwt_detection_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'cwt_detection_status_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "cwt_result_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'cwt_result_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "data_review_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'data_review_status_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "data_source_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'data_source_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "data_source_unit_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'data_source_unit_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "disposition_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'disposition_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "disposition_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'disposition_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "fish_abundance_condition_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_abundance_condition_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "fish_behavior_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_behavior_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "fish_capture_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_capture_status_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "fish_condition_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_condition_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Common table
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "fish_length_measurement_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_length_measurement_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "fish_presence_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_presence_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "fish_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_status_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "fish_trauma_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_trauma_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "gear_performance_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'gear_performance_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "gill_condition_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'gill_condition_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "incomplete_survey_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'incomplete_survey_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "location_orientation_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'location_orientation_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "location_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'location_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mark_color_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mark_color_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Common Table
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mark_orientation_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mark_orientation_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Common Table
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mark_placement_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mark_placement_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mark_shape_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mark_shape_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mark_size_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mark_size_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mark_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mark_status_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Common Table
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mark_type_category_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mark_type_category_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Common Table
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mark_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mark_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Common Table
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "maturity_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'maturity_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "media_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'media_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mobile_device_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mobile_device_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "mortality_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mortality_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "observation_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'observation_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "origin_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'origin_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "redd_confidence_review_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'redd_confidence_review_status_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "redd_confidence_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'redd_confidence_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "redd_dewatered_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'redd_dewatered_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "redd_shape_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'redd_shape_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "redd_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'redd_status_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "run_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'run_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Common table
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "sex_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'sex_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "spawn_condition_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'spawn_condition_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "species_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'species_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "stream_channel_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'stream_channel_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "stream_condition_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'stream_condition_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "stream_flow_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'stream_flow_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "substrate_level_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'substrate_level_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "substrate_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'substrate_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Common Table
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "survey_completion_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_completion_status_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "survey_count_condition_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_count_condition_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "survey_design_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_design_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "survey_direction_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_direction_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "survey_method_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_method_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "survey_timing_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_timing_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "visibility_condition_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'visibility_condition_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "visibility_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'visibility_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "water_clarity_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'water_clarity_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, 'weather_type_lut')
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'weather_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))


#=================================================================================================
# Location data
#=================================================================================================

# # Get values from source.
# pg_con = pg_con_local(dbname = "spawning_ground")
# dat = dbReadTable(pg_con, 'wria_lut')
# dbDisconnect(pg_con)

#======= wria_lut ... want simplified version ======================

# Get values from source...for this iteration, want to simplify WRIA
pg_con = pg_con_local(dbname = "spawning_ground")
dat = st_read(pg_con, query = "select * from wria_lut where wria_code in ('22', '23')")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime)) %>%
  select(-gid)

# Check size of object
object.size(dat)

# Simplify polygons for speed
wria_polys = dat %>%
  ms_simplify()

# Check size of object
object.size(wria_polys)
# plot(wria_polys)

# Verify crs
st_crs(wria_polys)$epsg

# Convert back to binary, then hex
dat = wria_polys %>%
  mutate(geom = st_as_binary(geom)) %>%
  mutate(geom = rawToHex(geom)) %>%
  mutate(geometry = geom) %>%
  st_drop_geometry() %>%
  select(wria_id, wria_code, wria_description, geom = geometry,
         obsolete_flag, obsolete_datetime)

# Check size of object
object.size(dat)

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'wria_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat", "wria_polys"))

# # Test reading in as binary....WORKS PERFECT !!!!!  ========================
#
# # Query to get wria data
# qry = glue("select wria_code, wria_description as wria_name, geom as geometry ",
#            "from wria_lut")
#
# # Run the query
# db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
# wria_st = st_read(db_con, query = qry, crs = 2927)
# dbDisconnect(db_con)
#
# # Check size of object and crs
# object.size(wria_st)
# st_crs(wria_st)$epsg
#
# # Simplify polygons for speed
# wria_st = wria_st %>%
#   mutate(wria_name = paste0(wria_code, " ", wria_name)) %>%
#   select(wria_name, geometry)
#
# # Verify with plot
# plot(wria_st)
#
# # Clean up
# rm(list = c("wria_st"))

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
  mutate(obsolete_datetime = as.character(obsolete_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Pull out stream data
st = dat %>%
  select(stream_id, waterbody_id, geom = geometry, created_datetime,
         created_by, modified_datetime, modified_by) %>%
  filter(!is.na(stream_id)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime))

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

#=================================================================================================
# location and location_coordinates
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct loc.location_id, loc.waterbody_id, loc.wria_id, ",
           "loc.location_type_id, loc.stream_channel_type_id, loc.location_orientation_type_id, ",
           "loc.river_mile_measure, loc.location_code, loc.location_name, ",
           "loc.location_description, loc.waloc_id, loc.created_datetime, ",
           "loc.created_by, loc.modified_datetime, loc.modified_by, ",
           "lc.location_coordinates_id, lc.horizontal_accuracy, lc.comment_text, ",
           "lc.geom, lc.created_datetime as lc_create_date, ",
           "lc.created_by as lc_create_by, lc.modified_datetime as lc_modify_date, ",
           "lc.modified_by as lc_modify_by ",
           "from location as loc ",
           "left join location_coordinates as lc on loc.location_id = lc.location_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where wr.wria_code in ('22', '23')")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
loc = dat %>%
  select(location_id, waterbody_id, wria_id, location_type_id,
         stream_channel_type_id, location_orientation_type_id,
         river_mile_measure, location_code, location_name,
         location_description, waloc_id, created_datetime,
         created_by, modified_datetime, modified_by) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'location', loc, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Pull out location_coordinates data
lc = dat %>%
  select(location_coordinates_id, location_id, horizontal_accuracy,
         comment_text, geom, created_datetime = lc_create_date,
         created_by = lc_create_by, modified_datetime = lc_modify_date,
         modified_by = lc_modify_by) %>%
  filter(!is.na(location_coordinates_id)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'location_coordinates', lc, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat", "loc", "lc"))

#=================================================================================================
# Will need media location later....none yet
#=================================================================================================

#=================================================================================================
# Stock
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct st.stock_id, st.waterbody_id, st.species_id, ",
           "st.run_id, st.sasi_stock_number, st.stock_name, ",
           "st.status_code, st.esa_code, st.obsolete_flag, ",
           "st.obsolete_datetime ",
           "from stock_lut as st ",
           "left join location as loc on st.waterbody_id = loc.waterbody_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where wr.wria_code in ('22', '23')")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'stock_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Survey
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct s.survey_id, s.survey_datetime, s.data_source_id, ",
           "s.data_source_unit_id, s.survey_method_id, s.data_review_status_id, ",
           "s.upper_end_point_id, s.lower_end_point_id, s.survey_completion_status_id, ",
           "s.incomplete_survey_type_id, s.survey_start_datetime, s.survey_end_datetime, ",
           "s.observer_last_name, s.data_submitter_last_name, s.created_datetime, ",
           "s.created_by, s.modified_datetime, s.modified_by ",
           "from survey as s ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ",
           "order by s.survey_datetime")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(survey_datetime = as.character(survey_datetime)) %>%
  mutate(survey_start_datetime = as.character(survey_start_datetime)) %>%
  mutate(survey_end_datetime = as.character(survey_end_datetime)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Survey comment
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct sc.survey_comment_id, sc.survey_id, sc.area_surveyed_id, ",
           "sc.fish_abundance_condition_id, sc.stream_condition_id, sc.stream_flow_type_id, ",
           "sc.survey_count_condition_id, sc.survey_direction_id, sc.survey_timing_id, ",
           "sc.visibility_condition_id, sc.visibility_type_id, sc.weather_type_id, ",
           "sc.comment_text, sc.created_datetime, sc.created_by, sc.modified_datetime, ",
           "sc.modified_by ",
           "from survey_comment as sc ",
           "inner join survey as s on sc.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_comment', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Survey intent
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct si.survey_intent_id, si.survey_id, si.species_id, ",
           "si.count_type_id, si.created_datetime, si.created_by, si.modified_datetime, ",
           "si.modified_by ",
           "from survey_intent as si ",
           "inner join survey as s on si.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_intent', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Mobile survey form
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct m.mobile_survey_form_id, m.survey_id, m.parent_form_survey_id, ",
           "m.parent_form_survey_guid, m.parent_form_name, m.parent_form_id, ",
           "m.created_datetime, m.created_by, m.modified_datetime, m.modified_by ",
           "from mobile_survey_form as m ",
           "inner join survey as s on m.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mobile_survey_form', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Survey mobile device
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct m.survey_mobile_device_id, m.survey_id, m.mobile_device_id ",
           "from survey_mobile_device as m ",
           "inner join survey as s on m.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_mobile_device', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Mobile device
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct m.mobile_device_id, m.mobile_device_type_id, ",
           "m.mobile_equipment_identifier, m.mobile_device_name, m.mobile_device_description, ",
           "m.active_indicator, m.inactive_datetime, m.created_datetime, m.created_by, ",
           "m.modified_datetime, m.modified_by ",
           "from mobile_device as m ",
           "inner join survey_mobile_device as sm on m.mobile_device_id = sm.mobile_device_id ",
           "inner join survey as s on sm.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(inactive_datetime = as.character(inactive_datetime)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'mobile_device', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Fish barrier
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct fb.fish_barrier_id, fb.survey_id, fb.barrier_location_id, ",
           "fb.barrier_type_id, fb.barrier_observed_datetime, fb.barrier_height_meter, ",
           "fb.barrier_height_type_id, fb.plunge_pool_depth_meter, fb.plunge_pool_depth_type_id, ",
           "fb.comment_text, fb.created_datetime, fb.created_by, fb.modified_datetime, fb.modified_by ",
           "from fish_barrier as fb ",
           "inner join survey as s on fb.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(barrier_observed_datetime = as.character(barrier_observed_datetime)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_barrier', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Waterbody measurement
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct wm.waterbody_measurement_id, wm.survey_id, wm.water_clarity_type_id, ",
           "wm.water_clarity_meter, wm.stream_flow_measurement_cfs, wm.start_water_temperature_datetime, ",
           "wm.start_water_temperature_celsius, wm.end_water_temperature_datetime, ",
           "wm.end_water_temperature_celsius, wm.waterbody_ph, wm.created_datetime, ",
           "wm.created_by, wm.modified_datetime, wm.modified_by ",
           "from waterbody_measurement as wm ",
           "inner join survey as s on wm.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(start_water_temperature_datetime = as.character(start_water_temperature_datetime)) %>%
  mutate(end_water_temperature_datetime = as.character(end_water_temperature_datetime)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'waterbody_measurement', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Other observation
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct oo.other_observation_id, oo.survey_id, oo.observation_location_id, ",
           "oo.observation_type_id, oo.observation_datetime, oo.observation_count, ",
           "oo.comment_text, oo.created_datetime, oo.created_by, oo.modified_datetime, ",
           "oo.modified_by ",
           "from other_observation as oo ",
           "inner join survey as s on oo.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(observation_datetime = as.character(observation_datetime)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'other_observation', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Fish capture
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct fc.fish_capture_id, fc.survey_id, fc.gear_performance_type_id, ",
           "fc.fish_start_datetime, fc.fish_end_datetime, fc.created_datetime, ",
           "fc.created_by, fc.modified_datetime, fc.modified_by ",
           "from fish_capture as fc ",
           "inner join survey as s on fc.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(fish_start_datetime = as.character(fish_start_datetime)) %>%
  mutate(fish_end_datetime = as.character(fish_end_datetime)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_capture', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Fish species presence
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct fs.fish_species_presence_id, fs.survey_id, ",
           "fs.fish_presence_type_id, fs.comment_text, fs.created_datetime, ",
           "fs.created_by, fs.modified_datetime, fs.modified_by ",
           "from fish_species_presence as fs ",
           "inner join survey as s on fs.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23') ")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_species_presence', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Survey event
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct se.survey_event_id, se.survey_id, se.species_id, ",
           "se.survey_design_type_id, se.cwt_detection_method_id, se.run_id, ",
           "se.run_year, se.estimated_percent_fish_seen, se.comment_text, ",
           "se.created_datetime, se.created_by, se.modified_datetime, se.modified_by ",
           "from survey_event as se ",
           "inner join survey as s on se.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23')")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'survey_event', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Fish encounter
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct fe.fish_encounter_id, fe.survey_event_id, fe.fish_location_id, ",
           "fe.fish_status_id, fe.sex_id, fe.maturity_id, fe.origin_id, fe.cwt_detection_status_id, ",
           "fe.adipose_clip_status_id, fe.fish_behavior_type_id, fe.mortality_type_id, ",
           "fe.fish_encounter_datetime, fe.fish_count, fe.previously_counted_indicator, ",
           "fe.created_datetime, fe.created_by, fe.modified_datetime, fe.modified_by ",
           "from fish_encounter as fe ",
           "inner join survey_event as se on fe.survey_event_id = se.survey_event_id ",
           "inner join survey as s on se.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23')")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(fish_encounter_datetime = as.character(fish_encounter_datetime)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_encounter', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Individual fish
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct ind.individual_fish_id, ind.fish_encounter_id, ind.fish_condition_type_id, ",
           "ind.fish_trauma_type_id, ind.gill_condition_type_id, ind.spawn_condition_type_id, ",
           "ind.cwt_result_type_id, ind.age_code_id, ind.percent_eggs_retained, ",
           "ind.eggs_retained_gram, ind.fish_sample_number, ind.scale_sample_card_number, ",
           "ind.scale_sample_position_number, ind.cwt_snout_sample_number, ind.cwt_tag_code, ",
           "ind.genetic_sample_number, ind.otolith_sample_number, ind.comment_text, ",
           "ind.created_datetime, ind.created_by, ind.modified_datetime, ind.modified_by ",
           "from individual_fish as ind ",
           "inner join fish_encounter as fe on ind.fish_encounter_id = fe.fish_encounter_id ",
           "inner join survey_event as se on fe.survey_event_id = se.survey_event_id ",
           "inner join survey as s on se.survey_id = s.survey_id ",
           "left join location as uploc on s.upper_end_point_id = uploc.location_id ",
           "left join location as loloc on s.lower_end_point_id = loloc.location_id ",
           "left join wria_lut as upwr on uploc.wria_id = upwr.wria_id ",
           "left join wria_lut as lowr on loloc.wria_id = lowr.wria_id ",
           "where upwr.wria_code in ('22', '23') or lowr.wria_code in ('22', '23')")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(fish_encounter_datetime = as.character(fish_encounter_datetime)) %>%
  mutate(created_datetime = as.character(created_datetime)) %>%
  mutate(modified_datetime = as.character(modified_datetime)) %>%
  distinct()

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'fish_encounter', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))













