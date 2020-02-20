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


#========== rpostgres =======================

# Get values from source
db_con = dbConnect(RPostgres::Postgres(), dbname = "shellfish", host = "localhost",
                   port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
rpgs_tide = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly convert timezones
rpgs_tide = rpgs_tide %>%
  mutate(tide_date = with_tz(tide_date, tzone = "America/Los_Angeles")) %>%
  mutate(char_date = format(tide_date))



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


#===============================================================================
# Playing with hex and binary
#===============================================================================

# Create point as wkt
(stpt = st_point(c(-122.1234,47.3487)))

# See wkt printed
st_as_text(stpt)

# Convert to binary
(st_bin = st_as_binary(stpt))

# Convert to hex
(st_hex = rawToHex(st_bin))

# Convert back to binary
(st_bin_two = wkb::hex2raw(st_hex))

# Convert back to sfc
(x = st_as_sfc(st_bin_two))

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

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, 'wria_lut')
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  filter(wria_code %in% c("22", "23")) %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime)) %>%
  select(-gid)

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'wria_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

# Test reading in as binary ========================

# Query to get wria data
qry = glue("select wria_code, wria_description as wria_name, geom as geometry ",
           "from wria_lut")

# Run the query
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
wria_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Check size of object
object.size(wria_st)

# Create wa_beaches with tide corrections and stations
wria_polys = wria_st %>%
  st_transform(4326) %>%
  ms_simplify() %>%
  mutate(wria_name = paste0(wria_code, " ", wria_name)) %>%
  select(wria_name, geometry)

# Check size of object
object.size(wria_polys)



#=================================================================================================
#=================================================================================================

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, 'waterbody_lut')
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = as.character(obsolete_datetime))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
dbWriteTable(db_con, 'waterbody_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))



