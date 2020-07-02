#===============================================================================
# Load WRIA 22 and 23 data to spawning_ground_lite
#
#
# Notes on sqlite
#  1. To load:
#     https://gdal.org/index.html   Fabulous resource
#     https://www.datacamp.com/community/tutorials/sqlite-in-r   Automate building parametrized queries
#     https://keen-swartz-3146c4.netlify.com/intro.html    New book...looks great.
#     https://r-spatial.github.io/sf/articles/sf2.html
#     https://github.com/poissonconsulting/readwritesqlite
#  2. Joe Thorley uses BLOB for geometry...check read write, manipulate, and display using sf.
#     Write as binary object...avoids select into queries. Also stores dates as REAL not TEXT
#     Investigate benefits of real vs text? Faster? Any chances of rounding? Truncation.
#  3. See: https://www.sqlitetutorial.net/sqlite-foreign-key/
#     Need to use: PRAGMA foreign_keys = ON; to make sure constraints are used.
#     I tested to verify foreign key constraints are possible for current DB
#
# Notes on R procedure:
#  1. Updated to convert all times to UTC for loading to sqlite
#  2. Update lastest batch of streams and points for Lea than ran script: 2020-05-20 at 1:13 pm.
#  3. Uploaded full set of Lea's data from IFB on 2020-06-30 at 09:10 AM
#
# ToDo:
#  1.
#
# AS 2020-06-30
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
    port = "5432")
  con
}

#======================================================================================================
# Copy LUTs
#======================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "adipose_clip_status_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'area_surveyed_lut', dat, row.names = FALSE, append = TRUE)
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'origin_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "passage_feature_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'passage_feature_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
#=================================================================================================

# Read
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, "passage_measurement_type_lut")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'passage_measurement_type_lut', dat, row.names = FALSE, append = TRUE)
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC")))

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'weather_type_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Location data
#=================================================================================================

#======= wria_lut ... want simplified version ======================

# Get values from source...for this iteration, want to simplify WRIA
pg_con = pg_con_local(dbname = "spawning_ground")
dat = st_read(pg_con, query = "select * from wria_lut where wria_code in ('22', '23')")
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC"))) %>%
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

# Write temp table
pg_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = wria_polys, dsn = pg_con, layer = "wria_temp")
dbDisconnect(pg_con)

# Get values from source.
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbReadTable(pg_con, 'wria_temp')
dbDisconnect(pg_con)

# Write to sink
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'wria_lut', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Dump the temp table
qry = glue("drop table wria_temp")
pg_con = pg_con_local(dbname = "spawning_ground")
dbExecute(pg_con, qry)
dbDisconnect(pg_con)

# # Test reading in as binary....WORKS PERFECT !!!!!  ========================
#
# # Query to get wria data
# qry = glue("select wria_code, wria_description as wria_name, geom as geometry ",
#            "from wria_lut")
#
# # Run the query
# db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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

# Clean up
rm(list = c("dat", "wria_polys"))

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

# Write to sink: 584
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'waterbody_lut', wb, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Pull out stream data
st = dat %>%
  select(stream_id, waterbody_id, geom = geometry, created_datetime,
         created_by, modified_datetime, modified_by) %>%
  filter(!is.na(stream_id)) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC")))

# Write to sink: 584
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
# db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 17646
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'location', loc, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Pull out location_coordinates data
lc = dat %>%
  select(location_coordinates_id, location_id, horizontal_accuracy,
         comment_text, geom, created_datetime = lc_create_date,
         created_by = lc_create_by, modified_datetime = lc_modify_date,
         modified_by = lc_modify_by) %>%
  filter(!is.na(location_coordinates_id)) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 13347
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'location_coordinates', lc, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat", "loc", "lc"))

#=================================================================================================
# Media location
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct md.media_location_id, md.location_id, ",
           "md.media_type_id, md.media_url, md.comment_text, ",
           "md.created_datetime, md.created_by, md.modified_datetime, ",
           "md.modified_by ",
           "from media_location as md ",
           "left join location as loc on md.location_id = loc.location_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where wr.wria_code in ('22', '23')")

# Get values from source
pg_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Convert datetime to character
dat = dat %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 178
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'media_location', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

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
  mutate(obsolete_datetime = format(with_tz(obsolete_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 1421
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(survey_datetime = format(with_tz(survey_datetime, tzone = "UTC"))) %>%
  mutate(survey_start_datetime = format(with_tz(survey_start_datetime, tzone = "UTC"))) %>%
  mutate(survey_end_datetime = format(with_tz(survey_end_datetime, tzone = "UTC"))) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 57055
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 57054
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 22716
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 2040
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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

# Write to sink: 0
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(inactive_datetime = format(with_tz(inactive_datetime, tzone = "UTC"))) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 0
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'mobile_device', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Fish passage feature
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct fb.fish_passage_feature_id, fb.survey_id, fb.feature_location_id, ",
           "fb.passage_feature_type_id, fb.feature_observed_datetime, fb.feature_height_meter, ",
           "fb.feature_height_type_id, fb.plunge_pool_depth_meter, fb.plunge_pool_depth_type_id, ",
           "fb.comment_text, fb.created_datetime, fb.created_by, fb.modified_datetime, fb.modified_by ",
           "from fish_passage_feature as fb ",
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
  mutate(feature_observed_datetime = format(with_tz(feature_observed_datetime, tzone = "UTC"))) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 76
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'fish_passage_feature', dat, row.names = FALSE, append = TRUE)
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
  mutate(start_water_temperature_datetime = format(with_tz(start_water_temperature_datetime, tzone = "UTC"))) %>%
  mutate(end_water_temperature_datetime = format(with_tz(end_water_temperature_datetime, tzone = "UTC"))) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 16653
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(observation_datetime = format(with_tz(observation_datetime, tzone = "UTC"))) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 238
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(fish_start_datetime = format(with_tz(fish_start_datetime, tzone = "UTC"))) %>%
  mutate(fish_end_datetime = format(with_tz(fish_end_datetime, tzone = "UTC"))) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 0
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 0
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 103528
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(fish_encounter_datetime = format(with_tz(fish_encounter_datetime, tzone = "UTC"))) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 256491
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 3039
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'individual_fish', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Fish length measurement
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct fl.fish_length_measurement_id, fl.individual_fish_id, ",
           "fl.fish_length_measurement_type_id, fl.length_measurement_centimeter, ",
           "fl.created_datetime, fl.created_by, fl.modified_datetime, fl.modified_by ",
           "from fish_length_measurement as fl ",
           "inner join individual_fish as ind on fl.individual_fish_id = ind.individual_fish_id ",
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 2585
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'fish_length_measurement', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Fish capture event
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct fc.fish_capture_event_id, fc.fish_encounter_id, ",
           "fc.fish_capture_status_id, fc.disposition_type_id, fc.disposition_id, ",
           "fc.disposition_location_id, fc.created_datetime, fc.created_by, ",
           "fc.modified_datetime, fc.modified_by ",
           "from fish_capture_event as fc ",
           "inner join fish_encounter as fe on fc.fish_encounter_id = fe.fish_encounter_id ",
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink:4654
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'fish_capture_event', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Fish mark
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct fm.fish_mark_id, fm.fish_encounter_id, ",
           "fm.mark_type_id, fm.mark_status_id, fm.mark_orientation_id, ",
           "fm.mark_placement_id, fm.mark_size_id, fm.mark_color_id, ",
           "fm.mark_shape_id, fm.tag_number, fm.created_datetime, ",
           "fm.created_by, fm.modified_datetime, fm.modified_by ",
           "from fish_mark as fm ",
           "inner join fish_encounter as fe on fm.fish_encounter_id = fe.fish_encounter_id ",
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 6880
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'fish_mark', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Redd encounter
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct re.redd_encounter_id, re.survey_event_id, ",
           "re.redd_location_id, re.redd_status_id, re.redd_encounter_datetime, ",
           "re.redd_count, re.comment_text, re.created_datetime, ",
           "re.created_by, re.modified_datetime, re.modified_by ",
           "from redd_encounter as re ",
           "inner join survey_event as se on re.survey_event_id = se.survey_event_id ",
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
  mutate(redd_encounter_datetime = format(with_tz(redd_encounter_datetime, tzone = "UTC"))) %>%
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 132466
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'redd_encounter', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Individual redd
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct ir.individual_redd_id, ir.redd_encounter_id, ",
           "ir.redd_shape_id, ir.redd_dewatered_type_id, ir.percent_redd_visible, ",
           "ir.redd_length_measure_meter, ir.redd_width_measure_meter, ",
           "ir.redd_depth_measure_meter, ir.tailspill_height_measure_meter, ",
           "ir.percent_redd_superimposed, ir.percent_redd_degraded, ",
           "ir.superimposed_redd_name, ir.comment_text, ir.created_datetime, ",
           "ir.created_by, ir.modified_datetime, ir.modified_by ",
           "from individual_redd as ir ",
           "inner join redd_encounter as re on ir.redd_encounter_id = re.redd_encounter_id ",
           "inner join survey_event as se on re.survey_event_id = se.survey_event_id ",
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 7736
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'individual_redd', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Redd substrate
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct rs.redd_substrate_id, rs.redd_encounter_id, ",
           "rs.substrate_level_id, rs.substrate_type_id, rs.substrate_percent, ",
           "rs.created_datetime, rs.created_by, rs.modified_datetime, rs.modified_by ",
           "from redd_substrate as rs ",
           "inner join redd_encounter as re on rs.redd_encounter_id = re.redd_encounter_id ",
           "inner join survey_event as se on re.survey_event_id = se.survey_event_id ",
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 4
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'redd_substrate', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))

#=================================================================================================
# Redd confidence
#=================================================================================================

# Get location and location coordinates data relevant to WRIAs 22 and 23
qry = glue("select distinct rc.redd_confidence_id, rc.redd_encounter_id, ",
           "rc.redd_confidence_type_id, rc.redd_confidence_review_status_id, rc.comment_text, ",
           "rc.created_datetime, rc.created_by, rc.modified_datetime, rc.modified_by ",
           "from redd_confidence as rc ",
           "inner join redd_encounter as re on rc.redd_encounter_id = re.redd_encounter_id ",
           "inner join survey_event as se on re.survey_event_id = se.survey_event_id ",
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
  mutate(created_datetime = format(with_tz(created_datetime, tzone = "UTC"))) %>%
  mutate(modified_datetime = format(with_tz(modified_datetime, tzone = "UTC"))) %>%
  distinct()

# Write to sink: 0
db_con <- dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbWriteTable(db_con, 'redd_confidence', dat, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Clean up
rm(list = c("dat"))









