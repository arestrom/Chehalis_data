#===========================================================================
# Correct WRIA 22 and 23 reach_points from Lea. After adding and editing
# noticed that some points, especially on SF Newaukum were duplicated.
# Checking that only one point per stream is present
#
# Notes:
#  1.
#
#  Completed: 2020-07-27
#
# AS 2020-07-27
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
# Get any survey_event_ids that have no redd or fish counts attached
#============================================================================

# Define query to get needed data
qry = glue("select s.survey_id, s.survey_datetime, uloc.river_mile_measure as up_rm, ",
           "lloc.river_mile_measure as lo_rm, wb.waterbody_name as stream_name, ",
           "se.survey_event_id, sp.common_name, rn.run_short_description as run, ",
           "fe.fish_count, rd.redd_count, wr.wria_code ",
           "from survey as s ",
           "left join location as uloc on s.upper_end_point_id = uloc.location_id ",
           "left join location as lloc on s.lower_end_point_id = lloc.location_id ",
           "left join wria_lut as wr on uloc.wria_id = wr.wria_id ",
           "left join waterbody_lut as wb on uloc.waterbody_id = wb.waterbody_id ",
           "left join survey_event as se on s.survey_id = se.survey_id ",
           "left join run_lut as rn on se.run_id = rn.run_id ",
           "left join species_lut as sp on se.species_id = sp.species_id ",
           "left join fish_encounter as fe on se.survey_event_id = fe.survey_event_id ",
           "left join redd_encounter as rd on se.survey_event_id = rd.survey_event_id ",
           "where survey_datetime between '2019-07-31' and now() ",
           "and wr.wria_code in ('22', '23') ",
           "and fish_count is null and redd_count is null ",
           "and se.survey_event_id is not null")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
no_counts = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Run some checks
unique(no_counts$wria_code)
min(no_counts$survey_datetime)
max(no_counts$survey_datetime)
table(no_counts$common_name, useNA = "ifany")

# Create id string
se_ids = no_counts %>%
  select(survey_event_id) %>%
  distinct() %>%
  filter(!is.na(survey_event_id)) %>%
  pull(survey_event_id)

se_id = paste0(paste0("'", se_ids, "'"), collapse = ", ")

#============================================================================
# DELETE any survey_event_ids that have no redd or fish counts attached
#============================================================================

# Define delete query to dump survey_event entries that are not needed
qry = glue("delete from survey_event where survey_event_id in ({se_id})")

# Run query on spawning_ground
db_con = pg_con_local(dbname = "spawning_ground")
dbExecute(db_con, qry)
dbDisconnect(db_con)

# Run query on spawning_ground_lite
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
dbExecute(con, qry)
dbDisconnect(con)

# SUCCESS !!!!!!!!!!!!!!!!!!!!!!!

