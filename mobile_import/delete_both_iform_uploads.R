#===============================================================================
# Delete both uploads from IFB on local so data can be reloaded.
# After a successful load and thorough testing, do the same for prod
#
# Notes:
#  1. Data were incorrectly uploaded to SG on both occasions using the
#     mobile_import scripts. Errors are in the fish and redd encounter
#     portions of the code. Rows of data were truncated to distinct.
#     This orphaned individual data in some cases, and left counts of
#     one in the encounter tables with multiple cases in the individual
#     tables in some cases.
#
#  Successfully deleted from local on 2021-06-22
#
# AS 2021-06-22
#===============================================================================

# Load libraries
library(DBI)
library(RPostgres)
library(dplyr)
library(tibble)
library(glue)
library(stringi)
library(tidyr)
library(lubridate)
library(openxlsx)
library(sf)

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
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

#===========================================================================================================
# Get IDs needed to delete uploaded data
#===========================================================================================================

# Get IDs
test_upload_ids = readRDS("data/test_upload_ids.rds")
test_upload_two_ids = readRDS("data/test_upload_two_ids.rds")
test_location_ids = readRDS("data/test_location_ids.rds")
test_location_two_ids = readRDS("data/test_location_two_ids.rds")

# Combine
test_upload_ids = rbind(test_upload_ids, test_upload_two_ids)
test_location_ids = rbind(test_location_ids, test_location_two_ids)

# Pull out survey IDs
s_id = unique(test_upload_ids$survey_id)
s_id = s_id[!is.na(s_id)]
s_ids = paste0(paste0("'", s_id, "'"), collapse = ", ")

# Pull out location IDs: 3160 rows
loc_ids = unique(test_location_ids$location_id)
loc_ids = paste0(paste0("'", loc_ids, "'"), collapse = ", ")

# Get survey_event_ids....Some event data may have been added to surveys
qry = glue("select survey_event_id ",
           "from spawning_ground.survey_event ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local("FISH")
se_id = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out survey_event IDs
se_id = unique(se_id$survey_event_id)
se_id = se_id[!is.na(se_id)]
se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")

# Get fish_encounter_ids
qry = glue("select fish_encounter_id ",
           "from spawning_ground.fish_encounter ",
           "where survey_event_id in ({se_ids})")
db_con = pg_con_local("FISH")
fish_id = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out fish IDs
fs_ids = unique(fish_id$fish_encounter_id)
fs_ids = paste0(paste0("'", fs_ids, "'"), collapse = ", ")

# Get individual_fish IDs
qry = glue("select individual_fish_id ",
           "from spawning_ground.individual_fish ",
           "where fish_encounter_id in ({fs_ids})")
db_con = pg_con_local("FISH")
indf_id = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out ind_fish IDs
ifs_ids = unique(indf_id$individual_fish_id)
ifs_ids = paste0(paste0("'", ifs_ids, "'"), collapse = ", ")

# Get redd_encounter_ids
qry = glue("select redd_encounter_id ",
           "from spawning_ground.redd_encounter ",
           "where survey_event_id in ({se_ids})")
db_con = pg_con_local("FISH")
redd_id = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out redd IDs
rd_ids = unique(redd_id$redd_encounter_id)
rd_ids = paste0(paste0("'", rd_ids, "'"), collapse = ", ")

# Get individual_redd_ids
qry = glue("select individual_redd_id ",
           "from spawning_ground.individual_redd ",
           "where redd_encounter_id in ({rd_ids})")
db_con = pg_con_local("FISH")
iredd_id = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out individual_redd IDs
ird_ids = unique(iredd_id$individual_redd_id)
ird_ids = paste0(paste0("'", ird_ids, "'"), collapse = ", ")

#==========================================================================================
# Delete IFB prior upload data
#==========================================================================================

#======== redd data ===============

# individual_redd: 12973 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.individual_redd ",
                       "where individual_redd_id in ({ird_ids})"))
dbDisconnect(db_con)

# redd_encounter: 20427 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.redd_encounter ",
                       "where redd_encounter_id in ({rd_ids})"))
dbDisconnect(db_con)

#======== fish data ===============

# fish_length_measurement: 3081 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.fish_length_measurement ",
                       "where individual_fish_id in ({ifs_ids})"))
dbDisconnect(db_con)

# individual_fish: 3642 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.individual_fish ",
                       "where fish_encounter_id in ({fs_ids})"))
dbDisconnect(db_con)

# fish_capture_event: 5515 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.fish_capture_event ",
                       "where fish_encounter_id in ({fs_ids})"))
dbDisconnect(db_con)

# fish_mark: 8270 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.fish_mark ",
                       "where fish_encounter_id in ({fs_ids})"))
dbDisconnect(db_con)

# fish_encounter: 12001 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.fish_encounter ",
                       "where fish_encounter_id in ({fs_ids})"))
dbDisconnect(db_con)

#======== Survey event ===============

# survey_event: 10574 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.survey_event ",
                       "where survey_event_id in ({se_ids})"))
dbDisconnect(db_con)

#======== Survey level tables ===============

# other_observation: 380 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.other_observation ",
                       "where survey_id in ({s_ids})"))
dbDisconnect(db_con)

# fish_passage_feature: 217 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.fish_passage_feature ",
                       "where survey_id in ({s_ids})"))
dbDisconnect(db_con)

# mobile_survey_form: 2044 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.mobile_survey_form ",
                       "where survey_id in ({s_ids})"))
dbDisconnect(db_con)

# survey_comment: 4280 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.survey_comment ",
                       "where survey_id in ({s_ids})"))
dbDisconnect(db_con)

# survey_intent: 37440 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.survey_intent ",
                       "where survey_id in ({s_ids})"))
dbDisconnect(db_con)

# waterbody_measurement: 3406 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.waterbody_measurement ",
                       "where survey_id in ({s_ids})"))
dbDisconnect(db_con)

# #================================================================================
# # Error in survey delete code still some survey_ids referenced below. Identify!
# # Maybe new rows were added to existing surveys manually
# #================================================================================
#
# # Query
# qry = glue("select survey_event_id ",
#            "from spawning_ground.survey_event ",
#            "where survey_id in ({s_ids})")
# # Run
# db_con = pg_con_local("FISH")
# new_event_ids = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out new SE IDs
# new_se_ids = unique(new_event_ids$survey_event_id)
# new_se_ids = paste0(paste0("'", new_se_ids, "'"), collapse = ", ")
#
# # See if they exist below survey_event table
# qry = glue("select fish_encounter_id ",
#            "from spawning_ground.fish_encounter ",
#            "where survey_event_id in ({new_se_ids})")
# # Run
# db_con = pg_con_local("FISH")
# new_fe_ids = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # END CHECK ==================================

# survey: 4280 rows
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.survey ",
                       "where survey_id in ({s_ids})"))
dbDisconnect(db_con)

#======== location ===============

# media_location: 397
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.media_location ",
                       "where location_id in ({loc_ids})"))
dbDisconnect(db_con)

# Location coordinates: 6121
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.location_coordinates ",
                       "where location_id in ({loc_ids})"))
dbDisconnect(db_con)

# Location: 9194
db_con = pg_con_local("FISH")
dbExecute(db_con, glue("delete from spawning_ground.location ",
                       "where location_id in ({loc_ids})"))
dbDisconnect(db_con)

# ALL deleted ====================













