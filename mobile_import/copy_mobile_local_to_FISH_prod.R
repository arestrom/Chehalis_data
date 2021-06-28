#===============================================================================
# Copy data from FISH on local to FISH on prod.
#
# Notes:
#  1. Data were incorrectly uploaded to SG on both occasions using the
#     mobile_import scripts. Errors are in the fish and redd encounter
#     portions of the code. Rows of data were truncated to distinct.
#     This orphaned individual data in some cases, and left counts of
#     one in the encounter tables with multiple cases in the individual
#     tables in some cases.
#
#  Successfully transferred all mobile data from FISH local to FISH prod
#  on 2012-06-28 at ~ 10:10 AM
#
# AS 2021-06-28
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

# Function to connect to postgres
pg_con_prod = function(dbname, port = '5432') {
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = pg_host("pg_host_prod"),
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_prod"),
    port = port)
  con
}

# #===========================================================================================================
# # Get IDs needed to delete previously uploaded data
# #===========================================================================================================
#
# # Get IDs
# test_upload_ids = readRDS("data/test_upload_ids.rds")
# test_upload_two_ids = readRDS("data/test_upload_two_ids.rds")
# test_location_ids = readRDS("data/test_location_ids.rds")
# test_location_two_ids = readRDS("data/test_location_two_ids.rds")
#
# # Combine
# test_upload_ids = rbind(test_upload_ids, test_upload_two_ids)
# test_location_ids = rbind(test_location_ids, test_location_two_ids)
#
# # Pull out survey IDs
# s_id = unique(test_upload_ids$survey_id)
# s_id = s_id[!is.na(s_id)]
# s_ids = paste0(paste0("'", s_id, "'"), collapse = ", ")
#
# # Pull out location IDs: 3160 rows
# loc_ids = unique(test_location_ids$location_id)
# loc_ids = paste0(paste0("'", loc_ids, "'"), collapse = ", ")
#
# # Get survey_event_ids....Some event data may have been added to surveys
# qry = glue("select survey_event_id ",
#            "from spawning_ground.survey_event ",
#            "where survey_id in ({s_ids})")
# db_con = pg_con_prod("FISH")
# se_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out survey_event IDs
# se_id = unique(se_id$survey_event_id)
# se_id = se_id[!is.na(se_id)]
# se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")
#
# # Get fish_encounter_ids
# qry = glue("select fish_encounter_id ",
#            "from spawning_ground.fish_encounter ",
#            "where survey_event_id in ({se_ids})")
# db_con = pg_con_prod("FISH")
# fish_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out fish IDs
# fs_ids = unique(fish_id$fish_encounter_id)
# fs_ids = paste0(paste0("'", fs_ids, "'"), collapse = ", ")
#
# # Get individual_fish IDs
# qry = glue("select individual_fish_id ",
#            "from spawning_ground.individual_fish ",
#            "where fish_encounter_id in ({fs_ids})")
# db_con = pg_con_prod("FISH")
# indf_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out ind_fish IDs
# ifs_ids = unique(indf_id$individual_fish_id)
# ifs_ids = paste0(paste0("'", ifs_ids, "'"), collapse = ", ")
#
# # Get redd_encounter_ids
# qry = glue("select redd_encounter_id ",
#            "from spawning_ground.redd_encounter ",
#            "where survey_event_id in ({se_ids})")
# db_con = pg_con_prod("FISH")
# redd_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out redd IDs
# rd_ids = unique(redd_id$redd_encounter_id)
# rd_ids = paste0(paste0("'", rd_ids, "'"), collapse = ", ")
#
# # Get individual_redd_ids
# qry = glue("select individual_redd_id ",
#            "from spawning_ground.individual_redd ",
#            "where redd_encounter_id in ({rd_ids})")
# db_con = pg_con_prod("FISH")
# iredd_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out individual_redd IDs
# ird_ids = unique(iredd_id$individual_redd_id)
# ird_ids = paste0(paste0("'", ird_ids, "'"), collapse = ", ")
#
# #======== redd data ===============
#
# # individual_redd: 12973 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.individual_redd ",
#                        "where individual_redd_id in ({ird_ids})"))
# dbDisconnect(db_con)
#
# # redd_encounter: 20427 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.redd_encounter ",
#                        "where redd_encounter_id in ({rd_ids})"))
# dbDisconnect(db_con)
#
# #======== fish data ===============
#
# # fish_length_measurement: 3081 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.fish_length_measurement ",
#                        "where individual_fish_id in ({ifs_ids})"))
# dbDisconnect(db_con)
#
# # individual_fish: 3642 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.individual_fish ",
#                        "where fish_encounter_id in ({fs_ids})"))
# dbDisconnect(db_con)
#
# # fish_capture_event: 5515 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.fish_capture_event ",
#                        "where fish_encounter_id in ({fs_ids})"))
# dbDisconnect(db_con)
#
# # fish_mark: 8270 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.fish_mark ",
#                        "where fish_encounter_id in ({fs_ids})"))
# dbDisconnect(db_con)
#
# # fish_encounter: 12001 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.fish_encounter ",
#                        "where fish_encounter_id in ({fs_ids})"))
# dbDisconnect(db_con)
#
# # fish_encounter: 0 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.fish_encounter ",
#                        "where survey_event_id in ({se_ids})"))
# dbDisconnect(db_con)
#
# #======== Survey event ===============
#
# # survey_event: 10574 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.survey_event ",
#                        "where survey_id in ({s_ids})"))
# dbDisconnect(db_con)
#
# #======== Survey level tables ===============
#
# # other_observation: 380 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.other_observation ",
#                        "where survey_id in ({s_ids})"))
# dbDisconnect(db_con)
#
# # fish_passage_feature: 217 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.fish_passage_feature ",
#                        "where survey_id in ({s_ids})"))
# dbDisconnect(db_con)
#
# # mobile_survey_form: 4280 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.mobile_survey_form ",
#                        "where survey_id in ({s_ids})"))
# dbDisconnect(db_con)
#
# # survey_comment: 4280 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.survey_comment ",
#                        "where survey_id in ({s_ids})"))
# dbDisconnect(db_con)
#
# # survey_intent: 37440 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.survey_intent ",
#                        "where survey_id in ({s_ids})"))
# dbDisconnect(db_con)
#
# # waterbody_measurement: 3406 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.waterbody_measurement ",
#                        "where survey_id in ({s_ids})"))
# dbDisconnect(db_con)
#
# # survey: 4280 rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.survey ",
#                        "where survey_id in ({s_ids})"))
# dbDisconnect(db_con)
#
# #======== location ===============
#
# # media_location: 397
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.media_location ",
#                        "where location_id in ({loc_ids})"))
# dbDisconnect(db_con)
#
# # Location coordinates: 6121
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.location_coordinates ",
#                        "where location_id in ({loc_ids})"))
# dbDisconnect(db_con)
#
# # Location: 6194
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.location ",
#                        "where location_id in ({loc_ids})"))
# dbDisconnect(db_con)
#
# # ALL deleted ====================

#==========================================================================================
# Copy previous mobile uploads...currently in FISH prod...to local FISH_Chehalis
#==========================================================================================

# Clean up
rm(list = c("fs_ids", "ifs_ids", "ird_ids", "loc_ids", "qry", "rd_ids",
            "s_id", "s_ids", "se_ids", "se_id", "fish_id", "indf_id",
            "iredd_id", "redd_id", "test_location_ids", "test_upload_ids",
            "test_location_two_ids", "test_upload_two_ids"))

# Get IDs
test_upload_ids = readRDS("data/cumulative_upload_ids.rds")
test_location_ids = readRDS("data/cumulative_location_ids.rds")

# Pull out survey IDs
s_id = unique(test_upload_ids$survey_id)
s_id = s_id[!is.na(s_id)]
s_ids = paste0(paste0("'", s_id, "'"), collapse = ", ")

# Pull out survey_event IDs
se_id = unique(test_upload_ids$survey_event_id)
se_id = se_id[!is.na(se_id)]
se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")

# Pull out location IDs:  rows
loc_ids = unique(test_location_ids$location_id)
loc_ids = paste0(paste0("'", loc_ids, "'"), collapse = ", ")

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
# Load data tables to local
#==========================================================================================

#======== Location tables =======================================

#=======  Insert location =================

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.location ",
           "where location_id in ({loc_ids})")
db_con = pg_con_local("FISH")
location_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# location: 6188 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "location")
dbWriteTable(db_con, tbl, location_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#=======  Insert location_coordinates =================

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.location_coordinates ",
           "where location_id in ({loc_ids})")
db_con = pg_con_local("FISH")
location_coordinates_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# location_coordinates: 6115 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "location_coordinates")
dbWriteTable(db_con, tbl, location_coordinates_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Reset gid_sequence ----------------------------

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from spawning_ground.location_coordinates"
db_con = pg_con_prod(dbname = "FISH")
max_gid = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)

# Code to reset sequence
qry = glue("SELECT setval('spawning_ground.location_coordinates_gid_seq', {max_gid}, true)")
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#-------------------------------------------------

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.media_location ",
           "where location_id in ({loc_ids})")
db_con = pg_con_local("FISH")
media_location_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# media_location: 396 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "media_location")
dbWriteTable(db_con, tbl, media_location_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== Survey level tables ===============

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.survey ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local("FISH")
survey_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# survey:  4273 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "survey")
dbWriteTable(db_con, tbl, survey_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.survey_comment ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local("FISH")
survey_comment_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# survey_comment: 4273 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "survey_comment")
dbWriteTable(db_con, tbl, survey_comment_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.survey_intent ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local("FISH")
survey_intent_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# survey_intent: 37391 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "survey_intent")
dbWriteTable(db_con, tbl, survey_intent_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.waterbody_measurement ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local("FISH")
waterbody_meas_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# waterbody_measurement: 3400 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_measurement")
dbWriteTable(db_con, tbl, waterbody_meas_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.mobile_survey_form ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local("FISH")
mobile_survey_form_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# mobile_survey_form: 4273 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "mobile_survey_form")
dbWriteTable(db_con, tbl, mobile_survey_form_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.fish_passage_feature ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local("FISH")
fish_passage_feature_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# fish_passage_feature: 216 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_passage_feature")
dbWriteTable(db_con, tbl, fish_passage_feature_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.other_observation ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local("FISH")
other_observation_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# other_observation: 380 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "other_observation")
dbWriteTable(db_con, tbl, other_observation_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== Survey event ===============

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.survey_event ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local("FISH")
survey_event_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# survey_event: 13879 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "survey_event")
dbWriteTable(db_con, tbl, survey_event_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== fish data ===============

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.fish_encounter ",
           "where survey_event_id in ({se_ids})")
db_con = pg_con_local("FISH")
fish_encounter_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# fish_encounter: 20693 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_encounter")
dbWriteTable(db_con, tbl, fish_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.fish_capture_event ",
           "where fish_encounter_id in ({fs_ids})")
db_con = pg_con_local("FISH")
fish_capture_event_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# fish_capture_event: 5527 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_capture_event")
dbWriteTable(db_con, tbl, fish_capture_event_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.fish_mark ",
           "where fish_encounter_id in ({fs_ids})")
db_con = pg_con_local("FISH")
fish_mark_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# fish_mark: 8238 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_mark")
dbWriteTable(db_con, tbl, fish_mark_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.individual_fish ",
           "where fish_encounter_id in ({fs_ids})")
db_con = pg_con_local("FISH")
individual_fish_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# individual_fish: 4714 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "individual_fish")
dbWriteTable(db_con, tbl, individual_fish_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.fish_length_measurement ",
           "where individual_fish_id in ({ifs_ids})")
db_con = pg_con_local("FISH")
fish_length_measurement_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# fish_length_measurement: 3616 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_length_measurement")
dbWriteTable(db_con, tbl, fish_length_measurement_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== redd data ===============

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.redd_encounter ",
           "where survey_event_id in ({se_ids})")
db_con = pg_con_local("FISH")
redd_encounter_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# redd_encounter: 20435 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "redd_encounter")
dbWriteTable(db_con, tbl, redd_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data from FISH local
qry = glue("select * ",
           "from spawning_ground.individual_redd ",
           "where individual_redd_id in ({ird_ids})")
db_con = pg_con_local("FISH")
individual_redd_prep = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# individual_redd: 12974 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "individual_redd")
dbWriteTable(db_con, tbl, individual_redd_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)




