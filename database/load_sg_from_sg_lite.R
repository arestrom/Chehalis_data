#===============================================================================
# Verify queries work
#
# Notes:
#  1.
#
# ToDo:
#  1.
#
#
#  Successfully loaded edited sg_lite data from Nick on ..... PM
#
# AS 2020-08-31
#===============================================================================

# Load libraries
library(DBI)
library(RPostgres)
library(RSQLite)
library(dplyr)
library(tibble)
library(glue)
library(iformr)
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
# Get newly created IDs needed to grab iform data from sqlite DB
#===========================================================================================================

# Set the load date
load_date = "2020-07-13"

# Get any location IDs from potentially new entries...after I loaded sqlite on 2020-07-13
qry = glue::glue("select distinct location_id, ",
                 "datetime(created_datetime, 'localtime') as created_date ",
                 "from location ",
                 "where date(created_datetime) > date('{load_date}')")

con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
new_location_ids =DBI::dbGetQuery(con, qry)
dbDisconnect(con)

# Get any survey IDs from potentially new entries...after I loaded sqlite on 2020-07-13
qry = glue::glue("select distinct survey_id, ",
                 "datetime(created_datetime, 'localtime') as created_date ",
                 "from survey ",
                 "where date(created_datetime) > date('{load_date}')")

con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
new_survey_ids =DBI::dbGetQuery(con, qry)
dbDisconnect(con)

#===========================================================================================================
# Get IDs created prior to any edits...and needed to grab iform data from sqlite DB
#===========================================================================================================

# Get the stored IDs from when iform data was last loaded
test_upload_ids = readRDS("data/test_upload_ids.rds")
test_location_ids = readRDS("data/test_location_ids.rds")

# Pull out location IDs: 3172 rows
loc_id = c(unique(test_location_ids$location_id), unique(new_location_ids$location_id))
loc_id = loc_id[!is.na(loc_id)]
loc_id = unique(loc_id)
length(loc_id)
length(unique(test_location_ids$location_id)) + length(unique(new_location_ids$location_id))
loc_ids = paste0(paste0("'", loc_id, "'"), collapse = ", ")

# Pull out survey IDs
s_id = c(unique(test_upload_ids$survey_id), unique(new_survey_ids$survey_id))
s_id = unique(s_id)
s_id = s_id[!is.na(s_id)]
length(s_id)
length(unique(test_upload_ids$survey_id)) + length(unique(new_survey_ids$survey_id))
s_ids = paste0(paste0("'", s_id, "'"), collapse = ", ")

# Get survey_event_ids from survey_ids then check if any have no encounter data attached
qry = glue("select survey_event_id ",
           "from survey_event ",
           "where survey_id in ({s_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
se_id = dbGetQuery(con, qry)
dbDisconnect(con)

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
           "where survey_datetime between '2019-07-31' and '2020-08-31' ",
           "and wr.wria_code in ('22', '23') ",
           "and fish_count is null and redd_count is null ",
           "and se.survey_event_id is not null")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
no_counts = dbGetQuery(con, qry)
dbDisconnect(con)

# Run some checks
unique(no_counts$wria_code)
min(no_counts$survey_datetime)
max(no_counts$survey_datetime)
table(no_counts$common_name, useNA = "ifany")

# Create id string
se_id_two = no_counts %>%
  select(survey_event_id) %>%
  distinct() %>%
  filter(!is.na(survey_event_id)) %>%
  pull(survey_event_id)

# Pull out survey_event IDs
se_id = se_id %>%
  filter(!survey_event_id %in% se_id_two) %>%
  filter(!is.na(survey_event_id)) %>%
  distinct() %>%
  pull(survey_event_id)

# Check and create string
length(se_id)
se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")

#============================================================================
# Verify none of the IDs are already in SG on local
#============================================================================

# Verify none of the location IDs are already in sg local: NONE !!!
qry = glue("select location_id ",
           "from location ",
           "where location_id in ({loc_ids})")
db_con = pg_con_local(dbname = "spawning_ground")
prev_loc = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Verify none of the survey IDs are already in sg local: NONE !!!
qry = glue("select survey_id ",
           "from survey ",
           "where survey_id in ({s_ids})")
db_con = pg_con_local(dbname = "spawning_ground")
prev_surv = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Verify none of the survey_event IDs are already in sg local: NONE !!!
qry = glue("select survey_event_id ",
           "from survey_event ",
           "where survey_event_id in ({se_ids})")
db_con = pg_con_local(dbname = "spawning_ground")
prev_se = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

#============================================================================
# Get the mobile data from sqlite that needs to be written to SG local
#============================================================================

#--------------- Location data -------------------------

# Get location data
qry = glue("select * ",
           "from location ",
           "where location_id in ({loc_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
location = dbGetQuery(con, qry)
dbDisconnect(con)

# Verify location
any(is.na(location$location_id))
any(duplicated(location$location_id))
any(is.na(location$waterbody_id))
any(is.na(location$wria_id))
any(is.na(location$location_type_id))
any(is.na(location$stream_channel_type_id))
any(is.na(location$location_orientation_type_id))
any(is.na(location$created_datetime))
any(is.na(location$created_by))

#=======  Insert location =================

# location: 3172 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'location', location, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get location_coordinates data
qry = glue("select * ",
           "from location_coordinates ",
           "where location_id in ({loc_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
location_coordinates = dbGetQuery(con, qry)
dbDisconnect(con)

# Verify location_coordinates
any(duplicated(location_coordinates$location_id))
any(is.na(location_coordinates$location_coordinates_id))
any(is.na(location_coordinates$location_id))

#=======  Insert location_coordinates =================

# STOPPED HERE !!!!!!!!!!!!!!!!!!!!!!


# location: 3172 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'location', location, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

















# Verify media_location
any(is.na(media_location_prep$media_location_id))
any(is.na(media_location_prep$location_id))
any(is.na(media_location_prep$media_type_id))
any(is.na(media_location_prep$media_url))
any(is.na(media_location_prep$created_datetime))
any(is.na(media_location_prep$created_by))




# Get fish_encounter_ids
qry = glue("select fish_encounter_id from fish_encounter where survey_event_id in ({se_ids})")
db_con = pg_con_local("spawning_ground")
fish_id = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out fish IDs
fs_ids = unique(fish_id$fish_encounter_id)
fs_ids = paste0(paste0("'", fs_ids, "'"), collapse = ", ")

# Get individual_fish IDs
qry = glue("select individual_fish_id from individual_fish where fish_encounter_id in ({fs_ids})")
db_con = pg_con_local("spawning_ground")
indf_id = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out ind_fish IDs
ifs_ids = unique(indf_id$individual_fish_id)
ifs_ids = paste0(paste0("'", ifs_ids, "'"), collapse = ", ")

# Get redd_encounter_ids
qry = glue("select redd_encounter_id from redd_encounter where survey_event_id in ({se_ids})")
db_con = pg_con_local("spawning_ground")
redd_id = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out redd IDs
rd_ids = unique(redd_id$redd_encounter_id)
rd_ids = paste0(paste0("'", rd_ids, "'"), collapse = ", ")

# Get individual_redd_ids
qry = glue("select individual_redd_id from individual_redd where redd_encounter_id in ({rd_ids})")
db_con = pg_con_local("spawning_ground")
iredd_id = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out individual_redd IDs
ird_ids = unique(iredd_id$individual_redd_id)
ird_ids = paste0(paste0("'", ird_ids, "'"), collapse = ", ")


#================================================================================================
# CHECK FOR MISSING REQUIRED VALUES
#================================================================================================



# Verify survey_prep
any(duplicated(survey_prep$survey_id))
any(is.na(survey_prep$survey_id))
any(is.na(survey_prep$survey_datetime))
any(is.na(survey_prep$data_source_id))
any(is.na(survey_prep$data_source_unit_id))
any(is.na(survey_prep$survey_method_id))
any(is.na(survey_prep$data_review_status_id))
any(is.na(survey_prep$upper_end_point_id))
any(is.na(survey_prep$lower_end_point_id))
any(is.na(survey_prep$survey_completion_status_id))
any(is.na(survey_prep$incomplete_survey_type_id))
any(is.na(survey_prep$created_datetime))
any(is.na(survey_prep$created_by))

# Check survey_comment
any(duplicated(comment_prep$survey_comment_id))
any(is.na(comment_prep$survey_comment_id))
any(is.na(comment_prep$survey_id))
any(is.na(comment_prep$created_datetime))
any(is.na(comment_prep$created_by))

# Check survey_intent
any(duplicated(intent_prep$survey_intent_id))
any(is.na(intent_prep$survey_intent_id))
any(is.na(intent_prep$survey_id))
any(is.na(intent_prep$species_id))
any(is.na(intent_prep$count_type_id))
any(is.na(intent_prep$created_datetime))
any(is.na(intent_prep$created_by))

# Check waterbody_measurement
any(duplicated(waterbody_meas_prep$waterbody_measurement_id))
any(is.na(waterbody_meas_prep$survey_id))
any(is.na(waterbody_meas_prep$water_clarity_type_id))
any(is.na(waterbody_meas_prep$water_clarity_meter))
any(is.na(waterbody_meas_prep$created_datetime))
any(is.na(waterbody_meas_prep$created_by))

# Check mobile_survey_form
any(duplicated(mobile_survey_form_prep$mobile_survey_form_id))
any(is.na(mobile_survey_form_prep$mobile_survey_form_id))
any(is.na(mobile_survey_form_prep$survey_id))
any(is.na(mobile_survey_form_prep$parent_form_survey_id))
any(is.na(mobile_survey_form_prep$parent_form_survey_guid))
any(is.na(mobile_survey_form_prep$created_datetime))
any(is.na(mobile_survey_form_prep$created_by))

# Check fish_passage_feature
any(duplicated(fish_passage_feature_prep$fish_passage_feature_id))
any(is.na(fish_passage_feature_prep$fish_passage_feature_id))
any(is.na(fish_passage_feature_prep$survey_id))
any(is.na(fish_passage_feature_prep$passage_feature_type_id))
any(is.na(fish_passage_feature_prep$feature_location_id))
any(is.na(fish_passage_feature_prep$feature_height_type_id))
any(is.na(fish_passage_feature_prep$plunge_pool_depth_type_id))
any(is.na(fish_passage_feature_prep$created_datetime))
any(is.na(fish_passage_feature_prep$created_by))

# Check other_observations
any(duplicated(other_observation_prep$other_observation_id))
any(is.na(other_observation_prep$other_observation_id))
any(is.na(other_observation_prep$survey_id))
any(is.na(other_observation_prep$observation_type_id))
any(is.na(other_observation_prep$created_datetime))
any(is.na(other_observation_prep$created_by))

# Check survey_event
any(duplicated(survey_event_prep$survey_event_id))
any(is.na(survey_event_prep$survey_event_id))
any(is.na(survey_event_prep$survey_id))
any(is.na(survey_event_prep$species_id))
any(is.na(survey_event_prep$survey_design_type_id))
any(is.na(survey_event_prep$cwt_detection_method_id))
any(is.na(survey_event_prep$run_id))
any(is.na(survey_event_prep$run_year))
any(is.na(survey_event_prep$created_datetime))
any(is.na(survey_event_prep$created_by))

# Check fish_encounter
any(duplicated(fish_encounter_prep$fish_encounter_id))
any(is.na(fish_encounter_prep$fish_encounter_id))
any(is.na(fish_encounter_prep$survey_event_id))
any(is.na(fish_encounter_prep$fish_status_id))
any(is.na(fish_encounter_prep$sex_id))
any(is.na(fish_encounter_prep$maturity_id))
any(is.na(fish_encounter_prep$origin_id))
any(is.na(fish_encounter_prep$cwt_detection_status_id))
any(is.na(fish_encounter_prep$adipose_clip_status_id))
any(is.na(fish_encounter_prep$fish_behavior_type_id))
any(is.na(fish_encounter_prep$mortality_type_id))
any(is.na(fish_encounter_prep$fish_count))
any(is.na(fish_encounter_prep$previously_counted_indicator))
any(is.na(fish_encounter_prep$created_datetime))
any(is.na(fish_encounter_prep$created_by))

# Check fish_capture_event
any(duplicated(fish_capture_event_prep$fish_capture_event_id))
any(is.na(fish_capture_event_prep$fish_capture_event_id))
any(is.na(fish_capture_event_prep$fish_capture_status_id))
any(is.na(fish_capture_event_prep$disposition_type_id))
any(is.na(fish_capture_event_prep$disposition_id))
any(is.na(fish_capture_event_prep$created_datetime))
any(is.na(fish_capture_event_prep$created_by))

# Check fish_mark
any(duplicated(fish_mark_prep$fish_mark_id))
any(is.na(fish_mark_prep$fish_mark_id))
any(is.na(fish_mark_prep$mark_type_id))
any(is.na(fish_mark_prep$mark_status_id))
any(is.na(fish_mark_prep$mark_orientation_id))
any(is.na(fish_mark_prep$mark_placement_id))
any(is.na(fish_mark_prep$mark_size_id))
any(is.na(fish_mark_prep$mark_color_id))
any(is.na(fish_mark_prep$mark_shape_id))
any(is.na(fish_mark_prep$created_datetime))
any(is.na(fish_mark_prep$created_by))

# Check individual_fish
any(duplicated(individual_fish_prep$individual_fish_id))
any(is.na(individual_fish_prep$individual_fish_id))
any(is.na(individual_fish_prep$fish_encounter_id))
any(is.na(individual_fish_prep$fish_condition_type_id))
any(is.na(individual_fish_prep$fish_trauma_type_id))
any(is.na(individual_fish_prep$gill_condition_type_id))
any(is.na(individual_fish_prep$spawn_condition_type_id))
any(is.na(individual_fish_prep$cwt_result_type_id))
any(is.na(individual_fish_prep$created_datetime))
any(is.na(individual_fish_prep$created_by))

# Check fish_length_measurement
any(duplicated(fish_length_measurement_prep$fish_length_measurement_id))
any(is.na(fish_length_measurement_prep$fish_length_measurement_id))
any(is.na(fish_length_measurement_prep$individual_fish_id))
any(is.na(fish_length_measurement_prep$fish_length_measurement_type_id))
any(is.na(fish_length_measurement_prep$length_measurement_centimeter))

# Check redd_encounter
any(duplicated(redd_encounter_prep$redd_encounter_id))
any(is.na(redd_encounter_prep$redd_encounter_id))
any(is.na(redd_encounter_prep$survey_event_id))
any(is.na(redd_encounter_prep$redd_status_id))
any(is.na(redd_encounter_prep$redd_count))
any(is.na(redd_encounter_prep$created_datetime))
any(is.na(redd_encounter_prep$created_by))

# Check individual_redd
any(duplicated(individual_redd_prep$individual_redd_id))
any(is.na(individual_redd_prep$individual_redd_id))
any(is.na(individual_redd_prep$redd_encounter_id))
any(is.na(individual_redd_prep$redd_shape_id))
any(is.na(individual_redd_prep$created_datetime))
any(is.na(individual_redd_prep$created_by))

#================================================================================================
# LOAD TO DB
#================================================================================================



#==========================================================================================
# Load data table
#==========================================================================================

#======== Location tables ===============

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

#=======  Insert location =================

# location: 3170 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'location', location_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from location_coordinates"
db_con = pg_con_local(dbname = "spawning_ground")
max_gid = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)
next_gid = max_gid$max + 1

# Pull out data for location_coordinates table
location_coordinates_temp = location_coordinates_prep %>%
  mutate(gid = seq(next_gid, nrow(location_coordinates_prep) + next_gid - 1)) %>%
  select(location_coordinates_id, location_id, horizontal_accuracy,
         comment_text, gid, created_datetime, created_by,
         modified_datetime, modified_by)

# Write coords_only data
db_con = pg_con_local(dbname = "spawning_ground")
st_write(obj = location_coordinates_temp, dsn = db_con, layer = "location_coordinates_temp")
DBI::dbDisconnect(db_con)

# Use select into query to get data into location_coordinates
qry = glue::glue("INSERT INTO location_coordinates ",
                 "SELECT CAST(location_coordinates_id AS UUID), CAST(location_id AS UUID), ",
                 "horizontal_accuracy, comment_text, gid, geometry AS geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM location_coordinates_temp")

# Insert select to spawning_ground: 3105 rows
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, "DROP TABLE location_coordinates_temp")
DBI::dbDisconnect(db_con)

# media_location: 178 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'media_location', media_location_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

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

#======== Survey level tables ===============

# survey: 2044 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'survey', survey_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# survey_comment: 2044 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'survey_comment', comment_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# survey_intent: 22749 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'survey_intent', intent_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# waterbody_measurement: 2044 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'waterbody_measurement', waterbody_meas_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# mobile_survey_form: 2044 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'mobile_survey_form', mobile_survey_form_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# fish_passage_feature: 76 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_passage_feature', fish_passage_feature_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# other_observation: 239 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'other_observation', other_observation_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== Survey event ===============

# survey_event: 9068 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'survey_event', survey_event_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== fish data ===============

# fish_encounter: 2136 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_encounter', fish_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# fish_capture_event: 4654 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_capture_event', fish_capture_event_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# fish_mark: 6880 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_mark', fish_mark_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# individual_fish: 2537 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'individual_fish', individual_fish_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# fish_length_measurement: 2263 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_length_measurement', fish_length_measurement_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== redd data ===============

# redd_encounter: 11498 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'redd_encounter', redd_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# individual_redd: 7724 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'individual_redd', individual_redd_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# #===========================================================================================================
# # Get IDs needed to delete freshly uploaded data
# #===========================================================================================================
#
# # Get IDs
# test_upload_ids = readRDS("data/test_upload_ids.rds")
# test_location_ids = readRDS("data/test_location_ids.rds")
#
# # Pull out survey IDs
# s_id = unique(test_upload_ids$survey_id)
# s_id = s_id[!is.na(s_id)]
# s_ids = paste0(paste0("'", s_id, "'"), collapse = ", ")
#
# # Pull out survey_event IDs
# se_id = unique(test_upload_ids$survey_event_id)
# se_id = se_id[!is.na(se_id)]
# se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")
#
# # Pull out location IDs: 3160 rows
# loc_ids = unique(test_location_ids$location_id)
# loc_ids = paste0(paste0("'", loc_ids, "'"), collapse = ", ")
#
# # Get fish_encounter_ids
# qry = glue("select fish_encounter_id from fish_encounter where survey_event_id in ({se_ids})")
# db_con = pg_con_local("spawning_ground")
# fish_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out fish IDs
# fs_ids = unique(fish_id$fish_encounter_id)
# fs_ids = paste0(paste0("'", fs_ids, "'"), collapse = ", ")
#
# # Get individual_fish IDs
# qry = glue("select individual_fish_id from individual_fish where fish_encounter_id in ({fs_ids})")
# db_con = pg_con_local("spawning_ground")
# indf_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out ind_fish IDs
# ifs_ids = unique(indf_id$individual_fish_id)
# ifs_ids = paste0(paste0("'", ifs_ids, "'"), collapse = ", ")
#
# # Get redd_encounter_ids
# qry = glue("select redd_encounter_id from redd_encounter where survey_event_id in ({se_ids})")
# db_con = pg_con_local("spawning_ground")
# redd_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out redd IDs
# rd_ids = unique(redd_id$redd_encounter_id)
# rd_ids = paste0(paste0("'", rd_ids, "'"), collapse = ", ")
#
# # Get individual_redd_ids
# qry = glue("select individual_redd_id from individual_redd where redd_encounter_id in ({rd_ids})")
# db_con = pg_con_local("spawning_ground")
# iredd_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out individual_redd IDs
# ird_ids = unique(iredd_id$individual_redd_id)
# ird_ids = paste0(paste0("'", ird_ids, "'"), collapse = ", ")
#
# #==========================================================================================
# # Delete test data
# #==========================================================================================
#
# #======== redd data ===============
#
# # individual_redd: 7724 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from individual_redd where individual_redd_id in ({ird_ids})'))
# dbDisconnect(db_con)
#
# # redd_encounter: 11498 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from redd_encounter where redd_encounter_id in ({rd_ids})'))
# dbDisconnect(db_con)
#
# #======== fish data ===============
#
# # fish_length_measurement: 2263 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from fish_length_measurement where individual_fish_id in ({ifs_ids})'))
# dbDisconnect(db_con)
#
# # individual_fish: 2537 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from individual_fish where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# # fish_capture_event: 4654 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from fish_capture_event where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# # fish_mark: 6880 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from fish_mark where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# # fish_encounter: 2136 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from fish_encounter where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# #======== Survey event ===============
#
# # survey_event: 9068 rows....only deleted 2106 rows Aug 31st...must be ok since all surveys were deleted.
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from survey_event where survey_event_id in ({se_ids})'))
# dbDisconnect(db_con)
#
# #======== Survey level tables ===============
#
# # other_observation: 239 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from other_observation where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # fish_passage_feature: 76 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from fish_passage_feature where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # mobile_survey_form: 2044 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from mobile_survey_form where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # survey_comment: 2044 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from survey_comment where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # survey_intent: 22749 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from survey_intent where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # waterbody_measurement: 2044 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from waterbody_measurement where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # survey: 2044 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from survey where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# #======== location ===============
#
# # media_location: 178
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue("delete from media_location where location_id in ({loc_ids})"))
# dbDisconnect(db_con)
#
# # Location coordinates: 3105
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue("delete from location_coordinates where location_id in ({loc_ids})"))
# dbDisconnect(db_con)
#
# # Location: 3165
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue("delete from location where location_id in ({loc_ids})"))
# dbDisconnect(db_con)















