#===============================================================================
# Load mobile data from edited sqlite DB to SG on fish prod
#
# Did not work....needed to update waterbody_lut and probably other stream data
#    so decided to load all data tables from local using:
#    copy_sg_local_data_to_fish_prod.R in spawning_grounds repo.
#
# AS 2020-09-01
#===============================================================================

# Load libraries
library(DBI)
library(RPostgres)
library(RSQLite)
library(odbc)
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

# Function to generate dataframe of tables and row counts in database
db_table_counts = function(dsn = "prod_fish_spawn", schema = "spawning_ground") {
  db_con = dbConnect(odbc::odbc(), dsn = dsn, timezone = "UTC", bigint = "numeric")
  qry = glue("select table_name FROM information_schema.tables where table_schema = '{schema}'")
  db_tables = DBI::dbGetQuery(db_con, qry) %>%
    pull(table_name)
  tabx = integer(length(db_tables))
  get_count = function(i) {
    tabxi = dbGetQuery(db_con, glue("select count(*) from {schema}.", db_tables[i]))
    as.integer(tabxi$count)
  }
  rc = lapply(seq_along(tabx), get_count)
  dbDisconnect(db_con)
  rcx = as.integer(unlist(rc))
  dtx = tibble(table = db_tables, row_count = rcx)
  dtx = dtx %>%
    arrange(table)
  dtx
}

#==============================================================================
# Identify tables where data needs to be loaded
#==============================================================================

# Get table names and row counts in source db
source_row_counts = db_table_counts(dsn = "local_spawn", schema = "public")

# Get table names and row counts in sink db
sink_row_counts = db_table_counts(dsn = "prod_fish_spawn", schema = "spawning_ground")

# Combine to a dataframe
compare_counts = source_row_counts %>%
  full_join(sink_row_counts, by = "table") %>%
  filter(!table == "spatial_ref_sys") %>%
  select(table, local_spawn = row_count.x, prod_spawn = row_count.y)

# Verify that table names and row counts in source and sink db's are identical
identical(compare_counts$prod_spawn, compare_counts$local_spawn)

# Pull out cases that differ
if (!identical(compare_counts$prod_spawn, compare_counts$local_spawn)) {
  diff_counts = compare_counts %>%
    filter(!prod_spawn == local_spawn)
}

#===========================================================================================================
# Get newly created IDs needed to grab iform data from sqlite DB
#===========================================================================================================

# Get IDs
loc_ids = readRDS("data/loc_ids.rds")
s_ids = readRDS("data/s_ids.rds")
se_ids = readRDS("data/se_ids.rds")

#============================================================================
# Verify none of the IDs are already in SG on local
#============================================================================

# Verify none of the location IDs are already in sg local: NONE !!!
qry = glue("select location_id ",
           "from spawning_ground.location ",
           "where location_id in ({loc_ids})")
db_con = pg_con_prod(dbname = "FISH")
prev_loc = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Verify none of the survey IDs are already in sg local: NONE !!!
qry = glue("select survey_id ",
           "from spawning_ground.survey ",
           "where survey_id in ({s_ids})")
db_con = pg_con_prod(dbname = "FISH")
prev_surv = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Verify none of the survey_event IDs are already in sg local: NONE !!!
qry = glue("select survey_event_id ",
           "from spawning_ground.survey_event ",
           "where survey_event_id in ({se_ids})")
db_con = pg_con_prod(dbname = "FISH")
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
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "location")
dbWriteTable(db_con, tbl, location, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)



# STOPPED HERE. waterbody_lut entry was missing.....reloading data from local



#============================================================
# Reset gid_sequence
#============================================================

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

# Get location_coordinates data
qry = glue("select * ",
           "from location_coordinates ",
           "where location_id in ({loc_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
location_coordinates = sf::st_read(con, query = qry, crs = 2927)
dbDisconnect(con)

# Verify location_coordinates
any(duplicated(location_coordinates$location_id))
any(is.na(location_coordinates$location_coordinates_id))
any(is.na(location_coordinates$location_id))

#=======  Insert location_coordinates =================

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from spawning_ground.location_coordinates"
db_con = pg_con_prod(dbname = "FISH")
max_gid = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)
next_gid = max_gid$max + 1

# Pull out data for location_coordinates table
location_coordinates_temp = location_coordinates %>%
  mutate(gid = seq(next_gid, nrow(location_coordinates) + next_gid - 1)) %>%
  select(location_coordinates_id, location_id, horizontal_accuracy,
         comment_text, gid, created_datetime, created_by,
         modified_datetime, modified_by)

# Write coords_only data
db_con = pg_con_prod(dbname = "FISH")
st_write(obj = location_coordinates_temp, dsn = db_con, layer = "spawning_ground.location_coordinates_temp")
DBI::dbDisconnect(db_con)

# Use select into query to get data into location_coordinates
qry = glue::glue("INSERT INTO spawning_ground.location_coordinates ",
                 "SELECT CAST(location_coordinates_id AS UUID), CAST(location_id AS UUID), ",
                 "horizontal_accuracy, comment_text, gid, geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM spawning_ground.location_coordinates_temp")

# Insert select to spawning_ground: 3112 rows
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_local(dbname = "spawning_ground")
DBI::dbExecute(db_con, "DROP TABLE location_coordinates_temp")
DBI::dbDisconnect(db_con)

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

#=======  Insert media_location =================

# Get media_location data
qry = glue("select * ",
           "from media_location ",
           "where location_id in ({loc_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
media_location = dbGetQuery(con, qry)
dbDisconnect(con)

# Rearrange columns
media_location = media_location %>%
  select(media_location_id, location_id, media_type_id,
         media_url, created_datetime, created_by,
         modified_datetime, modified_by, comment_text)

# Verify media_location
any(is.na(media_location$media_location_id))
any(is.na(media_location$location_id))
any(is.na(media_location$media_type_id))
any(is.na(media_location$media_url))
any(is.na(media_location$created_datetime))
any(is.na(media_location$created_by))

# media_location: 178 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'media_location', media_location, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== Survey level tables ===============

# Get data
qry = glue("select * ",
           "from survey ",
           "where survey_id in ({s_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
survey = dbGetQuery(con, qry)
dbDisconnect(con)

# Check survey
any(duplicated(survey$survey_id))
any(is.na(survey$survey_id))
any(is.na(survey$survey_datetime))
any(is.na(survey$data_source_id))
any(is.na(survey$data_source_unit_id))
any(is.na(survey$survey_method_id))
any(is.na(survey$data_review_status_id))
any(is.na(survey$upper_end_point_id))
any(is.na(survey$lower_end_point_id))
any(is.na(survey$survey_completion_status_id))
any(is.na(survey$incomplete_survey_type_id))
any(is.na(survey$created_datetime))
any(is.na(survey$created_by))

# survey: 2047 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'survey', survey, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data
qry = glue("select * ",
           "from survey_comment ",
           "where survey_id in ({s_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
survey_comment = dbGetQuery(con, qry)
dbDisconnect(con)

# Check survey_comment
any(duplicated(survey_comment$survey_comment_id))
any(is.na(survey_comment$survey_comment_id))
any(is.na(survey_comment$survey_id))
any(is.na(survey_comment$created_datetime))
any(is.na(survey_comment$created_by))

# survey_comment: 2047 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'survey_comment', survey_comment, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data
qry = glue("select * ",
           "from survey_intent ",
           "where survey_id in ({s_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
survey_intent = dbGetQuery(con, qry)
dbDisconnect(con)

# Check survey_intent
any(duplicated(survey_intent$survey_intent_id))
any(is.na(survey_intent$survey_intent_id))
any(is.na(survey_intent$survey_id))
any(is.na(survey_intent$species_id))
any(is.na(survey_intent$count_type_id))
any(is.na(survey_intent$created_datetime))
any(is.na(survey_intent$created_by))

# survey_intent: 22749 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'survey_intent', survey_intent, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data
qry = glue("select * ",
           "from waterbody_measurement ",
           "where survey_id in ({s_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
waterbody_measurement = dbGetQuery(con, qry)
dbDisconnect(con)

# Check waterbody_measurement
any(duplicated(waterbody_measurement$waterbody_measurement_id))
any(is.na(waterbody_measurement$survey_id))
any(is.na(waterbody_measurement$water_clarity_type_id))
any(is.na(waterbody_measurement$water_clarity_meter))
any(is.na(waterbody_measurement$created_datetime))
any(is.na(waterbody_measurement$created_by))

# waterbody_measurement: 2043 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'waterbody_measurement', waterbody_measurement, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data
qry = glue("select * ",
           "from mobile_survey_form ",
           "where survey_id in ({s_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
mobile_survey_form = dbGetQuery(con, qry)
dbDisconnect(con)

# Check mobile_survey_form
any(duplicated(mobile_survey_form$mobile_survey_form_id))
any(is.na(mobile_survey_form$mobile_survey_form_id))
any(is.na(mobile_survey_form$survey_id))
any(is.na(mobile_survey_form$parent_form_survey_id))
any(is.na(mobile_survey_form$parent_form_survey_guid))
any(is.na(mobile_survey_form$created_datetime))
any(is.na(mobile_survey_form$created_by))

# mobile_survey_form: 2044 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'mobile_survey_form', mobile_survey_form, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data
qry = glue("select * ",
           "from fish_passage_feature ",
           "where survey_id in ({s_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
fish_passage_feature = dbGetQuery(con, qry)
dbDisconnect(con)

# Check fish_passage_feature
any(duplicated(fish_passage_feature$fish_passage_feature_id))
any(is.na(fish_passage_feature$fish_passage_feature_id))
any(is.na(fish_passage_feature$survey_id))
any(is.na(fish_passage_feature$passage_feature_type_id))
any(is.na(fish_passage_feature$feature_location_id))
any(is.na(fish_passage_feature$feature_height_type_id))
any(is.na(fish_passage_feature$plunge_pool_depth_type_id))
any(is.na(fish_passage_feature$created_datetime))
any(is.na(fish_passage_feature$created_by))

# fish_passage_feature: 76 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_passage_feature', fish_passage_feature, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data
qry = glue("select * ",
           "from other_observation ",
           "where survey_id in ({s_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
other_observation = dbGetQuery(con, qry)
dbDisconnect(con)

# Check other_observations
any(duplicated(other_observation$other_observation_id))
any(is.na(other_observation$other_observation_id))
any(is.na(other_observation$survey_id))
any(is.na(other_observation$observation_type_id))
any(is.na(other_observation$created_datetime))
any(is.na(other_observation$created_by))

# other_observation: 239 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'other_observation', other_observation, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== Survey event ===============

# Get data
qry = glue("select * ",
           "from survey_event ",
           "where survey_event_id in ({se_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
survey_event = dbGetQuery(con, qry)
dbDisconnect(con)

# Check survey_event
any(duplicated(survey_event$survey_event_id))
any(is.na(survey_event$survey_event_id))
any(is.na(survey_event$survey_id))
any(is.na(survey_event$species_id))
any(is.na(survey_event$survey_design_type_id))
any(is.na(survey_event$cwt_detection_method_id))
any(is.na(survey_event$run_id))
any(is.na(survey_event$run_year))
any(is.na(survey_event$created_datetime))
any(is.na(survey_event$created_by))

# survey_event: 2101 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'survey_event', survey_event, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== fish data ===============

# Get data
qry = glue("select * ",
           "from fish_encounter ",
           "where survey_event_id in ({se_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
fish_encounter = dbGetQuery(con, qry)
dbDisconnect(con)

# Check fish_encounter
any(duplicated(fish_encounter$fish_encounter_id))
any(is.na(fish_encounter$fish_encounter_id))
any(is.na(fish_encounter$survey_event_id))
any(is.na(fish_encounter$fish_status_id))
any(is.na(fish_encounter$sex_id))
any(is.na(fish_encounter$maturity_id))
any(is.na(fish_encounter$origin_id))
any(is.na(fish_encounter$cwt_detection_status_id))
any(is.na(fish_encounter$adipose_clip_status_id))
any(is.na(fish_encounter$fish_behavior_type_id))
any(is.na(fish_encounter$mortality_type_id))
any(is.na(fish_encounter$fish_count))
any(is.na(fish_encounter$previously_counted_indicator))
any(is.na(fish_encounter$created_datetime))
any(is.na(fish_encounter$created_by))

# fish_encounter: 2136 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_encounter', fish_encounter, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get fish_encounter_ids
qry = glue("select fish_encounter_id from fish_encounter where survey_event_id in ({se_ids})")
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
fish_id = dbGetQuery(con, qry)
dbDisconnect(con)

# Pull out fish IDs
fs_ids = unique(fish_id$fish_encounter_id)
fs_ids = paste0(paste0("'", fs_ids, "'"), collapse = ", ")

# Get data
qry = glue("select * ",
           "from fish_capture_event ",
           "where fish_encounter_id in ({fs_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
fish_capture_event = dbGetQuery(con, qry)
dbDisconnect(con)

# Check fish_capture_event
any(duplicated(fish_capture_event$fish_capture_event_id))
any(is.na(fish_capture_event$fish_capture_event_id))
any(is.na(fish_capture_event$fish_capture_status_id))
any(is.na(fish_capture_event$disposition_type_id))
any(is.na(fish_capture_event$disposition_id))
any(is.na(fish_capture_event$created_datetime))
any(is.na(fish_capture_event$created_by))

# fish_capture_event: 4654 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_capture_event', fish_capture_event, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data
qry = glue("select * ",
           "from fish_mark ",
           "where fish_encounter_id in ({fs_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
fish_mark = dbGetQuery(con, qry)
dbDisconnect(con)

# Check fish_mark
any(duplicated(fish_mark$fish_mark_id))
any(is.na(fish_mark$fish_mark_id))
any(is.na(fish_mark$mark_type_id))
any(is.na(fish_mark$mark_status_id))
any(is.na(fish_mark$mark_orientation_id))
any(is.na(fish_mark$mark_placement_id))
any(is.na(fish_mark$mark_size_id))
any(is.na(fish_mark$mark_color_id))
any(is.na(fish_mark$mark_shape_id))
any(is.na(fish_mark$created_datetime))
any(is.na(fish_mark$created_by))

# fish_mark: 6880 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_mark', fish_mark, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get data
qry = glue("select * ",
           "from individual_fish ",
           "where fish_encounter_id in ({fs_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
individual_fish = dbGetQuery(con, qry)
dbDisconnect(con)

# Check individual_fish
any(duplicated(individual_fish$individual_fish_id))
any(is.na(individual_fish$individual_fish_id))
any(is.na(individual_fish$fish_encounter_id))
any(is.na(individual_fish$fish_condition_type_id))
any(is.na(individual_fish$fish_trauma_type_id))
any(is.na(individual_fish$gill_condition_type_id))
any(is.na(individual_fish$spawn_condition_type_id))
any(is.na(individual_fish$cwt_result_type_id))
any(is.na(individual_fish$created_datetime))
any(is.na(individual_fish$created_by))

# individual_fish: 2537 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'individual_fish', individual_fish, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get individual_fish IDs
qry = glue("select individual_fish_id from individual_fish where fish_encounter_id in ({fs_ids})")
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
indf_id = dbGetQuery(con, qry)
dbDisconnect(con)

# Pull out ind_fish IDs
ifs_ids = unique(indf_id$individual_fish_id)
ifs_ids = paste0(paste0("'", ifs_ids, "'"), collapse = ", ")

# Get data
qry = glue("select * ",
           "from fish_length_measurement ",
           "where individual_fish_id in ({ifs_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
fish_length_measurement = dbGetQuery(con, qry)
dbDisconnect(con)

# Check fish_length_measurement
any(duplicated(fish_length_measurement$fish_length_measurement_id))
any(is.na(fish_length_measurement$fish_length_measurement_id))
any(is.na(fish_length_measurement$individual_fish_id))
any(is.na(fish_length_measurement$fish_length_measurement_type_id))
any(is.na(fish_length_measurement$length_measurement_centimeter))

# fish_length_measurement: 2263 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'fish_length_measurement', fish_length_measurement, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== redd data ===============

# Get data
qry = glue("select * ",
           "from redd_encounter ",
           "where survey_event_id in ({se_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
redd_encounter = dbGetQuery(con, qry)
dbDisconnect(con)

# Check redd_encounter
any(duplicated(redd_encounter$redd_encounter_id))
any(is.na(redd_encounter$redd_encounter_id))
any(is.na(redd_encounter$survey_event_id))
any(is.na(redd_encounter$redd_status_id))
any(is.na(redd_encounter$redd_count))
any(is.na(redd_encounter$created_datetime))
any(is.na(redd_encounter$created_by))

# redd_encounter: 11492 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'redd_encounter', redd_encounter, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# Get redd_encounter_ids
qry = glue("select redd_encounter_id from redd_encounter where survey_event_id in ({se_ids})")
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
redd_id = dbGetQuery(con, qry)
dbDisconnect(con)

# Pull out redd IDs
rd_ids = unique(redd_id$redd_encounter_id)
rd_ids = paste0(paste0("'", rd_ids, "'"), collapse = ", ")

# Get data
qry = glue("select * ",
           "from individual_redd ",
           "where redd_encounter_id in ({rd_ids})")

# Get values from source
con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
individual_redd = dbGetQuery(con, qry)
dbDisconnect(con)

# Check individual_redd
any(duplicated(individual_redd$individual_redd_id))
any(is.na(individual_redd$individual_redd_id))
any(is.na(individual_redd$redd_encounter_id))
any(is.na(individual_redd$redd_shape_id))
any(is.na(individual_redd$created_datetime))
any(is.na(individual_redd$created_by))

# individual_redd: 7722 rows
db_con = pg_con_local("spawning_ground")
dbWriteTable(db_con, 'individual_redd', individual_redd, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#===========================================================================================================
# Save reference IDs for possible later use
#===========================================================================================================

# # Output IDs to data folder
# saveRDS(object = loc_ids, file = "data/loc_ids.rds")
# saveRDS(object = s_ids, file = "data/s_ids.rds")
# saveRDS(object = se_ids, file = "data/se_ids.rds")

# #===========================================================================================================
# # Get IDs needed to delete freshly uploaded data
# #===========================================================================================================
#
# # Get IDs
# loc_ids = readRDS("data/loc_ids.rds")
# s_ids = readRDS("data/s_ids.rds")
# se_ids = readRDS("data/se_ids.rds")
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















