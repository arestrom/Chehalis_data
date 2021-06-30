#===============================================================================
# Look for cases where IFB data in SG was edited by Amy, Lea, or Nick
#
# Notes:
#  1. Data were incorrectly uploaded to SG on two occasions using the
#     origina mobile_import scripts. Errors are in the fish and redd
#     encounter portions of the code. Rows of data were truncated to
#     distinct. This orphaned individual data in some cases, and left
#     counts of one in the encounter tables with multiple cases in the
#     individual tables in some cases.
#  2. This script is intended to find cases where data was edited by
#     Amy, Nick, or Lea, so at to identify what was done and enable
#     recreating the edits.
#  3. Using the created_by and created_datetime fields is not useful
#     because too many fields were edited directly in IFB before and
#     after the uploads. And these time-stamps are preserved on both
#     the IFB and SG sides. See below.
#  4. A cumulative upload of all IFB data was done to the FISH local
#     SG database on 2021-06-23 after deleting the previously uploaded
#     mobile data. A new local DB FISH_Chehalis was then created to
#     hold data previously uploaded to FISH on prod so that original
#     and corrected cumulative upload could be compared to identify
#     where edits had occurred, or new data entered. The initially
#     uploaded IFB data to SG on prod was then deleted and a fresh,
#     corrected, upload was done to SG on prod by copying the data
#     from local over to prod. So all IDs and row numbers are now
#     identical between FISH on prod and FISH on local. To identify
#     differences the comparisons below are between FISH on prod and
#     the local FISH_Chehalis.
#
# ToDo:
#  1.
#
# AS 2021-06-22
#===============================================================================

# Load libraries
library(DBI)
library(RPostgres)
library(dplyr)
library(tibble)
library(glue)
library(iformr)
library(stringi)
library(tidyr)
library(lubridate)
library(openxlsx)
library(sf)
library(remisc)

# Keep connections pane from opening
options("connectionObserver" = NULL)

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

# #===================================================================================================
# # Look for edits made since July 13th 2020 in all tables
# # Result: Too many edits were made directly in IFB after surveys.
# #         So a better strategy is to compare tables from
# #         local to prod using an anti-join to uncover diffs.
# #===================================================================================================
#
# # Globals
# start_date = as.Date("2020-07-13")
#
# # Survey
#
# # Query
# qry = glue("select s.survey_datetime as survey_date, sm.survey_method_code as survey_method, ",
#            "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
#            "sc.completion_status_description as completed_survey, ",
#            "ic.incomplete_survey_description as incomplete_type, ",
#            "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
#            "s.observer_last_name as observer, s.data_submitter_last_name as submitter, ",
#            "s.created_datetime as create_time, s.created_by as create_by, ",
#            "s.modified_datetime as mod_time, s.modified_by as mod_by ",
#            "from spawning_ground.survey as s ",
#            "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
#            "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
#            "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
#            "left join spawning_ground.survey_completion_status_lut as sc on s.survey_completion_status_id = sc.survey_completion_status_id ",
#            "left join spawning_ground.incomplete_survey_type_lut as ic on s.incomplete_survey_type_id = ic.incomplete_survey_type_id ",
#            "where date(s.modified_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
#            "and date(s.modified_datetime) > date(s.created_datetime + interval '1 day') ",
#            "and s.modified_by in ('vanbunwv', 'edwarare', 'ronnelmr')")
#
# # Run query
# con = pg_con_prod("FISH")
# survey_edits = DBI::dbGetQuery(con, qry)
# DBI::dbDisconnect(con)
#
# # Adjust times
# survey_edits = survey_edits %>%
#   mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
#   mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
#   mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
#   mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
#   mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
#   mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
#   mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
#   mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
#   mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
#   mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))

#================================================================================
# Identify differences between FISH local and FISH prod....Result: all identical!
#================================================================================

# Get table names and row counts in sink db
source_row_counts = db_table_counts(dsn = "prod_fish_spawn", schema = "spawning_ground")

# Get table names and row counts in source db
sink_row_counts = db_table_counts(dsn = "local_fish_spawn", schema = "spawning_ground")

# Combine to a dataframe
compare_counts = source_row_counts %>%
  full_join(sink_row_counts, by = "table") %>%
  filter(!table == "spatial_ref_sys") %>%
  select(table, prod_spawn = row_count.x, local_spawn = row_count.y)

# Verify that table names and row counts in source and sink db's are identical
identical(compare_counts$prod_spawn, compare_counts$local_spawn)

# Pull out cases that differ
diff_counts = NULL
if (!identical(compare_counts$prod_spawn, compare_counts$local_spawn)) {
  diff_counts = compare_counts %>%
    filter(!prod_spawn == local_spawn)
}

#================================================================================
# Identify differences between original and cumulative uploads. See diff_counts.
#================================================================================

# Get table names and row counts in sink db
source_row_counts = db_table_counts(dsn = "prod_fish_spawn", schema = "spawning_ground")

# Get table names and row counts in source db
sink_row_counts = db_table_counts(dsn = "local_fish_chehalis", schema = "spawning_ground")

# Combine to a dataframe
compare_counts = source_row_counts %>%
  full_join(sink_row_counts, by = "table") %>%
  filter(!table == "spatial_ref_sys") %>%
  select(table, prod_spawn = row_count.x, local_spawn = row_count.y)

# Verify that table names and row counts in source and sink db's are identical
identical(compare_counts$prod_spawn, compare_counts$local_spawn)

# Pull out cases that differ
diff_counts = NULL
if (!identical(compare_counts$prod_spawn, compare_counts$local_spawn)) {
  diff_counts = compare_counts %>%
    filter(!prod_spawn == local_spawn)
}

# Add a diff value: Result: looks like 7 surveys were added manually to SG
diff_counts = diff_counts %>%
  mutate(n_diff = local_spawn - prod_spawn)

#================================================================================
# Identify diffs in survey table
#================================================================================

# Survey from FISH prod ===============================

# First mobile entry was on 2019-08-07....also first IFB survey_id = 36
start_date = as.Date("2019-08-07")

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "sc.completion_status_description as completed_survey, ",
           "ic.incomplete_survey_description as incomplete_type, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, s.data_submitter_last_name as submitter, ",
           "s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "inner join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "left join spawning_ground.survey_completion_status_lut as sc on s.survey_completion_status_id = sc.survey_completion_status_id ",
           "left join spawning_ground.incomplete_survey_type_lut as ic on s.incomplete_survey_type_id = ic.incomplete_survey_type_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "order by msf.parent_form_survey_id")

# Run query. Result: Correct number of rows: 4273 returned. See cumulative_mobile_reload.R.
con = pg_con_prod("FISH")
survey_new = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
survey_new = survey_new %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))

# Survey from FISH_Chehalis local ======================================

# Added wria_lut and left join on mobile_survey_form to ensure all surveys (possibly manually entered) included

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "sc.completion_status_description as completed_survey, ",
           "ic.incomplete_survey_description as incomplete_type, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, s.data_submitter_last_name as submitter, ",
           "s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "left join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "inner join spawning_ground.wria_lut as wr on locl.wria_id = wr.wria_id ",
           "left join spawning_ground.survey_completion_status_lut as sc on s.survey_completion_status_id = sc.survey_completion_status_id ",
           "left join spawning_ground.incomplete_survey_type_lut as ic on s.incomplete_survey_type_id = ic.incomplete_survey_type_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "and wr.wria_code in ('22', '23') ",
           "order by msf.parent_form_survey_id")

# Run query. Result: 3 more rows than expected were included: 4283. Expected 4280. See copy_mobile_prod_to_Fish_Chehalis.R
con = pg_con_local("FISH_Chehalis")
survey_old = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
survey_old = survey_old %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))

# # Identify those that don't match: Need to dump start and end times to get reasonable result.
# edit_surveys = survey_old %>%
#   anti_join(survey_new, by = c("survey_date", "parent_id", "survey_method", "rm_upper", "rm_lower",
#                                "completed_survey", "incomplete_type", "survey_start", "survey_end",
#                                "observer", "submitter", "create_time", "create_by", "mod_time",
#                                "mod_by"))

# Identify those that don't match
edit_surveys = survey_old %>%
  anti_join(survey_new, by = c("survey_date", "parent_id", "survey_method", "rm_upper", "rm_lower"))

# Output
num_cols = ncol(edit_surveys)
current_date = format(Sys.Date())
out_name = paste0("data/", current_date, "_", "EditedSurveys.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "EditedSurveys", gridLines = TRUE)
writeData(wb, sheet = 1, edit_surveys, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)

#================================================================================
# Identify diffs in survey_intent data
#================================================================================

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, sp.common_name as species, ct.count_type_code as count_type, ",
           "s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "inner join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "inner join spawning_ground.survey_intent as sint on s.survey_id = sint.survey_id ",
           "inner join spawning_ground.species_lut as sp on sint.species_id = sp.species_id ",
           "inner join spawning_ground.count_type_lut as ct on sint.count_type_id = ct.count_type_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "order by msf.parent_form_survey_id")

# Run query. Result: Correct number of rows: 4273 returned. See cumulative_mobile_reload.R.
con = pg_con_prod("FISH")
intent_new = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
intent_new = intent_new %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))

# Survey from FISH_Chehalis local ======================================

# Added wria_lut and left join on mobile_survey_form to ensure all surveys (possibly manually entered) included

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, sp.common_name as species, ct.count_type_code as count_type, ",
           "s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "left join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "inner join spawning_ground.wria_lut as wr on locl.wria_id = wr.wria_id ",
           "inner join spawning_ground.survey_intent as sint on s.survey_id = sint.survey_id ",
           "inner join spawning_ground.species_lut as sp on sint.species_id = sp.species_id ",
           "inner join spawning_ground.count_type_lut as ct on sint.count_type_id = ct.count_type_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "and wr.wria_code in ('22', '23') ",
           "order by msf.parent_form_survey_id")

# Run query. Result: Correct number of rows: 4273 returned. See cumulative_mobile_reload.R.
con = pg_con_prod("FISH")
intent_old = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
intent_old = intent_old %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))


# Identify those that don't match....All match
edit_intents = intent_old %>%
  anti_join(intent_new, by = c("survey_date", "parent_id", "survey_method", "rm_upper", "rm_lower",
                               "species", "count_type"))

#================================================================================
# Identify diffs in waterbody_meas data
#================================================================================

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, wc.clarity_type_short_description as water_clarity_type, ",
           "wb.water_clarity_meter, wb.stream_flow_measurement_cfs as cfs, ",
           "s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "inner join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "inner join spawning_ground.waterbody_measurement as wb on s.survey_id = wb.survey_id ",
           "left join spawning_ground.water_clarity_type_lut as wc on wb.water_clarity_type_id = wc.water_clarity_type_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "order by msf.parent_form_survey_id")

# Run query. Result: Correct number of rows: 4273 returned. See cumulative_mobile_reload.R.
con = pg_con_prod("FISH")
water_meas_new = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
water_meas_new = water_meas_new %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))

# Survey from FISH_Chehalis local ======================================

# Added wria_lut and left join on mobile_survey_form to ensure all surveys (possibly manually entered) included

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, wc.clarity_type_short_description as water_clarity_type, ",
           "wb.water_clarity_meter, wb.stream_flow_measurement_cfs as cfs, ",
           "s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "left join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "inner join spawning_ground.wria_lut as wr on locl.wria_id = wr.wria_id ",
           "inner join spawning_ground.waterbody_measurement as wb on s.survey_id = wb.survey_id ",
           "left join spawning_ground.water_clarity_type_lut as wc on wb.water_clarity_type_id = wc.water_clarity_type_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "and wr.wria_code in ('22', '23') ",
           "order by msf.parent_form_survey_id")

# Run query. Result: Correct number of rows: 4273 returned. See cumulative_mobile_reload.R.
con = pg_con_prod("FISH")
water_meas_old = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
water_meas_old = water_meas_old %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))


# Identify those that don't match....All match
edit_water_meas = water_meas_old %>%
  anti_join(water_meas_new, by = c("survey_date", "parent_id", "survey_method", "rm_upper", "rm_lower",
                                   "water_clarity_type", "water_clarity_meter", "cfs"))

#================================================================================
# Identify diffs in survey_event data
#================================================================================

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, sp.common_name as species, sd.survey_design_type_code as survey_type, ",
           "rn.run_short_description as run_type, se.run_year, se.estimated_percent_fish_seen, ",
           "se.comment_text, s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "inner join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "inner join spawning_ground.survey_event as se on s.survey_id = se.survey_id ",
           "inner join spawning_ground.species_lut as sp on se.species_id = sp.species_id ",
           "inner join spawning_ground.survey_design_type_lut as sd on se.survey_design_type_id = sd.survey_design_type_id ",
           "inner join spawning_ground.run_lut as rn on se.run_id = rn.run_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "order by msf.parent_form_survey_id")

# Run query. Result: Correct number of rows: 4273 returned. See cumulative_mobile_reload.R.
con = pg_con_prod("FISH")
species_new = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
species_new = species_new %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))

# Survey from FISH_Chehalis local ======================================

# Added wria_lut and left join on mobile_survey_form to ensure all surveys (possibly manually entered) included

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, sp.common_name as species, sd.survey_design_type_code as survey_type, ",
           "rn.run_short_description as run_type, se.run_year, se.estimated_percent_fish_seen, ",
           "se.comment_text, s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "left join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "inner join spawning_ground.wria_lut as wr on locl.wria_id = wr.wria_id ",
           "inner join spawning_ground.survey_event as se on s.survey_id = se.survey_id ",
           "inner join spawning_ground.species_lut as sp on se.species_id = sp.species_id ",
           "inner join spawning_ground.survey_design_type_lut as sd on se.survey_design_type_id = sd.survey_design_type_id ",
           "inner join spawning_ground.run_lut as rn on se.run_id = rn.run_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "and wr.wria_code in ('22', '23') ",
           "order by msf.parent_form_survey_id")

# Run query. Result: Correct number of rows: 4273 returned. See cumulative_mobile_reload.R.
con = pg_con_prod("FISH")
species_old = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
species_old = species_old %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))


# Identify those that don't match....All match
edit_species = species_old %>%
  anti_join(species_new, by = c("survey_date", "parent_id", "survey_method", "rm_upper", "rm_lower",
                                "species", "survey_type", "run_type", "run_year", "estimated_percent_fish_seen",
                                "comment_text"))

#================================================================================
# Identify diffs in fish_encounter data
#================================================================================

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, sp.common_name as species, ",
           "fs.fish_status_description as fish_status, sx.sex_description as sex, ",
           "mat.maturity_short_description as maturity, ori.origin_description as origin, ",
           "det.detection_status_description as cwt_detected, ad.adipose_clip_status_code as clip_status, ",
           "fb.behavior_short_description as fish_behavior, mort.mortality_type_short_description as mortality_type, ",
           "fe.fish_encounter_datetime as time_encountered, fe.fish_count, fe.previously_counted_indicator as prev_counted, ",
           "s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "inner join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "inner join spawning_ground.survey_event as se on s.survey_id = se.survey_id ",
           "inner join spawning_ground.species_lut as sp on se.species_id = sp.species_id ",
           "inner join spawning_ground.fish_encounter as fe on se.survey_event_id = fe.survey_event_id ",
           "inner join spawning_ground.fish_status_lut as fs on fe.fish_status_id = fs.fish_status_id ",
           "inner join spawning_ground.sex_lut as sx on fe.sex_id = sx.sex_id ",
           "inner join spawning_ground.maturity_lut as mat on fe.maturity_id = mat.maturity_id ",
           "inner join spawning_ground.origin_lut as ori on fe.origin_id = ori.origin_id ",
           "inner join spawning_ground.cwt_detection_status_lut as det on fe.cwt_detection_status_id = det.cwt_detection_status_id ",
           "inner join spawning_ground.adipose_clip_status_lut as ad on fe.adipose_clip_status_id = ad.adipose_clip_status_id ",
           "inner join spawning_ground.fish_behavior_type_lut as fb on fe.fish_behavior_type_id = fb.fish_behavior_type_id ",
           "inner join spawning_ground.mortality_type_lut as mort on fe.mortality_type_id = mort.mortality_type_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "order by msf.parent_form_survey_id")

# Run query. Result: Correct number of rows: 4273 returned. See cumulative_mobile_reload.R.
con = pg_con_prod("FISH")
fish_new = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
fish_new = fish_new %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))

# Survey from FISH_Chehalis local ======================================

# Added wria_lut and left join on mobile_survey_form to ensure all surveys (possibly manually entered) included

# Query
qry = glue("select s.survey_datetime as survey_date, msf.parent_form_survey_id as parent_id, ",
           "sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, sp.common_name as species, ",
           "fs.fish_status_description as fish_status, sx.sex_description as sex, ",
           "mat.maturity_short_description as maturity, ori.origin_description as origin, ",
           "det.detection_status_description as cwt_detected, ad.adipose_clip_status_code as clip_status, ",
           "fb.behavior_short_description as fish_behavior, mort.mortality_type_short_description as mortality_type, ",
           "fe.fish_encounter_datetime as time_encountered, fe.fish_count, fe.previously_counted_indicator as prev_counted, ",
           "s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "left join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "inner join spawning_ground.wria_lut as wr on locl.wria_id = wr.wria_id ",
           "inner join spawning_ground.survey_event as se on s.survey_id = se.survey_id ",
           "inner join spawning_ground.species_lut as sp on se.species_id = sp.species_id ",
           "inner join spawning_ground.fish_encounter as fe on se.survey_event_id = fe.survey_event_id ",
           "inner join spawning_ground.fish_status_lut as fs on fe.fish_status_id = fs.fish_status_id ",
           "inner join spawning_ground.sex_lut as sx on fe.sex_id = sx.sex_id ",
           "inner join spawning_ground.maturity_lut as mat on fe.maturity_id = mat.maturity_id ",
           "inner join spawning_ground.origin_lut as ori on fe.origin_id = ori.origin_id ",
           "inner join spawning_ground.cwt_detection_status_lut as det on fe.cwt_detection_status_id = det.cwt_detection_status_id ",
           "inner join spawning_ground.adipose_clip_status_lut as ad on fe.adipose_clip_status_id = ad.adipose_clip_status_id ",
           "inner join spawning_ground.fish_behavior_type_lut as fb on fe.fish_behavior_type_id = fb.fish_behavior_type_id ",
           "inner join spawning_ground.mortality_type_lut as mort on fe.mortality_type_id = mort.mortality_type_id ",
           "where date(s.created_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "and wr.wria_code in ('22', '23') ",
           "order by msf.parent_form_survey_id")

# Run query. Result: Correct number of rows: 4273 returned. See cumulative_mobile_reload.R.
con = pg_con_prod("FISH")
fish_old = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
fish_old = fish_old %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(survey_date = format(survey_date, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_start = with_tz(survey_start, tzone = "America/Los_Angeles")) %>%
  mutate(survey_start = format(survey_start, "%m/%d/%Y %H:%M")) %>%
  mutate(survey_end = with_tz(survey_end, tzone = "America/Los_Angeles")) %>%
  mutate(survey_end = format(survey_end, "%m/%d/%Y %H:%M")) %>%
  mutate(create_time = with_tz(create_time, tzone = "America/Los_Angeles")) %>%
  mutate(create_time = format(create_time, "%m/%d/%Y %H:%M")) %>%
  mutate(mod_time = with_tz(mod_time, tzone = "America/Los_Angeles")) %>%
  mutate(mod_time = format(mod_time, "%m/%d/%Y %H:%M"))


# Identify those that don't match....All match
edit_fish = fish_old %>%
  anti_join(fish_new, by = c("survey_date", "parent_id", "survey_method", "rm_upper", "rm_lower",
                             "species", "fish_status", "sex", "maturity", "origin", "cwt_detected",
                             "clip_status", "fish_behavior", "mortality_type", "fish_count",
                             "prev_counted"))

# STOPPED HERE !!!!!!!!!!!!!!!!!!!!!!!!!










