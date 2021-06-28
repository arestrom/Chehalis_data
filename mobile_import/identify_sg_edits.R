#===============================================================================
# Look for cases where IFB data in SG was edited by Amy, Lea, or Nick
#
# Notes:
#  1. Data were incorrectly uploaded to SG on both occasions using the
#     mobile_import scripts. Errors are in the fish and redd encounter
#     portions of the code. Rows of data were truncated to distinct.
#     This orphaned individual data in some cases, and left counts of
#     one in the encounter tables with multiple cases in the individual
#     tables in some cases.
#  2. This script is intended to find case where data was edited by
#     Amy, Nick, or Lea, so at to identify what was done and enable
#     recreating the edits.
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

#===================================================================================================
# Look for edits made since July 13th 2020 in all tables
# Result: Too many edits were made directly in IFB after surveys.
#         So a better strategy is to compare tables from
#         local to prod using an anti-join to uncover diffs.
#===================================================================================================

# Globals
start_date = as.Date("2020-07-13")

# Survey

# Query
qry = glue("select s.survey_datetime as survey_date, sm.survey_method_code as survey_method, ",
           "locu.river_mile_measure as rm_upper, locl.river_mile_measure as rm_lower, ",
           "sc.completion_status_description as completed_survey, ",
           "ic.incomplete_survey_description as incomplete_type, ",
           "s.survey_start_datetime as survey_start, s.survey_end_datetime as survey_end, ",
           "s.observer_last_name as observer, s.data_submitter_last_name as submitter, ",
           "s.created_datetime as create_time, s.created_by as create_by, ",
           "s.modified_datetime as mod_time, s.modified_by as mod_by ",
           "from spawning_ground.survey as s ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.location as locu on s.upper_end_point_id = locu.location_id ",
           "inner join spawning_ground.location as locl on s.lower_end_point_id = locl.location_id ",
           "left join spawning_ground.survey_completion_status_lut as sc on s.survey_completion_status_id = sc.survey_completion_status_id ",
           "left join spawning_ground.incomplete_survey_type_lut as ic on s.incomplete_survey_type_id = ic.incomplete_survey_type_id ",
           "where date(s.modified_datetime::timestamp at time zone 'America/Los_Angeles') >= '{start_date}' ",
           "and date(s.modified_datetime) > date(s.created_datetime + interval '1 day') ",
           "and s.modified_by in ('vanbunwv', 'edwarare', 'ronnelmr')")

# Run query
con = pg_con_prod("FISH")
survey_edits = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Adjust times
survey_edits = survey_edits %>%
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

#==============================================================================
# Identify differences between local and prod....Result...only fake surveys
# I am removing these manually.....After editing in front-end all identical!
#==============================================================================

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

# STOPPED HERE....Initial uploaded data is now in local instance, in FISH_Chehalis DB
# Data from that location needs to be compared to mobile data in FISH prod.












