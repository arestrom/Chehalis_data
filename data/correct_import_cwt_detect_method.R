#=================================================================
# Correct SG mobile import entries to electronic if cwt_detected
#  = "Undetermined" and species in chinook, coho, sthd
#
# NOTES:
#  1. Lea verified only carcasses are checked for cwt
#
# Successfully completed on 2021-05-12.
#
#  ToDo:
#  1.
#
# AS 2021-05-12
#=================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Libraries
library(dplyr)
library(DBI)
library(RPostgres)
library(glue)
library(stringi)
library(lubridate)

# Keep connections pane from opening. Kill dplyr group_by, summarize crap.
options("connectionObserver" = NULL)
options(dplyr.summarise.inform = FALSE)

# Set global for species.
selected_species = c("Chinook salmon", "Coho salmon", "Steelhead trout")
wria_codes = c("22", "23")
wria_codes = paste0(paste0("'", wria_codes, "'"), collapse = ", ")

#============================================================================
# Define functions
#============================================================================

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

# Function to connect to postgres on prod
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

# Function to connect to postgres on local
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
# Query: Zero's were added for live when downloaded from iforms if live
# counts were intended. Otherwise zeros were added to dead counts, or redds
# counts...in that order, as needed. The reason for adding zero's was to
# ensure that a survey_type was entered for each combination of survey and
# intended species. Survey type was requirement in output to Curt Holt.
#============================================================================

# Define query
qry = glue("select s.survey_id, s.survey_datetime as survey_date, ",
           "wb.waterbody_display_name as stream_name, ",
           "wb.latitude_longitude_id as LLID, ",
           "wb.stream_catalog_code, wb.waterbody_id, ",
           "locup.river_mile_measure as upper_rm, ",
           "loclo.river_mile_measure as lower_rm, ",
           "scmp.completion_status_description as completed_survey, ",
           "incmp.incomplete_survey_description as why_incomplete, ",
           "sm.survey_method_description as survey_method, ",
           "ds.data_source_name as data_source, ",
           "msf.parent_form_name as mobile_form, ",
           "sp.common_name as species, ",
           "sdt.survey_design_type_code as survey_type, ",
           "se.run_year, rn.run_short_description as run, ",
           "se.survey_event_id, ",
           "cwtm.detection_method_description as cwt_detect_method, ",
           "fe.fish_count, fs.fish_status_description as fish_status, ",
           "mat.maturity_short_description as maturity, ",
           "ads.adipose_clip_status_code as ad_clip_code, ",
           "ads.adipose_clip_status_description as ad_clip_status, ",
           "cwts.detection_status_description as cwt_detected, ",
           "fe.previously_counted_indicator as prev_counted, ",
           "ind.cwt_snout_sample_number as cwt_snout_num ",
           "from spawning_ground.survey as s ",
           "inner join spawning_ground.location as locup on s.upper_end_point_id = locup.location_id ",
           "inner join spawning_ground.location as loclo on s.lower_end_point_id = loclo.location_id ",
           "inner join spawning_ground.waterbody_lut as wb on loclo.waterbody_id = wb.waterbody_id or locup.waterbody_id = wb.waterbody_id ",
           "inner join spawning_ground.wria_lut as wr on locup.wria_id = wr.wria_id or loclo.wria_id = wr.wria_id ",
           "left join spawning_ground.survey_completion_status_lut as scmp on s.survey_completion_status_id = scmp.survey_completion_status_id ",
           "left join spawning_ground.incomplete_survey_type_lut as incmp on s.incomplete_survey_type_id = incmp.incomplete_survey_type_id ",
           "inner join spawning_ground.survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
           "inner join spawning_ground.data_source_lut as ds on s.data_source_id = ds.data_source_id ",
           "inner join spawning_ground.mobile_survey_form as msf on s.survey_id = msf.survey_id ",
           "inner join spawning_ground.survey_event as se on s.survey_id = se.survey_id ",
           "inner join spawning_ground.species_lut as sp on se.species_id = sp.species_id ",
           "left join spawning_ground.survey_design_type_lut as sdt on se.survey_design_type_id = sdt.survey_design_type_id ",
           "left join spawning_ground.run_lut as rn on se.run_id = rn.run_id ",
           "left join spawning_ground.cwt_detection_method_lut as cwtm on se.cwt_detection_method_id = cwtm.cwt_detection_method_id ",
           "inner join spawning_ground.fish_encounter as fe on se.survey_event_id = fe.survey_event_id ",
           "inner join spawning_ground.fish_status_lut as fs on fe.fish_status_id = fs.fish_status_id ",
           "left join spawning_ground.maturity_lut as mat on fe.maturity_id = mat.maturity_id ",
           "left join spawning_ground.adipose_clip_status_lut as ads on fe.adipose_clip_status_id = ads.adipose_clip_status_id ",
           "left join spawning_ground.cwt_detection_status_lut as cwts on fe.cwt_detection_status_id = cwts.cwt_detection_status_id ",
           "left join spawning_ground.individual_fish as ind on fe.fish_encounter_id = ind.fish_encounter_id ",
           "where wr.wria_code in ({wria_codes}) ",
           "order by survey_date desc, stream_name, lower_rm, upper_rm, species, survey_type, fish_status")

# Run query: 9940
strt = Sys.time()
con = pg_con_prod("FISH")
#con = pg_con_local("FISH")
fish_counts = DBI::dbGetQuery(con, qry)
dbDisconnect(con)
nd = Sys.time(); nd - strt  # 3.47 secs on local. 8.8 secs on prod

# Filter to selected species
fish_counts = fish_counts %>%
  filter(species %in% selected_species) %>%
  filter(prev_counted == FALSE) %>%
  filter(fish_count == 1L) %>%
  filter(fish_status == "Dead") %>%
  filter(cwt_detect_method == "Not applicable") %>%
  filter(cwt_detected == "Coded-wire tag undetermined, e.g., no head")

# Check
unique(fish_counts$mobile_form)
unique(fish_counts$cwt_detect_method)
unique(fish_counts$fish_count)
unique(fish_counts$fish_status)
unique(fish_counts$cwt_detected)

# Pull out fish_encounter_ids
se_ids = paste0(paste0("'", unique(fish_counts$survey_event_id), "'"), collapse = ", ")

# Update cwt_detection_method to "Electronic" for all fish_enc_ids
qry = glue("update spawning_ground.survey_event ",
           "set cwt_detection_method_id = 'd2de4873-e9ab-4eda-b1a0-fb9dcc2face7' ",
           "where survey_event_id in ({se_ids})")

# Run update query
strt = Sys.time()
con = pg_con_prod("FISH")
#con = pg_con_local("FISH")
fish_counts = DBI::dbExecute(con, qry)
dbDisconnect(con)
nd = Sys.time(); nd - strt  # 3.47 secs on local. 8.8 secs on prod


