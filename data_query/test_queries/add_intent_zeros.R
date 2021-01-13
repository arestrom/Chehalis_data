#===============================================================================
# Need survey_type for curt_holt_exports. So adding zeros for one count type
# per intended species.
#
# Notes:
#  1. Pull all surveys and intents. Where survey_guid is missing use stream, date,
#     and end-point RMs to match with surveys already uploaded.
#
# ToDo:
#  1. Stopped at line 862....Need to add data and then manually add survey type for exported xlsx data
#
#
# AS 2021-01-12
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

# Set data for query
# Profile ID
profile_id = 417821L
since_id = 0L

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

#===== Get page_ids of forms =======================

# Form names
parent_form_name = "r6_stream_surveys"

# Get access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Get the parent form id
parent_form_page_id = iformr::get_page_id(
  server_name = "wdfw",
  profile_id = profile_id,
  page_name = parent_form_name,
  access_token = access_token)

# Check
parent_form_page_id

#===================================================================================================
# Initial check section to get survey counts and verify streams and reach points are all present
#===================================================================================================

#============================================
# Functions from mobile_import_global.R
#============================================

# Get new survey data
get_new_surveys = function(profile_id, parent_form_page_id, access_token) {
  # Define query for new mobile surveys
  qry = glue("select distinct s.survey_id ",
             "from spawning_ground.survey as s")
  # Checkout connection
  con = pg_con_prod("FISH")
  #con = poolCheckout(pool)
  existing_surveys = DBI::dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  # Define fields...just parent_id and survey_id this time
  fields = paste0("id, headerid, survey_date, stream_name, ",
                  "stream_name_text, new_stream_name, reach, ",
                  "reach_text, new_reach, gps_loc_lower, ",
                  "gps_loc_upper")
  start_id = 0L
  # Get list of all survey_ids and parent_form iform ids on server
  field_string <- paste0("id:<(>\"", start_id, "\"),", fields)
  # Loop through all survey records
  header_data = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = parent_form_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = start_id)
  # Keep only records where headerid is not in survey_id
  new_survey_data = header_data %>%
    mutate(llid = remisc::get_text_item(reach, 1, "_")) %>%
    mutate(reach_trim = remisc::get_text_item(reach, 2, "_")) %>%
    mutate(lo_rm = remisc::get_text_item(reach_trim, 1, "-")) %>%
    mutate(up_rm = remisc::get_text_item(reach_trim, 2, "-")) %>%
    select(parent_form_survey_id = id, survey_id = headerid, survey_date,
           stream_name, stream_name_text, new_stream_name, llid,
           reach, reach_text, lo_rm, up_rm, new_reach, gps_loc_lower,
           gps_loc_upper) %>%
    distinct() %>%
    anti_join(existing_surveys, by = "survey_id") %>%
    arrange(as.Date(survey_date))
  return(new_survey_data)
}

# Get all stream data in DB
get_mobile_streams = function() {
  qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name, ",
             "wb.latitude_longitude_id as llid, st.stream_id as stream_geometry_id ",
             "from spawning_ground.waterbody_lut as wb ",
             "left join spawning_ground.stream as st on wb.waterbody_id = st.waterbody_id ",
             "order by waterbody_display_name")
  con = pg_con_prod("FISH")
  #con = poolCheckout(pool)
  streams = dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  return(streams)
}

# Get existing rm_data
get_mobile_river_miles = function() {
  qry = glue("select distinct loc.location_id, wb.latitude_longitude_id as llid, ",
             "wb.waterbody_id, wb.waterbody_display_name, wr.wria_id, ",
             "wr.wria_code, loc.river_mile_measure as river_mile ",
             "from spawning_ground.location as loc ",
             "left join spawning_ground.waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
             "left join spawning_ground.wria_lut as wr on loc.wria_id = wr.wria_id ",
             "where wria_code in ('22', '23')")
  # Checkout connection
  con = pg_con_prod("FISH")
  #con = poolCheckout(pool)
  river_mile_data = DBI::dbGetQuery(con, qry) %>%
    filter(!is.na(llid) & !is.na(river_mile))
  DBI::dbDisconnect(con)
  #poolReturn(con)
  return(river_mile_data)
}

#==========================================================================
# Reactives from mobile_import_srv.R
#==========================================================================

# Update DB and reload DT
get_new_survey_data = function(profile_id, parent_form_page_id, access_token) {
    new_surveys = get_new_surveys(profile_id = profile_id,
                                  parent_form_page_id = parent_form_page_id,
                                  access_token = access_token)
    stream_data = get_mobile_streams()
    new_surveys = new_surveys %>%
      left_join(stream_data, by = "llid") %>%
      mutate(stream_name_text = if_else(!is.na(waterbody_id), waterbody_display_name, stream_name_text)) %>%
      select(-c(waterbody_display_name))
  return(new_surveys)
}

# Get access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Get new survey_data: currently 2040 records
new_survey_data = get_new_survey_data(profile_id, parent_form_page_id, access_token)

# Reactive to process gps data
get_core_survey_data = function(new_survey_data) {
  survey_data = new_survey_data %>%
    mutate(lower_coords = gsub("[Latitudeong,:]", "", gps_loc_lower)) %>%
    mutate(lower_coords = gsub("[\n]", ":", lower_coords)) %>%
    mutate(lower_coords = gsub("[\r]", "", lower_coords)) %>%
    mutate(lower_coords = gsub("[\t]", ":", lower_coords)) %>%
    mutate(lower_coords = trimws(remisc::get_text_item(lower_coords, 1, ":A"))) %>%
    mutate(upper_coords = gsub("[Latitudeong,:]", "", gps_loc_upper)) %>%
    mutate(upper_coords = gsub("[\n]", ":", upper_coords)) %>%
    mutate(upper_coords = gsub("[\r]", "", upper_coords)) %>%
    mutate(upper_coords = gsub("[\t]", ":", upper_coords)) %>%
    mutate(upper_coords = trimws(remisc::get_text_item(upper_coords, 1, ":A"))) %>%
    select(parent_form_survey_id, survey_id, survey_date, stream_name,
           stream_name_text, new_stream_name, llid, reach, reach_text,
           lo_rm, up_rm, new_reach, lower_coords, upper_coords,
           waterbody_id, stream_geometry_id)
}

# Run
core_survey_data = get_core_survey_data(new_survey_data)

# Reactive for missing streams
get_missing_stream_vals = function(core_survey_data) {
  streams = get_mobile_streams()
  missing_streams = core_survey_data %>%
    filter(is.na(waterbody_id)) %>%
    select(-c(waterbody_id, stream_geometry_id)) %>%
    rename(waterbody_id = stream_name, iform_llid = llid) %>%
    left_join(streams, by = "waterbody_id") %>%
    mutate(stream_name_text = if_else(!is.na(waterbody_id), waterbody_display_name, stream_name_text)) %>%
    mutate(no_llid = if_else(is.na(iform_llid) | !nchar(iform_llid) == 13L, "yes", "no")) %>%
    mutate(stream_name = case_when(
      waterbody_id == "unnamed_tributary" & no_llid == "no" ~ iform_llid,
      waterbody_id == "unnamed_tributary" & no_llid == "yes" ~ remisc::get_text_item(reach_text, 1, "_"),
      waterbody_id == "not_listed" & !is.na(new_stream_name) ~ new_stream_name,
      !waterbody_id %in% c("unnamed_tributary", "not_listed") ~ waterbody_id)) %>%
    mutate(iform_llid = if_else(iform_llid %in% c("", "not"), NA_character_, iform_llid)) %>%
    mutate(reach = if_else(reach %in% c("", "not_listed"), NA_character_, reach)) %>%
    select(parent_form_survey_id, survey_date, waterbody_id, stream_name, stream_name_text,
           waterbody_display_name, iform_llid, llid, reach, lower_coords, upper_coords) %>%
    distinct()
  return(missing_streams)
}

# Reactive for missing reaches
get_missing_reach_vals = function(core_survey_data) {
  streams = get_mobile_streams()
  missing_reaches = core_survey_data %>%
    mutate(lo_rm = trimws(lo_rm)) %>%
    mutate(up_rm = trimws(up_rm)) %>%
    filter(is.na(lo_rm) | is.na(up_rm) | lo_rm == "listed" | up_rm == "listed" |
             lo_rm == "" | up_rm == "") %>%
    select(-c(waterbody_id, stream_geometry_id)) %>%
    rename(waterbody_id = stream_name, iform_llid = llid) %>%
    left_join(streams, by = "waterbody_id") %>%
    mutate(stream_name_text = if_else(!is.na(waterbody_id), waterbody_display_name, stream_name_text)) %>%
    mutate(reach_text = if_else(reach_text == "Not Listed", new_reach, reach_text)) %>%
    mutate(up_rm = if_else(up_rm %in% c("", "listed"), NA_character_, up_rm)) %>%
    mutate(lo_rm = if_else(up_rm %in% c("", "listed"), NA_character_, lo_rm)) %>%
    select(parent_form_survey_id, survey_date, waterbody_id, stream_name_text,
           waterbody_display_name, reach, reach_text, up_rm, lo_rm, lower_coords, upper_coords) %>%
    distinct()
  return(missing_reaches)
}

# Reactive for new end-points needed
get_add_end_points = function(core_survey_data) {
  add_reach_vals = core_survey_data %>%
    filter(nchar(reach) >= 27L) %>%
    mutate(llid = remisc::get_text_item(reach, 1, "_")) %>%
    mutate(reach = remisc::get_text_item(reach, 2, "_")) %>%
    mutate(lo_rm = as.numeric(remisc::get_text_item(reach, 1, "-"))) %>%
    mutate(up_rm = as.numeric(remisc::get_text_item(reach, 2, "-")))
  # Get mobile river miles
  rm_data = get_mobile_river_miles()
  # Pull out lower_rms
  lo_rm_data = rm_data %>%
    select(lower_end_point_id = location_id, llid, lo_rm = river_mile,
           lo_wbid = waterbody_id, lo_name = waterbody_display_name) %>%
    distinct()
  # Pull out upper_rms
  up_rm_data = rm_data %>%
    select(upper_end_point_id = location_id, llid, up_rm = river_mile,
           up_wbid = waterbody_id, up_name = waterbody_display_name) %>%
    distinct()
  # Join to survey_data
  add_reach_vals = add_reach_vals %>%
    left_join(lo_rm_data, by = c("llid", "lo_rm")) %>%
    left_join(up_rm_data, by = c("llid", "up_rm"))
  # Pull out cases where no match exists
  no_reach_point = add_reach_vals %>%
    filter(is.na(lower_end_point_id) | is.na(upper_end_point_id)) %>%
    mutate(lower_comment = if_else(!is.na(lower_end_point_id), "have_point", "need_point")) %>%
    mutate(upper_comment = if_else(!is.na(upper_end_point_id), "have_point", "need_point")) %>%
    select(parent_form_survey_id, iform_waterbody_id = stream_name, iform_llid = llid,
           db_lo_wbid = lo_wbid, db_up_wbid = up_wbid, iform_stream_name_text = stream_name_text,
           db_lo_name = lo_name, db_up_name = up_name, reach_text, lo_rm, up_rm, lower_coords,
           upper_coords, lower_comment, upper_comment) %>%
    distinct() %>%
    arrange(iform_stream_name_text, lo_rm, up_rm)
  return(no_reach_point)
}

# Run: All good...just zeros
missing_stream_vals = get_missing_stream_vals(core_survey_data)
missing_reach_vals = get_missing_reach_vals(core_survey_data)
add_end_points = get_add_end_points(core_survey_data)

#======================================================================================
# Get top-level header data
#======================================================================================

# Get header data
get_header_data = function(profile_id, parent_form_page_id, start_id, access_token) {
  # Define fields
  fields = glue::glue("target_species, survey_date, ",
                      "start_time, observers, stream_name, stream_name_text, ",
                      "new_stream_name, reach, reach_text, new_reach, survey_type, ",
                      "survey_method, survey_direction, no_survey, reachsplit_yn, ",
                      "user_name, headerid, end_time, header_comments, stream_temp, ",
                      "survey_completion_status, chinook_count_type, coho_count_type, ",
                      "steelhead_count_type, chum_count_type, gps_loc_lower, ",
                      "gps_loc_upper, coho_run_year, steelhead_run_year, chum_run_year, ",
                      "chinook_run_year, code_reach")
  # Order by id ascending and get all records after start_id
  field_string = glue('id:<(>"{start_id}"), {fields}')
  # Loop through all survey records
  parent_records = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = parent_form_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = since_id)
  return(parent_records)
}

# New access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Test...currently  records....approx 4.0 seconds
# Get start_id
# Pull out start_id
# start_id = min(new_survey_data$parent_form_survey_id) - 1
start_id = 1L
strt = Sys.time()
header_data = get_header_data(profile_id, parent_form_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Check some date and time values
any(is.na(header_data$survey_date))

# # Test how to handle timezone issue. Profile is set to New York
# chk_time = header_data %>%
#   mutate(created_datetime = as.POSIXct(iformr::idate_time(created_date), tz = "America/Los_Angeles"))

# Update run_year using a rule. Lea's form is incorrect: HACK WARNING !!!!!!!!!!!!!!!!!!!!!!!!
header_data = header_data %>%
  mutate(run_month = as.integer(substr(survey_date, 6, 7))) %>%
  mutate(run_year = as.integer(substr(survey_date, 1, 4))) %>%
  mutate(steelhead_run_year = "2020") %>%
  mutate(coho_run_year = case_when(
    run_year == 2019 ~ "2019",
    run_year == 2020 & run_month < 4 ~ "2019",
    run_year == 2020 & run_month <= 4 ~ "2020",
    TRUE ~ "2019")) %>%
  mutate(chum_run_year = case_when(
    run_year == 2019 ~ "2019",
    run_year == 2020 & run_month < 4 ~ "2019",
    run_year == 2020 & run_month <= 4 ~ "2020",
    TRUE ~ "2019")) %>%
  mutate(chinook_run_year = case_when(
    run_year == 2019 ~ "2019",
    run_year == 2020 & run_month < 4 ~ "2019",
    run_year == 2020 & run_month <= 4 ~ "2020",
    TRUE ~ "2019"))

# Check
table(header_data$steelhead_run_year, useNA = "ifany")
table(header_data$coho_run_year, useNA = "ifany")
table(header_data$chum_run_year, useNA = "ifany")
table(header_data$chinook_run_year, useNA = "ifany")

# Process dates etc
# Rename id to parent_record_id for more explicit joins to subform data...convert dates, etc.
header_data = header_data %>%
  rename(parent_record_id = id, survey_uuid = headerid) %>%
  mutate(survey_start_datetime = as.POSIXct(paste0(survey_date, " ", start_time), tz = "America/Los_Angeles")) %>%
  mutate(survey_end_datetime = as.POSIXct(paste0(survey_date, " ", end_time), tz = "America/Los_Angeles")) %>%
  mutate(survey_start_datetime = with_tz(survey_start_datetime, tzone = "UTC")) %>%
  mutate(survey_end_datetime = with_tz(survey_end_datetime, tzone = "UTC")) %>%
  mutate(survey_date = as.POSIXct(survey_date, tz = "America/Los_Angeles")) %>%
  mutate(survey_date = with_tz(survey_date, tzone = "UTC")) %>%
  # Temporary fix for target species...mostly key values...but a couple with names !!!!!!!
  mutate(target_species = case_when(
    target_species == "Chinook" ~ "e42aa0fc-c591-4fab-8481-55b0df38dcb1",
    target_species == "Chum" ~ "69d1348b-7e8e-4232-981a-702eda20c9b1",
    !target_species %in% c("Chinook", "Chum") ~ target_species)) %>%
  mutate(created_by = stringi::stri_replace_all_fixed(observers, "@dfw.wa.gov", "")) %>%
  mutate(observers = stringi::stri_replace_all_fixed(observers, "@dfw.wa.gov", "")) %>%
  mutate(observers = stringi::stri_replace_all_fixed(observers, ".", " ")) %>%
  mutate(observers = stringi::stri_trans_totitle(observers)) %>%
  select(parent_record_id, survey_uuid, target_species, survey_date, survey_type,
         survey_start_datetime, survey_end_datetime, observers, stream_name, stream_name_text,
         new_stream_name, reach, reach_text, new_reach, survey_type, survey_method,
         survey_direction, no_survey, reachsplit_yn, user_name, end_time, header_comments,
         stream_temp, survey_completion_status, chinook_count_type, coho_count_type,
         steelhead_count_type, chum_count_type, gps_loc_lower, gps_loc_upper,
         coho_run_year, steelhead_run_year, chum_run_year, chinook_run_year,
         code_reach)

# CHECKS -------------------------------------------------------

# Check some values
unique(header_data$observers)
unique(header_data$reachsplit_yn)
unique(header_data$survey_type)

# Check for missing values...required fields
any(is.na(header_data$survey_date))
any(is.na(header_data$survey_start_datetime))
any(is.na(header_data$survey_end_datetime))
any(is.na(header_data$survey_type))
any(is.na(header_data$survey_method))

# Optional
any(is.na(header_data$survey_direction))
unique(header_data$chinook_count_type)
unique(header_data$coho_count_type)
unique(header_data$steelhead_count_type)
unique(header_data$chum_count_type)

# Run year
unique(header_data$coho_run_year)
unique(header_data$steelhead_run_year)
unique(header_data$chum_run_year)
unique(header_data$chinook_run_year)

# Pull out missing required fields....now none
missing_req = header_data %>%
  filter(is.na(survey_date) | is.na(survey_type) | is.na(survey_method)) %>%
  select(parent_record_id, survey_date, observers, stream_name_text,
         reach_text, survey_type, survey_method)
#write.csv(missing_req, "data/missing_data.csv")

# Check
any(is.na(header_data$survey_uuid))
any(header_data$survey_uuid == "")
unique(nchar(header_data$survey_uuid))
min(header_data$parent_record_id)

#---------- END OF CHECKS -----------------------------------------------------

# Survey =========================================

# Generate survey and associated tables
survey_prep = header_data %>%
  mutate(data_submitter_last_name = observers) %>%
  # Set incomplete_survey type to not_applicable if survey was marked as completed...otherwise use no_survey key
  mutate(no_survey = if_else(survey_completion_status == "d192b32e-0e4f-4719-9c9c-dec6593b1977",
                             "cde5d9fb-bb33-47c6-9018-177cd65d15f5", no_survey)) %>%
  select(parent_record_id, stream_name, stream_name_text, reach_text, survey_type,
         survey_id = survey_uuid, survey_datetime = survey_date, survey_method,
         stream_name, reach, survey_completion_status_id = survey_completion_status,
         incomplete_survey_type_id = no_survey, survey_start_datetime,
         survey_end_datetime, observers, data_submitter_last_name)

# Identify the upper and lower end point location_ids
unique(survey_prep$reach)
survey_prep = survey_prep %>%
  mutate(llid = remisc::get_text_item(reach, 1, "_")) %>%
  mutate(reach = remisc::get_text_item(reach, 2, "_"))

# Inspect reach values that are not listed....All have survey_id so do not need reach
chk_not_listed = survey_prep %>%
  filter(reach == "listed")


# After updating not-listed pull RMs and convert to numeric
survey_prep = survey_prep %>%
  mutate(reach = if_else(reach == "listed", "000.00-000.00", reach)) %>%   # Generate fake to avoid error in next steps
  mutate(lo_rm = as.numeric(remisc::get_text_item(reach, 1, "-"))) %>%     # Those that are fake have survey_id so dont need.
  mutate(up_rm = as.numeric(remisc::get_text_item(reach, 2, "-")))

# Dump any data without RMs or llid
river_mile_data = get_mobile_river_miles() %>%
  filter(!is.na(llid) & !is.na(river_mile))

# Pull out lower_rms
lo_rm_data = river_mile_data %>%
  select(lower_end_point_id = location_id, llid, lo_rm = river_mile,
         lo_wria_id = wria_id, lo_wb_id = waterbody_id) %>%
  distinct()

# Identify duplicates
lo_rm_dups = lo_rm_data %>%
  group_by(llid, lo_rm) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1L) %>%
  select(llid, lo_rm, n_seq) %>%
  left_join(lo_rm_data, by = c("llid", "lo_rm"))

# Pull out upper_rms
up_rm_data = river_mile_data %>%
  select(upper_end_point_id = location_id, llid, up_rm = river_mile,
         up_wria_id = wria_id, up_wb_id = waterbody_id) %>%
  distinct()

# Identify duplicates
up_rm_dups = up_rm_data %>%
  group_by(llid, up_rm) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1L) %>%
  select(llid, up_rm, n_seq) %>%
  left_join(up_rm_data, by = c("llid", "up_rm"))

if ( nrow(lo_rm_dups) > 0L | nrow(up_rm_dups) > 0L ) {
  cat("\nWARNING: Duplicated RMs identified. Fix using front-end\n\n")
} else {
  cat("\nNo duplicate RMs found. Ok to proceed.\n\n")
}

# Join to survey_prep
survey_prep = survey_prep %>%
  left_join(lo_rm_data, by = c("llid", "lo_rm")) %>%
  left_join(up_rm_data, by = c("llid", "up_rm")) %>%
  distinct()


# Get location info for adding to tables below
survey_loc_info = survey_prep %>%
  select(parent_record_id, survey_id, lo_wria_id, up_wria_id,
         lo_wb_id, up_wb_id) %>%
  mutate(wria_id = if_else(lo_wria_id == up_wria_id, lo_wria_id, NA_character_)) %>%
  mutate(waterbody_id = if_else(lo_wb_id == up_wb_id, lo_wb_id, NA_character_))

# Warn if any missing wria_id or waterbody_id
if ( any(is.na(survey_loc_info$wria_id)) | any(is.na(survey_loc_info$waterbody_id)) ) {
  cat("\nWARNING: Some surveys conducted on different streams. Ok for this purpose probably.\n\n")
} else {
  cat("\nAll stream and wria_ids match. Ok to proceed.\n\n")
  survey_loc_info = survey_loc_info %>%
    select(parent_record_id, survey_id, wria_id, waterbody_id)
}

# Finalize survey table
survey_prep = survey_prep %>%
  mutate(observer_last_name = remisc::get_text_item(observers, 2, " ")) %>%
  mutate(data_submitter_last_name = remisc::get_text_item(data_submitter_last_name, 2, " ")) %>%
  select(parent_record_id, survey_id, survey_datetime, survey_type,
         survey_method_id = survey_method,
         upper_end_point_id, lower_end_point_id, survey_completion_status_id,
         incomplete_survey_type_id, survey_start_datetime,
         survey_end_datetime, observer_last_name, data_submitter_last_name,
         llid, lo_rm, up_rm)

# Check
any(is.na(survey_prep$survey_id))
any(is.na(survey_prep$parent_record_id))
any(is.na(survey_prep$survey_datetime))
any(is.na(survey_prep$upper_end_point_id))
any(is.na(survey_prep$lower_end_point_id))
any(is.na(survey_prep$survey_type))


# Survey intent ================================

# Pull out survey_intent data
intent_prep = header_data %>%
  mutate(survey_id = survey_uuid) %>%
  select(parent_record_id, survey_id, target_species, chinook_count_type,
         coho_count_type, steelhead_count_type, chum_count_type)

# Check target_species
unique(intent_prep$target_species)

# Get all stream data in DB
get_mobile_intent = function() {
  qry = glue("select species_id as target_species, ",
             "common_name as target_species_name ",
             "from spawning_ground.species_lut")
  con = pg_con_prod("FISH")
  #con = poolCheckout(pool)
  intent = dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  return(intent)
}

# Add to intent_pred
intent_prep = intent_prep %>%
  left_join(get_mobile_intent(), by = "target_species")

# # Check
# unique(intent_prep$target_species_name)

# Split and combine
chinook_intent = intent_prep %>%
  mutate(common_name = "Chinook salmon") %>%
  select(parent_record_id, survey_id, common_name, target_species_name,
         count_type = chinook_count_type)

# Split and combine
coho_intent = intent_prep %>%
  mutate(common_name = "Coho salmon") %>%
  select(parent_record_id, survey_id, common_name, target_species_name,
         count_type = coho_count_type)

# Split and combine
sthd_intent = intent_prep %>%
  mutate(common_name = "Steelhead trout") %>%
  select(parent_record_id, survey_id, common_name, target_species_name,
         count_type = steelhead_count_type)

# Split and combine
chum_intent = intent_prep %>%
  mutate(common_name = "Chum salmon") %>%
  select(parent_record_id, survey_id, common_name, target_species_name,
         count_type = chum_count_type)

# Get surveys where no survey for species was intended
no_survey_intent = rbind(chinook_intent, coho_intent, sthd_intent, chum_intent) %>%
  filter(count_type == "no_survey") %>%
  arrange(survey_id)

# Combine        VALIDATE THIS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
intent_prep = rbind(chinook_intent, coho_intent, sthd_intent, chum_intent) %>%
  filter(!count_type == "no_survey") %>%
  arrange(survey_id)

# Add id for count_type
unique(intent_prep$count_type)

# From option list R6_Count_Type_2
# full_survey, lives_and_deads_only, redds_only, lives_only, deads_only, no_survey

# Create df for conversion to intent categories
intent_categories = tibble(count_type = c("full_survey", "lives_and_deads_only",
                                          "lives_only", "redds_only", "deads_only"),
                           count_categories = c("Live, Carcass, Redd", "Live, Carcass",
                                                "Live", "Redd", "Carcass"))

# Join to intent_prep
intent_prep = intent_prep %>%
  left_join(intent_categories, by = "count_type")

# Check for missing categories
any(is.na(intent_prep$count_categories))

# Create new rows for count_categories
intent_prep = intent_prep %>%
  separate_rows(., count_categories, sep = ",") %>%
  mutate(count_categories = trimws(count_categories)) %>%
  arrange(survey_id, common_name)

# Check
unique(intent_prep$count_categories)

# Get species_ids to add to intent_prep
species_ids = get_mobile_intent() %>%
  select(species_id = target_species, common_name = target_species_name)

# Get all stream data in DB
get_mobile_count_type = function() {
  qry = glue("select count_type_id, count_type_code as count_categories ",
             "from spawning_ground.count_type_lut")
  con = pg_con_prod("FISH")
  #con = poolCheckout(pool)
  count_type = dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  return(count_type)
}

# Pull out needed data
intent_prep = intent_prep %>%
  mutate(survey_intent_id = remisc::get_uuid(nrow(intent_prep))) %>%
  left_join(species_ids, by = "common_name") %>%
  left_join(get_mobile_count_type(), by = "count_categories") %>%
  select(survey_intent_id, parent_record_id, survey_id, species_id,
         count_type_id)

# Check
any(is.na(intent_prep$survey_intent_id))
any(is.na(intent_prep$parent_record_id))
any(is.na(intent_prep$survey_id))
any(is.na(intent_prep$species_id))
any(is.na(intent_prep$count_type_id))

#==============================================================
# Add sev data from survey_intent
#==============================================================

# Pull out and create needed columns for intent_sev
intent_sev = intent_prep %>%
  select(parent_record_id, survey_id, species_id, count_type_id) %>%
  mutate(count_type = case_when(
    count_type_id =="0e1980b4-aa59-4e8e-a820-ce5e4629a549" ~ "carcass",
    count_type_id == "68a9427b-c856-4751-a7ea-e35b515a36d7" ~ "redd",
    count_type_id == "7a785819-3a9f-4728-88db-7e77e580cd41" ~ "live")) %>%
  select(parent_record_id, intent_survey_id = survey_id, count_type, species_id)

# Check survey_prep
any(is.na(survey_prep$parent_record_id))

# Add intent to survey_prep
survey_dat = survey_prep %>%
  left_join(intent_sev, by = "parent_record_id") %>%
  select(parent_record_id, survey_id, intent_survey_id, survey_datetime, survey_type,
         survey_method_id, upper_end_point_id, lower_end_point_id, survey_start_datetime,
         survey_end_datetime, observer_last_name, data_submitter_last_name, llid,
         lo_rm, up_rm, count_type, species_id) %>%
  distinct()

#==============================================================
# Identify surveys where survey_type is missing
#==============================================================

# Find the earliest survey in survey_dat
min_date = format(as.Date(min(survey_dat$survey_datetime)))

# Define query
qry = glue("select s.survey_id, ",
           "ms.parent_form_survey_id as parent_record_id, ",
           "s.survey_datetime, se.survey_event_id, se.survey_design_type_id, ",
           "sd.survey_design_type_code as survey_type, wr.wria_code, ",
           "s.observer_last_name, s.data_submitter_last_name ",
           "from spawning_ground.survey as s ",
           "left join spawning_ground.survey_event as se on s.survey_id = se.survey_id ",
           "left join spawning_ground.survey_design_type_lut as sd on ",
           "se.survey_design_type_id = sd.survey_design_type_id ",
           "left join spawning_ground.mobile_survey_form as ms on s.survey_id = ms.survey_id ",
           "left join spawning_ground.location as loc on s.lower_end_point_id = loc.location_id ",
           "left join spawning_ground.wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where date(survey_datetime) >= '{min_date}' ",
           "and wr.wria_code in ('22', '23') ")

# Checkout connection
con = pg_con_prod("FISH")
#con = poolCheckout(pool)
no_survey_type = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Get rid of fake surveys
no_survey_type = no_survey_type %>%
  filter(!data_submitter_last_name == "Fake")

# Check
table(no_survey_type$survey_type, useNA = "ifany")
table(no_survey_type$wria_code, useNA = "ifany")

# Pull out surveys without survey_type
no_survey_type = no_survey_type %>%
  filter(is.na(survey_type))

# Inspect
unique(no_survey_type$observer_last_name)
unique(no_survey_type$data_submitter_last_name)

# Check for missing parent_record_id...Only three records
any(is.na(no_survey_type$parent_record_id))
chk_no_parent = no_survey_type %>%
  filter(is.na(parent_record_id))

# Check date range
min(no_survey_type$survey_datetime)
max(no_survey_type$survey_datetime)

min(survey_dat$survey_datetime)
max(survey_dat$survey_datetime)

# Split survey data that needs survey_event_id into two sets and
# join by survey_id or parent_record_id as available
parent_join = no_survey_type %>%
  filter(!is.na(parent_record_id)) %>%
  select(-survey_type)
survey_join = no_survey_type %>%
  filter(is.na(parent_record_id)) %>%
  select(-survey_type)

# Verify rows add up
nrow(parent_join) + nrow(survey_join)

# Add survey_type by parent_record_id
pdat = survey_dat %>%
  select(parent_record_id, psurvey_id = survey_id, survey_type,
         count_type, species_id) %>%
  distinct()
any(is.na(pdat$parent_record_id))

# Add to parent_join
parent_join = parent_join %>%
  left_join(pdat, by = "parent_record_id")

# Add survey_type by survey_id
sdat = survey_dat %>%
  select(sparent_record_id = parent_record_id, survey_id, survey_type,
         count_type, species_id) %>%
  distinct()
any(is.na(sdat$survey_id))

# Add to survey_join....No join occurred. Output to excel...only use parent_join
survey_join = survey_join %>%
  left_join(sdat, by = "survey_id")

# # Write to xlsx
# out_name = paste0("data_query/test_queries/NeedManualSurveyType.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NoSurveyType", gridLines = TRUE)
# writeData(wb, sheet = 1, survey_join, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:ncol(survey_join), gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

#==============================================================
# Prepare survey_event table
#==============================================================

# Pull out data for survey_event and fish_encounter tables
sf_event = parent_join %>%
  select(parent_record_id, survey_id, survey_design_type_id = survey_type,
         count_type, species_id) %>%
  distinct()

# Check for missing values
any(is.na(sf_event$parent_record_id))
any(is.na(sf_event$survey_id))
any(is.na(sf_event$species_id))
any(is.na(sf_event$survey_design_type_id))

# Inspect for missing values
chk_missing_type = sf_event %>%
  filter(is.na(survey_design_type_id) |
           is.na(count_type) | is.na(species_id))

# # Write to xlsx
# out_name = paste0("data_query/test_queries/NeedManualSurveyType2.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NoSurveyType", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_missing_type, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:ncol(chk_missing_type), gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Filter out the data with missing values
sf_event = sf_event %>%
  filter(!is.na(survey_design_type_id) &
           !is.na(count_type) & !is.na(species_id))

# Check again for missing values
any(is.na(sf_event$parent_record_id))
any(is.na(sf_event$survey_id))
any(is.na(sf_event$species_id))
any(is.na(sf_event$survey_design_type_id))
unique(sf_event$survey_design_type_id)
unique(sf_event$species_id)

# Convert to values
sf_event = sf_event %>%
  mutate(species = case_when(
    species_id == "e42aa0fc-c591-4fab-8481-55b0df38dcb1" ~ "Chinook",
    species_id == "69d1348b-7e8e-4232-981a-702eda20c9b1" ~ "Chum",
    species_id == "a0f5b3af-fa07-449c-9f02-14c5368ab304" ~ "Coho",
    species_id == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1" ~ "Steelhead")) %>%
  mutate(survey_type = case_when(
    survey_design_type_id == "beb561df-9fd0-4e7d-842f-0c64a515d23b" ~ "Index",
    survey_design_type_id == "d455abb6-e9db-49f2-abc8-2e70890f301b" ~ "Xplor",
    survey_design_type_id == "88b954cb-b7a4-4946-845a-3abd86022fbb" ~ "Supp")) %>%
  mutate(count_type = if_else(count_type == "live", "alive", count_type)) %>%
  select(species, survey_type, count_type, parent_record_id, survey_id,
         survey_design_type_id, species_id) %>%
  distinct() %>%
  arrange(survey_id, species, count_type, survey_type)

# Check
unique(sf_event$survey_type)
any(is.na(sf_event$survey_type))
unique(sf_event$species)
any(is.na(sf_event$species))
unique(sf_event$count_type)

# Select the optimal count type for each species
sfev = sf_event %>%
  group_by(survey_id, species, survey_type) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq == 1L) %>%
  select(-n_seq)

# Check the count type
unique(sfev$count_type)
table(sfev$count_type, useNA = "ifany")

# Add survey_event_id
sfev = sfev %>%
  mutate(survey_event_id = remisc::get_uuid(nrow(sfev))) %>%
  mutate(cwt_detection_method_id = "89a9b6b4-6ea4-44c4-b2e4-e537060e73d3") %>%        # Not applicable
  mutate(run_id = "59e1e01f-3aef-498c-8755-5862c025eafa")                             # Not applicable

# Add run_year
run_dat = header_data %>%
  select(parent_record_id, coho_run_year,
         steelhead_run_year, chum_run_year,
         chinook_run_year)

# Check
any(is.na(run_dat$parent_record_id))
any(is.na(sfev$parent_record_id))

# Add run info
sfev = sfev %>%
  left_join(run_dat, by = "parent_record_id") %>%
  mutate(run_year = case_when(
    species == "Chinook" ~ chinook_run_year,
    species == "Steelhead" ~ steelhead_run_year,
    species == "Chum" ~ chum_run_year,
    species == "Coho" ~ coho_run_year)) %>%
  mutate(run_year = as.integer(run_year)) %>%
  select(species, survey_type, count_type, survey_id,
         survey_design_type_id, species_id, survey_event_id,
         cwt_detection_method_id, run_id, run_year)

# Check run_year
unique(sfev$run_year)
any(is.na(sfev$run_year))

# Add remaining fields
sfev = sfev %>%
  mutate(estimated_percent_fish_seen = NA_integer_) %>%
  mutate(comment_text = NA_character_) %>%
  mutate(created_datetime = with_tz(as.POSIXct(Sys.time()), tzone = "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = as.POSIXct(NA)) %>%
  mutate(modified_by = NA_character_)

# Pull out just the survey_event data
survey_event_prep = sfev %>%
  select(survey_event_id, survey_id, species_id, survey_design_type_id,
         cwt_detection_method_id, run_id, run_year, estimated_percent_fish_seen,
         comment_text, created_datetime, created_by, modified_datetime,
         modified_by) %>%
  distinct()

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

#================================================================================================
# Pull out fish encounter data
#================================================================================================

# Pull out fish_encounter
fish_encounter_prep = sfev %>%
  filter(count_type == "alive") %>%
  select(survey_event_id, created_datetime, created_by,
         modified_datetime, modified_by) %>%
  distinct()

# Add fish_encounter_id
fish_encounter_prep = fish_encounter_prep %>%
  mutate(fish_encounter_id = remisc::get_uuid(nrow(fish_encounter_prep))) %>%
  mutate(fish_location_id = NA_character_) %>%
  mutate(fish_status_id = "6a200904-8a57-4dd5-8c82-2353f91186ac") %>%               # Live
  mutate(sex_id = "b97eaba9-4205-431b-ab09-a34636557666") %>%                       # Not applicable
  mutate(maturity_id = "060732b9-230c-48f3-8264-bcb3c078a6e7") %>%                  # Not applicable
  mutate(origin_id = "80c121fe-da0a-41f9-890f-3a5f39a314bb") %>%                    # Not applicable
  mutate(cwt_detection_status_id = "bd7c5765-2ca3-4ab4-80bc-ce1a61ad8115") %>%      # Not applicable
  mutate(adipose_clip_status_id = "1d61246c-003b-49e9-b2a3-cffdddb3905c") %>%       # Not applicable
  mutate(fish_behavior_type_id = "70454429-724e-4ccf-b8a6-893cafba356a") %>%        # Not applicable
  mutate(mortality_type_id = "149aefd0-0369-4f2c-b85f-4ec6c5e8679c") %>%            # Not applicable
  mutate(fish_encounter_datetime = as.POSIXct(NA)) %>%
  mutate(fish_count = 0L) %>%
  mutate(previously_counted_indicator = 0L) %>%
  select(fish_encounter_id, survey_event_id, fish_location_id, fish_status_id,
         sex_id, maturity_id, origin_id, cwt_detection_status_id,
         adipose_clip_status_id, fish_behavior_type_id, mortality_type_id,
         fish_encounter_datetime, fish_count, previously_counted_indicator,
         created_datetime, created_by, modified_datetime, modified_by)

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

#================================================================================================
# Pull out redd encounter data
#================================================================================================

# Pull out fish_encounter
redd_encounter_prep = sfev %>%
  filter(count_type == "redd") %>%
  select(survey_event_id, created_datetime, created_by,
         modified_datetime, modified_by) %>%
  distinct()

# Add fish_encounter_id
redd_encounter_prep = redd_encounter_prep %>%
  mutate(redd_encounter_id = remisc::get_uuid(nrow(redd_encounter_prep))) %>%
  mutate(redd_location_id = NA_character_) %>%
  mutate(redd_status_id = "59cea5af-5ecc-470f-be20-1c55a3535ab8") %>%               # Not applicable
  mutate(sex_id = "b97eaba9-4205-431b-ab09-a34636557666") %>%                       # Not applicable
  mutate(maturity_id = "060732b9-230c-48f3-8264-bcb3c078a6e7") %>%                  # Not applicable
  mutate(origin_id = "80c121fe-da0a-41f9-890f-3a5f39a314bb") %>%                    # Not applicable
  mutate(redd_encounter_datetime = as.POSIXct(NA)) %>%
  mutate(redd_count = 0L) %>%
  mutate(comment_text = NA_character_) %>%
  select(redd_encounter_id, survey_event_id, redd_location_id, redd_status_id,
         redd_encounter_datetime, redd_count, comment_text, created_datetime,
         created_by, modified_datetime, modified_by)

# Check fish_encounter
any(duplicated(redd_encounter_prep$redd_encounter_id))
any(is.na(redd_encounter_prep$redd_encounter_id))
any(is.na(redd_encounter_prep$survey_event_id))
any(is.na(redd_encounter_prep$redd_status_id))
any(is.na(redd_encounter_prep$redd_count))
any(is.na(redd_encounter_prep$created_datetime))
any(is.na(redd_encounter_prep$created_by))

#================================================================================================
# LOAD TO DBs
#================================================================================================

# Create rd file to hold copy of survey_ids and survey_event_ids in case test fails
type_se_upload_ids = survey_event_prep %>%
  select(survey_id, survey_event_id) %>%
  distinct()

# Output redd_encounter to rds: 724591 records
saveRDS(object = type_se_upload_ids, file = "data/type_se_upload_ids.rds")

#==========================================================================================
# Load data tables to local
#==========================================================================================

#======== Survey event ===============

# survey_event: 2790 rows
db_con = pg_con_local("spawning_ground")
tbl = Id(schema = "public", table = "survey_event")
dbWriteTable(db_con, tbl, survey_event_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== fish data ===============

# fish_encounter: 2789 rows
db_con = pg_con_local("spawning_ground")
tbl = Id(schema = "public", table = "fish_encounter")
dbWriteTable(db_con, tbl, fish_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== redd data ===============

# redd_encounter: 1 rows
db_con = pg_con_local("spawning_ground")
tbl = Id(schema = "public", table = "redd_encounter")
dbWriteTable(db_con, tbl, redd_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#==========================================================================================
# Load data tables to prod
#==========================================================================================

#======== Survey event ===============

# survey_event: 2790 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "survey_event")
dbWriteTable(db_con, tbl, survey_event_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== fish data ===============

# fish_encounter: 2789 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_encounter")
dbWriteTable(db_con, tbl, fish_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== redd data ===============

# redd_encounter: 1 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "redd_encounter")
dbWriteTable(db_con, tbl, redd_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# #===========================================================================================================
# # Get IDs needed to delete freshly uploaded data    edit code !!!!!
# #===========================================================================================================
#
# # Get IDs
# type_se_upload_ids = readRDS("data/type_se_upload_ids.rds")
#
# # Pull out survey_event IDs
# se_id = unique(test_upload_ids$survey_event_id)
# se_id = se_id[!is.na(se_id)]
# se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")
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
# #==========================================================================================
# # Delete test data      edit code!!!!
# #==========================================================================================
#
# #======== fish data ===============
#
# # fish_encounter: 2789 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from fish_encounter where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# #======== redd data ===============
#
# # redd_encounter: 1 rows
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from fish_encounter where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# #======== Survey event ===============
#
# # survey_event: 2790 rows....only deleted 2106 rows Aug 31st...must be ok since all surveys were deleted.
# db_con = pg_con_local("spawning_ground")
# dbExecute(db_con, glue('delete from survey_event where survey_event_id in ({se_ids})'))
# dbDisconnect(db_con)















