#===============================================================================
# Verify queries work
#
# AS 2020-05-20
#===============================================================================

# Load libraries
library(DBI)
library(RSQLite)
library(dplyr)
library(tibble)
library(glue)
library(iformr)
library(stringi)
library(tidyr)
library(lubridate)
library(openxlsx)

# Set data for query
# Profile ID
profile_id = 417821L
since_id = 0L

#===== Get page_ids of forms =======================

# Form names
parent_form_name = "r6_stream_surveys"
other_obs_form_name = "r6_stream_surveys_other_observation"
other_pics_form_name = "r6_stream_surveys_other_pics"
dead_fish_form_name = "r6_stream_surveys_deads"
live_fish_form_name = "r6_stream_surveys_lives"

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

# Get the other obs form id
other_obs_page_id = iformr::get_page_id(
  server_name = "wdfw",
  profile_id = profile_id,
  page_name = other_obs_form_name,
  access_token = access_token)

# Check
other_obs_page_id

# Get the other pics form id
other_pics_page_id = iformr::get_page_id(
  server_name = "wdfw",
  profile_id = profile_id,
  page_name = other_pics_form_name,
  access_token = access_token)

# Check
other_pics_page_id

# Get the dead fish form id
dead_fish_page_id = iformr::get_page_id(
  server_name = "wdfw",
  profile_id = profile_id,
  page_name = dead_fish_form_name,
  access_token = access_token)

# Check
dead_fish_page_id

# Get the dead fish form id
live_fish_page_id = iformr::get_page_id(
  server_name = "wdfw",
  profile_id = profile_id,
  page_name = live_fish_form_name,
  access_token = access_token)

# Check
live_fish_page_id

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
             "from survey as s")
  # Checkout connection
  con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
             "from waterbody_lut as wb ",
             "left join stream as st on wb.waterbody_id = st.waterbody_id ",
             "order by waterbody_display_name")
  con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
  #con = poolCheckout(pool)
  streams = dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  return(streams)
}

# Get existing rm_data
get_mobile_river_miles = function() {
  qry = glue("select distinct loc.location_id, wb.latitude_longitude_id as llid, ",
             "wb.waterbody_id, wb.waterbody_display_name, loc.river_mile_measure as river_mile ",
             "from location as loc ",
             "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
             "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
             "where wria_code in ('22', '23')")
  # Checkout connection
  con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
new_survey_data = function(profile_id, parent_form_page_id, access_token) {
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

# Get new survey_data
new_survey_data = new_survey_data(profile_id, parent_form_page_id, access_token)

# Reactive for missing streams
missing_stream_vals = function(new_survey_data) {
  streams = get_mobile_streams()
  missing_streams = new_survey_data %>%
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
    mutate(lower_coords = gsub("[Latitudeong,:]", "", gps_loc_lower)) %>%
    mutate(lower_coords = gsub("[\n]", ":", lower_coords)) %>%
    mutate(lower_coords = gsub("[\r]", "", lower_coords)) %>%
    mutate(lower_coords = trimws(remisc::get_text_item(lower_coords, 1, ":A"))) %>%
    mutate(upper_coords = gsub("[Latitudeong,:]", "", gps_loc_upper)) %>%
    mutate(upper_coords = gsub("[\n]", ":", upper_coords)) %>%
    mutate(upper_coords = gsub("[\r]", "", upper_coords)) %>%
    mutate(upper_coords = trimws(remisc::get_text_item(upper_coords, 1, ":A"))) %>%
    mutate(iform_llid = if_else(iform_llid %in% c("", "not"), NA_character_, iform_llid)) %>%
    mutate(reach = if_else(reach %in% c("", "not_listed"), NA_character_, reach)) %>%
    select(parent_form_survey_id, survey_date, waterbody_id, stream_name, stream_name_text,
           waterbody_display_name, iform_llid, llid, reach, lower_coords, upper_coords) %>%
    distinct()
  return(missing_streams)
}

# Reactive for missing reaches
missing_reach_vals = function(new_survey_data) {
  streams = get_mobile_streams()
  missing_reaches = new_survey_data %>%
    mutate(lo_rm = trimws(lo_rm)) %>%
    mutate(up_rm = trimws(up_rm)) %>%
    filter(is.na(lo_rm) | is.na(up_rm) | lo_rm == "listed" | up_rm == "listed" |
             lo_rm == "" | up_rm == "") %>%
    select(-c(waterbody_id, stream_geometry_id)) %>%
    rename(waterbody_id = stream_name, iform_llid = llid) %>%
    left_join(streams, by = "waterbody_id") %>%
    mutate(stream_name_text = if_else(!is.na(waterbody_id), waterbody_display_name, stream_name_text)) %>%
    mutate(lower_coords = gsub("[Latitudeong,:]", "", gps_loc_lower)) %>%
    mutate(lower_coords = gsub("[\n]", ":", lower_coords)) %>%
    mutate(lower_coords = gsub("[\r]", "", lower_coords)) %>%
    mutate(lower_coords = trimws(remisc::get_text_item(lower_coords, 1, ":A"))) %>%
    mutate(upper_coords = gsub("[Latitudeong,:]", "", gps_loc_upper)) %>%
    mutate(upper_coords = gsub("[\n]", ":", upper_coords)) %>%
    mutate(upper_coords = gsub("[\r]", "", upper_coords)) %>%
    mutate(upper_coords = trimws(remisc::get_text_item(upper_coords, 1, ":A"))) %>%
    mutate(reach_text = if_else(reach_text == "Not Listed", new_reach, reach_text)) %>%
    mutate(up_rm = if_else(up_rm %in% c("", "listed"), NA_character_, up_rm)) %>%
    mutate(lo_rm = if_else(up_rm %in% c("", "listed"), NA_character_, lo_rm)) %>%
    select(parent_form_survey_id, survey_date, waterbody_id, stream_name_text,
           waterbody_display_name, reach, reach_text, up_rm, lo_rm, lower_coords, upper_coords) %>%
    distinct()
  return(missing_reaches)
}

# Reactive for new end-points needed
add_end_points = function(new_survey_data) {
  add_reach_vals = new_survey_data %>%
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
    mutate(lower_coords = gsub("[Latitudeong,:]", "", gps_loc_lower)) %>%
    mutate(lower_coords = gsub("[\n]", ":", lower_coords)) %>%
    mutate(lower_coords = gsub("[\r]", "", lower_coords)) %>%
    mutate(lower_coords = trimws(remisc::get_text_item(lower_coords, 1, ":A"))) %>%
    mutate(upper_coords = gsub("[Latitudeong,:]", "", gps_loc_upper)) %>%
    mutate(upper_coords = gsub("[\n]", ":", upper_coords)) %>%
    mutate(upper_coords = gsub("[\r]", "", upper_coords)) %>%
    mutate(upper_coords = trimws(remisc::get_text_item(upper_coords, 1, ":A"))) %>%
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

# Run
missing_stream_vals = missing_stream_vals(new_survey_data)
missing_reach_vals = missing_reach_vals(new_survey_data)
add_end_points = add_end_points(new_survey_data)

#======================================================================================
# Get top-level header data
#======================================================================================

# Get header data
get_header_data = function(profile_id, parent_form_page_id, start_id, access_token) {
  # Define fields
  fields = glue::glue("created_date, created_by, created_device_id, modified_date, ",
                      "modified_by, data_entry_type, target_species, survey_date, ",
                      "start_time, observers, stream_name, stream_name_text, ",
                      "new_stream_name, reach, reach_text, new_reach, survey_type, ",
                      "survey_method, survey_direction, clarity_ft, clarity_code, ",
                      "weather, flow, stream_conditions, no_survey, reachsplit_yn, ",
                      "user_name, headerid, end_time, header_comments, stream_temp, ",
                      "survey_completion_status, chinook_count_type, coho_count_type, ",
                      "steelhead_count_type, chum_count_type, gps_loc_lower, ",
                      "gps_loc_upper, coho_run_year, steelhead_run_year, chum_run_year, ",
                      "chinook_run_year, carcass_tagging, code_reach")
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

# Test...currently 1910 records....approx 4.0 seconds
# Get start_id
# Pull out start_id
start_id = min(new_survey_data$parent_form_survey_id) - 1
# start_id = new_survey_counts$first_id -1
strt = Sys.time()
header_data = get_header_data(profile_id, parent_form_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Check some date and time values
any(is.na(header_data$survey_date))
# chk_date = header_data %>%
#   filter(is.na(survey_date))
# # TEMPORARY FIX !!!!!!!!!!!!
# header_data = header_data %>%
#   filter(!is.na(survey_date))

# Process dates etc
# Rename id to parent_record_id for more explicit joins to subform data...convert dates, etc.
header_data = header_data %>%
  rename(parent_record_id = id, survey_uuid = headerid) %>%
  mutate(created_datetime = as.POSIXct(iformr::idate_time(created_date))) %>%
  mutate(modified_datetime = as.POSIXct(iformr::idate_time(modified_date))) %>%
  mutate(survey_start_datetime = as.POSIXct(paste0(survey_date, " ", start_time), tz = "America/Los_Angeles")) %>%
  mutate(survey_end_datetime = as.POSIXct(paste0(survey_date, " ", end_time), tz = "America/Los_Angeles")) %>%
  mutate(created_datetime = with_tz(created_datetime, tzone = "UTC")) %>%
  mutate(modified_datetime = with_tz(modified_datetime, tzone = "UTC")) %>%
  mutate(survey_start_datetime = with_tz(survey_start_datetime, tzone = "UTC")) %>%
  mutate(survey_end_datetime = with_tz(survey_end_datetime, tzone = "UTC")) %>%
  # Temporary fix for target species...mostly key values...but a couple with names !!!!!!!
  mutate(target_species = case_when(
    target_species == "Chinook" ~ "e42aa0fc-c591-4fab-8481-55b0df38dcb1",
    target_species == "Chum" ~ "69d1348b-7e8e-4232-981a-702eda20c9b1",
    !target_species %in% c("Chinook", "Chum") ~ target_species)) %>%
  mutate(created_by = stringi::stri_replace_all_fixed(observers, "@dfw.wa.gov", "")) %>%
  mutate(observers = stringi::stri_replace_all_fixed(observers, "@dfw.wa.gov", "")) %>%
  mutate(observers = stringi::stri_replace_all_fixed(observers, ".", " ")) %>%
  mutate(observers = stringi::stri_trans_totitle(observers)) %>%
  select(parent_record_id, survey_uuid, created_datetime, created_by, modified_datetime,
         modified_by, created_device_id, data_entry_type, target_species, survey_date,
         survey_start_datetime, survey_end_datetime, observers, stream_name, stream_name_text,
         new_stream_name, reach, reach_text, new_reach, survey_type, survey_method,
         survey_direction, clarity_ft, clarity_code, weather, flow,stream_conditions,
         no_survey, reachsplit_yn, user_name, end_time, header_comments,
         stream_temp, survey_completion_status, chinook_count_type, coho_count_type,
         steelhead_count_type, chum_count_type, gps_loc_lower, gps_loc_upper,
         coho_run_year, steelhead_run_year, chum_run_year, chinook_run_year,
         carcass_tagging, code_reach)

# CHECKS -------------------------------------------------------

# Check some values
unique(header_data$observers)
unique(header_data$data_entry_type)
unique(header_data$reachsplit_yn)

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
unique(header_data$carcass_tagging)

# Pull out missing required fields....now zero
missing_req = header_data %>%
  filter(is.na(survey_date) | is.na(survey_type) | is.na(survey_method)) %>%
  select(parent_record_id, created_by, survey_date, observers, stream_name_text,
         reach_text, survey_type, survey_method)
#write.csv(missing_req, "data/missing_data.csv")

# Check
any(is.na(header_data$survey_uuid))
any(header_data$survey_uuid == "")
unique(nchar(header_data$survey_uuid))
min(header_data$parent_record_id)

# Generate new survey_id where missing...pull into separate datasets
no_survey_id = header_data %>%
  filter(is.na(survey_uuid))

with_survey_id = header_data %>%
  filter(!is.na(survey_uuid))

# Add uuid
no_survey_id = no_survey_id %>%
  mutate(survey_uuid = remisc::get_uuid(nrow(no_survey_id)))

# Combine
header_data = rbind(no_survey_id, with_survey_id)

# Check
any(is.na(header_data$survey_uuid))
any(header_data$survey_uuid == "")
unique(nchar(header_data$survey_uuid))
any(duplicated(header_data$survey_uuid))
min(header_data$parent_record_id)

# Pull out data with duplicated survey_uuid
chk_uuid = header_data %>%
  filter(duplicated(survey_uuid)) %>%
  select(survey_uuid) %>%
  left_join(header_data, by = "survey_uuid")

# STOPPED HERE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


























