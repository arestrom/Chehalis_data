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

#==========================================================================
# Functions from mobile_import_global.R
#==========================================================================

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

# # Get existing rm_data
# get_mobile_river_miles = function() {
#   qry = glue("select distinct loc.location_id, wb.latitude_longitude_id as llid, ",
#              "wb.waterbody_id, wb.waterbody_display_name, loc.river_mile_measure as rm ",
#              "from location as loc ",
#              "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
#              "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
#              "where wria_code in ('22', '23')")
#   # Checkout connection
#   con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
#   #con = poolCheckout(pool)
#   rm_data = DBI::dbGetQuery(con, qry)
#   DBI::dbDisconnect(con)
#   #poolReturn(con)
#   # Dump any data without RMs or llid
#   rm_data = rm_data %>%
#     filter(!is.na(llid) & !is.na(rm))
#   return(rm_data)
# }

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
    mutate(lower_comment = if_else(!is.na(lower_end_point_id), "have_point", "need_point")) %>%
    mutate(upper_comment = if_else(!is.na(upper_end_point_id), "have_point", "need_point")) %>%
    select(parent_form_survey_id, iform_waterbody_id = stream_name, iform_llid = llid,
           db_lo_wbid = lo_wbid, db_up_wbid = up_wbid, iform_stream_name_text = stream_name_text,
           db_lo_name = lo_name, db_up_name = up_name, reach_text, lo_rm, up_rm, lower_comment,
           upper_comment) %>%
    distinct() %>%
    arrange(iform_stream_name_text, lo_rm, up_rm)
  return(no_reach_point)
}

# Run
missing_stream_vals = missing_stream_vals(new_survey_data)
missing_reach_vals = missing_reach_vals(new_survey_data)
add_end_points = add_end_points(new_survey_data)


































