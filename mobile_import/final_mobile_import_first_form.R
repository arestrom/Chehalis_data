#===============================================================================
# Verify queries work
#
# Notes:
#  1. Look for split surveys !!!! Hopefully none.
#
# ToDo:
#  1.
#  2. Add survey_type, origin, and run to each entry in survey that has a
#     survey_intent and species entered. Use "Unknown" for both origin
#     and survey type...Based on directions from Lea. I will separate
#     origin into categories based on species and date for Kim and Curt
#     when exporting to Curts format.
#  3. Harmonize cwt_detection_method for any cases where there are more
#     than one combination of species and cwt_detection_method for a
#     given species. In that case assign to method = electronic if
#     present?
#  4. Make sure redd_name is transferred to all entries of old redds
#  5. Change old redd run designations to Unknown to be consistent
#     with new redds, live, dead, etc, and only end up with one
#     survey_event table entry.
#  6.
#  7. Need a zero for each case where a species was intended but
#     none were encountered. Needed for Curt Holt script to work correctly.
#     See: intent_zeros.R in \data_query\test_queries\
#  8. Carefully inspect code where species are assigned to previous redds
#     based on what was assigned initally to the new redd that shares
#     the same ReddID. In the iform there are now cases where those
#     redd_species have been assigned incorrectly.
#  9. Several not observable surveys were entered where the header_id
#     was copied from another already existing survey. These all need
#     to be given new unique header_ids. Maybe just pull out the
#     second instance for each case, then assign a new id, verify
#     that in each case the parent_form_id is unique. If so then
#     join the new header_id back into the header data based on the
#     parent_form_id
#
#  Successfully loaded final batch from inital iform on 2021-04-  at  PM
#
# AS 2021-04-22
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

# Set data for query
# Profile ID
profile_id = 417821L

# Set since_id...will be updated later based on parent_form_survey_ids in SG
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
other_obs_form_name = "r6_stream_surveys_other_observation"
other_pics_form_name = "r6_stream_surveys_other_pics"
dead_fish_form_name = "r6_stream_surveys_deads"
live_fish_form_name = "r6_stream_surveys_lives"
redds_form_name = "r6_stream_surveys_redds"
recaps_form_name = "r6_tagged_carcass_recoveries"

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

# Get the dead fish form id
redds_page_id = iformr::get_page_id(
  server_name = "wdfw",
  profile_id = profile_id,
  page_name = redds_form_name,
  access_token = access_token)

# Check
redds_page_id

# Get the dead fish form id
recaps_page_id = iformr::get_page_id(
  server_name = "wdfw",
  profile_id = profile_id,
  page_name = recaps_form_name,
  access_token = access_token)

# Check
recaps_page_id

#===================================================================================================
# Initial check section to get survey counts and verify streams and reach points are all present
#===================================================================================================

#============================================
# Functions from mobile_import_global.R
#============================================

# Get new survey data...updated to use FISH prod
get_new_surveys = function(profile_id, parent_form_page_id, access_token) {
  # Define query for new mobile surveys
  qry = glue("select distinct ms.survey_id, ms.parent_form_survey_id ",
             "from spawning_ground.mobile_survey_form as ms ",
             "order by parent_form_survey_id desc")
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
  start_id = max(existing_surveys$parent_form_survey_id)
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
             "order by wb.waterbody_display_name")
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
             "where wr.wria_code in ('22', '23')")
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

# Get new survey_data: currently 2236 records
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

# Run: 2236 records
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

# # Export missing stream vals
# num_cols = ncol(missing_stream_vals)
# out_name = paste0("data/MissingStreams_2021-02-18.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "MissingStreams", gridLines = TRUE)
# writeData(wb, sheet = 1, missing_stream_vals, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)
#
# # Export add_end_points
# num_cols = ncol(add_end_points)
# out_name = paste0("data/AddEndPoints_2021-02-18.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "AddEndPoints", gridLines = TRUE)
# writeData(wb, sheet = 1, add_end_points, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)
#
# # For Lea to verify...this time only...she thought 6348 was earliest survey for this final batch
# # Am pulling only survey before that for her to inspect.
# earliest_surveys = new_survey_data %>%
#   filter(parent_form_survey_id < 6348)
# paste0(paste0("'", unique(earliest_surveys$survey_id), "'"), collapse = ", ")
#
# # Export add_end_points
# num_cols = ncol(earliest_surveys)
# out_name = paste0("data/EarliestSurveys_2021-02-18.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "FirstSurveys", gridLines = TRUE)
# writeData(wb, sheet = 1, earliest_surveys, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

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

# Test...currently 2236 records....approx 4.8 seconds
# Get start_id
# Pull out start_id
start_id = min(new_survey_data$parent_form_survey_id) - 1
# start_id = new_survey_counts$first_id -1
strt = Sys.time()
header_data = get_header_data(profile_id, parent_form_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Check some date and time values
any(is.na(header_data$survey_date))

#===============================================================================
# Need to reconfirm with Lea below. New 2019 surveys, plus others have been added
# Need to update run_year using a rule. HACK WARNING
#===============================================================================

# Inspect run year
min(header_data$survey_date)
max(header_data$survey_date)
table(header_data$steelhead_run_year, useNA = "ifany")
table(header_data$coho_run_year, useNA = "ifany")
table(header_data$chum_run_year, useNA = "ifany")
table(header_data$chinook_run_year, useNA = "ifany")

# More inspections
chk_sthd_runyr = header_data %>%
  mutate(year_month = substr(survey_date, 1, 7)) %>%
  mutate(corrected_run_year = NA_character_) %>%
  select(year_month, steelhead_count_type, steelhead_run_year, corrected_run_year) %>%
  distinct() %>%
  arrange(year_month)
chk_chum_runyr = header_data %>%
  mutate(year_month = substr(survey_date, 1, 7)) %>%
  mutate(corrected_run_year = NA_character_) %>%
  select(year_month, chum_count_type, chum_run_year, corrected_run_year) %>%
  distinct() %>%
  arrange(year_month)
chk_coho_runyr = header_data %>%
  mutate(year_month = substr(survey_date, 1, 7)) %>%
  mutate(corrected_run_year = NA_character_) %>%
  select(year_month, coho_count_type, coho_run_year, corrected_run_year) %>%
  distinct() %>%
  arrange(year_month)
chk_chin_runyr = header_data %>%
  mutate(year_month = substr(survey_date, 1, 7)) %>%
  mutate(corrected_run_year = NA_character_) %>%
  select(year_month, chinook_count_type, chinook_run_year, corrected_run_year) %>%
  distinct() %>%
  arrange(year_month)

# # Need to verify run_year assignments with Lea
# num_cols = ncol(chk_sthd_runyr)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "CheckSthdRunYr.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "CheckRunYear", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_sthd_runyr, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)
#
# # Need to verify run_year assignments with Lea
# num_cols = ncol(chk_chum_runyr)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "CheckChumRunYr.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "CheckRunYear", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_chum_runyr, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)
#
# # Need to verify run_year assignments with Lea
# num_cols = ncol(chk_coho_runyr)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "CheckCohoRunYr.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "CheckRunYear", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_coho_runyr, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)
#
# # Need to verify run_year assignments with Lea
# num_cols = ncol(chk_chin_runyr)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "CheckChinRunYr.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "CheckRunYear", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_chin_runyr, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# header_data = header_data %>%
#   mutate(survey_run_month = as.integer(substr(survey_date, 6, 7))) %>%
#   mutate(survey_run_year = as.integer(substr(survey_date, 1, 4))) %>%
#   # mutate(steelhead_run_year = "2021") %>%
#   mutate(coho_run_year = case_when(
#     survey_run_year == 2020 ~ "2020",
#     survey_run_year == 2021 & survey_run_month < 4 ~ "2020",
#     survey_run_year == 2021 & survey_run_month <= 4 ~ "2020",
#     TRUE ~ "2020")) %>%
#   mutate(chum_run_year = case_when(
#     survey_run_year == 2020 ~ "2020",
#     survey_run_year == 2021 & survey_run_month < 4 ~ "2020",
#     survey_run_year == 2021 & survey_run_month <= 4 ~ "2020",
#     TRUE ~ "2020")) %>%
#   mutate(chinook_run_year = case_when(
#     survey_run_year == 2020 ~ "2020",
#     survey_run_year == 2021 & survey_run_month < 4 ~ "2020",
#     survey_run_year == 2021 & survey_run_month <= 4 ~ "2020",
#     TRUE ~ "2020"))
#===============================================================================

# HACK FOR NOW....FILL IN BLANKS
header_data = header_data %>%
  mutate(chinook_run_year = if_else(is.na(chinook_run_year), substr(survey_date, 1, 4), chinook_run_year)) %>%
  mutate(chum_run_year = if_else(is.na(chum_run_year), substr(survey_date, 1, 4), chum_run_year)) %>%
  mutate(coho_run_year = if_else(is.na(coho_run_year), substr(survey_date, 1, 4), coho_run_year)) %>%
  mutate(steelhead_run_year = if_else(is.na(steelhead_run_year), substr(survey_date, 1, 4), steelhead_run_year))

# Check
table(header_data$steelhead_run_year, useNA = "ifany")
table(header_data$coho_run_year, useNA = "ifany")
table(header_data$chum_run_year, useNA = "ifany")
table(header_data$chinook_run_year, useNA = "ifany")

# Process dates etc
# Rename id to parent_record_id for more explicit joins to subform data...convert dates, etc.
header_data = header_data %>%
  rename(parent_record_id = id, survey_uuid = headerid) %>%
  mutate(created_datetime = as.POSIXct(iformr::idate_time(created_date), tz = "America/Los_Angeles")) %>%
  mutate(modified_datetime = as.POSIXct(iformr::idate_time(modified_date), tz = "America/Los_Angeles")) %>%
  mutate(survey_start_datetime = as.POSIXct(paste0(survey_date, " ", start_time), tz = "America/Los_Angeles")) %>%
  mutate(survey_end_datetime = as.POSIXct(paste0(survey_date, " ", end_time), tz = "America/Los_Angeles")) %>%
  mutate(created_datetime = with_tz(created_datetime, tzone = "UTC")) %>%
  mutate(modified_datetime = with_tz(modified_datetime, tzone = "UTC")) %>%
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

# Pull out missing required fields....now none
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

# Check for dups in key IDs....Since no dups in parent_id use to assign new header_id
any(duplicated(header_data$survey_uuid))
any(duplicated(header_data$parent_record_id))

# Pull out cases where header_id is duplicated
dup_head_id = header_data %>%
  arrange(parent_record_id, created_by) %>%
  group_by(survey_uuid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1L) %>%
  select(parent_record_id, survey_uuid,
         created_datetime, created_by)

# Check
any(duplicated(header_data$parent_record_id))
any(duplicated(dup_head_id$parent_record_id))

# Generate new header_id for duplicated cases
new_head_id = dup_head_id %>%
  mutate(new_header_id = get_uuid(nrow(dup_head_id))) %>%
  select(parent_record_id, new_header_id)

# Check
any(duplicated(new_head_id$new_header_id))

# Join to header_data by parent_record_id to assign new value
header_data = header_data %>%
  left_join(new_head_id, by = "parent_record_id") %>%
  mutate(survey_uuid = if_else(!is.na(new_header_id), new_header_id, survey_uuid))

# Check
any(duplicated(header_data$survey_uuid))
any(is.na(header_data$survey_uuid))
any(header_data$survey_uuid == "")
unique(nchar(header_data$survey_uuid))
min(header_data$parent_record_id)

#---------- END OF CHECKS -----------------------------------------------------

# Generate created and modified data
header_data = header_data %>%
  mutate(mod_diff = modified_datetime - created_datetime) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(substr(modified_by, 1, 3) %in% c("VAR", "FW0"),
                               NA_character_, modified_by)) %>%
  mutate(modified_by = stringi::stri_replace_all_fixed(modified_by, ".", "_")) %>%
  mutate(modified_by = remisc::get_text_item(modified_by, 1, "_")) %>%
  mutate(created_by = if_else(is.na(created_by) & !is.na(modified_by),
                              modified_by, created_by)) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), modified_datetime)) %>%
  mutate(created_by = if_else(is.na(created_by), "lea.ronne", created_by))

# Survey =========================================

# Generate survey and associated tables
survey_prep = header_data %>%
  mutate(data_source_id = "c40563fa-efce-4cac-a2e3-6223a5e24030") %>%           # WDFW
  mutate(data_source_unit_id = "e2d51ceb-398c-49cb-9aa5-d20a839e9ad9") %>%      # Not applicable
  mutate(data_review_status_id = "b0ea75d4-7e77-4161-a533-5b3fce38ac2a") %>%    # Preliminary
  mutate(data_submitter_last_name = observers) %>%
  # Set incomplete_survey type to not_applicable if survey was marked as completed...otherwise use no_survey key
  mutate(no_survey = if_else(survey_completion_status == "d192b32e-0e4f-4719-9c9c-dec6593b1977",
                             "cde5d9fb-bb33-47c6-9018-177cd65d15f5", no_survey)) %>%
  select(parent_record_id, stream_name, stream_name_text, reach_text,
         survey_id = survey_uuid, survey_datetime = survey_date, data_source_id,
         data_source_unit_id, survey_method, data_review_status_id,
         stream_name, reach, survey_completion_status_id = survey_completion_status,
         incomplete_survey_type_id = no_survey, survey_start_datetime,
         survey_end_datetime, observers, data_submitter_last_name,
         created_datetime, created_by, modified_datetime, modified_by)

# Identify the upper and lower end point location_ids
survey_prep = survey_prep %>%
  mutate(llid = remisc::get_text_item(reach, 1, "_")) %>%
  mutate(reach = remisc::get_text_item(reach, 2, "_")) %>%
  mutate(lo_rm = as.numeric(remisc::get_text_item(reach, 1, "-"))) %>%
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

# Pull out cases where no match exists
no_reach_point = survey_prep %>%
  filter(is.na(lower_end_point_id) | is.na(upper_end_point_id)) %>%
  distinct() %>%
  arrange(stream_name_text, lo_rm, up_rm)

# Format for output
need_reach_point = no_reach_point %>%
  select(parent_record_id, stream_name_text, reach_text, reach, survey_datetime,
         observers, created_by, modified_by, llid, lo_rm, up_rm, lower_end_point_id,
         upper_end_point_id)

# Warn if any missing
if ( nrow(need_reach_point) > 0L ) {
  cat("\nWARNING: Some end-points still missing. Do not pass go!\n\n")
  # Output to excel
  num_cols = ncol(need_reach_point)
  current_date = format(Sys.Date())
  out_name = paste0("data/NeedReachPoint_", current_date, ".xlsx")
  wb <- createWorkbook(out_name)
  addWorksheet(wb, "NeedReachPoint", gridLines = TRUE)
  writeData(wb, sheet = 1, need_reach_point, rowNames = FALSE)
  ## create and add a style to the column headers
  headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                             fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
  addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
  saveWorkbook(wb, out_name, overwrite = TRUE)
} else {
  cat("\nAll end-points now present. Ok to proceed.\n\n")
}

# Get location info for adding to tables below
survey_loc_info = survey_prep %>%
  select(parent_record_id, survey_id, lo_wria_id, up_wria_id,
         lo_wb_id, up_wb_id) %>%
  mutate(wria_id = if_else(lo_wria_id == up_wria_id, lo_wria_id, NA_character_)) %>%
  mutate(waterbody_id = if_else(lo_wb_id == up_wb_id, lo_wb_id, NA_character_))

# Warn if any missing wria_id or waterbody_id
if ( any(is.na(survey_loc_info$wria_id)) | any(is.na(survey_loc_info$waterbody_id)) ) {
  cat("\nWARNING: Some surveys conducted on different streams. Do not pass go!\n\n")
} else {
  cat("\nAll stream and wria_ids match. Ok to proceed.\n\n")
  survey_loc_info = survey_loc_info %>%
    select(parent_record_id, survey_id, wria_id, waterbody_id)
}

# Finalize survey table
survey_prep = survey_prep %>%
  mutate(observer_last_name = remisc::get_text_item(observers, 2, " ")) %>%
  mutate(data_submitter_last_name = remisc::get_text_item(data_submitter_last_name, 2, " ")) %>%
  select(survey_id, survey_datetime, data_source_id, data_source_unit_id,
         survey_method_id = survey_method, data_review_status_id,
         upper_end_point_id, lower_end_point_id, survey_completion_status_id,
         incomplete_survey_type_id, survey_start_datetime,
         survey_end_datetime, observer_last_name, data_submitter_last_name,
         created_datetime, created_by, modified_datetime, modified_by)

# Check
any(is.na(survey_prep$survey_id))
any(is.na(survey_prep$survey_datetime))
any(is.na(survey_prep$data_source_id))
any(is.na(survey_prep$data_source_unit_id))
any(is.na(survey_prep$data_review_status_id))
any(is.na(survey_prep$upper_end_point_id))
any(is.na(survey_prep$lower_end_point_id))
any(is.na(survey_prep$survey_completion_status_id))
any(is.na(survey_prep$incomplete_survey_type_id))
any(is.na(survey_prep$created_datetime))
any(is.na(survey_prep$created_by))

# # Pull out cases with no entry in incomplete_survey_type
# chk_incomplete = survey_prep %>%
#   filter(is.na(incomplete_survey_type_id))
#
# # Pull out cases with no entry in incomplete_survey_type
# chk_created = survey_prep %>%
#   filter(is.na(created_by))

# Survey comment ================================

# Pull out survey_comment data
comment_prep = header_data %>%
  mutate(survey_id = survey_uuid) %>%
  mutate(area_surveyed_id = NA_character_) %>%
  mutate(fish_abundance_condition_id = NA_character_) %>%
  mutate(stream_condition_id = NA_character_) %>%
  mutate(stream_flow_type_id = flow) %>%
  mutate(survey_count_condition_id = NA_character_) %>%
  mutate(survey_direction_id = survey_direction) %>%
  mutate(survey_timing_id = NA_character_) %>%
  mutate(visibility_condition_id = NA_character_) %>%
  mutate(visibility_type_id = stream_conditions) %>%
  mutate(weather_type_id = weather) %>%
  mutate(comment_text = header_comments) %>%
  select(survey_id, area_surveyed_id, fish_abundance_condition_id,
         stream_condition_id, stream_flow_type_id, survey_count_condition_id,
         survey_direction_id, survey_timing_id, visibility_condition_id,
         visibility_type_id, weather_type_id, comment_text, created_datetime,
         created_by, modified_datetime, modified_by) %>%
  distinct()

# Get rid of empty rows
comment_prep = comment_prep %>%
  mutate(comment_text = trimws(comment_text)) %>%
  mutate(del_row = if_else(
    is.na(stream_flow_type_id) &
      is.na(survey_direction_id) &
      is.na(visibility_type_id) &
      is.na(weather_type_id) &
      (is.na(comment_text) | comment_text == ""), "yes", "no"))

# Check...for curiosity
table(comment_prep$del_row, useNA = "ifany")

# Remove empty rows
comment_prep = comment_prep %>%
  filter(del_row == "no") %>%
  select(-del_row)

# Check for missing or duplicated survey_ids
any(is.na(comment_prep$survey_id))
any(duplicated(comment_prep$survey_id))

# Finalize
comment_prep = comment_prep %>%
  mutate(survey_comment_id = remisc::get_uuid(nrow(comment_prep))) %>%
  select(survey_comment_id, survey_id, area_surveyed_id, fish_abundance_condition_id,
         stream_condition_id, stream_flow_type_id, survey_count_condition_id,
         survey_direction_id, survey_timing_id, visibility_condition_id,
         visibility_type_id, weather_type_id, comment_text, created_datetime,
         created_by, modified_datetime, modified_by)

# Check
any(is.na(comment_prep$survey_comment_id))
any(is.na(comment_prep$survey_id))
any(is.na(comment_prep$created_datetime))
any(is.na(comment_prep$created_by))

# Survey intent ================================
# A zero is needed for every species that was intended to be counted on each survey
# This is to ensure that a survey_type is entered for each intended species, regardless
# of whether any were encountered. Need survey_type for curt_holt_exports

# Pull out survey_intent data
intent_prep = header_data %>%
  mutate(survey_id = survey_uuid) %>%
  select(survey_id, target_species, chinook_count_type, coho_count_type,
         steelhead_count_type, chum_count_type, created_datetime,
         created_by, modified_datetime, modified_by)

# Check target_species
unique(intent_prep$target_species)

# Get all intent data in DB
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

# Add to intent_prep
intent_prep = intent_prep %>%
  left_join(get_mobile_intent(), by = "target_species")

# Check
unique(intent_prep$target_species_name)

# Split and combine
chinook_intent = intent_prep %>%
  mutate(common_name = "Chinook salmon") %>%
  select(survey_id, common_name, target_species_name, count_type = chinook_count_type,
         created_datetime, created_by, modified_datetime, modified_by)

# Split and combine
coho_intent = intent_prep %>%
  mutate(common_name = "Coho salmon") %>%
  select(survey_id, common_name, target_species_name, count_type = coho_count_type,
         created_datetime, created_by, modified_datetime, modified_by)

# Split and combine
sthd_intent = intent_prep %>%
  mutate(common_name = "Steelhead trout") %>%
  select(survey_id, common_name, target_species_name, count_type = steelhead_count_type,
         created_datetime, created_by, modified_datetime, modified_by)

# Split and combine
chum_intent = intent_prep %>%
  mutate(common_name = "Chum salmon") %>%
  select(survey_id, common_name, target_species_name, count_type = chum_count_type,
         created_datetime, created_by, modified_datetime, modified_by)

# Get surveys where no survey for species was intended
no_survey_intent = rbind(chinook_intent, coho_intent, sthd_intent, chum_intent) %>%
  filter(count_type == "no_survey") %>%
  arrange(survey_id)

# Combine
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
  select(survey_intent_id, survey_id, species_id, count_type_id,
         created_datetime, created_by, modified_datetime, modified_by)

# Check
any(is.na(intent_prep$survey_intent_id))
any(is.na(intent_prep$survey_id))
any(is.na(intent_prep$species_id))
any(is.na(intent_prep$count_type_id))
any(is.na(intent_prep$created_datetime))
any(is.na(intent_prep$created_by))

# Waterbody measurements ================================

# Check stream_temp column
unique(header_data$stream_temp)
unique(header_data$clarity_code)

# Save corrections in case need to correct again later
clarity_type_corrections = header_data %>%
  select(survey_id = survey_uuid, clarity_code) %>%
  filter(clarity_code %in% c("1", "2", "3"))

# Check occurances
table(header_data$clarity_code, useNA = "ifany")

# Pull out waterbody_meas data
waterbody_meas_prep = header_data %>%
  mutate(stream_flow_measurement_cfs = NA_integer_) %>%
  mutate(start_water_temperature_datetime = as.POSIXct(NA)) %>%
  mutate(start_water_temperature_celsius = NA_real_) %>%
  mutate(end_water_temperature_datetime = as.POSIXct(NA)) %>%
  mutate(end_water_temperature_celsius = NA_real_) %>%
  mutate(waterbody_ph = NA_real_) %>%
  filter(!is.na(clarity_ft)) %>%
  mutate(water_clarity_meter = as.numeric(clarity_ft) * 0.3048) %>%
  mutate(water_clarity_meter = round(water_clarity_meter, digits = 2)) %>%
  mutate(clarity_code = case_when(
    clarity_code == "1" ~ "03f7595f-1a18-47a2-ae5c-e5e817a25080",               # To bottom
    clarity_code == "2" ~ "37d458a2-7dc9-48a3-92c3-dd7928a4dc53",               # Actual depth greater
    clarity_code == "3" ~ "ad71c310-3d86-41bd-8b86-ff76f6373c94",               # Actual depth meas
    clarity_code == "03f7595f-1a18-47a2-ae5c-e5e817a25080" ~ "03f7595f-1a18-47a2-ae5c-e5e817a25080",
    clarity_code == "37d458a2-7dc9-48a3-92c3-dd7928a4dc53" ~ "37d458a2-7dc9-48a3-92c3-dd7928a4dc53",
    clarity_code == "ad71c310-3d86-41bd-8b86-ff76f6373c94" ~ "ad71c310-3d86-41bd-8b86-ff76f6373c94",
    is.na(clarity_code) ~ "282d8ea4-4e78-4c4d-be05-dfd7fa457c09")) %>%          # No data
  select(survey_id = survey_uuid, water_clarity_type_id = clarity_code,
         water_clarity_meter, stream_flow_measurement_cfs, start_water_temperature_datetime,
         start_water_temperature_celsius, end_water_temperature_datetime,
         end_water_temperature_celsius, waterbody_ph, created_datetime,
         created_by, modified_datetime, modified_by) %>%
  distinct()

# Check occurances
table(waterbody_meas_prep$water_clarity_type_id, useNA = "ifany")

# Check
any(is.na(waterbody_meas_prep$survey_id))
any(is.na(waterbody_meas_prep$water_clarity_type_id))
any(is.na(waterbody_meas_prep$water_clarity_meter))

# Mobile survey form ================================

# Pull out data
mobile_survey_form_prep = header_data %>%
  mutate(mobile_survey_form_id = remisc::get_uuid(nrow(header_data))) %>%
  mutate(parent_form_name = parent_form_name) %>%
  mutate(parent_form_id = parent_form_page_id) %>%
  select(mobile_survey_form_id, survey_id = survey_uuid,
         parent_form_survey_id = parent_record_id,
         parent_form_survey_guid = survey_uuid, parent_form_name,
         parent_form_id, created_datetime, created_by,
         modified_datetime, modified_by) %>%
  arrange(parent_form_survey_id)

# Check
any(is.na(mobile_survey_form_prep$survey_id))
any(is.na(mobile_survey_form_prep$parent_form_survey_id))
any(is.na(mobile_survey_form_prep$parent_form_survey_guid))

#======================================================================================================================
# Other observations
#======================================================================================================================

# Function to get other observations
get_other_obs = function(profile_id, other_obs_page_id, start_id, access_token) {
  # Define fields
  fields = glue::glue("parent_page_id, parent_element_id, created_date, ",
                      "created_by, created_location, created_device_id, modified_date, ",
                      "modified_by, modified_location, modified_device_id, server_modified_date, ",
                      "observation_type, miscellaneous_observation_name, known_total_barrier, ",
                      "barrier_descrip, barrier_ht, barrier_ht_meas_est, pp_depth_of_barrier, ",
                      "barrier_pp_depth_meas_est, observation_details, observation_location, ",
                      "observation_gps_warning_select, garmin_waypoint, garmin_waypoint_accuracy, ",
                      "gps_latitude, gps_longitude,gps_accuracy, gps_elevation, comments, ",
                      "headerid_fkey, sgs_observationid, observation_row, observation_name, ",
                      "poor_accuracy_counter, poor_accuracy_counter_2")
  # Order by id ascending and get all records after start_id
  field_string = glue('id:<, parent_record_id(>"{start_id}"), {fields}')
  # Loop through all survey records
  other_obs = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = other_obs_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = since_id)
  return(other_obs)
}

# New access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Set start_id as the minimum parent_record_id minus one
start_id = min(header_data$parent_record_id) - 1

# Test...currently 280 records
strt = Sys.time()
other_obs = get_other_obs(profile_id, other_obs_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Set start_id as the minimum parent_record_id minus one
start_id = min(other_obs$id) - 1

# Function to get other observation pictures
get_other_pictures = function(profile_id, other_pics_page_id, start_id, access_token) {
  fields = glue::glue("parent_page_id, parent_element_id, created_date, created_by, ",
                      "created_location, created_device_id, modified_date, modified_by, ",
                      "modified_location, modified_device_id, server_modified_date, ",
                      "pics, comments")
  # Order by id ascending and get all records after start_id
  field_string = glue('id:<, parent_record_id(>"{start_id}"), {fields}')
  # Loop through all survey records
  other_pics = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = other_pics_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = since_id)
  return(other_pics)
}

# Test...currently 221 records
strt = Sys.time()
other_pictures = get_other_pictures(profile_id, other_pics_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Format other obs
other_obs = other_obs %>%
  mutate(created_datetime = as.POSIXct(iformr::idate_time(created_date), tz = "America/Los_Angeles")) %>%
  mutate(modified_datetime = as.POSIXct(iformr::idate_time(modified_date), tz = "America/Los_Angeles")) %>%
  mutate(created_datetime = with_tz(created_datetime, tzone = "UTC")) %>%
  mutate(modified_datetime = with_tz(modified_datetime, tzone = "UTC")) %>%
  mutate(obs_lat = as.numeric(gps_latitude)) %>%
  mutate(obs_lon = as.numeric(gps_longitude)) %>%
  mutate(obs_acc = as.numeric(gps_accuracy))

# Add survey_id to other_obs....use survey_id from header_data
s_id = header_data %>%
  select(parent_record_id, survey_id = survey_uuid,
         head_create_by = created_by, head_mod_by = modified_by) %>%
  distinct()

# Check
any(is.na(s_id$parent_record_id))
any(is.na(s_id$survey_id))

# Join to other_obs
other_obs = other_obs %>%
  left_join(s_id, by = "parent_record_id")

# Check
any(is.na(other_obs$survey_id))

# Add survey_id to other_pictures....use survey_id from other_obs
sp_id = other_obs %>%
  select(other_obs_id = id, survey_id,
         head_create_by, head_mod_by) %>%
  distinct()

# Check
any(is.na(sp_id$other_obs_id))
any(is.na(sp_id$survey_id))

# Dump any records that are not in the filtered other_obs dataset
# None were dumped
oobs_id = unique(other_obs$id)
# Filter to data with no missing stream or rms....Still 178 records
other_pictures = other_pictures %>%
  filter(parent_record_id %in% oobs_id)

# Update id for join
other_pictures = other_pictures %>%
  rename(other_obs_id = parent_record_id)

# Pull out comments from pictures to join to other obs
picture_comments = other_pictures %>%
  filter(!is.na(comments)) %>%
  select(other_obs_id, pic_comments = comments) %>%
  distinct() %>%
  mutate(pic_comments = trimws(pic_comments)) %>%
  mutate(pic_comments = if_else(pic_comments == "",
                                NA_character_, pic_comments)) %>%
  filter(!is.na(pic_comments)) %>%
  distinct()

# Combine other_obs and other pictures...need to parse multiple pictures per location
# So we need one location per picture
pictures_trim = other_pictures %>%
  select(other_obs_id, pics, pic_comments = comments,
         pic_create_date = created_date,
         pic_modify_date = modified_date) %>%
  distinct()

# Generate sigle location_id for each observation. Add pictures to other obs
other_obs = other_obs %>%
  mutate(location_id = remisc::get_uuid(nrow(other_obs))) %>%
  rename(other_obs_id = id) %>%
  left_join(pictures_trim, by = "other_obs_id")

# Pull out barrier info
barrier = other_obs %>%
  filter(!is.na(known_total_barrier)) %>%
  mutate(barrier_observed_datetime = as.POSIXct(NA)) %>%
  mutate(barrier_height_meter = as.numeric(barrier_ht) * 0.3048) %>%
  mutate(plunge_pool_depth_meter = as.numeric(pp_depth_of_barrier) * 0.3048) %>%
  # Barrier description is required...obs_details are not
  mutate(physical_desc = paste0("Physical description: ", barrier_descrip)) %>%
  mutate(observation_details = trimws(observation_details)) %>%
  mutate(barrier_details = if_else(!is.na(observation_details) &
                                     !observation_details == "",
                                   paste0("Details: ", observation_details), NA_character_)) %>%
  mutate(comment_text = if_else(!is.na(barrier_details),
                                paste0(physical_desc, "; ", barrier_details),
                                physical_desc)) %>%
  select(other_obs_id, survey_id, location_id, obs_lat, obs_lon, obs_acc,
         barrier_type = observation_type, barrier_observed_datetime,
         barrier_height_meter, barrier_ht_type = barrier_ht_meas_est,
         plunge_pool_depth_meter, pp_depth_type = barrier_pp_depth_meas_est,
         comment_text, created_datetime, created_by = head_create_by,
         modified_datetime, modified_by = head_mod_by) %>%
  distinct()

# Convert types to ids
barrier_prep = barrier %>%
  mutate(fish_barrier_id = remisc::get_uuid(nrow(barrier))) %>%
  mutate(barrier_type_id = case_when(
    barrier_type == "waterfall" ~ "0731c4c4-810d-4de6-9649-e0db43207eb3",
    barrier_type == "gradient" ~ "9f6d5a70-c524-4bd5-a4ae-24d4e161469b",
    barrier_type == "culvert" ~ "4dbe30db-52d5-42b6-ad96-541e657d8374",
    barrier_type == "dam" ~ "71f60180-d53f-45ec-8cae-1c484f899530",
    barrier_type == "screen" ~ "21b16ab6-8f54-4dee-b66a-ff97b7a21895",
    barrier_type == "log_jam" ~ "c440240e-ac6b-488e-a920-2578082ee2c9",
    barrier_type == "beaver_dam" ~ "25250be2-6c97-4a77-9d1d-df123c32e7ca",
    is.na(barrier_type) ~ as.character(barrier_type),
    TRUE ~ as.character(barrier_type))) %>%
  mutate(barrier_height_type_id = case_when(
    barrier_ht_type == "measured" ~ "6352ee46-9d51-4849-919c-4ad37acf2150",
    barrier_ht_type == "estimated" ~ "95e6627b-93c9-4e94-b89b-d1e1789ae699",
    is.na(barrier_ht_type) ~ as.character(barrier_ht_type),
    TRUE ~ as.character(barrier_ht_type))) %>%
  mutate(plunge_pool_depth_type_id = case_when(
    pp_depth_type == "measured" ~ "6352ee46-9d51-4849-919c-4ad37acf2150",
    pp_depth_type == "estimated" ~ "95e6627b-93c9-4e94-b89b-d1e1789ae699",
    is.na(pp_depth_type) ~ as.character(pp_depth_type),
    TRUE ~ as.character(pp_depth_type))) %>%
  select(other_obs_id, fish_barrier_id, survey_id, barrier_location_id = location_id,
         obs_lat, obs_lon, obs_acc, barrier_type_id, barrier_observed_datetime,
         barrier_height_meter, barrier_height_type_id, plunge_pool_depth_meter,
         plunge_pool_depth_type_id, comment_text, created_datetime, created_by,
         modified_datetime, modified_by)

# Pull out media_location data
media_loc = other_obs %>%
  filter(!is.na(pics)) %>%
  mutate(media_type_id = "2e29bcbc-a8e5-4cf3-9892-9a8329e3960e") %>%             # Photo
  select(other_obs_id, survey_id, location_id, media_type_id,
         media_url = pics, pic_comments, pic_create_date,
         head_create_by, pic_modify_date, head_mod_by) %>%
  distinct() %>%
  arrange(other_obs_id)

# Clean up comments
media_location = media_loc %>%
  mutate(pic_comments = trimws(pic_comments)) %>%
  mutate(pic_comments = if_else(pic_comments == "",
                                NA_character_, pic_comments)) %>%
  mutate(media_location_id = remisc::get_uuid(nrow(media_loc))) %>%
  select(media_location_id, location_id, media_type_id, media_url,
         comment_text = pic_comments, pic_create_date,
         head_create_by, pic_modify_date, head_mod_by)

# Pull out other obs data...include location info
other_observation = other_obs %>%
  filter(is.na(known_total_barrier))

# Process
other_observation = other_observation %>%
  mutate(other_observation_id = remisc::get_uuid(nrow(other_observation))) %>%
  mutate(observation_type_id = case_when(
    observation_type == "miscellaneous_observation" ~ "30b93c02-1e8e-4c2a-b316-c961def4955d",
    observation_type == "Japanese Knotweed" ~ "51ec0a5c-23eb-417a-bab6-fa4dd69209d6",
    observation_type == "Clarity Pool" ~ "38f359db-e7ca-4bb2-8d98-db9885bf5794",
    observation_type == "new_survey_bottom" ~ "a171d5a0-aba8-4c31-90e9-c4c5e60a8051",
    observation_type == "new_survey_top" ~ "08bca0d8-1c27-4d34-abeb-0f38e6a774ff",
    observation_type == "landowner_denial" ~ "9feb413d-2d14-4d26-a5c6-152645f91f5f",
    observation_type == "access_point" ~ "f1630571-06ef-4c29-a6df-f71072397337",
    observation_type == "forest_gate" ~ "dc037ad0-1d65-45bd-9445-e8ce54a82007",
    observation_type == "lowno_flow" ~ "6a27edc0-ac8c-4fe4-befa-bf88c1692d48",
    observation_type == "high_water_line" ~ "623acb24-ae3c-4c32-9be4-9e64ac0aa342")) %>%
  mutate(observation_datetime = created_date) %>%
  mutate(observation_count = NA_integer_) %>%
  select(other_obs_id, other_observation_id, survey_id, observation_location_id = location_id,
         obs_lat, obs_lon, obs_acc, observation_type_id, observation_datetime,
         observation_count, comment_text = comments, created_datetime,
         created_by = head_create_by, modified_datetime, modified_by = head_mod_by)

# Pull out location table data for both fish_barrier and other_observations
barrier_location = barrier_prep %>%
  select(survey_id, location_id = barrier_location_id,
         lat = obs_lat, lon = obs_lon, acc = obs_acc,
         created_datetime, created_by, modified_datetime,
         modified_by) %>%
  distinct()

obs_location = other_observation %>%
  select(survey_id, location_id = observation_location_id,
         lat = obs_lat, lon = obs_lon, acc = obs_acc,
         created_datetime, created_by, modified_datetime,
         modified_by) %>%
  distinct()

#================================================================================================
# Create header location tables
#================================================================================================

# Get info needed for location table
survey_loc_trim = survey_loc_info %>%
  select(survey_id, wria_id, waterbody_id) %>%
  distinct()

# Prepare barrier_location: 140 rows
barrier_location = barrier_location %>%
  mutate(location_type_id = "8944c684-7cfc-44cf-a2d8-be66723c1ae0")                      # Fish barrier

# Check
any(duplicated(barrier_location$location_id))

# Prepare obs_location: 139 rows
obs_location = obs_location %>%
  mutate(location_type_id = "ead419f2-0e72-4d41-9715-7833879b71a8")                      # Other

# Check
any(duplicated(obs_location$location_id))

# Combine then add loc info
header_location = rbind(barrier_location, obs_location) %>%
  left_join(survey_loc_trim, by = "survey_id") %>%
  mutate(stream_channel_type_id = "713a39a5-8e95-4069-b078-066699c321d8") %>%            # No data
  mutate(location_orientation_type_id = "eb4652b7-5390-43d4-a98e-60ea54a1d518") %>%      # No data
  mutate(river_mile_measure = NA_real_) %>%
  mutate(location_code = NA_character_) %>%
  mutate(location_name = NA_character_) %>%
  mutate(location_description = NA_character_) %>%
  mutate(waloc_id = NA_character_) %>%
  select(location_id, waterbody_id, wria_id, location_type_id,
         stream_channel_type_id, location_orientation_type_id,
         river_mile_measure, location_code, location_name,
         location_description, waloc_id, created_datetime,
         created_by, modified_datetime, modified_by, lat, lon,
         acc)

# Pull out just the location data
header_location_prep = header_location %>%
  select(location_id, waterbody_id, wria_id, location_type_id,
         stream_channel_type_id, location_orientation_type_id,
         river_mile_measure, location_code, location_name,
         location_description, waloc_id, created_datetime,
         created_by, modified_datetime, modified_by)

# Pull out header location_coordinates
header_location_coords_prep = header_location %>%
  st_as_sf(., coords = c("lon", "lat"), crs = 4326) %>%
  st_transform(., 2927) %>%
  mutate(acc = as.numeric(acc)) %>%
  mutate(comment_text = NA_character_) %>%
  select(location_id, horizontal_accuracy = acc, comment_text,
         created_datetime, created_by, modified_datetime,
         modified_by)

# Check
any(duplicated(header_location_prep$location_id))
any(duplicated(header_location_coords_prep$location_id))
any(is.na(header_location_prep$stream_channel_type_id))

#======================================================================================================================
# Import from dead fish subform
#======================================================================================================================

# Function to get dead fish records....No nested subforms
get_dead = function(profile_id, dead_fish_page_id, start_id, access_token) {
  # Define fields
  fields = glue::glue("parent_page_id, parent_element_id, created_date, created_by, ",
                      "created_location, created_device_id, modified_date, modified_by, ",
                      "modified_location, modified_device_id, server_modified_date, ",
                      "species_fish, run_type, number_fish, clip_status, fish_sex, ",
                      "carcass_condition, carc_tag_1, carc_tag_2, spawn_condition, ",
                      "length_measurement_cm, cwt_detected, cwt_label, dna_number, ",
                      "scale_card_number, scale_card_position_number, encounter_comments, ",
                      "fish_condition, mark_status_1, mark_status_2, scan_cwt")
  # Order by id ascending and get all records after start_id
  field_string = glue('id:<, parent_record_id(>"{start_id}"), {fields}')
  # Loop through all survey records
  dead_fish = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = dead_fish_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = since_id)
  return(dead_fish)
}

# New access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Set start_id as the minimum parent_record_id minus one
start_id = min(header_data$parent_record_id) - 1
#start_id = 1

# Test...currently 2272 records
strt = Sys.time()
dead = get_dead(profile_id, dead_fish_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Process dates
dead = dead %>%
  mutate(created_date = as.POSIXct(iformr::idate_time(created_date), tz = "America/Los_Angeles")) %>%
  mutate(modified_date = as.POSIXct(iformr::idate_time(modified_date), tz = "America/Los_Angeles")) %>%
  mutate(created_date = with_tz(created_date, tzone = "UTC")) %>%
  mutate(modified_date = with_tz(modified_date, tzone = "UTC"))

# Rule: If fish_count > 1 add comments to survey_event table...otherwise add to individual_fish table

# Get higher level survey_event fields from header table
header_se = header_data %>%
  select(parent_record_id, survey_id = survey_uuid, data_entry_type,
         survey_design_type_id = survey_type, coho_run_year,
         steelhead_run_year, chum_run_year, chinook_run_year,
         head_created_by = created_by, head_mod_by = modified_by)

#============ survey_event ==========================

# Pull out survey_event data. Calculate cwt_detection method later. Chum = NA, blank = unknown, other = uuid
dead_se = dead %>%
  left_join(header_se, by = "parent_record_id") %>%
  select(dead_id = id, parent_record_id, survey_id, survey_design_type_id,
         species_fish, run_type, encounter_comments, created_date,
         created_by, modified_date, modified_by)

# Get run types
get_run_type = function() {
  qry = glue("select run_id as run_type, run_short_description as run ",
             "from spawning_ground.run_lut")
  con = pg_con_prod("FISH")
  #con = poolCheckout(pool)
  run_types = dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  return(run_types)
}

# Add run categories to dead_se
dead_se = dead_se %>%
  left_join(get_run_type(), by = "run_type") %>%
  select(dead_id, parent_record_id, survey_id, survey_design_type_id,
         species_fish, run_type, run, encounter_comments, created_date,
         created_by, modified_date, modified_by)

# Check species
table(dead_se$species_fish, useNA = "ifany")

# Inspect chinook run_type.....Lea will update run_type as needed in iforms
chin_run = dead_se %>%
  filter(species_fish == "e42aa0fc-c591-4fab-8481-55b0df38dcb1")
table(chin_run$run, useNA = "ifany")

# Inspect coho run_type.......Lea's instructions...Convert all cases to fall
coho_run = dead_se %>%
  filter(species_fish == "a0f5b3af-fa07-449c-9f02-14c5368ab304")
table(coho_run$run, useNA = "ifany")

# Inspect sthd run_type.....Lea's instructions...Convert all cases to winter
sthd_run = dead_se %>%
  filter(species_fish == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1")
table(sthd_run$run, useNA = "ifany")

# Inspect chum run_type.....Lea's instructions...Convert all cases to fall
chum_run = dead_se %>%
  filter(species_fish == "69d1348b-7e8e-4232-981a-702eda20c9b1" | species_fish == "Chum")
table(chum_run$run, useNA = "ifany")

# Output Chinook run data to Lea
chin_head = header_data %>%
  select(parent_record_id, survey_date,
         stream_name_text, reach) %>%
  distinct()

# Add header info for Lea
chin_run = chin_run %>%
  filter(run_type %in% c("2a9f3c67-aa7a-4214-87a6-af09879fc914", "94e1757f-b9c7-4b06-a461-17a2d804cd2f")) %>%
  left_join(chin_head, by = "parent_record_id")

# # Output
# out_name = paste0("data/ChinookRunTypeCheck_DeadSubform.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "RunTypeCheck", gridLines = TRUE)
# writeData(wb, sheet = 1, chin_run, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:ncol(chin_run), gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Correct species and run_type....HACK WARNING. Will not need to do in future. Will be fixed in IFB
dead_se = dead_se %>%
  # Convert all coho to fall
  mutate(run_type = if_else(species_fish == "a0f5b3af-fa07-449c-9f02-14c5368ab304",
                            "dc8de7ed-c825-4f3b-93d4-87fee088ac51", run_type)) %>%
  # Convert all sthd to winter
  mutate(run_type = if_else(species_fish == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1",
                            "1b413852-b4e8-42d1-81d9-a7bc373be906", run_type)) %>%
  # Convert all chum to fall...need to convert odd species entries first
  mutate(species_fish = if_else(species_fish == "Chum",
                                "69d1348b-7e8e-4232-981a-702eda20c9b1", species_fish)) %>%
  mutate(run_type = if_else(species_fish == "69d1348b-7e8e-4232-981a-702eda20c9b1",
                            "dc8de7ed-c825-4f3b-93d4-87fee088ac51", run_type))

# Inspect again
# Inspect chinook run_type.....Lea will update run_type as needed in iforms
chin_run = dead_se %>%
  filter(species_fish == "e42aa0fc-c591-4fab-8481-55b0df38dcb1")
table(chin_run$run_type, useNA = "ifany")

# Inspect coho run_type.......Lea's instructions...Convert all cases to fall
coho_run = dead_se %>%
  filter(species_fish == "a0f5b3af-fa07-449c-9f02-14c5368ab304")
table(coho_run$run_type, useNA = "ifany")

# Inspect sthd run_type.....Lea's instructions...Convert all cases to winter
sthd_run = dead_se %>%
  filter(species_fish == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1")
table(sthd_run$run_type, useNA = "ifany")

# Inspect chum run_type.....Lea's instructions...Convert all cases to fall
chum_run = dead_se %>%
  filter(species_fish == "69d1348b-7e8e-4232-981a-702eda20c9b1")
table(chum_run$run_type, useNA = "ifany")

#============ fish_encounter ========================

# Inspect the sex_id...includes maturity info
unique(dead$fish_sex)
table(dead$fish_sex, useNA = "ifany")

# Inpect cases where sex is NA
chk_dead_na = dead %>%
  filter(is.na(fish_sex))

# Pull out fish_encounter data....calculate maturity later...from sex
dead_fe = dead %>%
  left_join(header_se, by = "parent_record_id") %>%
  mutate(fish_status_id = "b185dc5d-6b15-4b5b-a54e-3301aec0270f") %>%            # Dead fish
  mutate(origin_id  = "2089de8c-3bd0-48fe-b31b-330a76d840d2") %>%                # Unknown
  mutate(fish_behavior_type_id = "70454429-724e-4ccf-b8a6-893cafba356a") %>%     # Not applicable...dead fish
  mutate(mortality_type_id = "149aefd0-0369-4f2c-b85f-4ec6c5e8679c") %>%         # Not applicable...not in form
  mutate(sex_id = if_else(is.na(fish_sex),
                          "c0f86c86-dc49-406b-805d-c21a6756de91",                # Unknown sex
                          fish_sex)) %>%
  mutate(sex_id = if_else(sex_id == "jack",
                          "ccdde151-4828-4597-8117-4635d8d47a71",                # Male
                          sex_id)) %>%
  mutate(maturity_id = if_else(fish_sex == "jack",
                               "0b0d12cf-ed27-48fb-ade2-b408067520e1",           # Subadult
                               "68347504-ee22-4632-9856-a4f4366b2bd8")) %>%      # Adult
  mutate(maturity_id = if_else(is.na(maturity_id),
                               "68347504-ee22-4632-9856-a4f4366b2bd8",           # Adult
                               maturity_id)) %>%
  mutate(previously_counted_indicator = 0L) %>%
  select(dead_id = id, parent_record_id, survey_id, created_date, created_by,
         created_location, modified_date, modified_by, modified_location, fish_status_id,
         sex_id, maturity_id, origin_id, cwt_detected, clip_status, fish_behavior_type_id,
         mortality_type_id, fish_count = number_fish, previously_counted_indicator)

# Inspect some values
unique(dead_fe$sex_id)
#table(dead_fe$sex_id, useNA = "ifany")
unique(dead_fe$maturity_id)
sort(unique(dead_fe$fish_count))

# # Inpect cases where sex is NA
# chk_dead_fe_na = dead_fe %>%
#   filter(is.na(sex_id))

#============== fish_capture_event and fish_mark =============

# Pull out fish mark data
fish_mark = dead %>%
  left_join(header_se, by = "parent_record_id") %>%
  select(dead_id = id, parent_record_id, survey_id, created_date, created_by,
         modified_date, modified_by, species_fish, number_fish, carcass_condition,
         carc_tag_1, carc_tag_2, mark_status_1, mark_status_2)

# Correct and add species common name
unique(fish_mark$species_fish)
unique(fish_mark$mark_status_1)
unique(fish_mark$mark_status_2)
fish_mark = fish_mark %>%
  mutate(common_name = case_when(
    species_fish == "69d1348b-7e8e-4232-981a-702eda20c9b1" ~ "Chum",
    species_fish == "e42aa0fc-c591-4fab-8481-55b0df38dcb1" ~ "Chinook",
    species_fish == "a0f5b3af-fa07-449c-9f02-14c5368ab304" ~ "Coho",
    species_fish == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1" ~ "Sthd",
    TRUE ~ species_fish)) %>%
  mutate(mark_status_one = case_when(
    mark_status_1 == "7118fcda-8804-4285-bc1f-5d5c33048d4e" ~ "Unknown",
    mark_status_1 == "f03eae60-91db-42ba-ba41-623d919262b6" ~ "Applied",
    mark_status_1 == "777753af-df58-4ca3-b4dd-696c52632e36" ~ "Not applicable",
    mark_status_1 == "68e174c7-ea47-4084-a567-bd33b241f94e" ~ "Observed",
    mark_status_1 == "cb1dfe91-a782-4cbf-83c9-299137a5df6e" ~ "Not present",
    TRUE ~ mark_status_1)) %>%
  mutate(mark_status_two = case_when(
    mark_status_2 == "7118fcda-8804-4285-bc1f-5d5c33048d4e" ~ "Unknown",
    mark_status_2 == "f03eae60-91db-42ba-ba41-623d919262b6" ~ "Applied",
    mark_status_2 == "777753af-df58-4ca3-b4dd-696c52632e36" ~ "Not applicable",
    mark_status_2 == "68e174c7-ea47-4084-a567-bd33b241f94e" ~ "Observed",
    mark_status_2 == "cb1dfe91-a782-4cbf-83c9-299137a5df6e" ~ "Not present",
    TRUE ~ mark_status_2))

# Pull out cases where mark status is not applicable to check for species: 1143 rows
chk_mark_species = fish_mark %>%
  filter(mark_status_one == "Not applicable" | mark_status_two == "Not applicable")
unique(chk_mark_species$common_name)  # Filtering out by Not applicable does not get rid of any chum
all(is.na(chk_mark_species$carcass_condition))
any(is.na(fish_mark$mark_status_1))
any(is.na(fish_mark$mark_status_2))

# Update blanks to NA values
fish_mark = fish_mark %>%
  mutate(carc_tag_1 = trimws(carc_tag_1)) %>%
  mutate(carc_tag_1 = if_else(carc_tag_1 == "", NA_character_, carc_tag_1)) %>%
  mutate(carc_tag_2 = trimws(carc_tag_2)) %>%
  mutate(carc_tag_2 = if_else(carc_tag_2 == "", NA_character_, carc_tag_2))

# Inspect cases where something is entered for carcass condition....indicator of mark chack
# Were any species besides chum checked? No blanks were entered for carcass_condition
table(fish_mark$carcass_condition, useNA = "ifany")
chk_condition = fish_mark %>%
  filter(!is.na(carcass_condition))
table(chk_condition$common_name, useNA = "ifany")

# Trim to only marked fish...went from 2272 rows to 1129 rows..correct: chk_mark_species: 1143 rows
fish_mark = fish_mark %>%
  filter(!mark_status_one == "Not applicable" | !mark_status_two == "Not applicable")

# Inspect cases where species is not chum
non_chum_mark = fish_mark %>%
  filter(!common_name == "Chum")

# # Am suspecting data entry error for coho and chinook in Not applicable category
# # There are 11 coho here and 22 no match cases near bottom of script
# num_cols = ncol(non_chum_mark)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "NonChumMarkData.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NotChum", gridLines = TRUE)
# writeData(wb, sheet = 1, non_chum_mark, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Inspect how fish_mark columns were entered
any(is.na(fish_mark$survey_id))
unique(fish_mark$number_fish)
table(chk_condition$common_name, useNA = "ifany")

# Keep only chum ????
# Keep only cases where fish_count = 1  ???
# Keep only observed or applied  ??
# Is mark placement












# # DONT DO THIS NOW !!!!!!!!!!!!!! Check with LEA !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# # Assume we can get rid of anything except chum. Leaves 1115 rows
# fish_mark = fish_mark %>%
#   filter(common_name == "Chum")
#=======================================================================================

# Inspect fish_mark again
table(fish_mark$common_name, useNA = "ifany")

# Inspect carcass_conditions remaining
table(fish_mark$carcass_condition, useNA = "ifany")

# # Pull out just unique cases for Lea to construct a rule
# fish_mark_rule = fish_mark %>%
#   select(mark_status_1, mark_status_2, common_name, number_fish, carcass_condition,
#          carc_tag_1, carc_tag_2, mark_status_one, mark_status_two) %>%
#   mutate(number_fish = if_else(number_fish > 1L, "more than one", "one")) %>%
#   mutate(carc_tag_1 = if_else(!is.na(carc_tag_1) & !carc_tag_1 == "" & as.integer(carc_tag_1) > 0,
#                               "tag_number_present", "no_tag_number")) %>%
#   mutate(carc_tag_2 = if_else(!is.na(carc_tag_2) & !carc_tag_2 == "" & as.integer(carc_tag_2) > 0,
#                               "tag_number_present", "no_tag_number")) %>%
#   distinct() %>%
#   mutate(`fish_capture_status?` = NA_character_) %>%
#   mutate(`disposition?` = NA_character_) %>%
#   arrange(number_fish, mark_status_one, mark_status_two)

# # Output with styling
# num_cols = ncol(fish_mark_rule)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "FishMarkRule.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "FishMarkRule", gridLines = TRUE)
# writeData(wb, sheet = 1, fish_mark_rule, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Pull out only cases Lea indicated could not be correct: 4-5 Not Tagged + both mark_status == Unknown.
fish_mark_incorrect = fish_mark %>%
  select(dead_id, parent_record_id, mark_status_1, mark_status_2,
         common_name, number_fish, carcass_condition, carc_tag_1,
         carc_tag_2, mark_status_one, mark_status_two) %>%
  filter(carcass_condition == "4_5 Not Tagged" & mark_status_one == "Unknown" & mark_status_two == "Unknown") %>%
  arrange(parent_record_id, dead_id)

# # Output with styling
# num_cols = ncol(fish_mark_incorrect)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "FishMarkIncorrect.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "FishMarkIncorrect", gridLines = TRUE)
# writeData(wb, sheet = 1, fish_mark_incorrect, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Generate data needed for both fish_capture_event and fish_mark
fish_mark = fish_mark %>%
  mutate(mark_type_id = "89111757-5bb5-469d-9ea0-e9596064b5dc") %>%                # Opercle tag
  mutate(mark_orientation_id = "fcc3fe36-cfa6-481a-8790-879e6616fb09") %>%         # Not applicable
  mutate(mark_placement_id = "f0a2cdcf-012a-4468-b223-c7cd0eb2ee58") %>%           # Not applicable
  mutate(mark_size_id = "44459930-c7d0-4b1a-8f60-71a992e574c4") %>%                # Not applicable
  mutate(mark_color_id = "66076cfd-f4a2-4ec8-861a-346e41c626b5") %>%               # Not applicable
  mutate(mark_shape_id = "469c9ba7-3288-404e-9c75-d9b8d9a7600b") %>%               # Not applicable
  mutate(fish_capture_status_id = case_when(
    mark_status_one == "Not present" | mark_status_two == "Not present" ~ "03f32160-6426-4c41-a088-3580a7d1a0c5",
    mark_status_one == "Unknown" & mark_status_two == "Unknown" ~ "105ce6c3-1d14-4ca0-8730-bb72527295e5",
    mark_status_one == "Applied" | mark_status_two == "Applied" ~ "03f32160-6426-4c41-a088-3580a7d1a0c5",
    TRUE ~ "105ce6c3-1d14-4ca0-8730-bb72527295e5")) %>%               # Unknown
  mutate(disposition_type_id = "24b51215-f7ea-4480-ba7d-436144969ac3") %>%         # Carcass returned
  mutate(disposition_id = "dd6d0cab-1cc8-4b07-af93-5cd0be1a7a7f")                  # Not applicable

# Check
unique(fish_mark$fish_capture_status_id)

# Check for missing
any(is.na(fish_mark$survey_id))

# Pull out fish_capture_event
fish_capture_event = fish_mark %>%
  mutate(disposition_location_id = NA_character_) %>%
  select(dead_id, parent_record_id, survey_id, fish_capture_status_id, disposition_type_id,
         disposition_id, disposition_location_id, created_datetime = created_date,
         created_by, modified_datetime = modified_date, modified_by)

# Check
any(is.na(fish_capture_event$fish_capture_status_id))
any(is.na(fish_capture_event$disposition_type_id))
any(is.na(fish_capture_event$disposition_id))
any(is.na(fish_capture_event$created_datetime))
any(is.na(fish_capture_event$created_by))

# Pull out fish mark data,  separate and combine
fish_mark_one = fish_mark %>%
  select(dead_id, parent_record_id, survey_id, mark_type_id, mark_status_id = mark_status_1,
         mark_orientation_id, mark_placement_id, mark_size_id, mark_color_id,
         mark_shape_id, tag_number = carc_tag_1, created_datetime = created_date,
         created_by, modified_datetime = modified_date, modified_by)
fish_mark_two = fish_mark %>%
  select(dead_id, parent_record_id, survey_id, mark_type_id, mark_status_id = mark_status_2,
         mark_orientation_id, mark_placement_id, mark_size_id, mark_color_id,
         mark_shape_id, tag_number = carc_tag_2, created_datetime = created_date,
         created_by, modified_datetime = modified_date, modified_by)
# Combine
fish_mark = rbind(fish_mark_one, fish_mark_two) %>%
  mutate(tag_number = trimws(tag_number)) %>%
  mutate(tag_number = if_else(tag_number == "", NA_character_, tag_number))

# Check
any(is.na(fish_mark$mark_type_id))
any(is.na(fish_mark$mark_status_id))
any(is.na(fish_mark$mark_orientation_id))
any(is.na(fish_mark$mark_placement_id))
any(is.na(fish_mark$mark_size_id))
any(is.na(fish_mark$mark_color_id))
any(is.na(fish_mark$mark_shape_id))
any(is.na(fish_mark$created_datetime))
any(is.na(fish_mark$created_by))

#============ individual_fish =============================================

# Pull out individual_fish data
dead_ind = dead %>%
  left_join(header_se, by = "parent_record_id") %>%
  filter(number_fish == 1L) %>%
  select(dead_id = id, parent_record_id, survey_id, fish_condition_type_id = fish_condition,
         length_measurement_cm, spawn_condition_type_id = spawn_condition,
         cwt_snout_sample_number = cwt_label, genetic_sample_number = dna_number,
         scale_sample_card_number = scale_card_number,
         scale_sample_position_number = scale_card_position_number, encounter_comments,
         created_date, created_by, modified_date, modified_by) %>%
  mutate(fish_condition_type_id = trimws(fish_condition_type_id)) %>%
  mutate(fish_condition_type_id = if_else(fish_condition_type_id == "",
                                          NA_character_, fish_condition_type_id)) %>%
  mutate(length_measurement_cm = as.numeric(length_measurement_cm)) %>%
  mutate(spawn_condition_type_id = trimws(spawn_condition_type_id)) %>%
  mutate(spawn_condition_type_id = if_else(is.na(spawn_condition_type_id) |
                                             spawn_condition_type_id == "",
                                           "e89d8979-ce87-4c0e-8ed7-9e07b831c963",   # No data
                                           spawn_condition_type_id)) %>%
  mutate(cwt_snout_sample_number = trimws(cwt_snout_sample_number)) %>%
  mutate(cwt_snout_sample_number = if_else(cwt_snout_sample_number == "",
                                           NA_character_, cwt_snout_sample_number)) %>%
  mutate(genetic_sample_number = trimws(genetic_sample_number)) %>%
  mutate(genetic_sample_number = if_else(genetic_sample_number == "",
                                         NA_character_, genetic_sample_number)) %>%
  mutate(scale_sample_card_number = trimws(scale_sample_card_number)) %>%
  mutate(scale_sample_card_number = if_else(scale_sample_card_number == "",
                                            NA_character_, scale_sample_card_number)) %>%
  mutate(scale_sample_position_number = trimws(scale_sample_position_number)) %>%
  mutate(scale_sample_position_number = if_else(scale_sample_position_number == "",
                                                NA_character_, scale_sample_position_number)) %>%
  mutate(encounter_comments = trimws(encounter_comments)) %>%
  mutate(encounter_comments = if_else(encounter_comments == "",
                                      NA_character_, encounter_comments)) %>%
  mutate(all_na = if_else(is.na(fish_condition_type_id) |
                            fish_condition_type_id == "0cd8f278-1c3e-4ad4-b7c7-9afebc8e2358" &
                            ( is.na(length_measurement_cm) &
                                is.na(spawn_condition_type_id) &
                                is.na(cwt_snout_sample_number) &
                                is.na(genetic_sample_number) &
                                is.na(scale_sample_card_number) &
                                is.na(scale_sample_position_number) ),
                          "yes", "no")) %>%
  filter(all_na == "no") %>%
  mutate(fish_trauma_type_id = "92aecee2-4a55-4b0f-ace6-8648d5da560a") %>%         # No data
  mutate(gill_condition_type_id = "33aea984-7639-491b-9663-c641ee78a90a") %>%      # No data
  mutate(cwt_result_type_id = "3b5e368f-11ce-4bb3-8c8f-70c08dde2f7e") %>%          # Not applicable
  select(dead_id, parent_record_id, survey_id, fish_condition_type_id,
         fish_trauma_type_id, gill_condition_type_id, spawn_condition_type_id,
         cwt_result_type_id, length_measurement_cm, spawn_condition_type_id,
         cwt_snout_sample_number, genetic_sample_number, scale_sample_card_number,
         scale_sample_position_number, encounter_comments, created_date,
         created_by, modified_date, modified_by)

# Check
any(is.na(dead_ind$fish_condition_type_id))
any(is.na(dead_ind$fish_trauma_type_id))
any(is.na(dead_ind$gill_condition_type_id))
any(is.na(dead_ind$spawn_condition_type_id))
any(is.na(dead_ind$cwt_result_type_id))
any(is.na(dead_ind$created_date))
any(is.na(dead_ind$created_by))

#======================================================================================================================
# Import from carcass recoveries subform
#======================================================================================================================

# Function to get dead fish records....No nested subforms
get_carcass_recoveries = function(profile_id, recaps_page_id, start_id, access_token) {
  # Define fields
  fields = glue::glue("parent_page_id, parent_element_id, created_date, created_by, ",
                      "created_location, created_device_id, modified_date, modified_by, ",
                      "modified_location, modified_device_id, server_modified_date, ",
                      "select_recovery_field, recovery_tag_2, recovery_tag_1, recap_species")
  # Order by id ascending and get all records after start_id
  field_string = glue('id:<, parent_record_id(>"{start_id}"), {fields}')
  # Loop through all survey records
  recaps = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = recaps_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = since_id)
  return(recaps)
}

# New access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Set start_id as the minimum parent_record_id minus one
start_id = min(header_data$parent_record_id) - 1

# Test...currently 164 records
strt = Sys.time()
recaps = get_carcass_recoveries(profile_id, recaps_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Process dates
recaps = recaps %>%
  mutate(created_date = as.POSIXct(iformr::idate_time(created_date), tz = "America/Los_Angeles")) %>%
  mutate(modified_date = as.POSIXct(iformr::idate_time(modified_date), tz = "America/Los_Angeles")) %>%
  mutate(created_date = with_tz(created_date, tzone = "UTC")) %>%
  mutate(modified_date = with_tz(modified_date, tzone = "UTC"))

#============ survey_event ==========================

# Pull out survey_event data. Calculate cwt_detection method later...Chum = NA, blank = unknown, other = uuid
recaps_se = recaps %>%
  left_join(header_se, by = "parent_record_id") %>%
  select(recap_id = id, parent_record_id, survey_id, survey_design_type_id,
         species_id = recap_species, recovery_tag_1, recovery_tag_2,
         select_recovery_field, created_date, created_by, modified_date,
         modified_by)

# Verify both tag numbers on same fish agree, then stack...2 cases...second tag "Not present" can ignore.
chk_recaps = recaps_se %>%
  mutate(chk_tag = if_else(recovery_tag_1 == recovery_tag_2 & select_recovery_field == "same",
                           "ok", "chk")) %>%
  filter(chk_tag == "chk")

# # Output for inspection
# num_cols = ncol(chk_recaps)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "CheckRecaps.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "CheckRecaps", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_recaps, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

#=========== fish_mark =============================

# # ADD run values to recaps
# # Get run type from dead as a quick fix, but can adjust later
# dead_run = dead %>%
#   select(run_id = run_type, carc_tag_1, carc_tag_2) %>%
#   mutate(tag_number = coalesce(carc_tag_1, carc_tag_2)) %>%
#   filter(!is.na(tag_number) & !tag_number == "") %>%
#   select(tag_number, run_id)
#
# # Inspect tag_number
# head(sort(unique(dead_run$tag_number)), 15)
# tail(sort(unique(dead_run$tag_number)), 15)
#
# # Join placed tags to recaps to get mark data
# # In the future...will need to query database to get tag info !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# tag_info_one = fish_mark %>%
#   filter(!is.na(tag_number)) %>%
#   left_join(dead_run, by = "tag_number") %>%
#   select(tag_number_one = tag_number, run_id_one = run_id, mark_type_id_one = mark_type_id,
#          mark_orientation_id_one = mark_orientation_id, mark_size_id_one = mark_size_id,
#          mark_color_id_one = mark_color_id, mark_shape_id_one = mark_shape_id)
# tag_info_two = fish_mark %>%
#   filter(!is.na(tag_number)) %>%
#   left_join(dead_run, by = "tag_number") %>%
#   select(tag_number_two = tag_number, run_id_two = run_id, mark_type_id_two = mark_type_id,
#          mark_orientation_id_two = mark_orientation_id, mark_size_id_two = mark_size_id,
#          mark_color_id_two = mark_color_id, mark_shape_id_two = mark_shape_id)


# CAN I JUST ASSIGN RUN = FALL?
# THEN ASSIGN REST OF INFO AS IN FISH_MARK....Don't have it from fish_mark without tag info

# Inspect tag info
unique(recaps_se$recovery_tag_1)
unique(recaps_se$recovery_tag_2)

# ALL HAVE TAGS except two cases of recovery_tag_2 not present. Make sure and coalesce correctly !


# Join to recaps
recaps_se = recaps_se %>%
  mutate(tag_number_one = recovery_tag_1) %>%
  mutate(tag_number_two = recovery_tag_2) %>%
  # left_join(tag_info_one, by = "tag_number_one") %>%
  # left_join(tag_info_two, by = "tag_number_two") %>%
  # mutate(run_id = coalesce(run_id_one, run_id_two)) %>%
  mutate(mark_type_id = coalesce(mark_type_id_one, mark_type_id_two)) %>%
  mutate(mark_orientation_id = coalesce(mark_orientation_id_one, mark_orientation_id_two)) %>%
  mutate(mark_size_id = coalesce(mark_size_id_one, mark_size_id_two)) %>%
  mutate(mark_color_id = coalesce(mark_color_id_one, mark_color_id_two)) %>%
  mutate(mark_shape_id = coalesce(mark_shape_id_one, mark_shape_id_two)) %>%
  select(recap_id, parent_record_id, survey_id, survey_design_type_id,
         species_id, tag_number_one, tag_number_two, run_id, mark_type_id,
         mark_orientation_id, mark_size_id, mark_color_id, mark_shape_id,
         created_date, created_by, modified_date, modified_by) %>%
  distinct()

# Pull out separately and stack
recaps_se_one = recaps_se %>%
  select(recap_id, parent_record_id, survey_id, survey_design_type_id,
         species_id, tag_number = tag_number_one, run_id, mark_type_id,
         mark_orientation_id, mark_size_id, mark_color_id, mark_shape_id,
         created_date, created_by, modified_date, modified_by)
recaps_se_two = recaps_se %>%
  select(recap_id, parent_record_id, survey_id, survey_design_type_id,
         species_id, tag_number = tag_number_two, run_id, mark_type_id,
         mark_orientation_id, mark_size_id, mark_color_id, mark_shape_id,
         created_date, created_by, modified_date, modified_by)
recaps_se = rbind(recaps_se_one, recaps_se_two) %>%
  arrange(recap_id, parent_record_id)

# Verify run values for each species
table(recaps_se$species_id, useNA = "ifany")
table(recaps_se$run_id, useNA = "ifany")

# All recaps are chum...so set all run_ids to fall
# Convert all chum to fall
recaps_se = recaps_se %>%
  mutate(run_id = if_else(species_id == "69d1348b-7e8e-4232-981a-702eda20c9b1",
                          "dc8de7ed-c825-4f3b-93d4-87fee088ac51", run_id))

# Verify again....All fall chum
table(recaps_se$species_id, useNA = "ifany")
table(recaps_se$run_id, useNA = "ifany")

# Inspect tag_number
tail(sort(unique(recaps_se$tag_number)), 15)
unique(recaps_se$tag_number)

# Organize
recaps_fish_mark = recaps_se %>%
  mutate(mark_status_id = case_when(
    nchar(tag_number) >= 4L &
      stri_detect_regex(tag_number, "[:digit:]") &
      !stri_detect_regex(tag_number, "[:alpha:]") ~ "68e174c7-ea47-4084-a567-bd33b241f94e",   # Observed
    tag_number == "Not present" ~ "cb1dfe91-a782-4cbf-83c9-299137a5df6e",                     # Not present
    tag_number == "not present" ~ "cb1dfe91-a782-4cbf-83c9-299137a5df6e",                     # Not present
    tag_number == "Unknown" ~ "7118fcda-8804-4285-bc1f-5d5c33048d4e",                         # Unknown
    TRUE ~ "68e174c7-ea47-4084-a567-bd33b241f94e")) %>%                                       # Observed
  mutate(mark_placement_id = "d35aacb1-20b3-41d7-b24e-00d4941b68a8") %>%                      # Opercle
  select(recap_id, parent_record_id, survey_id, survey_design_type_id,
         species_id, run_id, mark_type_id, mark_status_id, mark_orientation_id,
         mark_placement_id, mark_size_id, mark_color_id, mark_shape_id,
         tag_number, created_date, created_by, modified_date, modified_by)

#=========== fish_capture_event =============================

# Organize
recaps_fish_capture_event = recaps_se %>%
  mutate(fish_capture_status_id = "3d115d9f-1e8b-4f5d-b05d-bcfa21a4d87f") %>%                # Recapture
  mutate(disposition_type_id = "24b51215-f7ea-4480-ba7d-436144969ac3") %>%                   # Carcass returned
  mutate(disposition_id = "dd6d0cab-1cc8-4b07-af93-5cd0be1a7a7f") %>%                        # Not applicable
  select(recap_id, parent_record_id, survey_id, survey_design_type_id,
         species_id, run_id, fish_capture_status_id, disposition_type_id,
         disposition_id, created_date, created_by, modified_date,
         modified_by)

#======================================================================================================================
# Import from live fish subform
#======================================================================================================================

# Function to get dead fish records....No nested subforms
get_live = function(profile_id, live_fish_page_id, start_id, access_token) {
  # Define fields
  fields = glue::glue("parent_page_id, parent_element_id, created_date, created_by, ",
                      "created_location, created_device_id, modified_date, modified_by, ",
                      "modified_location, modified_device_id, server_modified_date, ",
                      "species_fish, run_type, live_type, unk_unknown, ",
                      "unknown_mark_unknown_sex_count, mark_sex_status_multiselect, ",
                      "um_male, unmarked_male_count, um_female, unmarked_female_count, ",
                      "um_jack, unmarked_jack_count, um_unknown, unmarked_unknown_sex_count, ",
                      "unk_male, unknown_mark_male_count, unk_female, ",
                      "unknown_mark_female_count, unk_jack, unknown_mark_jack_count, ",
                      "ad_male, ad_male_count, ad_female, ad_female_count, ad_jack, ",
                      "ad_jack_count, ad_unknown, ad_mark_unknown_sex_count")
  # Order by id ascending and get all records after start_id
  field_string = glue('id:<, parent_record_id(>"{start_id}"), {fields}')
  # Loop through all survey records
  live_fish = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = live_fish_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = since_id)
  return(live_fish)
}

# New access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Set start_id as the minimum parent_record_id minus one
start_id = min(header_data$parent_record_id) - 1

# Test...currently 1127 records: Checked that I got first and last parent_record_id
strt = Sys.time()
live = get_live(profile_id, live_fish_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Process dates
live = live %>%
  mutate(created_date = as.POSIXct(iformr::idate_time(created_date), tz = "America/Los_Angeles")) %>%
  mutate(modified_date = as.POSIXct(iformr::idate_time(modified_date), tz = "America/Los_Angeles")) %>%
  mutate(created_date = with_tz(created_date, tzone = "UTC")) %>%
  mutate(modified_date = with_tz(modified_date, tzone = "UTC"))

#============ survey_event ==========================

# Pull out survey_event data. Calculate cwt_detection method later...Chum = NA, blank = unknown, other = uuid
live_se = live %>%
  left_join(header_se, by = "parent_record_id") %>%
  left_join(get_run_type(), by = "run_type") %>%
  select(live_id = id, parent_record_id, survey_id, species_fish,
         run_type, run, live_type, created_date, created_by,
         modified_date, modified_by)

# Check species
table(live_se$species_fish, useNA = "ifany")

# Inspect chinook run_type.....Lea will update run_type as needed in iforms
chin_run = live_se %>%
  filter(species_fish == "e42aa0fc-c591-4fab-8481-55b0df38dcb1")
table(chin_run$run, useNA = "ifany")

# Inspect coho run_type.......Lea's instructions...Convert all cases to fall
coho_run = live_se %>%
  filter(species_fish == "a0f5b3af-fa07-449c-9f02-14c5368ab304")
table(coho_run$run, useNA = "ifany")

# Inspect sthd run_type.....Lea's instructions...Convert all cases to winter
sthd_run = live_se %>%
  filter(species_fish == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1")
table(sthd_run$run, useNA = "ifany")

# Inspect chum run_type.....Lea's instructions...Convert all cases to fall
chum_run = live_se %>%
  filter(species_fish == "69d1348b-7e8e-4232-981a-702eda20c9b1")
table(chum_run$run, useNA = "ifany")

# Inspect plamp run_type.....Lea's instructions...Convert all cases to fall
plamp_run = live_se %>%
  filter(species_fish == "2afac5a6-e3b9-4b37-911e-59b93240789d")
table(plamp_run$run, useNA = "ifany")

# Inspect chum run_type.....Lea's instructions...Convert all cases to fall
wblamp_run = live_se %>%
  filter(species_fish == "d29ca246-acfa-48d5-ba55-e61323d59fa7")
table(wblamp_run$run, useNA = "ifany")

# Add header info for Lea
chin_run = chin_run %>%
  filter(run_type %in% c("2a9f3c67-aa7a-4214-87a6-af09879fc914", "94e1757f-b9c7-4b06-a461-17a2d804cd2f")) %>%
  left_join(chin_head, by = "parent_record_id")

# # Output
# out_name = paste0("data/ChinookRunTypeCheck_LiveSubform.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "RunTypeCheck", gridLines = TRUE)
# writeData(wb, sheet = 1, chin_run, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:ncol(chin_run), gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Correct species and run_type....HACK WARNING. Will not need to do in future. Will be fixed in IFB
live_se = live_se %>%
  # Convert all coho to fall
  mutate(run_type = if_else(species_fish == "a0f5b3af-fa07-449c-9f02-14c5368ab304",
                            "dc8de7ed-c825-4f3b-93d4-87fee088ac51", run_type)) %>%
  # Convert all sthd to winter
  mutate(run_type = if_else(species_fish == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1",
                            "1b413852-b4e8-42d1-81d9-a7bc373be906", run_type)) %>%
  # Convert all chum to fall
  mutate(run_type = if_else(species_fish == "69d1348b-7e8e-4232-981a-702eda20c9b1",
                            "dc8de7ed-c825-4f3b-93d4-87fee088ac51", run_type)) %>%
  # Convert all plamp to Not applicable
  mutate(run_type = if_else(species_fish == "2afac5a6-e3b9-4b37-911e-59b93240789d",
                            "59e1e01f-3aef-498c-8755-5862c025eafa", run_type)) %>%
  # Convert all wblamp to Not applicable
  mutate(run_type = if_else(species_fish == "d29ca246-acfa-48d5-ba55-e61323d59fa7",
                            "59e1e01f-3aef-498c-8755-5862c025eafa", run_type)) %>%
  select(-run)

# Add run again
live_se = live_se %>%
  left_join(get_run_type(), by = "run_type") %>%
  select(live_id, parent_record_id, survey_id, species_fish,
         run_type, run, live_type, created_date, created_by,
         modified_date, modified_by)

# Inspect again....now all spring or fall
# Inspect chinook run_type.....Lea will update run_type as needed in iforms
chin_run = live_se %>%
  filter(species_fish == "e42aa0fc-c591-4fab-8481-55b0df38dcb1")
table(chin_run$run, useNA = "ifany")

# Inspect coho run_type.......Lea's instructions...Convert all cases to fall
coho_run = live_se %>%
  filter(species_fish == "a0f5b3af-fa07-449c-9f02-14c5368ab304")
table(coho_run$run, useNA = "ifany")

# Inspect sthd run_type.....Lea's instructions...Convert all cases to winter
sthd_run = live_se %>%
  filter(species_fish == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1")
table(sthd_run$run, useNA = "ifany")

# Inspect chum run_type.....Lea's instructions...Convert all cases to fall
chum_run = live_se %>%
  filter(species_fish == "69d1348b-7e8e-4232-981a-702eda20c9b1")
table(chum_run$run, useNA = "ifany")

# Inspect plamp run_type.....Lea's instructions...Convert all cases to fall
plamp_run = live_se %>%
  filter(species_fish == "2afac5a6-e3b9-4b37-911e-59b93240789d")
table(plamp_run$run, useNA = "ifany")

# Inspect chum run_type.....Lea's instructions...Convert all cases to fall
wblamp_run = live_se %>%
  filter(species_fish == "d29ca246-acfa-48d5-ba55-e61323d59fa7")
table(wblamp_run$run, useNA = "ifany")

#============ fish_encounter ========================

# Pull out fish_encounter data categories
live_fe_unknown_mark_unknown_sex = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = unknown_mark_unknown_sex_count) %>%
  mutate(sex_id = "c0f86c86-dc49-406b-805d-c21a6756de91") %>%                    # Unknown
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Unable to check

# Pull out fish_encounter data categories
live_fe_unmarked_male = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = unmarked_male_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "66d9a635-a127-4d12-8998-6e86dd93afa6")        # Not clipped

# Pull out fish_encounter data categories
live_fe_unmarked_female = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = unmarked_female_count) %>%
  mutate(sex_id = "1511973c-cbe1-4101-9481-458130041ee7") %>%                    # Female
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "66d9a635-a127-4d12-8998-6e86dd93afa6")        # Not clipped

# Pull out fish_encounter data categories
live_fe_unmarked_jack = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = unmarked_jack_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "0b0d12cf-ed27-48fb-ade2-b408067520e1") %>%               # Subadult
  mutate(adipose_clip_status_id = "66d9a635-a127-4d12-8998-6e86dd93afa6")        # Not clipped

# Pull out fish_encounter data categories
live_fe_unmarked_unknown_sex = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = unmarked_unknown_sex_count) %>%
  mutate(sex_id = "c0f86c86-dc49-406b-805d-c21a6756de91") %>%                    # Unknown
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "66d9a635-a127-4d12-8998-6e86dd93afa6")        # Not clipped

# Pull out fish_encounter data categories
live_fe_unknown_mark_male = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = unknown_mark_male_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Unable to check

# Pull out fish_encounter data categories
live_fe_unknown_mark_female = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = unknown_mark_female_count) %>%
  mutate(sex_id = "1511973c-cbe1-4101-9481-458130041ee7") %>%                    # Female
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Unable to check

# Pull out fish_encounter data categories
live_fe_unknown_mark_jack = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = unknown_mark_jack_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "0b0d12cf-ed27-48fb-ade2-b408067520e1") %>%               # Subadult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Unable to check

# Pull out fish_encounter data categories
live_fe_ad_male = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = ad_male_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Clipped

# Pull out fish_encounter data categories
live_fe_ad_female = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = ad_female_count) %>%
  mutate(sex_id = "1511973c-cbe1-4101-9481-458130041ee7") %>%                    # Female
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Clipped

# Pull out fish_encounter data categories
live_fe_ad_jack = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = ad_jack_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "0b0d12cf-ed27-48fb-ade2-b408067520e1") %>%               # Subadult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Clipped

# Pull out fish_encounter data categories
live_fe_ad_mark_unknown_sex = live %>%
  select(id, parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, species_fish,
         live_type, fish_count = ad_mark_unknown_sex_count) %>%
  mutate(sex_id = "c0f86c86-dc49-406b-805d-c21a6756de91") %>%                    # Unknown
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Clipped

# Combine
live_fe = rbind(live_fe_unknown_mark_unknown_sex,
                live_fe_unmarked_male,
                live_fe_unmarked_female,
                live_fe_unmarked_jack,
                live_fe_unmarked_unknown_sex,
                live_fe_unknown_mark_male,
                live_fe_unknown_mark_female,
                live_fe_unknown_mark_jack,
                live_fe_ad_male,
                live_fe_ad_female,
                live_fe_ad_jack,
                live_fe_ad_mark_unknown_sex)

# Check counts...then filter to > 0 below
sort(unique(live_fe$fish_count))

# # Do a quick check on ad_clip for chum...only UN and NC ok to convert all to NC
# chk_chum_clip = live_fe %>%
#   filter(species_fish == "69d1348b-7e8e-4232-981a-702eda20c9b1")
# table(chk_chum_clip$adipose_clip_status_id, useNA = "ifany")

# Check how many zeros
chk_zero_live = live_fe %>%
  filter(fish_count == 0L)

# Change any "Unable to check" cases to not checked for chum
live_fe = live_fe %>%
  filter(fish_count > 0L) %>%
  mutate(adipose_clip_status_id = if_else(species_fish == "69d1348b-7e8e-4232-981a-702eda20c9b1",
                                          "33b78489-7ad3-4482-9455-3988e05bfb28",
                                          adipose_clip_status_id))

# Add some columns
live_fe = live_fe %>%
  left_join(header_se, by = "parent_record_id") %>%
  mutate(fish_status_id = "6a200904-8a57-4dd5-8c82-2353f91186ac") %>%            # Live fish
  mutate(origin_id  = "2089de8c-3bd0-48fe-b31b-330a76d840d2") %>%                # Unknown....not in form
  mutate(mortality_type_id = "149aefd0-0369-4f2c-b85f-4ec6c5e8679c") %>%         # Not applicable...live fish
  mutate(previously_counted_indicator = 0L) %>%                                  # Not in form
  select(live_id = id, parent_record_id, survey_id, created_date,
         created_by, created_location, modified_date, modified_by,
         modified_location, fish_status_id, sex_id, maturity_id,
         origin_id, adipose_clip_status_id, fish_behavior_type_id = live_type,
         mortality_type_id, fish_count, previously_counted_indicator)

# Inspect some values
unique(live_fe$sex_id)
#table(dead_fe$sex_id, useNA = "ifany")
unique(live_fe$maturity_id)
# Check counts again
sort(unique(live_fe$fish_count))

#======================================================================================================================
# Import from redds subform
#======================================================================================================================

# Function to get dead fish records....No nested subforms
get_redds = function(profile_id, redds_page_id, start_id, access_token) {
  # Define fields
  fields = glue::glue("parent_page_id, parent_element_id, created_date, created_by, ",
                      "created_location, created_device_id, modified_date, modified_by, ",
                      "modified_location, modified_device_id, server_modified_date, ",
                      "redd_type, species_redd, redd_count, new_redd_name, transitory_redd_list, ",
                      "other_redd_name, previous_redd_name, sgs_redd_name, redd_location, ",
                      "redd_loc_in_river, river_location_text, redd_length, redd_width, ",
                      "redd_degraded, dewatered, dewater_text, si_redd, percent_si, ",
                      "si_by, si_text, sgs_redd_status, redd_latitude, redd_longitude, ",
                      "fish_on_redd, live_unknown_mark_unknown_sex_count, ",
                      "live_unmarked_unknown_sex_count, status_marksex, live_unmarked_female_count, ",
                      "live_unmarked_male_count, live_ad_clipped_female_count, ",
                      "live_ad_clipped_male_count, live_ad_clipped_unknown_sex_count, ",
                      "live_unknown_mark_female_count, live_unknown_mark_male_count, ",
                      "live_unmarked_jack_count, live_unknown_mark_jack_count, ",
                      "live_ad_clipped_jack_count,stat_week,species_code, ",
                      "new_redd_count, redd_number_generator, sch_redd_passing, ",
                      "fch_redd_passing, coho_redd_passing, steelhead_redd_passing, ",
                      "pacific_lamprey_redd_passing, wb_lamprey_redd_passing, ",
                      "encounter_comments, prev_redd_grade, prev_redd_location, ",
                      "prev_redd_si, prev_redd_dewatered, prev_live_redd, prev_comments, ",
                      "my_element_55, total_fish_on_redd, redd_status, redd_loc_accuracy, ",
                      "sgs_species, sgs_run, redd_orientation, redd_channel_type, ",
                      "redd_time_stamp, prev_species_code, prev_si_by")
  # Order by id ascending and get all records after start_id
  field_string = glue('id:<, parent_record_id(>"{start_id}"), {fields}')
  # Loop through all survey records
  redds = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = redds_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = since_id)
  return(redds)
}

# New access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Set start_id as the minimum parent_record_id minus one
start_id = min(header_data$parent_record_id) - 1

# Test...currently 8934 records: 25 secs
strt = Sys.time()
redds = get_redds(profile_id, redds_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Process dates
redds = redds %>%
  mutate(created_date = as.POSIXct(iformr::idate_time(created_date), tz = "America/Los_Angeles")) %>%
  mutate(modified_date = as.POSIXct(iformr::idate_time(modified_date), tz = "America/Los_Angeles")) %>%
  mutate(created_date = with_tz(created_date, tzone = "UTC")) %>%
  mutate(modified_date = with_tz(modified_date, tzone = "UTC"))

# Check for missing or blank species
table(redds$sgs_species, useNA = "ifany")

#================================================================================================
# Pull out redd location data
#================================================================================================

# Investigate redd type to see which need to be pulled for entry to location table
unique(redds$redd_type)
table(redds$redd_type, useNA = "ifany")

# Strategy...initial check:
# 1. Pull out just the new redd names for location table...only ten old redds had coordinates...all old had names
# 2. Pull out all the previously flagged. Identify any with no match in new, then add comment in redd_encounter
# 3. Verify that all the previously flagged have matching redd_names in location table...110 did not. Need to flag

# Pull out river_mile data for stream name
stream_info = river_mile_data %>%
  select(waterbody_id, stream_name = waterbody_display_name) %>%
  distinct()

# Pull out header data for survey_date and observers
header_info = header_data %>%
  select(survey_id = survey_uuid, survey_date, observers) %>%
  distinct()

# Get additional info from new_survey_data
redd_loc_info = survey_loc_info %>%
  left_join(stream_info, by = "waterbody_id") %>%
  left_join(header_info, by = "survey_id") %>%
  select(parent_record_id, survey_id, wria_id, waterbody_id,
         survey_date, observers, stream_name) %>%
  distinct()

# Add info to redds
redds = redds %>%
  rename(redd_id = id) %>%
  left_join(redd_loc_info, by = "parent_record_id")

# Based on instructions from Lea, convert any cases where redd_name == "6RD" |
# redd_name == "" to NA.
redds = redds %>%
  mutate(sgs_redd_name = if_else(sgs_redd_name %in% c("", "6RD", "6RD "),
                                 NA_character_, sgs_redd_name))

# Get redd location data: 2762 records
new_redd_loc = redds %>%
  filter(redd_type == "first_time_redd_encountered") %>%
  select(redd_id, parent_record_id, survey_id, created_date, created_by,
         created_location, modified_date, modified_by, survey_id, survey_date,
         observers, stream_name, waterbody_id, wria_id, redd_type, river_location_text,
         sgs_redd_name, redd_latitude, redd_longitude, redd_loc_accuracy,
         redd_orientation, redd_channel_type, sgs_run, species_redd, sgs_species) %>%
  distinct()

# Check run values: 2 Not applicable
table(new_redd_loc$sgs_run, useNA = "ifany")
any(duplicated(new_redd_loc$redd_id))

# Get redd location data: 6172 records
old_redd_loc = redds %>%
  filter(redd_type == "previously_flagged") %>%
  select(redd_id, parent_record_id, survey_id, created_date, created_by,
         created_location, modified_date, modified_by, survey_id, survey_date,
         observers, stream_name, waterbody_id, wria_id, redd_type, river_location_text,
         sgs_redd_name, redd_latitude, redd_longitude, redd_loc_accuracy,
         redd_orientation, redd_channel_type, sgs_run, species_redd, sgs_species)

# Check run values: 6093 Not applicable, 64 Unknown, 19 Spring, 14 Fall.
table(old_redd_loc$sgs_run, useNA = "ifany")
table(old_redd_loc$species_redd, useNA = "ifany")
table(old_redd_loc$sgs_species, useNA = "ifany")

# Format
new_redd_loc = new_redd_loc %>%
  mutate(redd_latitude = as.numeric(redd_latitude)) %>%
  mutate(redd_longitude = as.numeric(redd_longitude)) %>%
  mutate(redd_loc_accuracy = as.numeric(redd_loc_accuracy)) %>%
  mutate(redd_latitude = if_else(redd_latitude == 0, NA_real_, redd_latitude)) %>%
  mutate(redd_longitude = if_else(redd_longitude == 0, NA_real_, redd_longitude)) %>%
  mutate(redd_loc_accuracy = if_else(is.na(redd_longitude) | is.na(redd_latitude),
                                     NA_real_, redd_loc_accuracy))

# Format
old_redd_loc = old_redd_loc %>%
  mutate(redd_latitude = as.numeric(redd_latitude)) %>%
  mutate(redd_longitude = as.numeric(redd_longitude)) %>%
  mutate(redd_loc_accuracy = as.numeric(redd_loc_accuracy)) %>%
  mutate(redd_latitude = if_else(redd_latitude == 0, NA_real_, redd_latitude)) %>%
  mutate(redd_longitude = if_else(redd_longitude == 0, NA_real_, redd_longitude)) %>%
  mutate(redd_loc_accuracy = if_else(is.na(redd_longitude) | is.na(redd_latitude),
                                     NA_real_, redd_loc_accuracy))

# Checks ================================================================

# Pull out cases with missing coordinates for inspection: 27 cases
no_redd_coords_new = new_redd_loc %>%
  filter(is.na(redd_latitude) | is.na(redd_longitude) |
           redd_latitude < 45 | redd_longitude > -121)

# Pull out cases with missing coordinates for inspection: 6111 cases...None had coordinates
no_redd_coords_old = old_redd_loc %>%
  filter(is.na(redd_latitude) | is.na(redd_longitude) |
           redd_latitude < 45 | redd_longitude > -121)

# Pull out cases with missing redd_names for inspection: 807 cases
no_redd_name_new = new_redd_loc %>%
  mutate(sgs_redd_name = trimws(sgs_redd_name)) %>%
  filter(is.na(sgs_redd_name) | sgs_redd_name == "")

# Pull out cases with missing coordinates for inspection: 9 cases
no_redd_coords_or_name_new = new_redd_loc %>%
  mutate(sgs_redd_name = trimws(sgs_redd_name)) %>%
  filter(is.na(redd_latitude) | is.na(redd_longitude) |
           redd_latitude < 45 | redd_longitude > -121) %>%
  filter(is.na(sgs_redd_name) | sgs_redd_name == "")

# Pull out cases where redd_name is missing: 807 cases...all new redds
no_redd_name = redds %>%
  filter(is.na(sgs_redd_name)) %>%
  select(redd_id, parent_record_id, survey_id, created_date, created_by, created_location,
         survey_id, survey_date, observers, stream_name, waterbody_id, wria_id, redd_type,
         species_redd, redd_count, new_redd_name, other_redd_name, previous_redd_name,
         sgs_redd_name, redd_location, redd_latitude, redd_longitude, stat_week,
         species_code, new_redd_count, redd_number_generator, redd_status,
         prev_species_code) %>%
  arrange(stream_name, created_date)

# Inspect
unique(no_redd_name$redd_type)

# Pull out all old redds to see if a matching new_redd name exists: 0 cases
old_redd_no_names = redds %>%
  filter(redd_type == "previously_flagged") %>%
  filter(is.na(sgs_redd_name)) %>%
  mutate(sgs_redd_name = trimws(sgs_redd_name)) %>%
  select(redd_id, parent_record_id, survey_id, created_date, created_by, created_location,
         survey_id, survey_date, observers, stream_name, waterbody_id, wria_id, redd_type,
         species_redd, redd_count, new_redd_name, other_redd_name, previous_redd_name,
         sgs_redd_name, redd_location, redd_latitude, redd_longitude, stat_week,
         species_code, new_redd_count, redd_number_generator, redd_status,
         prev_species_code, sgs_run, species_redd, sgs_species) %>%
  arrange(stream_name, created_date)

# Pull out all old redds to see if a matching new_redd name exists: 6172
old_redd_with_names = redds %>%
  filter(redd_type == "previously_flagged") %>%
  filter(!is.na(sgs_redd_name)) %>%
  mutate(sgs_redd_name = trimws(sgs_redd_name)) %>%
  select(redd_id, parent_record_id, survey_id, created_date, created_by, created_location,
         survey_id, survey_date, observers, stream_name, waterbody_id, wria_id, redd_type,
         species_redd, redd_count, new_redd_name, other_redd_name, previous_redd_name,
         sgs_redd_name, redd_location, redd_latitude, redd_longitude, stat_week,
         species_code, new_redd_count, redd_number_generator, redd_status,
         prev_species_code, sgs_run, species_redd, sgs_species) %>%
  arrange(stream_name, created_date)

# Inspect any with nchar less than 7: 0 cases
short_old_redd_name = old_redd_with_names %>%
  filter(nchar(sgs_redd_name) < 7L)
unique(short_old_redd_name$sgs_redd_name)

# See if any match in new_redd_names: 0 cases
short_new_redd_name = redds %>%
  filter(redd_type == "first_time_redd_encountered") %>%
  filter(sgs_redd_name %in% c("6RD347", "6R1997", "1", "6RD060", "0604", "3799",
                              "6RD336", "", "6RD", "1431", "1432", "1433"))  %>%
  select(redd_id, parent_record_id, survey_id, created_date, created_by, created_location,
         survey_id, survey_date, observers, stream_name, waterbody_id, wria_id, redd_type,
         species_redd, redd_count, new_redd_name, other_redd_name, previous_redd_name,
         sgs_redd_name, redd_location, redd_latitude, redd_longitude, stat_week,
         species_code, new_redd_count, redd_number_generator, redd_status,
         prev_species_code) %>%
  arrange(stream_name, created_date)

# # Output with styling
# num_cols = ncol(old_redd_no_names)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "OldReddNoNames.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NoReddName", gridLines = TRUE)
# writeData(wb, sheet = 1, old_redd_no_names, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)
#
# # Output
# num_cols = ncol(short_old_redd_name)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "ShortOldReddNames.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "ShortOldReddNames", gridLines = TRUE)
# writeData(wb, sheet = 1, short_old_redd_name, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)
#
# # Output
# num_cols = ncol(short_new_redd_name)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "ShortNewReddNames.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "ShortNewReddNames", gridLines = TRUE)
# writeData(wb, sheet = 1, short_new_redd_name, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Old_redd_ids
old_sgs_redd_names = old_redd_with_names %>%
  pull(sgs_redd_name)

# Pull out all new redds
new_redd_names = new_redd_loc %>%
  mutate(sgs_redd_name = trimws(sgs_redd_name)) %>%
  filter(!is.na(sgs_redd_name))

# Old_redd_ids
new_sgs_redd_names = new_redd_names %>%
  pull(sgs_redd_name)

# Filter to matching redd_names 6189
matching_old_redd_names = old_redd_with_names %>%
  filter(sgs_redd_name %in% new_sgs_redd_names)

# Identify those where no match occurs: 0 cases.
no_match_to_old_redd_names = old_redd_with_names %>%
  filter(!sgs_redd_name %in% new_sgs_redd_names)

# # Output with styling
# num_cols = ncol(no_match_to_old_redd_names)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "NoMatchOldReddNames.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NoMatchReddNames", gridLines = TRUE)
# writeData(wb, sheet = 1, no_match_to_old_redd_names, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

#============================================================================
# Process for redd_location and redd_encounter table
#
# Notes:
#  1. Only 10 old redds had any coordinates entered. All old redds had redd_names
#  2. Only 110 old redds had no match to new redd names. These can me manually fixed
#     later using the front-end.
#  3. 532 new redds had no redd_name...These still need to be in location table if
#     coordinates are present.
#  4. Ten new redd cases had no coords or redd_name. These should not be entered
#     to location table....no need.
#
# Strategy:
#  1. Only enter new redds to location table...only 10 old redds had coordinates
#  2. Enter all redds to redd_encounter, but only include a location_id if
#     a redd_name exists or coordinates exist
#  3. Old redds will be matched by redd_name
#============================================================================

# Pull out ids of records to dump from redd_location_prep...no coords or redd_name
redd_dump_ids = no_redd_coords_or_name_new %>%
  pull(redd_id)

# # Pull out created by data
# by_dat = s_id %>%
#   select(survey_id, create_by = head_create_by,
#          mod_by = head_mod_by)

# Prep redd_location data: 2755 remaining
redd_location_prep = new_redd_loc %>%
  filter(!redd_id %in% redd_dump_ids)

# Check for dups
any(duplicated(redd_location_prep$redd_id))
any(duplicated(redd_location_prep$sgs_redd_name))    # Several are blank....pre-Lea verifying...First run through.

# Pull out the duplicated redd names from the new redds....THERE CANT BE DUPS IN NEW REDD NAMES
new_redd_name_dups = redd_location_prep %>%
  filter(!is.na(sgs_redd_name)) %>%
  filter(!sgs_redd_name == "") %>%
  filter(duplicated(sgs_redd_name)) %>%
  select(sgs_redd_name) %>%
  left_join(redd_location_prep, by = "sgs_redd_name")

# Check
unique(new_redd_name_dups$sgs_redd_name)

# # Output with styling
# num_cols = ncol(new_redd_name_dups)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "DuplicateNewReddNames.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NoReddName", gridLines = TRUE)
# writeData(wb, sheet = 1, new_redd_name_dups, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# VERIFIED: NO DUPS and NO MISSING REDD NAMES (no_match_to_old_redd_names above)

# Create and organize needed fields: 2755 records
redd_location_prep = redd_location_prep %>%
  mutate(location_id = remisc::get_uuid(nrow(redd_location_prep))) %>%
  mutate(location_type_id = "d5edb1c0-f645-4e82-92af-26f5637b2de0") %>%      # Redd encounter
  select(redd_id, parent_record_id, survey_id, location_id,
         waterbody_id, wria_id, location_type_id,
         stream_channel_type_id = redd_channel_type,
         location_orientation_type_id = redd_orientation,
         location_name = sgs_redd_name, latitude = redd_latitude,
         longitude = redd_longitude, sgs_run, species_redd,
         sgs_species, horizontal_accuracy = redd_loc_accuracy,
         created_datetime = created_date, created_by,
         modified_datetime = modified_date, modified_by)

# Check for dups
any(duplicated(redd_location_prep$redd_id))

# Pull out redd_coordinates
redd_location_coords_prep = redd_location_prep %>%
  filter(!is.na(latitude) & !is.na(longitude)) %>%
  select(location_id, latitude, longitude, horizontal_accuracy,
         created_datetime, created_by, modified_datetime,
         modified_by)

# Link redd_location back to redds data....this is all from new_redd_loc
new_redd_loc_id = redd_location_prep %>%
  select(new_redd_id = redd_id, redd_location_id = location_id,
         run_id = sgs_run, sgs_redd_name = location_name,
         sgs_species) %>%
  mutate(sgs_redd_name = trimws(sgs_redd_name)) %>%
  mutate(sgs_redd_name = if_else(sgs_redd_name == "", NA_character_, sgs_redd_name)) %>%
  filter(!sgs_redd_name %in% c("6RD", "*")) %>%
  distinct()

# Check
any(duplicated(new_redd_loc_id$new_redd_id))

# Check for missing species and run
table(new_redd_loc_id$run_id, useNA = "ifany")
table(new_redd_loc_id$sgs_species, useNA = "ifany")

# Inspect missing species...Chinook
chk_species = new_redd_loc_id %>%
  filter(is.na(sgs_species))

# # Fill in missing species...All species present this time
# new_redd_loc_id = new_redd_loc_id %>%
#   mutate(sgs_species = if_else(is.na(sgs_species),
#                                "e42aa0fc-c591-4fab-8481-55b0df38dcb1", sgs_species))

# Check again for missing species and run
table(new_redd_loc_id$run_id, useNA = "ifany")
table(new_redd_loc_id$sgs_species, useNA = "ifany")

# Check
any(duplicated(new_redd_loc_id$new_redd_id))

# Check old redd names....Need to get species and run from new_redd data
# There were many cases of missing run and species in old_redd_names. Run
# was often labeled Not applicable.
table(old_redd_with_names$sgs_run, useNA = "ifany")
table(old_redd_with_names$sgs_species, useNA = "ifany")
unique(old_redd_with_names$sgs_redd_name[nchar(old_redd_with_names$sgs_redd_name) < 7])

# Trim to only cases with sgs_redd_name and join by sgs_redd_name
old_redd_loc_id = old_redd_with_names %>%
  mutate(sgs_redd_name = trimws(sgs_redd_name)) %>%
  mutate(sgs_redd_name = if_else(sgs_redd_name == "", NA_character_, sgs_redd_name)) %>%
  filter(!sgs_redd_name %in% c("6RD", "*", "", "6RD ")) %>%
  filter(!is.na(sgs_redd_name)) %>%
  select(redd_id, sgs_redd_name, redd_status) %>%
  distinct()

# Trim new_redd_loc_id to join with old_redd_loc_id....can only join by sgs_redd_name
# So need for redd_name not to be missing
new_redd_loc_id_trim = new_redd_loc_id %>%
  filter(!is.na(sgs_redd_name))

# Check
any(duplicated(old_redd_loc_id$redd_id))
any(duplicated(new_redd_loc_id_trim$sgs_redd_name))

# Join location_id to old_redds
old_redd_loc_id = old_redd_loc_id %>%
  left_join(new_redd_loc_id_trim, by = "sgs_redd_name") %>%
  distinct()

# Arrange for inspection
old_redd_loc_id = old_redd_loc_id %>%
  select(redd_id, new_redd_id, sgs_redd_name, redd_status,
         redd_location_id, run_id, sgs_species) %>%
  arrange(sgs_redd_name, new_redd_id, redd_id)

# Pull out fields needed for join
new_redd_loc_id = new_redd_loc_id %>%
  select(redd_id = new_redd_id, redd_location_id, run_id,
         species_id = sgs_species) %>%
  distinct()

# Pull out fields needed for join
old_redd_loc_id = old_redd_loc_id %>%
  select(redd_id, old_redd_location_id = redd_location_id,
         old_run_id = run_id, old_species_id = sgs_species) %>%
  distinct()

# Check new redd id
any(is.na(new_redd_loc_id$redd_id))
any(is.na(new_redd_loc_id$redd_location_id))
any(is.na(new_redd_loc_id$run_id))
any(is.na(new_redd_loc_id$species_id))
any(duplicated(new_redd_loc$redd_id))

# Check old redd id
any(is.na(old_redd_loc_id$redd_id))
any(is.na(old_redd_loc_id$old_redd_location_id))
any(is.na(old_redd_loc_id$old_run_id))
any(is.na(old_redd_loc_id$old_species_id))
any(duplicated(old_redd_loc_id$redd_id))

# Identify the duplicated old redd_id
dup_old_redd_id = old_redd_loc_id %>%
  filter(duplicated(redd_id)) %>%
  select(redd_id) %>%
  left_join(old_redd_loc_id, by = "redd_id") %>%
  arrange(redd_id)

# Pull out the redd_ids and get the corresponding redds data
dup_redd_ids = unique(dup_old_redd_id$redd_id)
dup_redd_locs = redds %>%
  filter(redd_id %in% dup_redd_ids)

# Join to redds by redd id...both old_redd_loc_id and new_redd_loc_id
# Should give complete set of location_ids, species_ids, and run_ids for both
# old and new redds...Started with 11498 rows...should stay the same
# There should be ten missing locations....from no_redd_coords_or_name_new above
redds = redds %>%
  left_join(new_redd_loc_id, by = "redd_id") %>%
  left_join(old_redd_loc_id, by = "redd_id") %>%
  mutate(redd_location_id = if_else(is.na(redd_location_id) & !is.na(old_redd_location_id),
                                    old_redd_location_id, redd_location_id)) %>%
  mutate(run_id = if_else(is.na(run_id) & !is.na(old_run_id), old_run_id, run_id)) %>%
  mutate(species_id = if_else(is.na(species_id) & !is.na(old_species_id), old_species_id, species_id)) %>%
  mutate(run_id = if_else(is.na(run_id) & !is.na(sgs_run), sgs_run, run_id)) %>%
  mutate(species_id = if_else(is.na(species_id) & !is.na(sgs_species), sgs_species, species_id)) %>%
  select(-c(old_redd_location_id, old_species_id, old_run_id))

# Check for missing location, species, run
any(is.na(redds$redd_location_id))
chk_no_loc = redds %>%
  filter(is.na(redd_location_id))
any(is.na(redds$species_id))
any(is.na(redds$run_id))
table(redds$species_id, useNA = "ifany")
table(redds$run_id, useNA = "ifany")

# Pull out redd survey_event data
redds_se = redds %>%
  select(redd_id, parent_record_id, survey_id, sgs_species = species_id,
         sgs_run = run_id, sgs_redd_name, created_date, created_by, modified_date,
         modified_by) %>%
  left_join(header_se, by = c("survey_id", "parent_record_id"))

# Check species
table(redds_se$sgs_species, useNA = "ifany")

# HACK ALERT !!! Pull out cases where species is blank
chk_redd_species = redds_se %>%
  filter(sgs_species == "" | is.na(sgs_species))

# # Output with styling
# num_cols = ncol(chk_redd_species)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "NoReddSpecies.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NoReddName", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_redd_species, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Inspect chinook run_type....Will keep as is in database but transform in export to Curt
chin_run = redds_se %>%
  filter(sgs_species == "e42aa0fc-c591-4fab-8481-55b0df38dcb1")
table(chin_run$sgs_run, useNA = "ifany")

# Update only the Not applicable run type for chinook to Unknown
redds_se = redds_se %>%
  mutate(sgs_run = if_else(sgs_species == "e42aa0fc-c591-4fab-8481-55b0df38dcb1" &
                             sgs_run == "59e1e01f-3aef-498c-8755-5862c025eafa",
                           "94e1757f-b9c7-4b06-a461-17a2d804cd2f", sgs_run))

# Inspect coho run_type.......Lea's instructions...Convert all cases to fall
coho_run = redds_se %>%
  filter(sgs_species == "a0f5b3af-fa07-449c-9f02-14c5368ab304")
table(coho_run$sgs_run, useNA = "ifany")

# Inspect sthd run_type.....Lea's instructions...Convert all cases to winter
sthd_run = redds_se %>%
  filter(sgs_species == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1")
table(sthd_run$sgs_run, useNA = "ifany")

# Inspect chum run_type.....Lea's instructions...Convert all cases to fall
chum_run = redds_se %>%
  filter(sgs_species == "69d1348b-7e8e-4232-981a-702eda20c9b1")
table(chum_run$sgs_run, useNA = "ifany")

# Inspect plamp run_type.....Lea's instructions...Convert all cases to fall
plamp_run = redds_se %>%
  filter(sgs_species == "2afac5a6-e3b9-4b37-911e-59b93240789d")
table(plamp_run$sgs_run, useNA = "ifany")

# Inspect chum run_type.....Lea's instructions...Convert all cases to fall
wblamp_run = redds_se %>%
  filter(sgs_species == "d29ca246-acfa-48d5-ba55-e61323d59fa7")
table(wblamp_run$sgs_run, useNA = "ifany")

# Add header info for Lea
chin_run = chin_run %>%
  filter(sgs_run %in% c("2a9f3c67-aa7a-4214-87a6-af09879fc914",
                        "94e1757f-b9c7-4b06-a461-17a2d804cd2f",
                        "59e1e01f-3aef-498c-8755-5862c025eafa")) %>%
  left_join(chin_head, by = "parent_record_id") %>%
  mutate(run_type = case_when(
    sgs_run == "2a9f3c67-aa7a-4214-87a6-af09879fc914" ~ "Spring",
    sgs_run == "94e1757f-b9c7-4b06-a461-17a2d804cd2f" ~ "Unknown",
    sgs_run == "59e1e01f-3aef-498c-8755-5862c025eafa" ~ "Not applicable"))

# # Output
# out_name = paste0("data/ChinookRunTypeCheck_ReddsSubform.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "RunTypeCheck", gridLines = TRUE)
# writeData(wb, sheet = 1, chin_run, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:ncol(chin_run), gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Check for missing run types in chin_run
chk_chin_run = chin_run %>%
  filter(is.na(sgs_run) | sgs_run == "")

# # Since Lea is out I'm changing all NAs in run to unknown....Can always update in front-end later
# redds_se = redds_se %>%
#   # Convert missing values for Chinook run to unknown
#   mutate(sgs_run = if_else(sgs_species == "e42aa0fc-c591-4fab-8481-55b0df38dcb1" &
#                            (is.na(sgs_run) | sgs_run == ""),
#                            "94e1757f-b9c7-4b06-a461-17a2d804cd2f", sgs_run))

# Correct species and run_type....HACK WARNING. Will not need to do in future. Will be fixed in IFB
redds_se = redds_se %>%
  # Convert all coho to fall
  mutate(sgs_run = if_else(sgs_species == "a0f5b3af-fa07-449c-9f02-14c5368ab304",
                           "dc8de7ed-c825-4f3b-93d4-87fee088ac51", sgs_run)) %>%
  # Convert all sthd to winter
  mutate(sgs_run = if_else(sgs_species == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1",
                           "1b413852-b4e8-42d1-81d9-a7bc373be906", sgs_run)) %>%
  # Convert all chum to fall
  mutate(sgs_run = if_else(sgs_species == "69d1348b-7e8e-4232-981a-702eda20c9b1",
                           "dc8de7ed-c825-4f3b-93d4-87fee088ac51", sgs_run)) %>%
  # Convert all plamp to Not applicable
  mutate(sgs_run = if_else(sgs_species == "2afac5a6-e3b9-4b37-911e-59b93240789d",
                           "59e1e01f-3aef-498c-8755-5862c025eafa", sgs_run)) %>%
  # Convert all wblamp to Not applicable
  mutate(sgs_run = if_else(sgs_species == "d29ca246-acfa-48d5-ba55-e61323d59fa7",
                           "59e1e01f-3aef-498c-8755-5862c025eafa", sgs_run))

# Inspect again
# Inspect chinook run_type.....Lea will update run_type as needed in iforms
chin_run = redds_se %>%
  filter(sgs_species == "e42aa0fc-c591-4fab-8481-55b0df38dcb1")
table(chin_run$sgs_run, useNA = "ifany")

# Inspect coho run_type.......Lea's instructions...Convert all cases to fall
coho_run = redds_se %>%
  filter(sgs_species == "a0f5b3af-fa07-449c-9f02-14c5368ab304")
table(coho_run$sgs_run, useNA = "ifany")

# Inspect sthd run_type.....Lea's instructions...Convert all cases to winter
sthd_run = redds_se %>%
  filter(sgs_species == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1")
table(sthd_run$sgs_run, useNA = "ifany")

# Inspect chum run_type.....Lea's instructions...Convert all cases to fall
chum_run = redds_se %>%
  filter(sgs_species == "69d1348b-7e8e-4232-981a-702eda20c9b1")
table(chum_run$sgs_run, useNA = "ifany")

# Inspect plamp run_type.....Lea's instructions...Convert all cases to fall
plamp_run = redds_se %>%
  filter(sgs_species == "2afac5a6-e3b9-4b37-911e-59b93240789d")
table(plamp_run$sgs_run, useNA = "ifany")

# Inspect wblamp run_type.....Lea's instructions...Convert all cases to fall
wblamp_run = redds_se %>%
  filter(sgs_species == "d29ca246-acfa-48d5-ba55-e61323d59fa7")
table(wblamp_run$sgs_run, useNA = "ifany")

#================================================================================================
# Combine all se tables to calculate survey_event_id...can tie back using subform id
#================================================================================================

# Prep tables for stacking ========================================

# Dead
dead_sev = dead_se %>%
  select(dead_id, survey_id, species_id = species_fish,
         run_id = run_type, created_date, created_by,
         modified_date, modified_by) %>%
  left_join(header_se, by = "survey_id") %>%
  mutate(subsource = "dead") %>%
  select(dead_id, survey_id, species_id, survey_design_type_id,
         run_id, subsource)
# Recaps
recaps_sev = recaps_se %>%
  mutate(subsource = "d_recap") %>%
  select(recap_id, survey_id, species_id, survey_design_type_id,
         run_id, subsource)
# Live
live_sev = live_se %>%
  left_join(header_se, by = "survey_id") %>%
  mutate(subsource = "alive") %>%
  select(live_id, survey_id, species_id = species_fish,
         survey_design_type_id, run_id = run_type,
         subsource)

# Redds
redds_sev = redds_se %>%
  mutate(subsource = "b_redd") %>%
  select(redd_id, survey_id, species_id = sgs_species,
         survey_design_type_id, run_id = sgs_run,
         subsource)

# Double-check
table(redds_sev$species_id, useNA = "ifany")
table(redds_sev$run_id, useNA = "ifany")

# Verify the columns
unique(redds_sev$species_id[!nchar(redds_sev$species_id) == 36L])
unique(redds_sev$run_id[!nchar(redds_sev$run_id) == 36L])

#==============================================================
# Add sev data from survey_intent
#==============================================================

# Pull out and create needed columns for intent_sev
intent_sev = intent_prep %>%
  select(survey_id, species_id, count_type_id) %>%
  mutate(count_type = case_when(
    count_type_id =="0e1980b4-aa59-4e8e-a820-ce5e4629a549" ~ "carcass",
    count_type_id == "68a9427b-c856-4751-a7ea-e35b515a36d7" ~ "redd",
    count_type_id == "7a785819-3a9f-4728-88db-7e77e580cd41" ~ "live")) %>%
  mutate(run_id = "94e1757f-b9c7-4b06-a461-17a2d804cd2f") %>%
  left_join(header_se, by = "survey_id") %>%
  mutate(subsource = "x_intent") %>%
  select(count_type, survey_id, species_id, survey_design_type_id,
         run_id, subsource) %>%
  distinct()

# Get run types
get_run_id = function() {
  qry = glue("select run_id, run_short_description as run ",
             "from spawning_ground.run_lut")
  con = pg_con_prod("FISH")
  #con = poolCheckout(pool)
  run_ids = dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  return(run_ids)
}

# Stack the sev tables: 27384 rows
survey_event = bind_rows(intent_sev, dead_sev, recaps_sev,
                         live_sev, redds_sev) %>%
  select(count_type, dead_id, live_id, recap_id, redd_id,
         survey_id, species_id, survey_design_type_id,
         run_id, subsource) %>%
  left_join(species_ids, by = "species_id") %>%
  left_join(get_run_id(), by = "run_id") %>%
  mutate(count_type = if_else(is.na(count_type), "actual", count_type)) %>%
  mutate(count_type = case_when(
    count_type == "live" ~ "alive",
    count_type == "redd" ~ "bredd",
    count_type == "carcass" ~ "carcass",
    count_type == "actual" ~ "actual",
    TRUE ~ count_type)) %>%
  mutate(run = if_else(run == "Unknown", "xUnknown", run)) %>%
  arrange(survey_id, species_id, run, subsource, count_type, survey_design_type_id) %>%
  group_by(survey_id, species_id) %>%
  mutate(n_seq_intent = row_number()) %>%
  ungroup() %>%
  arrange(survey_id, species_id, n_seq_intent, subsource, count_type, survey_design_type_id)

# Check
table(survey_event$species_id, useNA = "ifany")
table(survey_event$run_id, useNA = "ifany")

# Verify the columns
unique(survey_event$survey_id[!nchar(survey_event$survey_id) == 36L])
unique(survey_event$species_id[!nchar(survey_event$species_id) == 36L])
unique(survey_event$survey_design_type_id[!nchar(survey_event$survey_design_type_id) == 36L])
unique(survey_event$run_id[!nchar(survey_event$run_id) == 36L])

#=========================================================================
# We only need one survey_type per intended species for each survey
# Dump intent subsource if survey_type for species already present
#=========================================================================

# First verify that all surveys are present in survey_event....All are present
s_ids = unique(survey_prep$survey_id)
chk_se = survey_event %>%
  filter(!survey_id %in% s_ids)

# Inspect values 27384 rows
unique(survey_event$subsource)
unique(survey_event$run)

# Keep only the first case of n_seq_intent....if source is intent then add zero's to either
# fish_encounter or redd_encounter depending on count_type
survey_event = survey_event %>%
  mutate(del_row_intent = if_else(subsource == "x_intent" & n_seq_intent > 1L, "dump", "keep"))

# Keep only the first case of survey_id, species, and run: Look below, should be 5686 records left
survey_event = survey_event %>%
  arrange(survey_id, common_name, run, subsource, survey_design_type_id) %>%
  group_by(survey_id, species_id, run) %>%
  mutate(n_seq_run = row_number()) %>%
  ungroup() %>%
  arrange(survey_id, common_name, run, n_seq_run, subsource, count_type, survey_design_type_id)

# Add detection method info
dead_detect = dead_fe %>%
  select(survey_id, cwt_detected, fish_count) %>%
  mutate(cwt_detected = if_else(is.na(cwt_detected) | cwt_detected == "",
                                "bd7c5765-2ca3-4ab4-80bc-ce1a61ad8115", cwt_detected)) %>%   # Not applicable
  mutate(cwt_detected = if_else(cwt_detected == "unknown",
                                "efe698a8-98dd-45df-ba5b-0d448c88121d", cwt_detected)) %>%   # Undetermined
  distinct()

# Select only one type of detection method per survey...method only comes from dead subform
# and should only apply when fish_count > 0. All data in dead_detect > 0.
dead_detect = dead_detect %>%
  mutate(cwt_detection_method_id =
           if_else(cwt_detected %in% c("6242055f-b2bc-44c1-b0d4-3ce24be44bbe",          # Beep or no beep
                                       "ba4209af-3839-46a7-bd5d-57bc8516f7af"),
                   "d2de4873-e9ab-4eda-b1a0-fb9dcc2face7",                              # Electronic
                   "89a9b6b4-6ea4-44c4-b2e4-e537060e73d3")) %>%                         # Not applicable
  arrange(survey_id, desc(cwt_detection_method_id)) %>%
  group_by(survey_id) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  arrange(survey_id, n_seq) %>%
  filter(n_seq == 1L) %>%
  select(survey_id, cwt_detection_method_id)

# Check
unique(dead_detect$cwt_detection_method_id)
any(duplicated(dead_detect$survey_id))
unique(dead_detect$cwt_detection_method_id[!nchar(dead_detect$cwt_detection_method_id) == 36L])
unique(survey_event$species_id)

# Add to survey_event
survey_event = survey_event %>%
  left_join(dead_detect, by = "survey_id") %>%
  mutate(cwt_detection_method_id =
           if_else(!species_id %in% c("e42aa0fc-c591-4fab-8481-55b0df38dcb1",
                                      "a0f5b3af-fa07-449c-9f02-14c5368ab304",
                                      "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1") |     # Detection species (chin, coho, sthd)
                     is.na(cwt_detection_method_id),                                # No sampled carcasses
                   "89a9b6b4-6ea4-44c4-b2e4-e537060e73d3",                          # Not applicable
                   cwt_detection_method_id))

# Check
unique(survey_event$cwt_detection_method_id[!nchar(survey_event$cwt_detection_method_id) == 36L])
table(survey_event$cwt_detection_method_id, useNA = "ifany")

# Add run_year
run_year_info = header_se %>%
  select(survey_id, coho_run_year, steelhead_run_year,
         chum_run_year, chinook_run_year) %>%
  distinct()

# Check run
chk_na_run = survey_event %>%
  filter(is.na(run_id))
unique(chk_na_run$species_id)
table(chk_na_run$species_id, useNA = "ifany")
unique(survey_event$run_id)
table(survey_event$run_id, useNA = "ifany")

# Add run definitions where needed...27384 rows
survey_event = survey_event %>%
  left_join(run_year_info, by = "survey_id") %>%
  mutate(run_year = case_when(
    species_id == "a0f5b3af-fa07-449c-9f02-14c5368ab304" ~ coho_run_year,
    species_id == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1" ~ steelhead_run_year,
    species_id == "69d1348b-7e8e-4232-981a-702eda20c9b1" ~ chum_run_year,
    species_id == "e42aa0fc-c591-4fab-8481-55b0df38dcb1" ~ chinook_run_year,
    TRUE ~ chinook_run_year)) %>%
  # Compute run definitions where missing
  # Chum == Fall
  mutate(run_id = if_else(is.na(run_id) &
                            species_id == "69d1348b-7e8e-4232-981a-702eda20c9b1",
                          "94e1757f-b9c7-4b06-a461-17a2d804cd2f", run_id)) %>%
  # Coho == Fall
  mutate(run_id = if_else(is.na(run_id) &
                            species_id == "a0f5b3af-fa07-449c-9f02-14c5368ab304",
                          "94e1757f-b9c7-4b06-a461-17a2d804cd2f", run_id)) %>%
  # Sthd == Winter
  mutate(run_id = if_else(is.na(run_id) &
                            species_id == "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1",
                          "1b413852-b4e8-42d1-81d9-a7bc373be906", run_id)) %>%
  # Compute run definitions for lamprey...even where not missing
  mutate(run_id = if_else(species_id %in%
                            c("2afac5a6-e3b9-4b37-911e-59b93240789d",             # P Lamprey
                              "d29ca246-acfa-48d5-ba55-e61323d59fa7"),            # WB Lamprey
                          "59e1e01f-3aef-498c-8755-5862c025eafa", run_id)) %>%    # Not applicable
  select(count_type, dead_id, live_id, recap_id, redd_id, survey_id,
         species_id, survey_design_type_id, run_id, subsource, common_name, run,
         n_seq_intent, del_row_intent, n_seq_run, cwt_detection_method_id, run_year)

# Check for missing species
any(is.na(survey_event$survey_id))
any(is.na(survey_event$species_id))
any(is.na(survey_event$survey_design_type_id))
any(is.na(survey_event$cwt_detection_method_id))
any(is.na(survey_event$run_id))
any(is.na(survey_event$run_year))

# Generate survey_event_id
survey_event = survey_event %>%
  group_by(survey_id, species_id, survey_design_type_id,
           cwt_detection_method_id, run_id, run_year) %>%
  mutate(survey_event_id = remisc::get_uuid(1L)) %>%
  ungroup() %>%
  select(count_type, dead_id, live_id, recap_id, redd_id, survey_event_id,
         survey_id, species_id, survey_design_type_id, run_id,
         cwt_detection_method_id, run_year, subsource, common_name, run,
         n_seq_intent, del_row_intent, n_seq_run) %>%
  arrange(survey_event_id)

# Pull out just the table data: 7045 rows
survey_event_prep = survey_event %>%
  mutate(comment_text = NA_character_) %>%
  mutate(estimated_percent_fish_seen = NA_integer_) %>%
  select(survey_event_id, survey_id, species_id,
         survey_design_type_id, cwt_detection_method_id,
         run_id, run_year, estimated_percent_fish_seen,
         comment_text) %>%
  distinct()

# Add trailing fields using data from header
survey_trailing = header_data %>%
  select(survey_id = survey_uuid, created_datetime, created_by,
         modified_datetime, modified_by) %>%
  group_by(survey_id) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq == 1L) %>%
  select(-n_seq) %>%
  distinct()

# Check
any(duplicated(survey_trailing$survey_id))

# Add trailing fields
survey_event_prep = survey_event_prep %>%
  left_join(survey_trailing, by = "survey_id") %>%
  select(survey_event_id, survey_id, species_id, survey_design_type_id,
         cwt_detection_method_id, run_id, run_year, estimated_percent_fish_seen,
         comment_text, created_datetime, created_by, modified_datetime,
         modified_by) %>%
  distinct()

# Verify values
any(duplicated(survey_event_prep$survey_event_id))
any(is.na(survey_event_prep$created_datetime))

#================================================================================================
# Pull out live fish encounter data from redds subform
#================================================================================================

# Live redd data
live_redd = redds %>%
  select(redd_id, survey_id, created_date, created_by, modified_date, modified_by,
         fish_location_id = redd_location_id, fish_on_redd, status_marksex,
         live_unknown_mark_unknown_sex_count, live_unmarked_unknown_sex_count,
         live_unmarked_female_count, live_unmarked_male_count,
         live_ad_clipped_female_count, live_ad_clipped_male_count,
         live_ad_clipped_unknown_sex_count, live_unknown_mark_female_count,
         live_unknown_mark_male_count, live_unmarked_jack_count,
         live_unknown_mark_jack_count, live_ad_clipped_jack_count,
         encounter_comments, total_fish_on_redd) %>%
  filter(total_fish_on_redd > 0)

# Pull out fish_encounter data categories
live_rd_unknown_mark_unknown_sex = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_unknown_mark_unknown_sex_count) %>%
  mutate(sex_id = "c0f86c86-dc49-406b-805d-c21a6756de91") %>%                    # Unknown
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Unable to check

# Pull out fish_encounter data categories
live_rd_unmarked_unknown_sex = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_unmarked_unknown_sex_count) %>%
  mutate(sex_id = "c0f86c86-dc49-406b-805d-c21a6756de91") %>%                    # Unknown
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "66d9a635-a127-4d12-8998-6e86dd93afa6")        # Not clipped

# Pull out fish_encounter data categories
live_rd_unmarked_female = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_unmarked_female_count) %>%
  mutate(sex_id = "1511973c-cbe1-4101-9481-458130041ee7") %>%                    # Female
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "66d9a635-a127-4d12-8998-6e86dd93afa6")        # Not clipped

# Pull out fish_encounter data categories
live_rd_unmarked_male = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_unmarked_male_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "66d9a635-a127-4d12-8998-6e86dd93afa6")        # Not clipped

# Pull out fish_encounter data categories
live_rd_ad_female = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_ad_clipped_female_count) %>%
  mutate(sex_id = "1511973c-cbe1-4101-9481-458130041ee7") %>%                    # Female
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Clipped

# Pull out fish_encounter data categories
live_rd_ad_male = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_ad_clipped_male_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Clipped

# Pull out fish_encounter data categories
live_rd_ad_mark_unknown_sex = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_ad_clipped_unknown_sex_count) %>%
  mutate(sex_id = "c0f86c86-dc49-406b-805d-c21a6756de91") %>%                    # Unknown
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Clipped

# Pull out fish_encounter data categories
live_rd_unknown_mark_female = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_unknown_mark_female_count) %>%
  mutate(sex_id = "1511973c-cbe1-4101-9481-458130041ee7") %>%                    # Female
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Unable to check

# Pull out fish_encounter data categories
live_rd_unknown_mark_male = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_unknown_mark_male_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "68347504-ee22-4632-9856-a4f4366b2bd8") %>%               # Adult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Unable to check

# Pull out fish_encounter data categories
live_rd_unmarked_jack = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_unmarked_jack_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "0b0d12cf-ed27-48fb-ade2-b408067520e1") %>%               # Subadult
  mutate(adipose_clip_status_id = "66d9a635-a127-4d12-8998-6e86dd93afa6")        # Not clipped

# Pull out fish_encounter data categories
live_rd_unknown_mark_jack = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_unknown_mark_jack_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "0b0d12cf-ed27-48fb-ade2-b408067520e1") %>%               # Subadult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Unable to check

# Pull out fish_encounter data categories
live_rd_ad_jack = live_redd %>%
  select(redd_id, survey_id, created_date, created_by, modified_date,
         modified_by, fish_location_id, fish_on_redd,
         total_fish_on_redd, fish_count = live_ad_clipped_jack_count) %>%
  mutate(sex_id = "ccdde151-4828-4597-8117-4635d8d47a71") %>%                    # Male
  mutate(maturity_id = "0b0d12cf-ed27-48fb-ade2-b408067520e1") %>%               # Subadult
  mutate(adipose_clip_status_id = "c989e267-c2cb-4d0a-842c-725f4257ace1")        # Clipped

# Combine
live_rd = rbind(live_rd_unknown_mark_unknown_sex,
                live_rd_unmarked_male,
                live_rd_unmarked_female,
                live_rd_unmarked_jack,
                live_rd_unmarked_unknown_sex,
                live_rd_unknown_mark_male,
                live_rd_unknown_mark_female,
                live_rd_unknown_mark_jack,
                live_rd_ad_male,
                live_rd_ad_female,
                live_rd_ad_jack,
                live_rd_ad_mark_unknown_sex) %>%
  mutate(fish_count = as.integer(fish_count)) %>%
  filter(fish_count > 0L)

# Check for zero counts, then dump counts < 1 below
sort(unique(live_rd$fish_count))

# Add species and survey_event
live_rd_se = survey_event %>%
  filter(!is.na(redd_id)) %>%
  select(redd_id, survey_event_id, species_id) %>%
  distinct()

# Add to live_rd
live_rd = live_rd %>%
  left_join(live_rd_se, by = "redd_id")

# # Do a quick check on ad_clip for chum...No chum in dataset
# chk_chum_clip = live_rd %>%
#   filter(species_id == "69d1348b-7e8e-4232-981a-702eda20c9b1")
# table(chk_chum_clip$adipose_clip_status_id, useNA = "ifany")
unique(live_rd$adipose_clip_status_id)
table(live_rd$adipose_clip_status_id, useNA = "ifany")
unique(live_rd$species_id)

# Change any "Unable to check" cases to not checked for non-sampled species
live_rd = live_rd %>%
  mutate(adipose_clip_status_id = if_else(!species_id %in% c("e42aa0fc-c591-4fab-8481-55b0df38dcb1",
                                                             "a0f5b3af-fa07-449c-9f02-14c5368ab304",
                                                             "aa9f07cf-91f8-4244-ad17-7530b8cd1ce1"),  # Detection species (chin, coho, sthd)
                                          "33b78489-7ad3-4482-9455-3988e05bfb28",
                                          adipose_clip_status_id))

# Check
table(live_rd$adipose_clip_status_id, useNA = "ifany")

# Add some columns
live_rd = live_rd %>%
  mutate(fish_status_id = "6a200904-8a57-4dd5-8c82-2353f91186ac") %>%            # Live fish
  mutate(origin_id  = "2089de8c-3bd0-48fe-b31b-330a76d840d2") %>%                # Unknown....not in form
  mutate(mortality_type_id = "149aefd0-0369-4f2c-b85f-4ec6c5e8679c") %>%         # Not applicable...live fish
  mutate(fish_behavior_type_id = "99a9ae14-460e-4bcf-b659-047f15591986") %>%     # Spawning
  mutate(previously_counted_indicator = 0L) %>%                                  # Not in form
  mutate(fish_count = as.integer(fish_count)) %>%
  select(redd_id, survey_id, survey_event_id, fish_location_id, fish_status_id,
         sex_id, maturity_id, origin_id, adipose_clip_status_id, fish_behavior_type_id,
         mortality_type_id, fish_count, previously_counted_indicator,
         fish_on_redd, total_fish_on_redd, created_date, created_by,
         modified_date, modified_by)

# Verify fish_count == total_fish_on_redd
chk_count = live_rd %>%
  group_by(redd_id) %>%
  mutate(sum_fish_count = sum(fish_count, na.rm = TRUE)) %>%
  ungroup() %>%
  select(redd_id, sum_fish_count, total_fish_on_redd) %>%
  mutate(count_dif = if_else(!sum_fish_count == total_fish_on_redd, "yes", "no")) %>%
  filter(count_dif == "yes") %>%
  distinct()

# Add values for output
add_chk_fields = redds %>%
  select(redd_id, parent_record_id, stream_name, survey_date,
         sgs_redd_name, observers) %>%
  distinct()

# Add to chk_count
chk_count = chk_count %>%
  left_join(add_chk_fields, by = "redd_id") %>%
  select(redd_id, parent_record_id, stream_name, survey_date,
         sgs_redd_name, observers, sum_fish_count, total_fish_on_redd)

# # Output with styling
# num_cols = ncol(chk_count)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "CheckReddLiveCounts.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "CheckReddLiveCounts", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_count, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

#================================================================================================
# Combine all fish encounter data and add fish_encounter id
#================================================================================================

# Strategy:
#  1. All fish_mark data derive from dead. Recaps are a separate encounter.
#     So define fish_encounter_id using combination of live, live_redd, and recap,
#     and dead data.
#  2. Join new fish_encounter_id to fish_mark and fish_capture_event using dead_id
#  3. Join new fish_encounter_id to recaps data using recap_id (recaps_fish_mark),
#     then to recaps_fish_capture using recaps_id
#  4. Join new fish_encounter_id to live_redd using redd_id

# Prep dead data
dead_se_id = survey_event %>%
  select(dead_id, survey_event_id) %>%
  filter(!is.na(dead_id)) %>%
  distinct()

# Add real_time info in case we want to use carcass_location
real_time_info = header_data %>%
  select(survey_id = survey_uuid, data_entry_type) %>%
  filter(!is.na(data_entry_type)) %>%
  distinct()

# Join to dead_fe
dead_fev = dead_fe %>%
  left_join(dead_se_id, by = "dead_id") %>%
  left_join(real_time_info, by = "survey_id") %>%
  mutate(cwt_detection_status_id = if_else(is.na(cwt_detected) | cwt_detected == "",
                                "bd7c5765-2ca3-4ab4-80bc-ce1a61ad8115", cwt_detected)) %>%   # Not applicable
  mutate(cwt_detection_status_id = if_else(cwt_detection_status_id == "unknown",
                                "efe698a8-98dd-45df-ba5b-0d448c88121d",                      # Undetermined
                                cwt_detection_status_id)) %>%
  select(dead_id, survey_id, survey_event_id, created_location,
         data_entry_type, fish_status_id, sex_id, maturity_id, origin_id,
         cwt_detection_status_id, adipose_clip_status_id = clip_status,
         fish_behavior_type_id, mortality_type_id, fish_count,
         previously_counted_indicator, created_date, created_by,
         modified_date, modified_by) %>%
  arrange(dead_id)

# Inspect some dead values
any(is.na(dead_fev$survey_event_id))
unique(dead_fev$sex_id)
unique(dead_fev$maturity_id)
unique(dead_fev$origin_id)
unique(dead_fev$cwt_detection_status_id)
table(dead_fev$cwt_detection_status_id, useNA = "ifany")
unique(dead_fev$adipose_clip_status_id)
unique(dead_fev$fish_behavior_type_id)
unique(dead_fev$mortality_type_id)
unique(dead_fev$data_entry_type)

# Prep the recap data ===============================

# First get tag_number and dead_id. Needed to link to dead_fev
dead_tag_number = fish_mark %>%
  filter(!is.na(dead_id)) %>%
  filter(!is.na(tag_number)) %>%
  #filter(!tag_number %in% c("Not present", "not present")) %>%
  select(dead_id, tag_number) %>%
  distinct()

# Get detailed info from match to tag_number and dead_fev
dead_tag_number = dead_tag_number %>%
  left_join(dead_fev, by = "dead_id") %>%
  mutate(previously_counted_indicator = 1L) %>%
  select(dead_id, tag_number, survey_id, survey_event_id,
         fish_status_id, sex_id, maturity_id, origin_id,
         cwt_detection_status_id, adipose_clip_status_id,
         fish_behavior_type_id, mortality_type_id,
         fish_count, previously_counted_indicator)

# Check
sort(unique(dead_tag_number$fish_count))
sort(unique(dead_tag_number$tag_number))
any(duplicated(dead_tag_number$tag_number))
dead_tag_number$tag_number[duplicated(dead_tag_number$tag_number)]

# Since next step joins on tag_number, inspect duplicates of tag_number
any(duplicated(recaps_fish_mark$tag_number))
dup_tag = recaps_fish_mark$tag_number[duplicated(recaps_fish_mark$tag_number)]
chk_tag_dup = recaps_fish_mark %>%          # 328 records in recaps_fish_mark
  filter(tag_number %in% dup_tag)           # 326 duplicated records in chk_tag_dup

# Identify non_duplicated records in recaps_fish_mark....only Not Present, not present
chk_no_tag_dup = recaps_fish_mark %>%
  filter(!tag_number %in% dup_tag)

# # Output
# num_cols = ncol(chk_no_tag_dup)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "RecapTagNotPresent.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "TagNotPresent", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_no_tag_dup, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Join to recap data
recap_fev = recaps_fish_mark %>%
  select(recap_id, tag_number) %>%
  distinct() %>%
  left_join(dead_tag_number, by = "tag_number") %>%
  filter(!is.na(survey_id)) %>%
  select(recap_id, survey_id, survey_event_id, fish_status_id,
         sex_id, maturity_id, origin_id, cwt_detection_status_id,
         adipose_clip_status_id, fish_behavior_type_id, mortality_type_id,
         fish_count, previously_counted_indicator) %>%
  distinct()

# Prep live data
live_se_id = survey_event %>%
  select(live_id, survey_event_id) %>%
  filter(!is.na(live_id)) %>%
  distinct()

# Prep live data from live subform
live_fev = live_fe %>%
  left_join(live_se_id, by = "live_id") %>%
  mutate(cwt_detection_status_id = "bd7c5765-2ca3-4ab4-80bc-ce1a61ad8115") %>%             # Not applicable
  select(live_id, survey_id, survey_event_id, created_location,
         fish_status_id, sex_id, maturity_id, origin_id,
         cwt_detection_status_id, adipose_clip_status_id,
         fish_behavior_type_id, mortality_type_id, fish_count,
         previously_counted_indicator, created_date, created_by,
         modified_date, modified_by)

# Inspect some live values
any(is.na(live_fev$survey_event_id))
unique(live_fev$sex_id)
unique(live_fev$maturity_id)
unique(live_fev$origin_id)
unique(live_fev$cwt_detection_status_id)
table(live_fev$cwt_detection_status_id, useNA = "ifany")
unique(live_fev$adipose_clip_status_id)
unique(live_fev$fish_behavior_type_id)
unique(live_fev$mortality_type_id)

# Prep live data from redd subform
live_rd_fev = live_rd %>%
  mutate(cwt_detection_status_id = "bd7c5765-2ca3-4ab4-80bc-ce1a61ad8115") %>%             # Not applicable
  select(redd_id, survey_id, survey_event_id, fish_location_id,
         fish_status_id, sex_id, maturity_id, origin_id,
         cwt_detection_status_id, adipose_clip_status_id,
         fish_behavior_type_id, mortality_type_id, fish_count,
         previously_counted_indicator, created_date, created_by,
         modified_date, modified_by)

# Inspect some live values
any(is.na(live_rd_fev$survey_event_id))
unique(live_rd_fev$sex_id)
unique(live_rd_fev$maturity_id)
unique(live_rd_fev$origin_id)
unique(live_rd_fev$cwt_detection_status_id)
table(live_rd_fev$cwt_detection_status_id, useNA = "ifany")
unique(live_rd_fev$adipose_clip_status_id)
unique(live_rd_fev$fish_behavior_type_id)
unique(live_rd_fev$mortality_type_id)

# Stack the fev tables
fish_encounter = bind_rows(dead_fev, recap_fev, live_fev, live_rd_fev) %>%
  select(dead_id, recap_id, live_id, redd_id, survey_id, survey_event_id,
         fish_location_id, fish_status_id, sex_id, maturity_id,
         origin_id, cwt_detection_status_id, adipose_clip_status_id,
         fish_behavior_type_id, mortality_type_id, fish_count,
         previously_counted_indicator)

# Verify the columns
unique(fish_encounter$survey_event_id[!nchar(fish_encounter$survey_event_id) == 36L])
unique(fish_encounter$fish_location_id[!nchar(fish_encounter$fish_location_id) == 36L])
unique(fish_encounter$fish_status_id[!nchar(fish_encounter$fish_status_id) == 36L])
unique(fish_encounter$sex_id[!nchar(fish_encounter$sex_id) == 36L])
unique(fish_encounter$maturity_id[!nchar(fish_encounter$maturity_id) == 36L])
unique(fish_encounter$origin_id[!nchar(fish_encounter$origin_id) == 36L])
unique(fish_encounter$cwt_detection_status_id[!nchar(fish_encounter$cwt_detection_status_id) == 36L])
unique(fish_encounter$adipose_clip_status_id[!nchar(fish_encounter$adipose_clip_status_id) == 36L])
unique(fish_encounter$fish_behavior_type_id[!nchar(fish_encounter$fish_behavior_type_id) == 36L])
unique(fish_encounter$mortality_type_id[!nchar(fish_encounter$mortality_type_id) == 36L])
unique(fish_encounter$previously_counted_indicator)

# Check the records with no count
chk_fish_count = fish_encounter %>%
  filter(is.na(fish_count))

#===============================================================================================
# Section to add zeros from survey_intent where needed. Just add zero for live if intended.
# Otherwise add to dead or redd in that order.
#===============================================================================================

# Get first instance of survey_event data generated from survey_intent...used to add zeros
intent_zero = survey_event %>%
  filter(subsource == "x_intent" & n_seq_intent == 1L)

# Verify all are from survey_intent. None are actual: Good...and none need to be added to redd_encounter!
unique(intent_zero$count_type)

# Pull out data needed for intent_fev
intent_fev = intent_zero %>%
  mutate(fish_location_id = NA_character_) %>%
  mutate(fish_status_id = if_else(count_type == "alive",
                                  "6a200904-8a57-4dd5-8c82-2353f91186ac",           # Live
                                  "b185dc5d-6b15-4b5b-a54e-3301aec0270f")) %>%      # Dead
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
  select(dead_id, recap_id, live_id, redd_id, survey_id, survey_event_id,
         fish_location_id, fish_status_id, sex_id, maturity_id,
         origin_id, cwt_detection_status_id, adipose_clip_status_id,
         fish_behavior_type_id, mortality_type_id, fish_count,
         previously_counted_indicator)

# Check
any(duplicated(intent_fev$survey_event_id))      # Correct...none
any(duplicated(intent_fev$survey_id))            # Correct...multiple species per survey

# Check n_species....None: Good !
chk_n_species = intent_fev %>%
  group_by(survey_id) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 4L)

# Add to fish_encounter
fish_encounter = bind_rows(fish_encounter, intent_fev) %>%
  select(dead_id, recap_id, live_id, redd_id, survey_id, survey_event_id,
         fish_location_id, fish_status_id, sex_id, maturity_id,
         origin_id, cwt_detection_status_id, adipose_clip_status_id,
         fish_behavior_type_id, mortality_type_id, fish_count,
         previously_counted_indicator)

#===============================================================================================

# Generate fish_encounter_id
fish_encounter = fish_encounter %>%
  group_by(survey_event_id, fish_location_id, fish_status_id,
           sex_id, maturity_id, origin_id, cwt_detection_status_id,
           adipose_clip_status_id, fish_behavior_type_id,
           mortality_type_id, fish_count, previously_counted_indicator) %>%
  mutate(fish_encounter_id = remisc::get_uuid(1L)) %>%
  ungroup() %>%
  select(dead_id, recap_id, live_id, redd_id, fish_encounter_id, survey_event_id,
         survey_id, fish_location_id, fish_status_id, sex_id, maturity_id,
         origin_id, cwt_detection_status_id, adipose_clip_status_id,
         fish_behavior_type_id, mortality_type_id, fish_count,
         previously_counted_indicator) %>%
  arrange(fish_encounter_id)

# Pull out just the table data
fish_encounter_prep = fish_encounter %>%
  mutate(fish_encounter_datetime = as.POSIXct(NA)) %>%
  select(fish_encounter_id, survey_event_id, fish_location_id,
         fish_status_id, sex_id, maturity_id, origin_id,
         cwt_detection_status_id, adipose_clip_status_id,
         fish_behavior_type_id, mortality_type_id,
         fish_encounter_datetime, fish_count,
         previously_counted_indicator) %>%
  distinct()

# Add trailing fields using data from header
survey_trailing = survey_event_prep %>%
  select(survey_event_id, created_datetime, created_by,
         modified_datetime, modified_by) %>%
  group_by(survey_event_id) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq == 1L) %>%
  select(-n_seq) %>%
  distinct()

# Check
any(duplicated(survey_trailing$survey_event_id))
sort(unique(fish_encounter_prep$fish_count))

# Add trailing fields...Now 11286 rows
fish_encounter_prep = fish_encounter_prep %>%
  left_join(survey_trailing, by = "survey_event_id") %>%
  select(fish_encounter_id, survey_event_id, fish_location_id,
         fish_status_id, sex_id, maturity_id, origin_id,
         cwt_detection_status_id, adipose_clip_status_id,
         fish_behavior_type_id, mortality_type_id,
         fish_encounter_datetime, fish_count,
         previously_counted_indicator,
         created_datetime, created_by,
         modified_datetime, modified_by) %>%
  distinct()

# Verify values
any(duplicated(fish_encounter_prep$fish_encounter_id))
any(is.na(fish_encounter_prep$created_datetime))

#================================================================================================
# Pull out redd encounter data
#================================================================================================

# Notes:
#  1. Need to record if redd_name is missing in redd_encounter comments
#  2. Need to record if no match to location in redd_encounter comments

# Add species and survey_event
redd_se_id = survey_event %>%
  filter(!is.na(redd_id)) %>%
  select(redd_id, survey_event_id) %>%
  distinct()

# Verify no dups in redd_id
any(duplicated(redds$redd_id))

# Pull out redd_encounter
redd_enc = redds %>%
  left_join(redd_se_id, by = "redd_id") %>%
  select(redd_id, survey_id, survey_event_id, created_date, created_by,
         modified_date, modified_by, redd_location_id, redd_time_stamp,
         redd_type, redd_status, sgs_redd_name, redd_count, redd_length,
         redd_width, redd_degraded, dewatered, dewater_text, si_redd,
         percent_si, si_by, si_text, sgs_redd_status, new_redd_count,
         encounter_comments, survey_date, observers, stream_name)

# Check on some columns
unique(redd_enc$sgs_redd_status)
table(redd_enc$sgs_redd_status, useNA = "ifany")

# Pull out NAs to check
chk_redd_status = redd_enc %>%
  filter(is.na(sgs_redd_status))

# Fill in sgs_redd_status...........HACK WARNING !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
redd_enc = redd_enc %>%
  mutate(redd_status_id = if_else(is.na(sgs_redd_status) &
                                    redd_type == "first_time_redd_encountered",
                                  "45fda25c-9120-406c-b159-2aead75623c0",               # New redd
                                  sgs_redd_status)) %>%
  mutate(redd_encounter_datetime = created_date) %>%
  select(redd_id, survey_id, survey_event_id, created_date, created_by,
         modified_date, modified_by, redd_location_id, redd_time_stamp,
         redd_encounter_datetime, redd_type, redd_status_id, sgs_redd_name,
         redd_count, redd_length, redd_width, redd_degraded, dewatered,
         dewater_text, si_redd, percent_si, si_by, si_text, sgs_redd_status,
         new_redd_count, encounter_comments, survey_date, observers,
         stream_name)

# Verify the columns
unique(redd_enc$survey_event_id[!nchar(redd_enc$survey_event_id) == 36L])
unique(redd_enc$redd_location_id[!nchar(redd_enc$redd_location_id) == 36L])
unique(redd_enc$redd_status_id[!nchar(redd_enc$redd_status_id) == 36L])
unique(redd_enc$redd_count)

# Check the records with no count
chk_redd_count = redd_enc %>%
  filter(is.na(redd_count))

# # Update redd_count HACK WARNING !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# redd_enc$redd_count[redd_enc$redd_id == 7947L] = 1

# Generate redd_encounter_id
redd_encounter = redd_enc %>%
  group_by(survey_event_id, redd_location_id, redd_status_id,
           redd_encounter_datetime, redd_count) %>%
  mutate(redd_encounter_id = remisc::get_uuid(1L)) %>%
  ungroup() %>%
  select(redd_id, redd_encounter_id, survey_id, survey_event_id,
         created_date, modified_date, redd_location_id,
         redd_time_stamp, redd_encounter_datetime,
         redd_type, redd_status_id, sgs_redd_name, redd_count,
         redd_length, redd_width, redd_degraded, dewatered,
         dewater_text, si_redd, percent_si, si_by, si_text,
         sgs_redd_status, new_redd_count, encounter_comments,
         survey_date, observers, stream_name) %>%
  arrange(survey_date, stream_name, redd_encounter_id)

# Check for dups..this would be ok....But none this time...created_datetime is unique
# This matches with redd_id...so probably what I want anyway.
any(duplicated(redd_encounter$redd_encounter_id))
any(duplicated(redd_encounter$redd_id))

# Pull out redd_encounter table....one entry for each in individual redd.
redd_encounter_prep = redd_encounter %>%
  left_join(survey_trailing, by = "survey_event_id") %>%
  select(redd_encounter_id, survey_event_id, redd_location_id,
         redd_status_id, redd_encounter_datetime, redd_count,
         comment_text = encounter_comments, created_datetime,
         created_by, modified_datetime, modified_by)

# Check redd counts
sort(unique(redd_encounter_prep$redd_count))

#================================================================================================
# Pull out individual_redd data
#================================================================================================

# Get the individual_redd data
ind_redd = redd_encounter %>%
  select(redd_id, redd_encounter_id, survey_id, redd_count,
         redd_length, redd_width, redd_degraded, dewatered,
         dewater_text, si_redd, percent_si, si_by, created_date,
         modified_date, survey_date, observers, stream_name) %>%
  distinct()

# Check values
unique(ind_redd$redd_count)
unique(ind_redd$redd_length)
unique(ind_redd$redd_width)
unique(ind_redd$redd_degraded)
unique(ind_redd$dewatered)
unique(ind_redd$dewater_text)
unique(ind_redd$percent_si)
#unique(ind_redd$si_by)

# First look for cases where redd_count > 1, but ind_redd data occurs
# Result: Only a few redd_degradeds. Will send to Lea...can safely dump data where redd_count > 1
chk_redd_count = ind_redd %>%
  filter(redd_count > 1) %>%
  filter(!is.na(redd_degraded) & !redd_degraded == "")

# # Output
# num_cols = ncol(chk_redd_count)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "MultipleIndivReddCounts.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "MultIndReddCounts", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_redd_count, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# Trim to only counts of one
ind_redd = ind_redd %>%
  filter(redd_count < 2)

# Check values...first three need no treatment
unique(ind_redd$redd_count)
unique(ind_redd$redd_length)
unique(ind_redd$redd_width)
unique(ind_redd$redd_degraded)

# Correct some percent degraded values
ind_redd$redd_degraded[ind_redd$redd_degraded %in% c("NV")] = NA_character_
ind_redd$redd_degraded[ind_redd$redd_degraded %in% c("SV")] = NA_character_
ind_redd$redd_degraded[ind_redd$redd_degraded %in% c("")] = NA_character_
ind_redd$redd_degraded[ind_redd$redd_degraded %in% c("0%")] = "0"
ind_redd$redd_degraded[ind_redd$redd_degraded %in% c("100%")] = "100"


# Correct some values
ind_redd = ind_redd %>%
  mutate(redd_degraded = trimws(redd_degraded)) %>%
  mutate(redd_degraded = if_else(redd_degraded == "", NA_character_, redd_degraded)) %>%
  mutate(percent_redd_degraded = as.integer(redd_degraded)) %>%
  mutate(percent_si = trimws(percent_si)) %>%
  mutate(percent_si = if_else(percent_si == "", NA_character_, percent_si)) %>%
  mutate(percent_redd_superimposed = as.integer(percent_si)) %>%
  mutate(si_by = trimws(si_by)) %>%
  mutate(si_by = if_else(si_by == "", NA_character_, si_by)) %>%
  mutate(superimposed_redd_name = si_by) %>%
  mutate(comment_text = NA_character_) %>%                                            # No real info
  select(redd_encounter_id, survey_id, redd_dewatered_type_id = dewatered, redd_length,
         redd_width, percent_redd_superimposed, percent_redd_degraded,
         superimposed_redd_name, comment_text, created_date, modified_date)

# Recheck
unique(ind_redd$redd_dewatered_type_id)
unique(ind_redd$percent_redd_superimposed)
unique(ind_redd$percent_redd_degraded)
# unique(ind_redd$superimposed_redd_name)
unique(ind_redd$comment_text)

# Get rid of any fields where no data has been entered
ind_redd = ind_redd %>%
  mutate(dump_rows = if_else(is.na(redd_dewatered_type_id) &
                               is.na(redd_length) &
                               is.na(redd_width) &
                               is.na(percent_redd_superimposed) &
                               is.na(percent_redd_degraded) &
                               is.na(superimposed_redd_name) &
                               is.na(comment_text),
                             "dump", "keep")) %>%
  filter(dump_rows == "keep")

# Generate individual_redd_id
individual_redd = ind_redd %>%
  group_by(redd_encounter_id, redd_dewatered_type_id,
           redd_length, redd_width, percent_redd_superimposed,
           percent_redd_degraded, superimposed_redd_name) %>%
  mutate(individual_redd_id = remisc::get_uuid(1L)) %>%
  ungroup() %>%
  select(individual_redd_id, redd_encounter_id, survey_id,
         redd_dewatered_type_id, redd_length, redd_width,
         percent_redd_superimposed, percent_redd_degraded,
         superimposed_redd_name, comment_text, created_date,
         modified_date)

# Pull out create_by mod_by fields from header data
head_create = header_se %>%
  select(survey_id, created_by = head_created_by,
         modified_by = head_mod_by) %>%
  distinct()

# Add final set of fields....HACK WARNING FOR MOD_BY !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
individual_redd_prep = individual_redd %>%
  left_join(head_create, by = "survey_id") %>%
  mutate(mod_diff = modified_date - created_date) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), modified_date)) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(!is.na(modified_datetime) & is.na(modified_by),
                               "ronnelmr", modified_by)) %>%
  mutate(redd_shape_id = "fdbc7d36-eb06-4eba-8df3-6e7145430d64") %>%                     # No data
  mutate(percent_redd_visible = NA_integer_) %>%
  mutate(redd_depth_measure_meter = NA_real_) %>%
  mutate(tailspill_height_measure_meter = NA_real_) %>%
  select(individual_redd_id, redd_encounter_id, redd_shape_id,
         redd_dewatered_type_id, percent_redd_visible,
         redd_length_measure_meter = redd_length,
         redd_width_measure_meter = redd_width,
         redd_depth_measure_meter, tailspill_height_measure_meter,
         percent_redd_superimposed, percent_redd_degraded,
         superimposed_redd_name, comment_text,
         created_datetime = created_date, created_by,
         modified_datetime, modified_by)

# Check for missing values
any(is.na(individual_redd_prep$individual_redd_id))
any(is.na(individual_redd_prep$redd_encounter_id))
any(is.na(individual_redd_prep$redd_shape_id))
any(is.na(individual_redd_prep$created_datetime))
any(is.na(individual_redd_prep$created_by))

#================================================================================================
# Create redd and fish_encounter location tables
#================================================================================================

# Check
all(fish_encounter_prep$fish_location_id %in% redd_encounter_prep$redd_location_id)
unique(redd_location_prep$stream_channel_type_id)
unique(redd_location_prep$location_orientation_type_id)

# Prepare redd_location
redd_location_prep = redd_location_prep %>%
  select(-c(created_by, modified_by)) %>%
  left_join(head_create, by = "survey_id") %>%
  mutate(mod_diff = modified_datetime - created_datetime) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), modified_datetime)) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(!is.na(modified_datetime) & is.na(modified_by),
                               "ronnelmr", modified_by)) %>%
  mutate(stream_channel_type_id = if_else(is.na(stream_channel_type_id),
                                          "713a39a5-8e95-4069-b078-066699c321d8",
                                          stream_channel_type_id)) %>%
  mutate(location_orientation_type_id = if_else(is.na(location_orientation_type_id),
                                          "eb4652b7-5390-43d4-a98e-60ea54a1d518",
                                          location_orientation_type_id)) %>%
  mutate(river_mile_measure = NA_real_) %>%
  mutate(location_code = NA_character_) %>%
  mutate(waloc_id = NA_character_) %>%
  mutate(location_description = NA_character_) %>%
  select(location_id, waterbody_id, wria_id, location_type_id,
         stream_channel_type_id, location_orientation_type_id,
         river_mile_measure, location_code, location_name,
         location_description, waloc_id, created_datetime,
         created_by, modified_datetime, modified_by)

# Pull out create values from above
redd_loc_create = redd_location_prep %>%
  select(location_id, created_datetime, created_by,
         modified_datetime, modified_by)

# Prepare redd_location_coords_prep
redd_location_coords_prep = redd_location_coords_prep %>%
  select(-c(created_by, modified_by, created_datetime, modified_datetime)) %>%
  left_join(redd_loc_create, by = "location_id") %>%
  st_as_sf(., coords = c("longitude", "latitude"), crs = 4326) %>%
  st_transform(., 2927) %>%
  mutate(comment_text = NA_character_) %>%
  select(location_id, horizontal_accuracy, comment_text,
         created_datetime, created_by, modified_datetime,
         modified_by)

# Check
any(is.na(redd_location_prep$stream_channel_type_id))
any(is.na(redd_location_prep$location_orientation_type_id))

#================================================================================================
# Combine header, redd, and fish location data
#================================================================================================

# Combine for one location table
location_prep = rbind(header_location_prep, redd_location_prep)

# Check
any(duplicated(location_prep$location_id))

# Combine for one location_coordinates table
location_coordinates_prep = rbind(header_location_coords_prep, redd_location_coords_prep)

# Check
any(duplicated(location_coordinates_prep$location_id))

location_coordinates_prep = location_coordinates_prep %>%
  group_by(location_id) %>%
  mutate(location_coordinates_id = remisc::get_uuid(1L)) %>%
  ungroup() %>%
  select(location_coordinates_id, location_id, horizontal_accuracy,
         comment_text, created_datetime, created_by, modified_datetime,
         modified_by)

#================================================================================================
# Prepare individual_fish...now that fish_encounter_id is available
#================================================================================================

# Get fish_encounter_id
fish_enc_id = fish_encounter %>%
  select(dead_id, fish_encounter_id, survey_event_id) %>%
  distinct()

# Add fish_encounter_id
individual_fish = dead_ind %>%
  left_join(fish_enc_id, by = "dead_id") %>%
  select(fish_encounter_id, survey_id, fish_condition_type_id, fish_trauma_type_id,
         gill_condition_type_id, spawn_condition_type_id, cwt_result_type_id,
         length_measurement_cm, scale_sample_card_number, scale_sample_position_number,
         cwt_snout_sample_number, genetic_sample_number, comment_text = encounter_comments,
         created_date, modified_date)

# Check some values
unique(individual_fish$fish_condition_type_id)
unique(individual_fish$fish_trauma_type_id)
unique(individual_fish$gill_condition_type_id)
unique(individual_fish$spawn_condition_type_id)
unique(individual_fish$cwt_result_type_id)
unique(individual_fish$length_measurement_cm)
unique(individual_fish$scale_sample_card_number)
unique(individual_fish$scale_sample_position_number)
unique(individual_fish$cwt_snout_sample_number)
unique(individual_fish$genetic_sample_number)
unique(individual_fish$comment_text)

# Correct one comment
individual_fish$comment_text[individual_fish$comment_text ==
                               "No head, only skin and tail \r\nFork length approx 72\r\n20kuc4 B-6"] =
  "No head, only skin and tail. Fork length approx 72. 20kuc4 B-6"
unique(individual_fish$comment_text)

# Dump rows with no data
individual_fish = individual_fish %>%
  mutate(dump_rows = if_else(
    is.na(fish_condition_type_id) &
      is.na(fish_trauma_type_id) &
      is.na(gill_condition_type_id) &
      is.na(spawn_condition_type_id) &
      is.na(cwt_result_type_id) &
      is.na(length_measurement_cm) &
      is.na(scale_sample_card_number) &
      is.na(scale_sample_position_number) &
      is.na(cwt_snout_sample_number) &
      is.na(genetic_sample_number),
    "dump", "keep"))

# Inspect rows to dump...check for cases with only comments....None...good.
chk_ind_dump = individual_fish %>%
  filter(dump_rows == "dump")

# Generate individual_fish_id
individual_fish = individual_fish %>%
  mutate(individual_fish_id = remisc::get_uuid(nrow(individual_fish))) %>%
  left_join(head_create, by = "survey_id") %>%
  mutate(mod_diff = modified_date - created_date) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), modified_date)) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(!is.na(modified_datetime) & is.na(modified_by),
                               "ronnelmr", modified_by)) %>%
  select(individual_fish_id, fish_encounter_id, fish_condition_type_id,
         fish_trauma_type_id, gill_condition_type_id, spawn_condition_type_id,
         cwt_result_type_id, length_measurement_cm, scale_sample_card_number,
         scale_sample_position_number, cwt_snout_sample_number,
         genetic_sample_number, comment_text, created_datetime = created_date,
         created_by, modified_datetime, modified_by)

# Prepare individual fish
individual_fish_prep = individual_fish %>%
  mutate(age_code_id = NA_character_) %>%
  mutate(percent_eggs_retained = NA_integer_) %>%
  mutate(eggs_retained_gram = NA_real_) %>%
  mutate(eggs_retained_number = NA_integer_) %>%
  mutate(fish_sample_number = NA_character_) %>%
  mutate(cwt_tag_code = NA_character_) %>%
  mutate(otolith_sample_number = NA_character_) %>%
  select(individual_fish_id, fish_encounter_id, fish_condition_type_id,
         fish_trauma_type_id, gill_condition_type_id, spawn_condition_type_id,
         cwt_result_type_id, age_code_id, percent_eggs_retained, eggs_retained_gram,
         eggs_retained_number, fish_sample_number, scale_sample_card_number,
         scale_sample_position_number, cwt_snout_sample_number, cwt_tag_code,
         genetic_sample_number, otolith_sample_number, comment_text,
         created_datetime, created_by, modified_datetime, modified_by)

# # HACK ALERT...UPDATE ONE INCORRECT UUID
# individual_fish_prep = individual_fish_prep %>%
#   mutate(spawn_condition_type_id = if_else(spawn_condition_type_id == "d8129c5a-8005-4ef6-90cb-5950325ac0e",
#                                            "d8129c5a-8005-4ef6-90cb-5950325ac0e1", spawn_condition_type_id))

# Check for dup ids...could be comments
any(duplicated(individual_fish_prep$individual_fish_id))

#================================================================================================
# Prepare fish_length_measurement table
#================================================================================================

# Pull out data for fish_measurement table
fish_meas = individual_fish %>%
  filter(!is.na(length_measurement_cm)) %>%
  select(individual_fish_id, length_measurement_cm,
         created_datetime, created_by, modified_datetime,
         modified_by)

# Generate id and pull out final data
fish_length_measurement_prep = fish_meas %>%
  mutate(fish_length_measurement_id = remisc::get_uuid(nrow(fish_meas))) %>%
  mutate(fish_length_measurement_type_id = "740dbd1a-93fe-4355-8e78-722afba53b9f") %>%       # Fork length
  select(fish_length_measurement_id, individual_fish_id, fish_length_measurement_type_id,
         length_measurement_centimeter = length_measurement_cm, created_datetime,
         created_by, modified_datetime, modified_by)

#================================================================================================
# Prepare fish_capture_event from dead_fish...now that fish_encounter_id is available
#================================================================================================

# Use header metadata for create, mod fields....HACK WARNING FOR MOD_BY !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
fish_capture_event_one = fish_capture_event %>%
  select(-c(created_by, modified_by)) %>%
  left_join(head_create, by = "survey_id") %>%
  mutate(mod_diff = modified_datetime - created_datetime) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), modified_datetime)) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(!is.na(modified_datetime) & is.na(modified_by),
                               "ronnelmr", modified_by)) %>%
  select(dead_id, parent_record_id, survey_id, fish_capture_status_id, disposition_type_id,
         disposition_id, disposition_location_id, created_datetime, created_by,
         modified_datetime, modified_by)

# Add fish_encounter_id
fish_capture_event_one = fish_capture_event_one %>%
  left_join(fish_enc_id, by = "dead_id") %>%
  select(fish_encounter_id, fish_capture_status_id, disposition_type_id,
         disposition_id, disposition_location_id, created_datetime,
         created_by, modified_datetime, modified_by)

# Check
any(is.na(fish_capture_event_one$fish_encounter_id))

#================================================================================================
# Prepare fish_capture_event from recaps...now that fish_encounter_id is available
#================================================================================================

# Use header metadata for create, mod fields....HACK WARNING FOR MOD_BY !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
fish_capture_event_two = recaps_fish_capture_event %>%
  select(-c(created_by, modified_by)) %>%
  left_join(head_create, by = "survey_id") %>%
  mutate(mod_diff = modified_date - created_date) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), modified_date)) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(!is.na(modified_datetime) & is.na(modified_by),
                               "ronnelmr", modified_by)) %>%
  mutate(disposition_location_id = NA_character_) %>%
  select(recap_id, parent_record_id, survey_id, fish_capture_status_id, disposition_type_id,
         disposition_id, disposition_location_id, created_datetime = created_date,
         created_by, modified_datetime, modified_by)

# Pull out fish_encounter_id for recaps
fish_enc_recap_id = fish_encounter %>%
  filter(!is.na(recap_id)) %>%
  select(recap_id, fish_encounter_id, survey_event_id) %>%
  distinct()

# Check recap_id before join...Both are integers...verified
unique(fish_capture_event_two$recap_id)
unique(fish_enc_recap_id$recap_id)

# Add fish_encounter_id
fish_capture_event_two = fish_capture_event_two %>%
  left_join(fish_enc_recap_id, by = "recap_id") %>%
  select(recap_id, parent_record_id, survey_id, fish_capture_status_id, disposition_type_id,
         disposition_id, disposition_location_id, fish_encounter_id, survey_event_id,
         created_datetime, created_by, modified_datetime, modified_by)

# Check
any(is.na(fish_capture_event_two$fish_encounter_id))

# Check
chk_capture_event = fish_capture_event_two %>%
  filter(is.na(fish_encounter_id))

# # Output
# num_cols = ncol(chk_capture_event)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "NoMatchCaptureEvent.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NoMatchCaptureEvent", gridLines = TRUE)
# writeData(wb, sheet = 1, chk_capture_event, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

# # Add fish_encounter_id...needed extra fields for output below...using bind_rows below now instead of rbind
# fish_capture_event_two = fish_capture_event_two %>%
#   left_join(fish_enc_recap_id, by = "recap_id") %>%
#   select(fish_encounter_id, fish_capture_status_id, disposition_type_id,
#          disposition_id, disposition_location_id, created_datetime,
#          created_by, modified_datetime, modified_by)

#================================================================================================
# Combine fish_capture_event data from dead and recaps
#================================================================================================

# Combine
fish_capture_event_prep = bind_rows(fish_capture_event_one, fish_capture_event_two) %>%
  filter(!is.na(fish_encounter_id)) %>%                  # HACK ALERT...Dumping missing fish_encounter_id rows !!!!!!!!!!!!!!!
  select(fish_encounter_id, fish_capture_status_id, disposition_type_id,
         disposition_id, disposition_location_id, created_datetime,
         created_by, modified_datetime, modified_by)

# Add id
fish_capture_event_prep = fish_capture_event_prep %>%
  mutate(fish_capture_event_id = remisc::get_uuid(nrow(fish_capture_event_prep))) %>%
  select(fish_capture_event_id, fish_encounter_id, fish_capture_status_id,
         disposition_type_id, disposition_id, disposition_location_id,
         created_datetime, created_by, modified_datetime, modified_by)

# ONLY THIS ONCE...correct UUID...WARNING..HACK ALERT !!!!!!!!!!!!!!!!!!!!!!!!!
unique(fish_capture_event_prep$fish_capture_status_id)
# fish_capture_event_prep = fish_capture_event_prep %>%
#   mutate(fish_capture_status_id = if_else(fish_capture_status_id == "v03f32160-6426-4c41-a088-3580a7d1a0c5",
#                                           "03f32160-6426-4c41-a088-3580a7d1a0c5", fish_capture_status_id))

#================================================================================================
# Prepare fish_mark_dead...now that fish_encounter_id is available
#================================================================================================

# Use header metadata for create, mod fields....HACK WARNING FOR MOD_BY !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
fish_mark_dead = fish_mark %>%
  select(-c(created_by, modified_by)) %>%
  left_join(head_create, by = "survey_id") %>%
  mutate(mod_diff = modified_datetime - created_datetime) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), modified_datetime)) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(!is.na(modified_datetime) & is.na(modified_by),
                               "ronnelmr", modified_by)) %>%
  select(dead_id, mark_type_id, mark_status_id, mark_orientation_id,
         mark_placement_id, mark_size_id, mark_color_id, mark_shape_id,
         tag_number, created_datetime, created_by, modified_datetime,
         modified_by)

# Add fish_encounter_id
fish_mark_dead = fish_mark_dead %>%
  left_join(fish_enc_id, by = "dead_id") %>%
  select(fish_encounter_id, mark_type_id, mark_status_id, mark_orientation_id,
         mark_placement_id, mark_size_id, mark_color_id, mark_shape_id,
         tag_number, created_datetime, created_by, modified_datetime,
         modified_by)

# Check
any(is.na(fish_mark_dead$fish_encounter_id))

#================================================================================================
# Prepare fish_mark_recap...now that fish_encounter_id is available
#================================================================================================

# Use header metadata for create, mod fields....HACK WARNING FOR MOD_BY !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
fish_mark_recap = recaps_fish_mark %>%
  select(-c(created_by, modified_by)) %>%
  left_join(head_create, by = "survey_id") %>%
  mutate(mod_diff = modified_date - created_date) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), modified_date)) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(!is.na(modified_datetime) & is.na(modified_by),
                               "ronnelmr", modified_by)) %>%
  select(recap_id, mark_type_id, mark_status_id, mark_orientation_id,
         mark_placement_id, mark_size_id, mark_color_id, mark_shape_id,
         tag_number, created_datetime = created_date, created_by,
         modified_datetime, modified_by)

# Add fish_encounter_id
fish_mark_recap = fish_mark_recap %>%
  left_join(fish_enc_recap_id, by = "recap_id") %>%
  select(fish_encounter_id, mark_type_id, mark_status_id, mark_orientation_id,
         mark_placement_id, mark_size_id, mark_color_id, mark_shape_id,
         tag_number, created_datetime, created_by, modified_datetime,
         modified_by)

# Check
any(is.na(fish_mark_recap$fish_encounter_id))

# HACK ALERT !!!!!!!!! Dumping any rows where fish_encounter_id is missing
fish_mark_recap = fish_mark_recap %>%
  filter(!is.na(fish_encounter_id))

#================================================================================================
# Combine fish_mark data from dead and recaps
#================================================================================================

# Combine
fish_mark_prep = rbind(fish_mark_dead, fish_mark_recap) %>%
  select(fish_encounter_id, mark_type_id, mark_status_id, mark_orientation_id,
         mark_placement_id, mark_size_id, mark_color_id, mark_shape_id,
         tag_number, created_datetime, created_by, modified_datetime,
         modified_by)

# Add id
fish_mark_prep = fish_mark_prep %>%
  mutate(fish_mark_id = remisc::get_uuid(nrow(fish_mark_prep))) %>%
  select(fish_mark_id, fish_encounter_id, mark_type_id, mark_status_id,
         mark_orientation_id, mark_placement_id, mark_size_id, mark_color_id,
         mark_shape_id, tag_number, created_datetime, created_by,
         modified_datetime, modified_by)

# Check
any(is.na(fish_mark_prep$fish_encounter_id))

#================================================================================================
# Prepare media_location table from pictures data
#================================================================================================

# Prepare media_location
media_location_prep = media_location %>%
  mutate(pic_create_date = as.POSIXct(iformr::idate_time(pic_create_date))) %>%
  mutate(pic_modify_date = as.POSIXct(iformr::idate_time(pic_modify_date))) %>%
  mutate(mod_diff = pic_modify_date - pic_create_date) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), pic_modify_date)) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, head_mod_by)) %>%
  mutate(modified_by = if_else(!is.na(modified_datetime) & is.na(modified_by),
                               "ronnelmr", modified_by)) %>%
  select(media_location_id, location_id, media_type_id, media_url,
         created_datetime = pic_create_date, created_by = head_create_by,
         modified_datetime, modified_by, comment_text)

#================================================================================================
# Prepare fish_barrier
#================================================================================================

# Pull out data for fish_barrier table
fish_passage_feature_prep = barrier_prep %>%
  select(fish_passage_feature_id = fish_barrier_id, survey_id,
         feature_location_id = barrier_location_id,
         passage_feature_type_id = barrier_type_id,
         feature_observed_datetime = barrier_observed_datetime,
         feature_height_meter = barrier_height_meter,
         feature_height_type_id = barrier_height_type_id,
         plunge_pool_depth_meter, plunge_pool_depth_type_id,
         comment_text, created_datetime, created_by,
         modified_datetime, modified_by)

#================================================================================================
# Prepare other_observations
#================================================================================================

# Pull out other_obs data
other_observation_prep = other_observation %>%
  select(other_observation_id, survey_id, observation_location_id,
         observation_type_id, observation_datetime, observation_count,
         comment_text, created_datetime, created_by, modified_datetime,
         modified_by)

#================================================================================================
# Prepare waterbody_measurement
#================================================================================================

# Finalize waterbody_meas data
waterbody_meas_prep = waterbody_meas_prep %>%
  select(-c(created_by, modified_by)) %>%
  left_join(head_create, by = "survey_id") %>%
  mutate(mod_diff = modified_datetime - created_datetime) %>%
  mutate(modified_datetime = if_else(mod_diff < 120, as.POSIXct(NA), modified_datetime)) %>%
  mutate(modified_by = if_else(mod_diff < 120, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(!is.na(modified_datetime) & is.na(modified_by),
                               "ronnelmr", modified_by)) %>%
  mutate(waterbody_measurement_id = remisc::get_uuid(nrow(waterbody_meas_prep))) %>%
  select(waterbody_measurement_id, survey_id, water_clarity_type_id,
         water_clarity_meter, stream_flow_measurement_cfs, start_water_temperature_datetime,
         start_water_temperature_celsius, end_water_temperature_datetime,
         end_water_temperature_celsius, waterbody_ph, created_datetime,
         created_by, modified_datetime, modified_by) %>%
  distinct()

#================================================================================================
# CHECK FOR MISSING REQUIRED VALUES
#================================================================================================

# unique(redd_enc$survey_event_id[!nchar(redd_enc$survey_event_id) == 36L])

# Verify location_prep
any(is.na(location_prep$location_id))
any(duplicated(location_prep$location_id))
any(is.na(location_prep$waterbody_id))
any(is.na(location_prep$wria_id))
any(is.na(location_prep$location_type_id))
any(is.na(location_prep$stream_channel_type_id))
any(is.na(location_prep$location_orientation_type_id))
any(is.na(location_prep$created_datetime))
any(is.na(location_prep$created_by))

# Verify location_coordinates_prep
any(duplicated(location_coordinates_prep$location_id))
any(is.na(location_coordinates_prep$location_coordinates_id))
any(is.na(location_coordinates_prep$location_id))

# Verify media_location
any(is.na(media_location_prep$media_location_id))
any(is.na(media_location_prep$location_id))
any(is.na(media_location_prep$media_type_id))
any(is.na(media_location_prep$media_url))
any(is.na(media_location_prep$created_datetime))
any(is.na(media_location_prep$created_by))

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
# Combine all survey_event_ids from downstream tables and see if any can be eliminated
# from the survey_event table
#================================================================================================

# Get all current survey_event_ids
se_ids = unique(survey_event_prep$survey_event_id)

# Get all downstream IDs
fe_se_ids = unique(fish_encounter_prep$survey_event_id)
rd_se_ids = unique(redd_encounter_prep$survey_event_id)

# Combine downstream_ids
down_se_ids = unique(c(fe_se_ids, rd_se_ids))

# Inspect survey_event data with no downstream survey_event_ids
chk_down = survey_event_prep %>%
  filter(!survey_event_id %in% down_se_ids) %>%
  left_join(species_ids, by = "species_id")

# Inspect
table(chk_down$common_name, useNA = "ifany")
table(chk_down$survey_design_type_id, useNA = "ifany")

# Look at survey info for potential delete events
s_id_down = unique(chk_down$survey_id)
survey_down = survey_prep %>%
  filter(survey_id %in% s_id_down)

# Inspect values...All survey were conducted...only nine were partial
table(survey_down$survey_completion_status_id, useNA = "ifany")

# Check what the survey_intent was for these surveys
intent_down = intent_prep %>%
  filter(survey_id %in% s_id_down)

# Check if any zero's already entered for the surveys
survey_event_down = survey_event_prep %>%
  filter(survey_id %in% s_id_down)

# Get fish_encounter data for these surveys
se_id_down = unique(survey_event_down$survey_event_id)
fish_encounter_down = fish_encounter_prep %>%
  filter(survey_event_id %in% se_id_down) %>%
  filter(fish_count == 0L)

# Looks like zeros were created for surveys that already had counts.
# Possibly there were multiple run_year or survey_types. All intended
# species were provided a zero if needed. So any excess can be deleted.
# If a survey does not have downstream counts for redd or fish then
# we should be able to safely delete.

# Double-check that no survey_event_ids in se_id_down exists in redd or fish encounter tables: Both zero rows!
no_se_id = unique(chk_down$survey_event_id)
fish_down = fish_encounter_prep %>%
  filter(survey_event_id %in% no_se_id)
redd_down = redd_encounter_prep %>%
  filter(survey_event_id %in% no_se_id)

# Dump the survey_event_prep rows not needed: Went from 7045 rows to 5684 rows
survey_event_prep = survey_event_prep %>%
  filter(!survey_event_id %in% no_se_id)

#================================================================================================
# LOAD TO DB
#  1. Load directly to FISH...this is now where all editing by field staff occurs.
#================================================================================================

# Create rd file to hold copy of survey_ids and survey_event_ids in case test fails
test_upload_ids = survey_prep %>%
  left_join(survey_event_prep, by = "survey_id") %>%
  select(survey_id, survey_event_id) %>%
  distinct()

# Create rd file to hold copy of survey_ids and survey_event_ids in case test fails
test_location_ids = location_prep %>%
  select(location_id) %>%
  distinct()

# Output redd_encounter to rds: 724591 records
saveRDS(object = test_upload_ids, file = "data/test_upload_ids.rds")
saveRDS(object = test_location_ids, file = "data/test_location_ids.rds")

#==========================================================================================
# Load data tables
#==========================================================================================

#======== Location tables =======================================

#=======  Insert location =================

# location: 3048 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "location")
dbWriteTable(db_con, tbl, location_prep, row.names = FALSE, append = TRUE, copy = TRUE)
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

#------------------------------------------------

#=======  Insert location_coordinates =================

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from spawning_ground.location_coordinates"
db_con = pg_con_prod(dbname = "FISH")
max_gid = DBI::dbGetQuery(db_con, qry)
DBI::dbDisconnect(db_con)
next_gid = max_gid$max + 1

# Pull out data for location_coordinates table: 3031 rows
location_coordinates_temp = location_coordinates_prep %>%
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

# Insert select to spawning_ground: 3031 rows
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "FISH")
DBI::dbExecute(db_con, "DROP TABLE spawning_ground.location_coordinates_temp")
DBI::dbDisconnect(db_con)

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

# media_location: 219 rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "media_location")
dbWriteTable(db_con, tbl, media_location_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== Survey level tables ===============

# survey:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "survey")
dbWriteTable(db_con, tbl, survey_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# survey_comment:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "survey_comment")
dbWriteTable(db_con, tbl, comment_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# survey_intent:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "survey_intent")
dbWriteTable(db_con, tbl, intent_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# waterbody_measurement:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "waterbody_measurement")
dbWriteTable(db_con, tbl, waterbody_meas_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# mobile_survey_form:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "mobile_survey_form")
dbWriteTable(db_con, tbl, mobile_survey_form_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# fish_passage_feature:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_passage_feature")
dbWriteTable(db_con, tbl, fish_passage_feature_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# other_observation:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "other_observation")
dbWriteTable(db_con, tbl, other_observation_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== Survey event ===============

# survey_event:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "survey_event")
dbWriteTable(db_con, tbl, survey_event_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== fish data ===============

# fish_encounter:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_encounter")
dbWriteTable(db_con, tbl, fish_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# fish_capture_event:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_capture_event")
dbWriteTable(db_con, tbl, fish_capture_event_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# fish_mark:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_mark")
dbWriteTable(db_con, tbl, fish_mark_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# individual_fish:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "individual_fish")
dbWriteTable(db_con, tbl, individual_fish_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# fish_length_measurement:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "fish_length_measurement")
dbWriteTable(db_con, tbl, fish_length_measurement_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

#======== redd data ===============

# redd_encounter:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "redd_encounter")
dbWriteTable(db_con, tbl, redd_encounter_prep, row.names = FALSE, append = TRUE, copy = TRUE)
dbDisconnect(db_con)

# individual_redd:  rows
db_con = pg_con_prod("FISH")
tbl = Id(schema = "spawning_ground", table = "individual_redd")
dbWriteTable(db_con, tbl, individual_redd_prep, row.names = FALSE, append = TRUE, copy = TRUE)
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
# # Pull out location IDs:  rows
# loc_ids = unique(test_location_ids$location_id)
# loc_ids = paste0(paste0("'", loc_ids, "'"), collapse = ", ")
#
# # Get fish_encounter_ids
# qry = glue("select spawning_ground.fish_encounter_id from fish_encounter where survey_event_id in ({se_ids})")
# db_con = pg_con_prod("FISH")
# fish_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out fish IDs
# fs_ids = unique(fish_id$fish_encounter_id)
# fs_ids = paste0(paste0("'", fs_ids, "'"), collapse = ", ")
#
# # Get individual_fish IDs
# qry = glue("select spawning_ground.individual_fish_id from individual_fish where fish_encounter_id in ({fs_ids})")
# db_con = pg_con_prod("FISH")
# indf_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out ind_fish IDs
# ifs_ids = unique(indf_id$individual_fish_id)
# ifs_ids = paste0(paste0("'", ifs_ids, "'"), collapse = ", ")
#
# # Get redd_encounter_ids
# qry = glue("select spawning_ground.redd_encounter_id from redd_encounter where survey_event_id in ({se_ids})")
# db_con = pg_con_prod("FISH")
# redd_id = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
#
# # Pull out redd IDs
# rd_ids = unique(redd_id$redd_encounter_id)
# rd_ids = paste0(paste0("'", rd_ids, "'"), collapse = ", ")
#
# # Get individual_redd_ids
# qry = glue("select spawning_ground.individual_redd_id from individual_redd where redd_encounter_id in ({rd_ids})")
# db_con = pg_con_prod("FISH")
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
# # individual_redd:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.individual_redd where individual_redd_id in ({ird_ids})'))
# dbDisconnect(db_con)
#
# # redd_encounter:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.redd_encounter where redd_encounter_id in ({rd_ids})'))
# dbDisconnect(db_con)
#
# #======== fish data ===============
#
# # fish_length_measurement:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.fish_length_measurement where individual_fish_id in ({ifs_ids})'))
# dbDisconnect(db_con)
#
# # individual_fish:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.individual_fish where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# # fish_capture_event:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.fish_capture_event where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# # fish_mark:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.fish_mark where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# # fish_encounter:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.fish_encounter where fish_encounter_id in ({fs_ids})'))
# dbDisconnect(db_con)
#
# #======== Survey event ===============
#
# # survey_event:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.survey_event where survey_event_id in ({se_ids})'))
# dbDisconnect(db_con)
#
# #======== Survey level tables ===============
#
# # other_observation:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.other_observation where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # fish_passage_feature:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.fish_passage_feature where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # mobile_survey_form:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.mobile_survey_form where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # survey_comment:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.survey_comment where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # survey_intent:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.survey_intent where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # waterbody_measurement:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.waterbody_measurement where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# # survey:  rows
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue('delete from spawning_ground.survey where survey_id in ({s_ids})'))
# dbDisconnect(db_con)
#
# #======== location ===============
#
# # media_location:
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.media_location where location_id in ({loc_ids})"))
# dbDisconnect(db_con)
#
# # Location coordinates:
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.location_coordinates where location_id in ({loc_ids})"))
# dbDisconnect(db_con)
#
# # Location:
# db_con = pg_con_prod("FISH")
# dbExecute(db_con, glue("delete from spawning_ground.location where location_id in ({loc_ids})"))
# dbDisconnect(db_con)
#














