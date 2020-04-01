#===============================================================================
# Verify queries work
#
# AS 2020-03-16
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

#========================================================================
# Check for new surveys and missing stream and reach
#========================================================================

# Get counts and date-range of surveys ready to download
get_core_survey_data = function(profile_id, parent_form_page_id, start_id, access_token) {
  # Define fields...just parent_id and survey_id this time
  fields = glue::glue("headerid, survey_date, stream_name, ",
                      "stream_name_text, new_stream_name, reach, ",
                      "reach_text, new_reach, gps_loc_lower, ",
                      "gps_loc_upper")
  # Order by id ascending and get all records after start_id
  field_string = glue('id:<(>"{start_id}"), {fields}')
  # Loop through all survey records
  core_data = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = parent_form_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = since_id)
  return(core_data)
}

# Get access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Test: Currently 1486 records....can use start_id in tandem with parent_record_id from DB in the future to filter
strt = Sys.time()
current_surveys = get_core_survey_data(profile_id, parent_form_page_id, start_id = 0L, access_token)
nd = Sys.time(); nd - strt

# Filter to data not already in DB
get_survey_counts = function(dat) {
  # Define query for new mobile surveys
  qry = glue("select distinct s.survey_id ",
             "from survey as s")
  # Checkout connection
  con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  existing_surveys = DBI::dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  # Keep only records where headerid is not in survey_id
  new_survey_data = dat %>%
    select(parent_form_survey_id = id, survey_id = headerid, survey_date,
           stream_name, stream_name_text, new_stream_name, reach, reach_text,
           new_reach, gps_loc_lower, gps_loc_upper) %>%
    distinct() %>%
    anti_join(existing_surveys, by = "survey_id") %>%
    arrange(as.Date(survey_date))
  return(new_survey_data)
}

# Get rid of existing surveys...can omit in the future
new_surveys = get_survey_counts(current_surveys)

# Pull out only needed data
new_survey_counts = new_surveys %>%
  # Count number of surveys and get dates and ids of first, last
  mutate(n_surveys = n()) %>%
  mutate(first_id = min(parent_form_survey_id)) %>%
  mutate(last_id = max(parent_form_survey_id)) %>%
  mutate(start_date = min(as.Date(survey_date))) %>%
  mutate(end_date = max(as.Date(survey_date))) %>%
  select(n_surveys, first_id, start_date, last_id, end_date) %>%
  distinct()

# Pull out only needed data
missing_stream_vals = new_surveys %>%
  filter(nchar(stream_name) < 36L) %>%
  mutate(llid = remisc::get_text_item(reach, item = 1L, sep = "_")) %>%
  mutate(no_llid = if_else(!nchar(llid) == 13L, "yes", "no")) %>%
  mutate(stream_name = case_when(
    stream_name == "unnamed_tributary" & no_llid == "no" ~ llid,
    stream_name == "unnamed_tributary" & no_llid == "yes" ~ remisc::get_text_item(reach_text, 1, "_"),
    stream_name == "not_listed" & !is.na(new_stream_name) ~ new_stream_name,
    !stream_name %in% c("unnamed_tributary", "not_listed") ~ stream_name)) %>%
  mutate(lower_coords = gsub("[Latitudeong,:]", "", gps_loc_lower)) %>%
  mutate(lower_coords = gsub("[\n]", ":", lower_coords)) %>%
  mutate(lower_coords = gsub("[\r]", "", lower_coords)) %>%
  mutate(lower_coords = trimws(remisc::get_text_item(lower_coords, 1, ":A"))) %>%
  mutate(upper_coords = gsub("[Latitudeong,:]", "", gps_loc_upper)) %>%
  mutate(upper_coords = gsub("[\n]", ":", upper_coords)) %>%
  mutate(upper_coords = gsub("[\r]", "", upper_coords)) %>%
  mutate(upper_coords = trimws(remisc::get_text_item(upper_coords, 1, ":A"))) %>%
  select(parent_form_survey_id, survey_date, stream_name, stream_name_text,
         llid, no_llid, reach, lower_coords, upper_coords) %>%
  distinct()

# Get waterbody_ids for streams missing from option list, based on match with LLIDs
qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, st.stream_id as stream_geometry_id ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "order by waterbody_display_name")

# Checkout connection
con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
#con = poolCheckout(pool)
streams = dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Combine with missing_stream_vals
missing_stream_vals = missing_stream_vals %>%
  left_join(streams, by = "llid") %>%
  select(parent_form_survey_id, survey_date, stream_name, waterbody_display_name,
         llid, reach, lower_coords, upper_coords, waterbody_id, stream_geometry_id) %>%
  distinct() %>%
  arrange(llid, waterbody_display_name)

# Output with styling
num_cols = ncol(missing_stream_vals)
current_date = format(Sys.Date())
out_name = paste0("data/", current_date, "_", "MissingStreams.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "MissingStreams", gridLines = TRUE)
writeData(wb, sheet = 1, missing_stream_vals, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)

# Pull out only needed data
missing_reach_vals = new_surveys %>%
  filter(nchar(reach) < 27L | is.na(reach)) %>%
  mutate(llid = remisc::get_text_item(reach, item = 1L, sep = "_")) %>%
  mutate(no_llid = if_else(!nchar(llid) == 13L, "yes", "no")) %>%
  mutate(stream_name = case_when(
    stream_name == "unnamed_tributary" & no_llid == "no" ~ llid,
    stream_name == "unnamed_tributary" & no_llid == "yes" ~ remisc::get_text_item(reach_text, 1, "_"),
    stream_name == "not_listed" & !is.na(new_stream_name) ~ new_stream_name,
    !stream_name %in% c("unnamed_tributary", "not_listed") ~ stream_name)) %>%
  mutate(lower_coords = gsub("[Latitudeong,:]", "", gps_loc_lower)) %>%
  mutate(lower_coords = gsub("[\n]", ":", lower_coords)) %>%
  mutate(lower_coords = gsub("[\r]", "", lower_coords)) %>%
  mutate(lower_coords = trimws(remisc::get_text_item(lower_coords, 1, ":A"))) %>%
  mutate(upper_coords = gsub("[Latitudeong,:]", "", gps_loc_upper)) %>%
  mutate(upper_coords = gsub("[\n]", ":", upper_coords)) %>%
  mutate(upper_coords = gsub("[\r]", "", upper_coords)) %>%
  mutate(upper_coords = trimws(remisc::get_text_item(upper_coords, 1, ":A"))) %>%
  mutate(reach_text = if_else(reach_text == "Not Listed", new_reach, reach_text)) %>%
  select(parent_form_survey_id, survey_date, stream_name, stream_name_text,
         reach, reach_text, lower_coords, upper_coords) %>%
  distinct()

#======================================================================================
# Generate dataset of reaches that have not yet been entered
#======================================================================================

# Pull out only needed data
add_end_points = function(new_surveys) {
  add_reach_vals = new_surveys %>%
    filter(nchar(reach) >= 27L & !is.na(reach)) %>%
    mutate(llid = remisc::get_text_item(reach, 1, "_")) %>%
    mutate(reach = remisc::get_text_item(reach, 2, "_")) %>%
    mutate(low_rm = as.numeric(remisc::get_text_item(reach, 1, "-"))) %>%
    mutate(up_rm = as.numeric(remisc::get_text_item(reach, 2, "-")))
  # Get existing rm_data
  qry = glue("select distinct loc.location_id, wb.latitude_longitude_id as llid, ",
             "loc.river_mile_measure as rm ",
             "from location as loc ",
             "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
             "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
             "where wria_code in ('22', '23')")
  # Checkout connection
  con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  rm_data = DBI::dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  # Dump any data without RMs or llid
  rm_data = rm_data %>%
    filter(!is.na(llid) & !is.na(rm))
  # Pull out lower_rms
  low_rm_data = rm_data %>%
    select(lower_end_point_id = location_id, llid, low_rm = rm) %>%
    distinct()
  # Pull out upper_rms
  up_rm_data = rm_data %>%
    select(upper_end_point_id = location_id, llid, up_rm = rm) %>%
    distinct()
  # Join to survey_prep
  add_reach_vals = add_reach_vals %>%
    left_join(low_rm_data, by = c("llid", "low_rm")) %>%
    left_join(up_rm_data, by = c("llid", "up_rm"))
  if ( any(nchar(add_reach_vals$reach) > 27) ) {
    cat("\nWarning: Some reach entries > 27 characters. Investigate!\n\n")
  } else {
    cat("\nAll reach entries exactly 27 characters. Ok to proceed.\n\n")
  }
  # Pull out cases where no match exists
  no_reach_point = add_reach_vals %>%
    filter(is.na(lower_end_point_id) | is.na(upper_end_point_id)) %>%
    mutate(lower_comment = if_else(!is.na(lower_end_point_id), "have_point", "need_point")) %>%
    mutate(upper_comment = if_else(!is.na(upper_end_point_id), "have_point", "need_point")) %>%
    select(parent_form_survey_id, waterbody_id = stream_name, stream_name_text, reach_text,
           llid, low_rm, up_rm, lower_comment, upper_comment) %>%
    distinct() %>%
    arrange(stream_name_text, low_rm, up_rm)
  # Split and combine
  no_up = no_reach_point %>%
    filter(upper_comment == "need_point") %>%
    select(parent_form_survey_id, waterbody_id, stream_name_text, reach_text, llid, river_mile = up_rm)  %>%
    distinct()
  # Split and combine
  no_low = no_reach_point %>%
    filter(lower_comment == "need_point") %>%
    select(parent_form_survey_id, waterbody_id, stream_name_text, reach_text, llid, river_mile = low_rm)  %>%
    distinct()
  # Combine
  no_end_point = rbind(no_up, no_low) %>%
    filter(!is.na(river_mile)) %>%
    arrange(stream_name_text, river_mile)
  return(no_end_point)
}

# Run
no_end_point = add_end_points(new_surveys)

# # Output with styling
# num_cols = ncol(no_end_point)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "NoEndPoint.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NoEndPoint", gridLines = TRUE)
# writeData(wb, sheet = 1, no_end_point, rowNames = FALSE)
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
  fields = glue::glue("created_date, created_by, created_device_id, modified_date, modified_by, ",
                      "data_entry_type, target_species, survey_date, start_time, observers, ",
                      "stream_name, stream_name_text, new_stream_name, reach, reach_text, new_reach, survey_type, ",
                      "survey_method, survey_direction, clarity_ft, clarity_code, weather, flow, stream_conditions, ",
                      "no_survey, reachsplit_yn, user_name, headerid, end_time, header_comments, stream_temp, ",
                      "survey_completion_status, chinook_count_type, coho_count_type, steelhead_count_type, ",
                      "chum_count_type, gps_loc_lower, gps_loc_upper, coho_run_year, steelhead_run_year, ",
                      "chum_run_year, chinook_run_year, carcass_tagging, code_reach")
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

# Test...currently 1512 records
# Get start_id
start_id = new_survey_counts$first_id -1
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

#======================================================================================================================
# For now, get rid of data with missing fields so I can develop the remaining functions...JUST FOR NOW !!!!!!!!!!!!!!!!
#======================================================================================================================

# Pull stream_ids
del_stream_id = missing_stream_vals %>%
  select(parent_form_survey_id) %>%
  distinct()

# Pull reach_ids
del_reach_id = missing_reach_vals %>%
  select(parent_form_survey_id) %>%
  distinct()

# Pull reach_ids
del_end_id = no_end_point %>%
  select(parent_form_survey_id) %>%
  distinct()

# Combine
del_id = bind_rows(del_stream_id, del_reach_id, del_end_id) %>%
  distinct() %>%
  pull(parent_form_survey_id)

# FOR NOW !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Filter to header_data with no missing stream or reach_id for now
header_data = header_data %>%
  filter(!parent_record_id %in% del_id)

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

#==================== End of trim section=============================================================================

# Generate created and modified data
header_data = header_data %>%
  mutate(modified_by = if_else(created_datetime == modified_datetime, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(substr(modified_by, 1, 3) %in% c("VAR", "FW0"), NA_character_, modified_by)) %>%
  mutate(modified_by = stringi::stri_replace_all_fixed(modified_by, ".", "_")) %>%
  mutate(modified_by = remisc::get_text_item(modified_by, 1, "_")) %>%
  mutate(modified_datetime = if_else(created_datetime == modified_datetime, as.POSIXct(NA), modified_datetime))

# Survey =========================================

# Generate survey and associated tables
survey_prep = header_data %>%
  mutate(data_source_id = "c40563fa-efce-4cac-a2e3-6223a5e24030") %>%                           # WDFW
  mutate(data_source_unit_id = "e2d51ceb-398c-49cb-9aa5-d20a839e9ad9") %>%                      # Not applicable
  mutate(data_review_status_id = "b0ea75d4-7e77-4161-a533-5b3fce38ac2a") %>%                    # Preliminary
  mutate(data_submitter_last_name = observers) %>%
  # Set incomplete_survey type to not_applicable if survey was marked as completed...otherwise use no_survey key
  mutate(incomplete_survey_type_id = if_else(survey_completion_status == "d192b32e-0e4f-4719-9c9c-dec6593b1977",
                                             "cde5d9fb-bb33-47c6-9018-177cd65d15f5", no_survey)) %>%
  select(parent_record_id, stream_name, stream_name_text, reach_text,
         survey_id = survey_uuid, survey_datetime = survey_date, data_source_id,
         data_source_unit_id, survey_method, data_review_status_id,
         stream_name, reach, survey_completion_status_id = survey_completion_status,
         incomplete_survey_type_id, survey_start_datetime,
         survey_end_datetime, observers, data_submitter_last_name,
         created_datetime, created_by, modified_datetime, modified_by)

# Identify the upper and lower end point location_ids
survey_prep = survey_prep %>%
  mutate(llid = remisc::get_text_item(reach, 1, "_")) %>%
  mutate(reach = remisc::get_text_item(reach, 2, "_")) %>%
  mutate(low_rm = as.numeric(remisc::get_text_item(reach, 1, "-"))) %>%
  mutate(up_rm = as.numeric(remisc::get_text_item(reach, 2, "-")))

qry = glue("select distinct loc.location_id, wb.latitude_longitude_id as llid, ",
           "loc.river_mile_measure as rm ",
           "from location as loc ",
           "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where wria_code in ('22', '23')")
# Checkout connection
con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
#con = poolCheckout(pool)
rm_data = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)
#poolReturn(con)

# Dump any data without RMs or llid
rm_data = rm_data %>%
  filter(!is.na(llid) & !is.na(rm))

# Pull out lower_rms
low_rm_data = rm_data %>%
  select(lower_end_point_id = location_id, llid, low_rm = rm) %>%
  distinct()

# Pull out upper_rms
up_rm_data = rm_data %>%
  select(upper_end_point_id = location_id, llid, up_rm = rm) %>%
  distinct()

# Join to survey_prep
survey_prep = survey_prep %>%
  left_join(low_rm_data, by = c("llid", "low_rm")) %>%
  left_join(up_rm_data, by = c("llid", "up_rm"))

# Pull out cases where no match exists
no_reach_point = survey_prep %>%
  filter(is.na(lower_end_point_id) | is.na(upper_end_point_id)) %>%
  distinct() %>%
  arrange(stream_name_text, low_rm, up_rm)

# Warn if any missing
if ( nrow(no_reach_point) > 0L ) {
  cat("\nWARNING: Some end-points still missing. Do not pass go!\n\n")
} else {
  cat("\nAll end-points now present. Ok to proceed.\n\n")
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

# Pull out survey_intent data
intent_prep = header_data %>%
  mutate(survey_id = survey_uuid) %>%
  select(survey_id, target_species, chinook_count_type, coho_count_type,
         steelhead_count_type, chum_count_type, created_datetime,
         created_by, modified_datetime, modified_by)

# Check target_species
unique(intent_prep$target_species)

# Add target species names for reference...just curiosity
qry = glue("select species_id as target_species, common_name as target_species_name ",
           "from species_lut")
# Checkout connection
con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
#con = poolCheckout(pool)
species_list = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Add to intent_pred
intent_prep = intent_prep %>%
  left_join(species_list, by = "target_species")

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

# Combine
intent_prep = rbind(chinook_intent, coho_intent, sthd_intent, chum_intent) %>%
  filter(!count_type == "no_survey") %>%
  arrange(survey_id)

# Add id for count_type
unique(intent_prep$count_type)

# From option list R6_Count_Type_2
# full_survey, lives_and_deads_only, redds_only, lives_only, deads_only, no_survey


# Create df for conversion to intent categories
intent_categories = tibble(count_type = c("full_survey", "lives_and_deads_only", "lives_only", "redds_only", "deads_only"),
                           count_categories = c("Live, Carcass, Redd", "Live, Carcass", "Live", "Redd", "Carcass"))

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
species_ids = species_list %>%
  select(species_id = target_species, common_name = target_species_name)

# Get count_type_ids to add to intent_prep
qry = glue("select count_type_id, count_type_code as count_categories ",
           "from count_type_lut")
# Checkout connection
con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
#con = poolCheckout(pool)
count_type_ids = DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

# Pull out needed data
intent_prep = intent_prep %>%
  mutate(survey_intent_id = remisc::get_uuid(nrow(intent_prep))) %>%
  left_join(species_ids, by = "common_name") %>%
  left_join(count_type_ids, by = "count_categories") %>%
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
  mutate(clarity_code = if_else(is.na(clarity_code), "282d8ea4-4e78-4c4d-be05-dfd7fa457c09", clarity_code)) %>%
  select(survey_id = survey_uuid, water_clarity_type_id = clarity_code,
         water_clarity_meter, stream_flow_measurement_cfs, start_water_temperature_datetime,
         start_water_temperature_celsius, end_water_temperature_datetime,
         end_water_temperature_celsius, waterbody_ph, created_datetime,
         created_by, modified_datetime, modified_by) %>%
  distinct()

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

# Test...currently 327 records
strt = Sys.time()
other_obs = get_other_obs(profile_id, other_obs_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Dump any records that are not in header_data
# FOR NOW !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Filter to header_data with no missing stream or reach_id for now. 176 records after filter
other_obs = other_obs %>%
  filter(!parent_record_id %in% del_id)

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

# Test...currently 163 records
strt = Sys.time()
other_pictures = get_other_pictures(profile_id, other_pics_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Format other obs
other_obs = other_obs %>%
  mutate(created_datetime = as.POSIXct(iformr::idate_time(created_date))) %>%
  mutate(modified_datetime = as.POSIXct(iformr::idate_time(modified_date))) %>%
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
  select(parent_record_id = id, survey_id,
         head_create_by, head_mod_by) %>%
  distinct()

# Check
any(is.na(sp_id$parent_record_id))
any(is.na(sp_id$survey_id))

# Dump any records that are not in the filtered other_obs dataset
# FOR NOW !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
other_obs_id = unique(other_obs$id)
# Filter to header_data with no missing stream or reach_id for now. 65 records after filter
other_pictures = other_pictures %>%
  filter(parent_record_id %in% other_obs_id)

# Join survey_id to other_pictures
other_pictures = other_pictures %>%
  left_join(sp_id, by = "parent_record_id")

# Pull out barrier info
barrier_prep  = other_obs %>%
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
  select(survey_id, obs_lat, obs_lon, obs_acc = gps_accuracy,
         barrier_type = observation_type, barrier_observed_datetime,
         barrier_height_meter, barrier_ht_type = barrier_ht_meas_est,
         plunge_pool_depth_meter, pp_depth_type = barrier_pp_depth_meas_est,
         comment_text, created_datetime, created_by = head_create_by,
         modified_datetime, modified_by = head_mod_by) %>%
  distinct()

# Convert types to ids
barrier_prep = barrier_prep %>%
  mutate(fish_barrier_id = remisc::get_uuid(nrow(barrier_prep))) %>%
  mutate(barrier_location_id = remisc::get_uuid(nrow(barrier_prep))) %>%
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
  select(fish_barrier_id, survey_id, barrier_location_id, obs_lat, obs_lon,
         obs_acc, barrier_type_id, barrier_observed_datetime, barrier_height_meter,
         barrier_height_type_id, plunge_pool_depth_meter, plunge_pool_depth_type_id,
         comment_text, created_datetime, created_by, modified_datetime,
         modified_by)

# Pull out other obs data...include location info
other_obs = other_obs %>%
  filter(is.na(known_total_barrier))

# Process
other_obs = other_obs %>%
  mutate(other_observation_id = remisc::get_uuid(nrow(other_obs))) %>%
  mutate(observation_location_id = remisc::get_uuid(nrow(other_obs))) %>%
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
  mutate(observation_datetime = as.POSIXct(NA)) %>%
  mutate(observation_count = NA_integer_) %>%
  select(other_observation_id, survey_id, observation_location_id, obs_lat, obs_lon,
         obs_acc, observation_type_id, observation_datetime, observation_count,
         created_datetime, created_by = head_create_by, modified_datetime,
         modified_by = head_mod_by)

# Pull out location table data for both fish_barrier and other_observations
barrier_location = barrier_prep %>%
  select(survey_id, location_id = barrier_location_id,
         lat = obs_lat, lon = obs_lon, acc = obs_acc,
         created_datetime, created_by, modified_datetime,
         modified_by)

obs_location = other_obs %>%
  select(survey_id, location_id = observation_location_id,
         lat = obs_lat, lon = obs_lon, acc = obs_acc,
         created_datetime, created_by, modified_datetime,
         modified_by)

# Combine....wait to process until all location data for all surveys are combined !!!
header_location = rbind(barrier_location, obs_location)

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

# Test...currently 2639 records
strt = Sys.time()
dead = get_dead(profile_id, dead_fish_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Dump any records that are not in header_data
# FOR NOW !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Filter to header_data with no missing stream or reach_id for now. 2539 records after filter
dead = dead %>%
  filter(!parent_record_id %in% del_id)

# Process dates
dead = dead %>%
  mutate(created_date = as.POSIXct(iformr::idate_time(created_date))) %>%
  mutate(modified_date = as.POSIXct(iformr::idate_time(modified_date))) %>%
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

# Pull out survey_event data. Calculate cwt_detection method later...Chum = NA, blank = unknown, other = uuid
dead_se = dead %>%
  select(parent_record_id, species_fish, run_type, number_fish,
         encounter_comments, created_date, created_by, modified_date,
         modified_by)

#============ fish_encounter ========================

# Pull out fish_encounter data....calculate maturity later...from sex
dead_fe = dead %>%
  mutate(fish_status_id = "b185dc5d-6b15-4b5b-a54e-3301aec0270f") %>%            # Dead fish
  mutate(origin_id  = "2089de8c-3bd0-48fe-b31b-330a76d840d2") %>%                # Unknown
  mutate(fish_behavior_type_id = "70454429-724e-4ccf-b8a6-893cafba356a") %>%     # Not applicable...dead fish
  mutate(mortality_type_id = "149aefd0-0369-4f2c-b85f-4ec6c5e8679c") %>%         # Not applicable...not in form
  mutate(previously_counted_indicator = 0L) %>%
  select(parent_record_id, created_date, created_by, created_location,
         modified_date, modified_by, modified_location, fish_status_id, fish_sex,
         origin_id, cwt_detected, clip_status, fish_behavior_type_id,
         mortality_type_id, fish_count = number_fish, previously_counted_indicator)

#============== fish_capture_event and fish_mark =============

# Pull out fish mark data
fish_mark = dead %>%
  select(parent_record_id, created_date, created_by, modified_date,
         modified_by, species_fish, number_fish, carcass_condition,
         carc_tag_1, carc_tag_2, mark_status_1, mark_status_2)

# Correct and add species common name
unique(fish_mark$species_fish)
fish_mark = fish_mark %>%
  mutate(species_fish = if_else(species_fish == "Chum", "69d1348b-7e8e-4232-981a-702eda20c9b1", species_fish)) %>%
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

# Pull out cases where mark status is not applicable to check for species
chk_mark_species = fish_mark %>%
  filter(mark_status_one == "Not applicable" | mark_status_two == "Not applicable")
unique(chk_mark_species$common_name)
all(is.na(chk_mark_species$carcass_condition))
any(is.na(fish_mark$mark_status_1))
any(is.na(fish_mark$mark_status_2))

# Trim to only marked fish...went from 2539 rows to 2207 rows..correct: chk_mark_species: 332 rows
fish_mark = fish_mark %>%
  filter(!mark_status_one == "Not applicable" | !mark_status_two == "Not applicable")

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
#
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

# # Pull out only cases Lea indicated could not be correct: 4-5 Not Tagged + both mark_status == Unknown.
# fish_mark_incorrect = fish_mark %>%
#   select(parent_record_id, mark_status_1, mark_status_2, common_name,
#          number_fish, carcass_condition, carc_tag_1, carc_tag_2,
#          mark_status_one, mark_status_two) %>%
#   filter(carcass_condition == "4_5 Not Tagged" & mark_status_one == "Unknown" & mark_status_two == "Unknown") %>%
#   arrange(parent_record_id)
#
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
    mark_status_one == "Not present" | mark_status_two == "Not present" ~ "v03f32160-6426-4c41-a088-3580a7d1a0c5",
    mark_status_one == "Unknown" & mark_status_two == "Unknown" ~ "105ce6c3-1d14-4ca0-8730-bb72527295e5",
    mark_status_one == "Applied" | mark_status_two == "Applied" ~ "03f32160-6426-4c41-a088-3580a7d1a0c5",
    TRUE ~ "105ce6c3-1d14-4ca0-8730-bb72527295e5")) %>%
  mutate(disposition_type_id = "24b51215-f7ea-4480-ba7d-436144969ac3") %>%         # Carcass returned
  mutate(disposition_id = "dd6d0cab-1cc8-4b07-af93-5cd0be1a7a7f")                  # Not applicable

# Check
unique(fish_mark$fish_capture_status_id)

# Add needed header data
fish_mark = fish_mark %>%
  left_join(header_se, by = "parent_record_id")

# Pull out fish_capture_event
fish_capture_event = fish_mark %>%
  mutate(disposition_location_id = NA_character_) %>%
  select(parent_record_id, fish_capture_status_id, disposition_type_id,
         disposition_id, disposition_location_id, created_datetime = created_date,
         created_by, head_created_by, modified_datetime = modified_date,
         modified_by, head_mod_by)

# Pull out fish mark data separate and combine
fish_mark_one = fish_mark %>%
  select(parent_record_id, mark_type_id, mark_status_id = mark_status_1,
         mark_orientation_id, mark_placement_id, mark_size_id, mark_color_id,
         mark_shape_id, tag_number = carc_tag_1, created_datetime = created_date,
         created_by, head_created_by, modified_datetime = modified_date,
         modified_by, head_mod_by)
fish_mark_two = fish_mark %>%
  select(parent_record_id, mark_type_id, mark_status_id = mark_status_2,
         mark_orientation_id, mark_placement_id, mark_size_id, mark_color_id,
         mark_shape_id, tag_number = carc_tag_2, created_datetime = created_date,
         created_by, head_created_by, modified_datetime = modified_date,
         modified_by, head_mod_by)
# Combine
fish_mark_prep = rbind(fish_mark_one, fish_mark_two) %>%
  mutate(tag_number = trimws(tag_number)) %>%
  mutate(tag_number = if_else(tag_number == "", NA_character_, tag_number))

#============ individual_fish =============================================

# Pull out individual_fish data
dead_ind = dead %>%
  filter(number_fish == 1L) %>%
  select(parent_record_id, fish_condition_type_id = fish_condition, length_measurement_cm,
         spawn_condition_type_id = spawn_condition, cwt_snout_sample_number = cwt_label,
         genetic_sample_number = dna_number, scale_sample_card_number = scale_card_number,
         scale_sample_position_number = scale_card_position_number, encounter_comments,
         created_date, created_by, modified_date, modified_by)

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

# Test...currently 2639 records
strt = Sys.time()
live = get_live(profile_id, live_fish_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Dump any records that are not in header_data
# FOR NOW !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Filter to header_data with no missing stream or reach_id for now. 2539 records after filter
live = live %>%
  filter(!parent_record_id %in% del_id)

# Process dates
live = live %>%
  mutate(created_date = as.POSIXct(iformr::idate_time(created_date))) %>%
  mutate(modified_date = as.POSIXct(iformr::idate_time(modified_date))) %>%
  mutate(created_date = with_tz(created_date, tzone = "UTC")) %>%
  mutate(modified_date = with_tz(modified_date, tzone = "UTC"))




















