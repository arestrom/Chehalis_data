#===============================================================================
# Verify queries work
#
# AS 2020-02-25
#===============================================================================

# Load libraries
library(DBI)
library(RSQLite)
library(dplyr)
library(tibble)
library(glue)
library(iformr)
library(stringi)

# Set data for query
# Profile ID
profile_id = 417821L
# Parent form id
parent_form_page_id = 3363255L

# Get counts and date-range of surveys ready to download
count_new_surveys = function(profile_id, parent_form_page_id, access_token) {
  # Define query for new mobile surveys
  qry = glue("select distinct s.survey_id ",
             "from survey as s")
  # Checkout connection
  con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
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
  parent_ids = iformr::get_all_records(
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
  new_survey_data = parent_ids %>%
    select(parent_form_survey_id = id, survey_id = headerid, survey_date,
           stream_name, stream_name_text, new_stream_name, reach, reach_text,
           new_reach, gps_loc_lower, gps_loc_upper) %>%
    distinct() %>%
    anti_join(existing_surveys, by = "survey_id") %>%
    arrange(as.Date(survey_date))
  return(new_survey_data)
}

access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Test
strt = Sys.time()
new_surveys = count_new_surveys(profile_id, parent_form_page_id, access_token)
nd = Sys.time(); nd - strt

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

# Get start_id
start_id = new_survey_counts$first_id

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
         reach, lower_coords, upper_coords) %>%
  distinct()

# Pull out only needed data
missing_reach_vals = new_surveys %>%
  filter(nchar(reach) < 27L) %>%
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

# Get header data
get_header_data = function(profile_id, parent_form_page_id, start_id, access_token) {
  # Define fields
  fields = glue::glue("id, created_date, created_by, created_device_id, modified_date, modified_by, ",
                      "data_entry_type, target_species, survey_date, start_time, observers, ",
                      "stream_name, stream_name_text, new_stream_name, reach, reach_text, new_reach, survey_type, ",
                      "survey_method, survey_direction, clarity_ft, clarity_code, weather, flow,stream_conditions, ",
                      "no_survey, reachsplit_yn, user_name, headerid, end_time, header_comments, stream_temp, ",
                      "survey_completion_status, chinook_count_type, coho_count_type, steelhead_count_type, ",
                      "chum_count_type, gps_loc_lower, gps_loc_upper, coho_run_year, steelhead_run_year, ",
                      "chum_run_year, chinook_run_year, carcass_tagging, code_reach")
  # Collapse vector of column names into a single string
  fields = paste0(fields, collapse = ',')
  # Get list of all survey_ids and parent_form iform ids on server
  field_string <- paste0("id:<(>\"", start_id, "\"),", fields)
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
    since_id = start_id)
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

# Test
strt = Sys.time()
header_data = get_header_data(profile_id, parent_form_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Check some date and time values
any(is.na(header_data$survey_date))
chk_date = header_data %>%
  filter(is.na(survey_date))
# TEMPORARY FIX !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
header_data = header_data %>%
  filter(!is.na(survey_date))

# Process dates etc
# Rename id to parent_record_id for more explicit joins to subform data...convert dates, etc.
header_data = header_data %>%
  rename(parent_record_id = id, survey_uuid = headerid) %>%
  mutate(created_datetime = iformr::idate_time(created_date)) %>%
  mutate(modified_datetime = iformr::idate_time(modified_date)) %>%
  mutate(survey_start_datetime = as.POSIXct(paste0(survey_date, " ", start_time), tz = "America/Los_Angeles")) %>%
  mutate(survey_end_datetime = as.POSIXct(paste0(survey_date, " ", end_time), tz = "America/Los_Angeles")) %>%
  # Temporary fix for target species!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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

# Pull out missing required fields
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

# Combine
del_id = bind_rows(del_stream_id, del_reach_id) %>%
  distinct() %>%
  pull(parent_form_survey_id)

# Filter to header_data with no missing stream or reach_id for now
header_data = header_data %>%
  filter(!parent_record_id %in% del_id) %>%
  filter(!is.na(survey_method)) %>%
  mutate(survey_uuid = if_else(is.na(survey_uuid), remisc::get_uuid(1L), survey_uuid))

# Check
any(is.na(header_data$survey_uuid))
any(header_data$survey_uuid == "")
unique(nchar(header_data$survey_uuid))

#==================== End of trim section=============================================================================

# Generate created and modified data
header_data = header_data %>%
  mutate(modified_by = if_else(created_datetime == modified_datetime, NA_character_, modified_by)) %>%
  mutate(modified_by = if_else(substr(modified_by, 1, 3) %in% c("VAR", "FW0"), NA_character_, modified_by)) %>%
  mutate(modified_by = stringi::stri_replace_all_fixed(modified_by, ".", "_")) %>%
  mutate(modified_by = remisc::get_text_item(modified_by, 1, "_")) %>%
  mutate(modified_datetime = if_else(created_datetime == modified_datetime, NA_character_, modified_datetime))

# Survey =========================================

# Generate survey and associated tables
survey_prep = header_data %>%
  mutate(data_source_id = "c40563fa-efce-4cac-a2e3-6223a5e24030") %>%                           # WDFW
  mutate(data_source_unit_id = "e2d51ceb-398c-49cb-9aa5-d20a839e9ad9") %>%                      # Not applicable
  mutate(data_review_status_id = "b0ea75d4-7e77-4161-a533-5b3fce38ac2a") %>%                    # Preliminary
  mutate(data_submitter_last_name = observers) %>%
  select(survey_id = survey_uuid, survey_datetime = survey_date, data_source_id,
         data_source_unit_id, survey_method, data_review_status_id,
         stream_name, reach, survey_completion_status_id = survey_completion_status,
         incomplete_survey_type_id = no_survey, survey_start_datetime,
         survey_end_datetime, observers, data_submitter_last_name,
         created_datetime, created_by, modified_datetime, modified_by)

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

# Pull out duplicated comment_prep rows
dup_survey_id = comment_prep %>%
  group_by(survey_id) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1L) %>%
  select(survey_id) %>%
  distinct() %>%
  pull(survey_id)

chk_dup_comment = comment_prep %>%
  filter(survey_id %in% dup_survey_id) %>%
  arrange(survey_id)

# Finalize
comment_prep = comment_prep %>%
  mutate(survey_comment_id = remisc::get_uuid(nrow(comment_prep))) %>%


# Survey intent ================================

intent_prep = header_data %>%
  select(survey_id)






















