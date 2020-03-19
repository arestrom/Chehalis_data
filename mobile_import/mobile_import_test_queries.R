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

# Get the parent form id
other_obs_page_id = iformr::get_page_id(
  server_name = "wdfw",
  profile_id = profile_id,
  page_name = other_obs_form_name,
  access_token = access_token)

# Check
other_obs_page_id

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

# Test: Currently 1425 records....can use start_id in tandem with parent_record_id from DB in the future to filter
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
         reach, lower_coords, upper_coords) %>%
  distinct()

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

# Test...currently 1414 records
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

# Test...currently 324 records
strt = Sys.time()
other_obs = get_other_obs(profile_id, other_obs_page_id, start_id, access_token)
nd = Sys.time(); nd - strt

# Dump any records that are not in header_data
# FOR NOW !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Filter to header_data with no missing stream or reach_id for now
other_obs = other_obs %>%
  filter(!parent_record_id %in% del_id) %>%
  mutate(created_datetime = as.POSIXct(iformr::idate_time(created_date))) %>%
  mutate(modified_datetime = as.POSIXct(iformr::idate_time(modified_date))) %>%
  mutate(created_datetime = with_tz(created_datetime, tzone = "UTC")) %>%
  mutate(modified_datetime = with_tz(modified_datetime, tzone = "UTC"))




# STOPPED HERE .....


# Next...no need to worry about mobile_device
# other_observation

# Then
# survey_event































