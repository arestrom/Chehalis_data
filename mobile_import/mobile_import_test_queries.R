#===============================================================================
# Verify queries work
#
# AS 2020-02-25
#===============================================================================

# Load libraries
library(DBI)
library(RSQLite)
library(tibble)
library(glue)
library(iformr)
library(stringi)

# Set data for query
# Profile ID
profile_id = 417821L
# Parent form id
parent_form_page_id = 3363255L

access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

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
                  "stream_name_text, reach, reach_text")
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
           stream_name, stream_name_text, reach, reach_text) %>%
    distinct() %>%
    anti_join(existing_surveys, by = "survey_id") %>%
    arrange(as.Date(survey_date))
  return(new_survey_data)
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
start_id = new_surveys$first_id

# Pull out only needed data
missing_stream_vals = new_surveys %>%
  filter(nchar(stream_name) < 36L) %>%
  select(parent_form_survey_id, survey_date, stream_name, stream_name_text) %>%
  distinct()

# New access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

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

  # Rename id to parent_record_id for more explicit joins to subform data...convert dates, etc.
  survey_records = parent_records %>%
    rename(parent_record_id = id, survey_uuid = headerid) %>%
    mutate(created_datetime = iformr::idate_time(created_date)) %>%
    mutate(modified_datetime = iformr::idate_time(modified_date)) %>%
    mutate(survey_start_datetime = as.POSIXct(paste0(survey_date, " ", start_time), tz = "America/Los_Angeles")) %>%
    mutate(survey_end_datetime = as.POSIXct(paste0(survey_date, " ", end_time), tz = "America/Los_Angeles")) %>%
    # Temporary fix for target species
    mutate(target_species = case_when(
      target_species == "Chinook" ~ "e42aa0fc-c591-4fab-8481-55b0df38dcb1",
      target_species == "Chum" ~ "69d1348b-7e8e-4232-981a-702eda20c9b1",
      !target_species %in% c("Chinook", "Chum") ~ target_species)) %>%
    mutate(observers = stringi::stri_replace_all_fixed(observers, "@dfw.wa.gov", "")) %>%
    mutate(observers = stringi::stri_replace_all_fixed(observers, ".", " ")) %>%
    mutate(observers = stringi::stri_trans_totitle(observers)) %>%
    select(parent_record_id, survey_uuid, created_datetime, created_by, created_device_id,
           data_entry_type, target_species, survey_date, survey_start_datetime, survey_end_datetime,
           observers, stream_name, stream_name_text, new_stream_name, reach, reach_text, new_reach,
           survey_type, survey_method, survey_direction, clarity_ft, clarity_code, weather,
           flow,stream_conditions, no_survey, reachsplit_yn, user_name, end_time, header_comments,
           stream_temp, survey_completion_status, chinook_count_type, coho_count_type,
           steelhead_count_type, chum_count_type, gps_loc_lower, gps_loc_upper,
           coho_run_year, steelhead_run_year, chum_run_year, chinook_run_year,
           carcass_tagging, code_reach)

  # Process stream and reach data
  chk_stream_name = survey_records %>%
    filter(nchar(stream_name) < 36L) %>%
    mutate(llid = remisc::get_text_item(reach, item = 1L, sep = "_")) %>%
    mutate(no_llid = if_else(!nchar(llid) == 13L, "yes", "no")) %>%
    select(parent_record_id, observers, stream_name, stream_name_text, new_stream_name,
           reach, reach_text, new_reach, code_reach, gps_loc_lower, gps_loc_upper,
           llid, no_llid)
}

fields = glue::glue("id,parent_record_id,parent_page_id,parent_element_id,created_date,created_by,",
                    "created_location,created_device_id,modified_date,modified_by,modified_location,",
                    "modified_device_id,data_entry_type,target_species,survey_date,start_time,observers,",
                    "stream_name,stream_name_text,new_stream_name,reach,reach_text,new_reach,survey_type,",
                    "survey_method,survey_direction,clarity_ft,clarity_code,weather,flow,stream_conditions,",
                    "no_survey,reachsplit_yn,user_name,headerid,end_time,header_comments,",
                    "stream_temp,survey_completion_status,chinook_count_type,",
                    "coho_count_type,steelhead_count_type,chum_count_type,gps_loc_lower,gps_loc_upper,",
                    "coho_run_year,steelhead_run_year,chum_run_year,chinook_run_year,carcass_tagging,code_reach")

# Collapse vector of column names into a single string
form_fields <- paste(fields, collapse = ',')

# Pull data from iForm
r6_stream_surveys = get_all_records("wdfw",417821,3363255,fields = "fields", limit = 1000, 0, access_token, form_fields,0)








