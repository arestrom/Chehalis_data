#================================================================
# Export all WRIA 22 and 23 data to excel in Curt Holt format
#
# Notes:
#  1. Consider entering one zero for each species and intent
#     This would allow pulling up survey_type for each species
#     and survey for use in Curts export.
#  2. Still not right...getting duplicates and too many rows
#     for NF and SF Newaukum. Add observer to data !!!!!!!!!!
#     Or it may be ok...just need to sort better????
#  3. Need to get rid of fake surveys.
#
# AS 2020-06-12
#================================================================

# Clear workspace
rm(list=ls(all=TRUE))

# Load any required libraries
library(RPostgres)
library(dplyr)
library(DBI)
library(glue)
library(tidyr)
library(remisc)
library(stringi)
library(openxlsx)
library(lubridate)
library(iformr)

# Set data for query
# Profile ID
profile_id = 417821L
since_id = 0L

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

# Function to connect to postgres
pg_con_local = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = "5432")
  con
}

#===== Get page_ids of parent_form...for survey_type calculation  =======================

# Form name
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

#======================================================================================
# Get survey_type from parent form....not ideal
#======================================================================================

# Get new survey data
get_all_surveys = function(profile_id, parent_form_page_id, access_token) {
  # Define fields...just parent_id and survey_id this time
  fields = paste0("id, headerid, survey_date, stream_name, ",
                  "stream_name_text, new_stream_name, reach, ",
                  "reach_text, new_reach, gps_loc_lower, ",
                  "gps_loc_upper, survey_type ")
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
           gps_loc_upper, survey_type) %>%
    distinct() %>%
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
  con = pg_con_local("spawning_ground")
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
             "from location as loc ",
             "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
             "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
             "where wria_code in ('22', '23')")
  # Checkout connection
  con = pg_con_local("spawning_ground")
  #con = poolCheckout(pool)
  river_mile_data = DBI::dbGetQuery(con, qry) %>%
    filter(!is.na(llid) & !is.na(river_mile))
  DBI::dbDisconnect(con)
  #poolReturn(con)
  return(river_mile_data)
}

# Update DB and reload DT
get_all_survey_data = function(profile_id, parent_form_page_id, access_token) {
  new_surveys = get_all_surveys(profile_id = profile_id,
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

# Get new survey_data: currently 1973 records
all_survey_data = get_all_survey_data(profile_id, parent_form_page_id, access_token)

# Reactive to process gps data
get_core_survey_data = function(all_survey_data) {
  survey_data = all_survey_data %>%
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
           waterbody_id, stream_geometry_id, survey_type)
}

# Run
core_survey_data = get_core_survey_data(all_survey_data)

# Reactive for missing reaches
get_survey_type_vals = function(core_survey_data) {
  streams = get_mobile_streams()
  surv_type = core_survey_data %>%
    mutate(lo_rm = trimws(lo_rm)) %>%
    mutate(up_rm = trimws(up_rm)) %>%
    # filter(is.na(lo_rm) | is.na(up_rm) | lo_rm == "listed" | up_rm == "listed" |
    #          lo_rm == "" | up_rm == "") %>%
    select(-c(waterbody_id, stream_geometry_id)) %>%
    rename(waterbody_id = stream_name, iform_llid = llid) %>%
    left_join(streams, by = "waterbody_id") %>%
    mutate(stream_name_text = if_else(!is.na(waterbody_id), waterbody_display_name, stream_name_text)) %>%
    mutate(reach_text = if_else(reach_text == "Not Listed", new_reach, reach_text)) %>%
    mutate(up_rm = if_else(up_rm %in% c("", "listed"), NA_character_, up_rm)) %>%
    mutate(lo_rm = if_else(up_rm %in% c("", "listed"), NA_character_, lo_rm)) %>%
    select(survey_date, stream_name = waterbody_display_name, up_rm, lo_rm, survey_type) %>%
    distinct()
  return(surv_type)
}

# Run
missing_survey_type = get_survey_type_vals(core_survey_data)

#============================================================================
# Get all surveys within data-range for WRIAs 22 and 23
#============================================================================

# Set selectable values
# run_year = 2016
# species = "Chin"
# run = "Fall"      # Fall or Spring
# #wria = "22.0360"
start_date = "2019-09-01"
end_date = "2020-02-29"

# Function to get header data...use multiselect for year
get_header_data = function(start_date, end_date) {
  qry = glue("select distinct wr.wria_code, s.survey_id, wb.stream_catalog_code as cat_code, ",
             "wb.waterbody_display_name as stream_name, wb.latitude_longitude_id as llid, ",
             "se.survey_event_id, sd.survey_design_type_code as type, s.survey_datetime as survey_date, ",
             "sp.common_name as species, locl.river_mile_measure as rml, locu.river_mile_measure as rmu, ",
             "sm.survey_method_code as method, sf.flow_type_short_description as flow, ",
             "vt.visibility_type_short_description as rifflevis, s.observer_last_name as obs ",
             "from waterbody_lut as wb ",
             "inner join stream as st on wb.waterbody_id = st.waterbody_id ",
             "inner join wria_lut as wr on st_intersects(st.geom, wr.geom) ",
             "left join location as loc on wb.waterbody_id = loc.waterbody_id ",
             "left join survey as s on (loc.location_id = s.upper_end_point_id or ",
             "loc.location_id = s.lower_end_point_id) ",
             "left join survey_event as se on s.survey_id = se.survey_id ",
             "left join species_lut as sp on se.species_id = sp.species_id ",
             "inner join survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
             "left join survey_design_type_lut as sd on se.survey_design_type_id = sd.survey_design_type_id ",
             "left join location as locu on s.upper_end_point_id = locu.location_id ",
             "left join location as locl on s.lower_end_point_id = locl.location_id ",
             "left join survey_comment as sc on s.survey_id = sc.survey_id ",
             "left join stream_flow_type_lut as sf on sc.stream_flow_type_id = sf.stream_flow_type_id ",
             "left join visibility_type_lut as vt on sc.visibility_type_id = vt.visibility_type_id ",
             "where survey_datetime between '{start_date}' and '{end_date}' ",
             "and wr.wria_code in ('22', '23')")
  con = pg_con_local("spawning_ground")
  surveys = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  surveys = surveys %>%
    # HACK WARNING: TimeZone was screwed up !!!!!!!!! Using New York in profile !!!!!
    # as.Date uses local timezone so conversion should come out correctly
    # But may need to adjust after dates are corrected in DB !!!!!!!!!!!!!!!
    mutate(survey_date = as.Date(survey_date)) %>%
    mutate(statweek = remisc::fish_stat_week(survey_date, start_day = "Sun")) %>%
    mutate(survey_date = format(survey_date, "%m/%d/%Y")) %>%
    select(survey_id, survey_event_id, wria = cat_code, stream_name,
           type, survey_date, statweek, species, rml, rmu, method, flow,
           rifflevis, obs) %>%
    arrange(stream_name, as.Date(survey_date, format = "%m/%d/%Y"))
  return(surveys)
}

# Run the function: 1777 rows
header_data = get_header_data(start_date, end_date)

#============================================================================
# Get intents for surveys above
#============================================================================

# Function to get fish_counts
get_intent = function(header_data) {
  # Pull out the survey_event_ids
  s_id = header_data %>%
    filter(!is.na(survey_id)) %>%
    pull(survey_id)
  s_ids = paste0(paste0("'", s_id, "'"), collapse = ", ")
  # Query to pull out all counts by species and sex-maturity-mark groups
  qry = glue("select si.survey_id, sp.common_name as species, ct.count_type_code as count_type ",
             "from survey_intent as si ",
             "left join species_lut as sp on si.species_id = sp.species_id ",
             "left join count_type_lut as ct on si.count_type_id = ct.count_type_id ",
             "where si.survey_id in ({s_ids})")
  con = pg_con_local("spawning_ground")
  count_intent = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  count_intent = count_intent %>%
    select(survey_id, species, count_type) %>%
    distinct()
  return(count_intent)
}

# Run the function: 2504 rows
count_intent = get_intent(header_data)

#============================================================================
# Get all live and dead counts for surveys above
#============================================================================

# Function to get fish_counts
get_fish_counts = function(header_data) {
  # Pull out the survey_event_ids
  se_id = header_data %>%
    filter(!is.na(survey_event_id)) %>%
    pull(survey_event_id)
  se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")
  # Query to pull out all counts by species and sex-maturity-mark groups
  qry = glue("select se.survey_event_id, fe.fish_encounter_id, sp.common_name as species, ",
             "fs.fish_status_description as fish_status, se.comment_text as fish_comments, ",
             "sx.sex_description as sex, mat.maturity_short_description as maturity, ",
             "ad.adipose_clip_status_code as clip_status, cwt.detection_status_description as cwt_detect, ",
             "fe.fish_count, fe.previously_counted_indicator as prev_count ",
             "from survey_event as se ",
             "left join fish_encounter as fe on se.survey_event_id = fe.survey_event_id ",
             "left join species_lut as sp on se.species_id = sp.species_id ",
             "left join fish_status_lut as fs on fe.fish_status_id = fs.fish_status_id ",
             "left join sex_lut as sx on fe.sex_id = sx.sex_id ",
             "left join maturity_lut as mat on fe.maturity_id = mat.maturity_id ",
             "left join adipose_clip_status_lut as ad on fe.adipose_clip_status_id = ad.adipose_clip_status_id ",
             "left join cwt_detection_status_lut as cwt on fe.cwt_detection_status_id = cwt.cwt_detection_status_id ",
             "where se.survey_event_id in ({se_ids})")
  con = pg_con_local("spawning_ground")
  fish_counts = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  fish_counts = fish_counts %>%
    select(survey_event_id, fish_encounter_id, species, fish_status, sex,
           maturity, clip_status, cwt_detect, fish_count, prev_count,
           fish_comments)
  return(fish_counts)
}

# Run the function: 2504 rows
fish_counts = get_fish_counts(header_data)

#============================================================================
# Get all redd counts for surveys above
#============================================================================

# Function to get redd counts
get_redd_counts = function(header_data) {
  se_id = header_data %>%
    filter(!is.na(survey_event_id)) %>%
    pull(survey_event_id)
  se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")
  qry = glue("select se.survey_event_id, rd.redd_encounter_id, sp.common_name as species, ",
             "rs.redd_status_short_description as redd_status, rd.redd_count ",
             "from survey_event as se ",
             "left join species_lut as sp on se.species_id = sp.species_id ",
             "left join redd_encounter as rd on se.survey_event_id = rd.survey_event_id ",
             "left join redd_status_lut as rs on rd.redd_status_id = rs.redd_status_id ",
             "where se.survey_event_id in ({se_ids})")
  con = pg_con_local("spawning_ground")
  redd_counts = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  redd_counts = redd_counts %>%
    select(survey_event_id, redd_encounter_id, species, redd_status, redd_count)
  return(redd_counts)
}

# Run the function: 7059 rows
redd_counts = get_redd_counts(header_data)

#============================================================================
# Get the cwt labels for surveys above
#============================================================================

# Function to get cwt labels
get_head_labels = function(header_data) {
  se_id = header_data %>%
    filter(!is.na(survey_event_id)) %>%
    pull(survey_event_id)
  se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")
  qry = glue("select se.survey_id, se.survey_event_id, sp.common_name as species, ",
             "indf.cwt_snout_sample_number as cwt_head_label ",
             "from survey_event as se ",
             "left join species_lut as sp on se.species_id = sp.species_id ",
             "left join fish_encounter as fe on se.survey_event_id = fe.survey_event_id ",
             "left join individual_fish as indf on fe.fish_encounter_id = indf.fish_encounter_id ",
             "where se.survey_event_id in ({se_ids}) and ",
             "indf.cwt_snout_sample_number is not null")
  con = pg_con_local("spawning_ground")
  head_labels = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  head_labels = head_labels %>%
    select(survey_id, survey_event_id, species, cwt_head_label)
  return(head_labels)
}

# Run the function: 3 rows .... all from fake data
head_labels = get_head_labels(header_data)

#============================================================================
# Add survey type to surveys where currently missing
# Due to no survey_type if no fish encountered...now using survey_intent
#============================================================================

# Check values
unique(missing_survey_type$survey_type)

# Pull out just survey_type where survey_id is present
stype = missing_survey_type %>%
  mutate(surv_type = case_when(
    survey_type == "d455abb6-e9db-49f2-abc8-2e70890f301b" ~ "Explor",
    survey_type == "beb561df-9fd0-4e7d-842f-0c64a515d23b" ~ "Index",
    survey_type == "88b954cb-b7a4-4946-845a-3abd86022fbb" ~ "Supp",
    is.na(survey_type) ~ "b87d5435-f69c-49df-ba1f-658df318bd19")) %>%
  mutate(rml = as.numeric(lo_rm)) %>%
  mutate(rmu = as.numeric(up_rm)) %>%
  select(join_date = survey_date, stream_name, rml, rmu, surv_type) %>%
  distinct()

# Join to header data
header_data = header_data %>%
  mutate(join_date = format(as.Date(survey_date, format = "%m/%d/%Y"))) %>%
  left_join(stype, by = c("join_date", "stream_name", "rml", "rmu")) %>%
  mutate(type = if_else(is.na(type) & !is.na(surv_type), surv_type, type)) %>%
  mutate(type = if_else(is.na(type), "Unknown", type))

#============================================================================
# Calculate fish counts by status, sex, and maturity
#============================================================================

# Cases occur with live detection method NA and dead detection method electronic
# So need to combine again by survey_id and species to avoid duplicate rows

# Add some helper fields from header_data
head_dat = header_data %>%
  filter(!is.na(survey_event_id)) %>%
  select(survey_id, survey_event_id, stream_name,
         survey_date, type, rml, rmu, method, obs) %>%
  distinct()

# Add to fish_counts
fish_dat = fish_counts %>%
  left_join(head_dat, by = "survey_event_id") %>%
  select(survey_id, survey_event_id, fish_encounter_id, stream_name, survey_date, type,
         rml, rmu, method, obs, species, fish_status, sex, maturity, clip_status,
         cwt_detect, fish_count, prev_count, fish_comments) %>%
  arrange(stream_name, as.Date(survey_date, format = "%m/%d/%Y"), rml, rmu)

# Compute sums for live and dead categories
fish_sums = fish_dat %>%
  filter(!is.na(fish_encounter_id)) %>%
  filter(!prev_count == TRUE) %>%
  mutate(lv_male_count = if_else(fish_status == "Live" & sex == "Male" & maturity == "Adult",
                                 fish_count, 0L)) %>%
  mutate(lv_female_count = if_else(fish_status == "Live" & sex == "Female" & maturity == "Adult",
                                   fish_count, 0L)) %>%
  mutate(lv_snd_count = if_else(fish_status == "Live" & sex == "Unknown" & maturity == "Adult",
                                fish_count, 0L)) %>%
  mutate(lv_jack_count = if_else(fish_status == "Live" & sex == "Male" & maturity == "Subadult",
                                 fish_count, 0L)) %>%
  mutate(dd_male_count = if_else(fish_status == "Dead" & sex == "Male" & maturity == "Adult",
                                 fish_count, 0L)) %>%
  mutate(dd_female_count = if_else(fish_status == "Dead" & sex == "Female" & maturity == "Adult",
                                   fish_count, 0L)) %>%
  mutate(dd_snd_count = if_else(fish_status == "Dead" & sex == "Unknown" & maturity == "Adult",
                                fish_count, 0L)) %>%
  mutate(dd_jack_count = if_else(fish_status == "Dead" & sex == "Male" & maturity == "Subadult",
                                 fish_count, 0L)) %>%
  group_by(survey_id, species) %>%
  mutate(lv_male_sum = sum(lv_male_count, na.rm = TRUE)) %>%
  mutate(lv_female_sum = sum(lv_female_count, na.rm = TRUE)) %>%
  mutate(lv_snd_sum = sum(lv_snd_count, na.rm = TRUE)) %>%
  mutate(lv_jack_sum = sum(lv_jack_count, na.rm = TRUE)) %>%
  mutate(dd_male_sum = sum(dd_male_count, na.rm = TRUE)) %>%
  mutate(dd_female_sum = sum(dd_female_count, na.rm = TRUE)) %>%
  mutate(dd_snd_sum = sum(dd_snd_count, na.rm = TRUE)) %>%
  mutate(dd_jack_sum = sum(dd_jack_count, na.rm = TRUE)) %>%
  ungroup() %>%
  select(survey_id, species, lv_male_sum, lv_female_sum, lv_snd_sum,
         lv_jack_sum, dd_male_sum, dd_female_sum, dd_snd_sum,
         dd_jack_sum) %>%
  distinct()

# Add back to fish_dat...for inspection
base_sums = fish_dat %>%
  select(survey_id, species, stream_name, survey_date, type, rml,
         rmu, method, obs) %>%
  distinct() %>%
  left_join(fish_sums, by = c("survey_id", "species"))

#============================================================================
# Calculate fish counts by clip and cwt detection status
#============================================================================

# Compute sums for live and dead categories
fish_clip_sums = fish_dat %>%
  filter(!is.na(fish_encounter_id)) %>%
  filter(!prev_count == TRUE) %>%
  filter(!cwt_detect == "Not applicable") %>%
  mutate(clip_beep = if_else(clip_status == "AD" & cwt_detect == "Coded-wire tag detected",
                                 fish_count, 0L)) %>%
  mutate(clip_nobeep = if_else(clip_status == "AD" & cwt_detect == "Coded-wire tag not detected",
                                   fish_count, 0L)) %>%
  mutate(clip_nohead = if_else(clip_status == "AD" & cwt_detect == "Coded-wire tag undetermined, e.g., no head",
                                fish_count, 0L)) %>%
  mutate(noclip_beep = if_else(clip_status == "UM" & cwt_detect == "Coded-wire tag detected",
                             fish_count, 0L)) %>%
  mutate(noclip_nobeep = if_else(clip_status == "UM" & cwt_detect == "Coded-wire tag not detected",
                               fish_count, 0L)) %>%
  mutate(noclip_nohead = if_else(clip_status == "UM" & cwt_detect == "Coded-wire tag undetermined, e.g., no head",
                               fish_count, 0L)) %>%
  mutate(unclip_beep = if_else(clip_status == "UN" & cwt_detect == "Coded-wire tag detected",
                               fish_count, 0L)) %>%
  mutate(unclip_nobeep = if_else(clip_status == "UN" & cwt_detect == "Coded-wire tag not detected",
                                 fish_count, 0L)) %>%
  mutate(unclip_nohead = if_else(clip_status == "UN" & cwt_detect == "Coded-wire tag undetermined, e.g., no head",
                                 fish_count, 0L)) %>%
  group_by(survey_id, species) %>%
  mutate(clip_beep_sum = sum(clip_beep, na.rm = TRUE)) %>%
  mutate(clip_nobeep_sum = sum(clip_nobeep, na.rm = TRUE)) %>%
  mutate(clip_nohead_sum = sum(clip_nohead, na.rm = TRUE)) %>%
  mutate(noclip_beep_sum = sum(noclip_beep, na.rm = TRUE)) %>%
  mutate(noclip_nobeep_sum = sum(noclip_nobeep, na.rm = TRUE)) %>%
  mutate(noclip_nohead_sum = sum(noclip_nohead, na.rm = TRUE)) %>%
  mutate(unclip_beep_sum = sum(unclip_beep, na.rm = TRUE)) %>%
  mutate(unclip_nobeep_sum = sum(unclip_nobeep, na.rm = TRUE)) %>%
  mutate(unclip_nohead_sum = sum(unclip_nohead, na.rm = TRUE)) %>%
  ungroup() %>%
  select(survey_id, species, ADClippedBeep = clip_beep_sum, ADClippedNoBeep = clip_nobeep_sum,
         ADClippedNoHead = clip_nohead_sum, UnMarkBeep = noclip_beep_sum,
         UnMarkNoBeep = noclip_nobeep_sum, UnMarkNoHead = noclip_nohead_sum,
         UnknownMarkBeep = unclip_beep_sum, UnknownMarkNoBeep = unclip_nobeep_sum,
         UnknownMarkNoHead = unclip_nohead_sum) %>%
  distinct()

#============================================================================
# Calculate redd sums
#============================================================================

# Add to fish_counts
redd_dat = redd_counts %>%
  left_join(head_dat, by = "survey_event_id") %>%
  select(survey_id, survey_event_id, redd_encounter_id, stream_name, survey_date, type,
         rml, rmu, method, obs, species, redd_status, redd_count) %>%
  filter(!is.na(redd_encounter_id)) %>%
  arrange(stream_name, as.Date(survey_date, format = "%m/%d/%Y"), rml, rmu)

# Compute sums for redd categories
redd_sums = redd_dat %>%
  arrange(type, stream_name, rml, rmu, as.Date(survey_date, format = "%m/%d/%Y")) %>%
  mutate(reach = paste0(type, "_", stream_name, "_", rml, "_", rmu)) %>%
  mutate(rnew = if_else(redd_status == "New redd", redd_count, 0L)) %>%
  mutate(old_redds = if_else(redd_status == "Previous redd, still visible", redd_count, 0L)) %>%
  mutate(comb_redds = if_else(redd_status == "Combined visible redds", redd_count, NA_integer_)) %>%
  mutate(rcomb = comb_redds) %>%
  mutate(rvis = if_else(!is.na(comb_redds), rnew + old_redds + comb_redds, rnew + old_redds)) %>%
  mutate(rnew_comb = if_else(!is.na(comb_redds), rnew + comb_redds, rnew)) %>%
  group_by(survey_id, species) %>%
  mutate(rnew_sum = sum(rnew, na.rm = TRUE)) %>%
  mutate(rvis_sum = sum(rvis, na.rm = TRUE)) %>%
  mutate(rcomb_sum = if_else(!is.na(rcomb), sum(rcomb, na.rm = TRUE), NA_integer_)) %>%
  mutate(rnew_comb_sum = sum(rnew_comb, na.rm = TRUE)) %>%
  ungroup() %>%
  select(survey_id, stream_name, species, survey_date, type, reach,
         rml, rmu, rnew_sum, rvis_sum, rcomb_sum, rnew_comb_sum) %>%
  distinct()

# Compute RCUM....Check later if Exploratory surveys should be cumsum same way as index
redd_sums = redd_sums %>%
  group_by(reach) %>%
  mutate(rcum = if_else(type %in% c("Supp", "Explor"), cumsum(rnew_comb_sum), cumsum(rnew_sum))) %>%
  ungroup() %>%
  select(survey_id, stream_name, species, survey_date, type, reach,
         rml, rmu, rnew = rnew_sum, rvis = rvis_sum, rcum,
         rcomb = rcomb_sum)

# Add sort_order
redd_sums = redd_sums %>%
  # Temporarily change Explor to xplore to set at bottom in sort order
  mutate(type = if_else(type == "Explor", "xplor", type)) %>%
  arrange(type, stream_name, rml, rmu, as.Date(survey_date, format = "%m/%d/%Y")) %>%
  mutate(sort_order = seq(1, nrow(redd_sums))) %>%
  mutate(type = if_else(type == "xplor", "Explor", type)) %>%
  select(survey_id, stream_name, species, survey_date, type, reach,
         rml, rmu, rnew, rvis, rcum, rcomb, sort_order) %>%
  arrange(sort_order)

#============================================================================
# Pivot out cwt_labels
#============================================================================

# Pivot out cwt_labels
label_pivot = head_labels %>%
  group_by(survey_id, species) %>%
  mutate(key = row_number(cwt_head_label)) %>%
  spread(key = key, value = cwt_head_label) %>%
  ungroup()

# Concatenate cwt_labels to one variable
label_concat = label_pivot %>%
  mutate(cwts = apply(label_pivot[,4:ncol(label_pivot)], 1, paste0, collapse = ", ")) %>%
  select(survey_id, species, cwts) %>%
  mutate(CWTHeadLabels = stri_replace_all_fixed(cwts, pattern = ", NA", replacement = "")) %>%
  select(survey_id, species, CWTHeadLabels)

#============================================================================
# Combine and format to final table
#============================================================================

# Combine fish sums and redd sums
fr_sums = fish_sums %>%
  left_join(redd_sums, by = c("survey_id", "species")) %>%
  select(survey_id, species, LM = lv_male_sum, LF = lv_female_sum,
         LSND = lv_snd_sum, LJ = lv_jack_sum, DM = dd_male_sum,
         DF = dd_female_sum, DSND = dd_snd_sum, DJ = dd_jack_sum,
         RNEW = rnew, RVIS = rvis, RCUM = rcum, RCOMB = rcomb)

# Add clip sums to fr_sums
frc_sums = fr_sums %>%
  left_join(fish_clip_sums, by = c("survey_id", "species"))

# Pull out comments, pivot and concatenate
fish_comments = fish_dat %>%
  filter(!is.na(fish_comments)) %>%
  select(survey_id, species, Comments = fish_comments)

# Pivot
comment_pivot = fish_comments %>%
  group_by(survey_id, species) %>%
  mutate(key = row_number(Comments)) %>%
  spread(key = key, value = Comments) %>%
  ungroup()

# Concatenate to one variable
comment_concat = comment_pivot %>%
  mutate(comments = apply(comment_pivot[,3:ncol(comment_pivot)], 1, paste0, collapse = ", ")) %>%
  select(survey_id, species, comments) %>%
  mutate(comments = stri_replace_all_fixed(comments, pattern = ", NA", replacement = "")) %>%
  select(survey_id, species, comments)

# Add comments to sums
frc_sums = frc_sums %>%
  left_join(comment_concat, by = c("survey_id", "species"))

# Add cwt labels to sums
frc_sums = frc_sums %>%
  left_join(label_concat, by = c("survey_id", "species"))

# Add remaining missing columns
head_fields = header_data %>%
  select(survey_id, WRIA = wria, StreamName = stream_name,
         Type = type, Date = survey_date, StatWeek = statweek,
         RML = rml, RMU = rmu, Method = method, Flow = flow,
         RiffleVis = rifflevis) %>%
  distinct()
any(duplicated(head_fields$survey_id))

# Pivot out count_types for intent
intent_pivot = count_intent %>%
  group_by(survey_id, species) %>%
  mutate(key = row_number(count_type)) %>%
  spread(key = key, value = count_type) %>%
  ungroup()

# Concatenate cwt_labels to one variable
intent_concat = intent_pivot %>%
  mutate(count_type = apply(intent_pivot[,3:ncol(intent_pivot)], 1, paste0, collapse = ", ")) %>%
  select(survey_id, species, count_type) %>%
  mutate(SurveyIntent = stri_replace_all_fixed(count_type, pattern = ", NA", replacement = "")) %>%
  select(survey_id, species, SurveyIntent)

# Combine head_fields and intent fields to get full set of species per survey
all_surveys = intent_concat %>%
  left_join(head_fields, by = "survey_id") %>%
  left_join(frc_sums, by = c("survey_id", "species")) %>%
  select(WRIA, StreamName, Type, Date, StatWeek, RML, RMU, Method,
         Flow, RiffleVis, Species = species, SurveyIntent, LM, LF,
         LSND, LJ, DM, DF, DSND, DJ, RNEW, RVIS, RCUM, RCOMB,
         Comments = comments, ADClippedBeep, ADClippedNoBeep,
         ADClippedNoHead, UnMarkBeep, UnMarkNoBeep,
         UnMarkNoHead, UnknownMarkBeep, UnknownMarkNoBeep,
         UnknownMarkNoHead, CWTHeadLabels)

# Create a new reach field to enable pulling out empty row
#unique(all_surveys$StreamName)
#unique(all_surveys$WRIA)
all_surveys = all_surveys %>%
  mutate(reach = paste0(Type, "_", WRIA, "_", StreamName, "_", RML, "_", RMU))

# Pull out unique set of Reaches to add blank rows
dat_empty = all_surveys
dat_empty[,1:35] = ""
dat_empty = unique(dat_empty)
dat = rbind(all_surveys, dat_empty)

# Add sort_order
dat = dat %>%
  arrange(Type, WRIA, RML, RMU, as.Date(Date, format = "%m/%d/%Y")) %>%
  mutate(sort_order = seq(1, nrow(dat)))

# Add sort order to all rows by reach
sort_dat = dat %>%
  select(reach, sort_order) %>%
  filter(as.integer(sort_order) > 0) %>%
  mutate(sort_order = as.integer(sort_order)) %>%
  group_by(reach) %>%
  mutate(sort_seq = row_number(reach)) %>%
  ungroup() %>%
  filter(sort_seq == 1) %>%
  distinct() %>%
  arrange(sort_order) %>%
  select(reach, sort_two = sort_order)

# Add sort_dat to dat
dat = dat %>%
  left_join(sort_dat, by = "reach")

# Order as before
dat = dat %>%
  arrange(sort_two, as.Date(Date, format = "%m/%d/%Y")) %>%
  mutate(Comments = if_else(is.na(Comments), "", Comments)) %>%
  mutate(CWTHeadLabels = if_else(is.na(CWTHeadLabels), "", CWTHeadLabels)) %>%
  select(-c(reach, sort_order))

# Format date mm/dd/yyyy
dat = dat %>%
  mutate(Date = format(as.Date(Date), format = "%m/%d/%Y")) %>%
  mutate(Date = if_else(is.na(Date), "", Date)) %>%
  select(-sort_two)

#============================================================================
# Output to Excel
#============================================================================

# # Write directly to excel
# out_name = paste0(run_year, "_", run, "_", "Chinook.xlsx")
# write.xlsx(dat, file = out_name, colNames = TRUE, sheetName = "Chinook")

# Or fancier with styling
out_name = paste0(run_year, "_", run, "_", "Chinook.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "Chinook", gridLines = TRUE)
writeData(wb, sheet = 1, dat, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:33, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)




