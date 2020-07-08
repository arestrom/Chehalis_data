#================================================================
# Export all WRIA 22 data to excel in Curt Holt format
#
# Notes:
#  1. Need to add an entry in survey_event for each species
#     in survey_intent to record survey_design_type. Lea
#     has this data in the header, and Curt needs a Type for
#     each survey.
#  2. Do not add "unknown redd status" to count totals.
#  3. Add run
#  4. Add zeros
#  5. Add NoSurvey reason right after SurveyIntent
#  6. Look at SF Newaukum 12.3 to 13.6, 10/31/2019. Why are
#     no redd_counts showing up...there should be 5 still
#     visible and 8 either unknown or not visible.
#  7. See line 571. Need to separate by species, run, and
#     origin before computing sums for redds. These are
#     already taken care of by survey_event (I think) in
#     the fish_counts summaries.
#  8. Add a "No survey conducted" indicator to any species
#     (of the four) that was not entered in the survey_intent
#     table. This will make the intent explicit in the export
#     that no survey was conducted for that species.
#  9. Kim said that WRIA 23 not needed for Chum
#
# AS 2020-07-06
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

#============================================================================
# Get all surveys within data-range for WRIAs 22 and 23
#============================================================================

# Set selectable values
selected_run_year = 2019                     # Only for computing Steelhead origin. Line 376
selected_species = "Chum salmon"
# run = "Fall"      # Fall or Spring
start_date = "2019-09-01"
end_date = "2020-02-29"

# Function to get header data...use multiselect for year
get_header_data = function(start_date, end_date) {
  qry = glue("select wr.wria_code, s.survey_id, wb.stream_catalog_code as cat_code, ",
             "wb.waterbody_display_name as stream_name, wb.latitude_longitude_id as llid, ",
             "se.survey_event_id, sd.survey_design_type_code as type, s.survey_datetime as survey_date, ",
             "sp.common_name as species, se.run_year, rn.run_short_description as run, ",
             "locl.river_mile_measure as rml, locu.river_mile_measure as rmu, ",
             "sm.survey_method_code as method, sf.flow_type_short_description as flow, ",
             "vt.visibility_type_short_description as rifflevis, s.observer_last_name as obs, ",
             "cs.completion_status_description as survey_status ",
             "from waterbody_lut as wb ",
             "inner join stream as st on wb.waterbody_id = st.waterbody_id ",
             "inner join wria_lut as wr on st_intersects(st.geom, wr.geom) ",
             "left join location as loc on wb.waterbody_id = loc.waterbody_id ",
             "left join survey as s on (loc.location_id = s.upper_end_point_id or ",
             "loc.location_id = s.lower_end_point_id) ",
             "left join survey_completion_status_lut as cs on s.survey_completion_status_id = cs.survey_completion_status_id ",
             "left join survey_event as se on s.survey_id = se.survey_id ",
             "left join species_lut as sp on se.species_id = sp.species_id ",
             "left join survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
             "left join run_lut as rn on se.run_id = rn.run_id ",
             "left join survey_design_type_lut as sd on se.survey_design_type_id = sd.survey_design_type_id ",
             "left join location as locu on s.upper_end_point_id = locu.location_id ",
             "left join location as locl on s.lower_end_point_id = locl.location_id ",
             "left join survey_comment as sc on s.survey_id = sc.survey_id ",
             "left join stream_flow_type_lut as sf on sc.stream_flow_type_id = sf.stream_flow_type_id ",
             "left join visibility_type_lut as vt on sc.visibility_type_id = vt.visibility_type_id ",
             "where survey_datetime between '{start_date}' and '{end_date}' ",
             "and wr.wria_code in ('22')")
  con = pg_con_local("spawning_ground")
  surveys = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  surveys = surveys %>%
    # HACK WARNING: TimeZone was screwed up !!!!!!!!! Using New York in profile !!!!!
    # as.Date uses local timezone so conversion should come out correctly
    # But may need to adjust after dates are corrected in DB !!!!!!!!!!!!!!!
    mutate(survey_date = as.Date(survey_date)) %>%
    mutate(survey_dt = survey_date) %>%
    mutate(statweek = remisc::fish_stat_week(survey_date, start_day = "Sun")) %>%
    mutate(survey_date = format(survey_date, "%m/%d/%Y")) %>%
    select(survey_id, survey_event_id, wria = cat_code, stream_name,
           llid, type, survey_date, statweek, species, run_year, run,
           rml, rmu, method, flow, rifflevis, obs, survey_status,
           survey_dt) %>%
    arrange(stream_name, survey_dt)
  return(surveys)
}

# Run the function: 10130 rows
header_data = get_header_data(start_date, end_date) %>%
  arrange(stream_name, rml, rmu, survey_dt)

# Identify fake surveys
fake_id = header_data %>%
  filter(obs == "Fake") %>%
  select(survey_id) %>%
  distinct() %>%
  pull(survey_id)

# Filter to real surveys...10124 left
header_data = header_data %>%
  filter(!survey_id %in% fake_id)

# Check
any(is.na(header_data$run_year))
any(is.na(header_data$species))

# Check:
table(header_data$type, useNA = "ifany")
table(header_data$run_year, useNA = "ifany")
table(header_data$run, useNA = "ifany")

#============================================================================
# Get intents for surveys above...can now use survey_event_id for filter
#============================================================================

# Function to get fish_counts
get_intent = function(header_data) {
  # Pull out the survey_event_ids
  se_id = header_data %>%
    filter(!is.na(survey_event_id)) %>%
    pull(survey_event_id)
  se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")
  # Query to pull intent and survey_type
  qry = glue("select si.survey_id, se.survey_event_id, sp.common_name as species, ",
             "ct.count_type_code as count_type, sd.survey_design_type_code as survey_type ",
             "from survey_intent as si ",
             "inner join survey as s on si.survey_id = s.survey_id ",
             "inner join survey_event as se on s.survey_id = se.survey_id ",
             "inner join survey_design_type_lut as sd on se.survey_design_type_id = sd.survey_design_type_id ",
             "left join species_lut as sp on si.species_id = sp.species_id ",
             "left join count_type_lut as ct on si.count_type_id = ct.count_type_id ",
             "where se.survey_event_id in ({se_ids})")
  con = pg_con_local("spawning_ground")
  count_intent = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  count_intent = count_intent %>%
    select(survey_id, survey_event_id, species, count_type, survey_type) %>%
    distinct()
  return(count_intent)
}

# Run the function: 58551 rows
count_intent = get_intent(header_data)

#============================================================================
# Filter surveys to those where counts for chinook were intended
#============================================================================

# Get chinook intent
chum_intent = count_intent %>%
  filter(species == selected_species)

# Pull out survey_ids
chum_survey_id = chum_intent %>%
  select(survey_id) %>%
  distinct() %>%
  pull(survey_id)

# Use species intent to filter header_data: 10014 rows left
header_data = header_data %>%
  filter(survey_id %in% chum_survey_id) %>%
  arrange(stream_name, type, survey_dt, run_year)

# Pull out full set of basic header data so surveys with no selected species are preserved for zero entries: 1254 rows
full_header = header_data %>%
  select(survey_id, wria, stream_name, llid, type, survey_date, statweek,
         rml, rmu, method, flow, rifflevis, obs, survey_status, survey_dt) %>%
  distinct() %>%
  arrange(stream_name, rml, rmu, survey_dt)

# Check: None, correct
any(duplicated(full_header$survey_id))
unique(header_data$species)

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
  qry = glue("select se.survey_id, se.survey_event_id, fe.fish_encounter_id, sp.common_name as species, ",
             "se.run_year, fs.fish_status_description as fish_status, se.comment_text as fish_comments, ",
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
    select(survey_id, survey_event_id, fish_encounter_id, species,
           run_year, fish_status, sex, maturity, clip_status,
           cwt_detect, fish_count, prev_count, fish_comments)
  return(fish_counts)
}

# Run the function: 1759 rows
fish_counts = get_fish_counts(header_data) %>%
  filter(species == selected_species)

#============================================================================
# Get all redd counts for surveys above
#============================================================================

# Function to get redd counts
get_redd_counts = function(header_data) {
  se_id = header_data %>%
    filter(!is.na(survey_event_id)) %>%
    pull(survey_event_id)
  se_ids = paste0(paste0("'", se_id, "'"), collapse = ", ")
  qry = glue("select se.survey_id, se.survey_event_id, rd.redd_encounter_id, sp.common_name as species, ",
             "se.run_year, rs.redd_status_short_description as redd_status, rd.redd_count ",
             "from survey_event as se ",
             "left join species_lut as sp on se.species_id = sp.species_id ",
             "left join redd_encounter as rd on se.survey_event_id = rd.survey_event_id ",
             "left join redd_status_lut as rs on rd.redd_status_id = rs.redd_status_id ",
             "where se.survey_event_id in ({se_ids})")
  con = pg_con_local("spawning_ground")
  redd_counts = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  redd_counts = redd_counts %>%
    select(survey_id, survey_event_id, redd_encounter_id, species,
           run_year, redd_status, redd_count)
  return(redd_counts)
}

# Run the function: 4040 rows
redd_counts = get_redd_counts(header_data) %>%
  filter(species == selected_species)

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
             "se.run_year, indf.cwt_snout_sample_number as cwt_head_label ",
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
    select(survey_id, survey_event_id, species, run_year,
           cwt_head_label)
  return(head_labels)
}

# Run the function: 3 rows .... all from fake data
head_labels = get_head_labels(header_data) %>%
  filter(species == selected_species)

#============================================================================
# Calculate fish counts by status, sex, and maturity
#============================================================================

# Cases occur with live detection method NA and dead detection method electronic
# So need to combine again by survey_id and species to avoid duplicate rows

# # Add some helper fields from header_data
# head_dat = header_data %>%
#   filter(!is.na(survey_event_id)) %>%
#   select(survey_id, survey_event_id, stream_name, survey_date,
#          species, run, run_year, type, rml, rmu, method, obs,
#          survey_dt) %>%
#   filter(species == selected_species) %>%
#   distinct()

# Add some helper fields from header_data
head_dat = header_data %>%
  filter(!is.na(survey_event_id)) %>%
  select(survey_id, stream_name, survey_date, species,
         run_year, type, rml, rmu, method, obs,
         survey_dt) %>%
  filter(species == selected_species) %>%
  distinct()

# Check
table(head_dat$run_year, useNA = "ifany")

# # HACK ALERT: Adjust run to most meaningful where more than one per se
# head_dat = head_dat %>%
#   mutate(run = if_else(run == "Not applicable", "xNot applicable", run)) %>%
#   arrange(stream_name, survey_date, rml, rmu, run) %>%
#   group_by(stream_name, survey_date, rml, rmu) %>%
#   mutate(n_seq = row_number()) %>%
#   ungroup()
#
# # Verify no spring run values will be lost: They will be, so only update NotApps
# chk_spring = head_dat %>%
#   filter(n_seq > 1L)
# table(chk_spring$run, useNA = "ifany")
#
# # Update run values
# head_dat = head_dat %>%
#   mutate(run = if_else(n_seq > 1L & run == "xNot applicable", "Unknown", run)) %>%
#   mutate(run = if_else(run == "xNot applicable", "Not applicable", run)) %>%
#   select(-n_seq) %>%
#   distinct()
#
# # Check
# table(head_dat$run, useNA = "ifany")

# Add to fish_counts
fish_dat = fish_counts %>%
  left_join(head_dat, by = c("survey_id", "species", "run_year")) %>%
  select(survey_id, survey_event_id, fish_encounter_id, stream_name,
         survey_date, type, rml, rmu, method, obs, species, run_year,
         fish_status, sex, maturity, clip_status, cwt_detect,
         fish_count, prev_count, fish_comments, survey_dt) %>%
  arrange(stream_name, survey_dt, rml, rmu)

# Calculate origin based on Kim's rules.....NEED TO VERIFY RUN YEAR FOR STEELHEAD...ALWAYS RUN_YEAR + 1 ????
unique(fish_dat$species)
unique(fish_dat$type)
fish_dat = fish_dat %>%
  mutate(origin = case_when(
    species == "Steelhead trout" &
      survey_dt < as.Date(glue("03/16/{selected_run_year}"), format = "%m/%d/%Y") &
      type == "Index" ~ "Hatchery",
    species == "Steelhead trout" &
      survey_dt >= as.Date(glue("03/16/{selected_run_year}"), format = "%m/%d/%Y") &
      type == "Index" ~ "Wild",
    species == "Steelhead trout" &
      type %in% c("Supp", "Explor") ~ "Unknown",
    !species == "Steelhead trout" ~ "Unknown"))

# Check
unique(fish_dat$origin)

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
  group_by(survey_id, type, species, run_year, origin) %>%
  mutate(lv_male_sum = sum(lv_male_count, na.rm = TRUE)) %>%
  mutate(lv_female_sum = sum(lv_female_count, na.rm = TRUE)) %>%
  mutate(lv_snd_sum = sum(lv_snd_count, na.rm = TRUE)) %>%
  mutate(lv_jack_sum = sum(lv_jack_count, na.rm = TRUE)) %>%
  mutate(dd_male_sum = sum(dd_male_count, na.rm = TRUE)) %>%
  mutate(dd_female_sum = sum(dd_female_count, na.rm = TRUE)) %>%
  mutate(dd_snd_sum = sum(dd_snd_count, na.rm = TRUE)) %>%
  mutate(dd_jack_sum = sum(dd_jack_count, na.rm = TRUE)) %>%
  ungroup() %>%
  select(survey_id, type, species, run_year, origin,
         lv_male_sum, lv_female_sum, lv_snd_sum, lv_jack_sum,
         dd_male_sum, dd_female_sum, dd_snd_sum, dd_jack_sum) %>%
  distinct()

# Add back to fish_dat...for inspection
base_sums = fish_dat %>%
  select(survey_id, species, run_year, origin,
         stream_name, survey_date, type, rml,
         rmu, method, obs) %>%
  left_join(fish_sums, by = c("survey_id", "type", "species",
                              "run_year", "origin")) %>%
  distinct()

#============================================================================
# Calculate fish counts by clip and cwt detection status
#============================================================================

# Compute sums for clip categories...only applies to live fish
fish_clip_sums = fish_dat %>%
  filter(!is.na(fish_encounter_id)) %>%
  filter(!prev_count == TRUE) %>%
  filter(!fish_status == "Live") %>%
  filter(maturity == "Adult") %>%
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
  group_by(survey_id, type, species, run_year, origin) %>%
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
  select(survey_id, species, type, species, run_year, origin,
         ADClippedBeep = clip_beep_sum, ADClippedNoBeep = clip_nobeep_sum,
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
  left_join(head_dat, by = c("survey_id", "species", "run_year")) %>%
  select(survey_id, survey_event_id, redd_encounter_id, stream_name, survey_date, type,
         rml, rmu, method, obs, species, run_year, redd_status, redd_count,
         survey_dt) %>%
  filter(!is.na(redd_encounter_id)) %>%
  arrange(stream_name, survey_dt, rml, rmu) %>%
  distinct()

# Calculate origin based on Kim's rules.....NEED TO VERIFY RUN YEAR FOR STEELHEAD...ALWAYS RUN_YEAR + 1 ????
# SHOULD DO THIS IN header_data !!!!!!!!!!!!
unique(redd_dat$species)
unique(redd_dat$type)
redd_dat = redd_dat %>%
  mutate(origin = case_when(
    species == "Steelhead trout" &
      survey_dt < as.Date(glue("03/16/{selected_run_year}"), format = "%m/%d/%Y") &
      type == "Index" ~ "Hatchery",
    species == "Steelhead trout" &
      survey_dt >= as.Date(glue("03/16/{selected_run_year}"), format = "%m/%d/%Y") &
      type == "Index" ~ "Wild",
    species == "Steelhead trout" &
      type %in% c("Supp", "Explor") ~ "Unknown",
    !species == "Steelhead trout" ~ "Unknown"))

# Check
unique(redd_dat$origin)

# Compute sums for redd categories.....Need to add in species and run...and origin? before computing totals
redd_sums = redd_dat %>%
  arrange(type, stream_name, rml, rmu, survey_dt) %>%
  mutate(reach = paste0(type, "_", stream_name, "_", rml, "_", rmu)) %>%
  mutate(rnew = if_else(redd_status == "New redd", redd_count, 0L)) %>%
  mutate(old_redds = if_else(redd_status == "Previous redd, still visible", redd_count, 0L)) %>%
  mutate(comb_redds = if_else(redd_status == "Combined visible redds", redd_count, NA_integer_)) %>%
  mutate(rcomb = comb_redds) %>%
  mutate(rvis = if_else(!is.na(comb_redds), rnew + old_redds + comb_redds, rnew + old_redds)) %>%
  mutate(rnew_comb = if_else(!is.na(comb_redds), rnew + comb_redds, rnew)) %>%
  group_by(survey_id, type, species, run_year, origin) %>%
  mutate(rnew_sum = sum(rnew, na.rm = TRUE)) %>%
  mutate(rvis_sum = sum(rvis, na.rm = TRUE)) %>%
  mutate(rcomb_sum = if_else(!is.na(rcomb), sum(rcomb, na.rm = TRUE), NA_integer_)) %>%
  mutate(rnew_comb_sum = sum(rnew_comb, na.rm = TRUE)) %>%
  ungroup() %>%
  select(survey_id, stream_name, survey_date, type, reach,
         rml, rmu, method, obs, species, run_year, origin, rnew_sum,
         rvis_sum, rcomb_sum, rnew_comb_sum) %>%
  distinct()

# Compute RCUM....Check later if Exploratory surveys should be cumsum same way as index
redd_sums = redd_sums %>%
  group_by(reach, type, species, run_year, origin) %>%
  mutate(rcum = if_else(type %in% c("Supp", "Explor"), cumsum(rnew_comb_sum), cumsum(rnew_sum))) %>%
  ungroup() %>%
  select(survey_id, stream_name, species, survey_date, type,
         reach, rml, rmu, method, obs, species, run_year, origin,
         rnew = rnew_sum, rvis = rvis_sum, rcum, rcomb = rcomb_sum)

# Add sort_order
redd_sums = redd_sums %>%
  # Temporarily change Explor to xplore to set at bottom in sort order
  mutate(type = if_else(type == "Explor", "xplor", type)) %>%
  arrange(type, stream_name, rml, rmu, as.Date(survey_date, format = "%m/%d/%Y")) %>%
  mutate(sort_order = seq(1, nrow(redd_sums))) %>%
  mutate(type = if_else(type == "xplor", "Explor", type)) %>%
  select(survey_id, stream_name, species, survey_date, type, reach,
         rml, rmu, method, obs, species, run_year, origin,
         rnew, rvis, rcum, rcomb, sort_order) %>%
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
if ( nrow(label_pivot) > 0) {
  label_concat = label_pivot %>%
  mutate(cwts = apply(label_pivot[,4:ncol(label_pivot)], 1, paste0, collapse = ", ")) %>%
  select(survey_id, species, cwts) %>%
  mutate(CWTHeadLabels = stri_replace_all_fixed(cwts, pattern = ", NA", replacement = "")) %>%
  select(survey_id, species, CWTHeadLabels)
} else {
  label_concat  = NULL
}

#============================================================================
# Combine and format to final table
#============================================================================

# Combine fish sums and redd sums
fr_sums = redd_sums %>%
  full_join(fish_sums, by = c("survey_id", "type", "species",
                              "run_year", "origin")) %>%
  select(survey_id, type, species, run_year, origin,
         LM = lv_male_sum, LF = lv_female_sum, LSND = lv_snd_sum,
         LJ = lv_jack_sum, DM = dd_male_sum, DF = dd_female_sum,
         DSND = dd_snd_sum, DJ = dd_jack_sum, RNEW = rnew,
         RVIS = rvis, RCUM = rcum, RCOMB = rcomb)

# Add clip sums to fr_sums
frc_sums = fr_sums %>%
  full_join(fish_clip_sums, by = c("survey_id", "type", "species",
                                   "run_year", "origin"))

# Pull out comments, pivot and concatenate
fish_comments = fish_dat %>%
  filter(!is.na(fish_comments)) %>%
  select(survey_id, species, Comments = fish_comments)

# Pivot
if ( nrow(fish_comments) > 0L ) {
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
} else {
  comment_concat = NULL
}

# Add comments to sums
if ( !is.null(comment_concat) ) {
  frc_sums = frc_sums %>%
    left_join(comment_concat, by = c("survey_id", "species"))
} else {
  frc_sums$comments = NA_character_
}

# Add cwt labels to sums
if ( !is.null(label_concat) ) {
  frc_sums = frc_sums %>%
    left_join(label_concat, by = c("survey_id", "species"))
} else {
  frc_sums$CWTHeadLabels = NA_character_
}

# Add remaining missing columns
head_fields = full_header %>%
  select(survey_id, WRIA = wria, StreamName = stream_name,
         LLID = llid, Type = type, Date = survey_date,
         survey_dt, StatWeek = statweek, Observer = obs,
         RML = rml, RMU = rmu, Method = method, Flow = flow,
         RiffleVis = rifflevis, SurveyStatus = survey_status) %>%
  distinct() %>%
  arrange(StreamName, RML, RMU, survey_dt)
any(duplicated(head_fields$survey_id))

# Pivot out count_types for intent
intent_pivot = count_intent %>%
  filter(species == selected_species) %>%
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
  mutate(drop_row = if_else(!is.na(Observer) & Observer == "Fake",
                            "drop", "keep")) %>%
  filter(drop_row == "keep") %>%
  distinct() %>%
  arrange(StreamName, RML, RMU, survey_dt)

# Set empty cells to zero
# names(all_surveys)
all_surveys[,21:41] = lapply(all_surveys[,21:41], set_na_zero)

# Recompute RCUM....Check later if Exploratory surveys should be cumsum same way as index
all_surveys = all_surveys %>%
  arrange(Type, StreamName, RML, RMU, survey_dt) %>%
  mutate(reach = paste0(Type, "_", StreamName, "_", RML, "_", RMU)) %>%
  group_by(reach, Type, species, run_year, origin) %>%
  # HACK ALERT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! VERIFY HERE !!!!!!!!!!!!!!!!!
  mutate(RCUM = if_else(Type %in% c("Supp", "Explor"), cumsum(RNEW), cumsum(RNEW))) %>%
  ungroup() %>%
  filter(survey_id %in% chum_survey_id) %>%
  arrange(Type, StreamName, RML, RMU, survey_dt)

# Format names
all_surveys = all_surveys %>%
  select(WRIA, StreamName, LLID, Type, Date, StatWeek,
         Observer, RML, RMU, Method, Flow, RiffleVis,
         Species = species, Origin = origin, RunYear = run_year,
         SurveyIntent, SurveyStatus, LM, LF, LSND, LJ, DM,
         DF, DSND, DJ, Comments = comments, survey_dt)

# Create a new reach field to enable pulling out empty row
#unique(all_surveys$StreamName)
#unique(all_surveys$WRIA)
all_surveys = all_surveys %>%
  mutate(Type = if_else(Type == "Explor", "xplor", Type)) %>%
  mutate(reach = paste0(Type, "_", WRIA, "_", StreamName, "_", RML, "_", RMU)) %>%
  mutate(nWRIA = as.numeric(substr(WRIA, 1, 7))) %>%
  mutate(aWRIA = if_else(nchar(WRIA) > 7, substr(WRIA, 8, 8), "A")) %>%
  arrange(Type, nWRIA, aWRIA, StreamName, RML, RMU, survey_dt) %>%
  mutate(sort_order = seq(1, nrow(all_surveys))) %>%
  mutate(Type = if_else(Type == "xplor", "Explor", Type)) %>%
  select(-survey_dt)

# Check run_year NAs for zero counts
chk_runyr = all_surveys %>%
  filter(is.na(RunYear))

# Pull out unique set of Reaches to add blank rows
dat_empty = all_surveys %>%
  mutate(sort_order = NA_integer_)
# names(dat_empty)
dat_empty[,1:26] = ""
dat_empty = dat_empty %>%
  distinct() %>%
  arrange(reach)

# Combine
dat = rbind(all_surveys, dat_empty) %>%
  arrange(reach, sort_order) %>%
  select(-c(reach, nWRIA, aWRIA, sort_order))

#============================================================================
# Output to Excel
#============================================================================

# # Write directly to excel
# out_name = paste0(run_year, "_", run, "_", "Chinook.xlsx")
# write.xlsx(dat, file = out_name, colNames = TRUE, sheetName = "Chinook")

# Or fancier with styling
out_name = paste0("data_query/ChumSurveys_WRIA_22.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "Sept-Feb_Surveys", gridLines = TRUE)
writeData(wb, sheet = 1, dat, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:ncol(dat), gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)




