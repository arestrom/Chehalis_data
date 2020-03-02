####################################################################
#Download Region 6 iForm db
###################################################################

# Load libraries
library(iformr)
library(glue)
library(odbc)
library(DBI)
pacman ::p_load(jsonlite, dplyr, RODBC, curl)

# Utility Clears the workspace
rm(list=ls(all=TRUE))

#Temporarily hardcode API keys so they are recognized
Sys.setenv(r6production_key = "37b9489a27608cd70c412a15a200b07530db55bd")
Sys.setenv(r6production_secret = "53e08f8e72cb473e03fcae8726fe0a8bab1c37c6")

# Set significant digitss to 14 for lat/lon under the options function
options(digits=14)

# Define the Path to Access Database where tables to update reside
# Test DB
# dbPath <- "C:\\data\\Database\\Access Development\\Projects\\In Progress\\20191112 Region 6 SGS help\\R6_SpawningGroundSurveys.accdb"

# Production db
 dbPath <- "S:\\Reg6\\FP\\Lea Ronne\\R6_Spawning_Ground_Surveys.accdb"

#Define funtion that loops trough form records in case greater than 1000 records
get_all_records = function(server_name, profile_id, page_id, fields = "fields", 
                           limit = 1000, user_offset, access_token, field_string, starting_id) {
  since_id = starting_id
  since_fields = paste0("id:<(>\"", since_id, "\"),", field_string)
  subform_records = get_selected_page_records(
    server_name = server_name,
    profile_id = profile_id,
    page_id = page_id,
    fields = since_fields,
    offset = user_offset,
    limit = 1000,
    access_token = access_token)
  all_records = subform_records
  while (nrow(subform_records) == 1000) {
    since_id = max(all_records$id)
    since_fields = paste0("id:<(>\"", since_id, "\"),", field_string)
    subform_records = get_selected_page_records(
      server_name = server_name,
      profile_id = profile_id,
      page_id = page_id,
      fields = since_fields,
      limit = 1000,
      access_token = access_token)
    all_records = rbind(all_records, subform_records)
  }
  all_records
}

# Generate API token. Should persist through session unless very slow.
# Loop through token process until valid token recieved
# Occasional 'Bad Request (HTTP 400)' error occurs randomly 
access_token = NULL
while (is.null(access_token)) {
  access_token = get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

#=================================================================
# r6_stream_surveys
#=================================================================

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


#=================================================================
# r6_stream_surveys_deads
#=================================================================

fields = glue::glue("id,parent_record_id,parent_page_id,parent_element_id,created_date,created_by,",
                    "created_location,created_device_id,modified_date,modified_by,modified_location,",
                    "modified_device_id,server_modified_date,species_fish,run_type,number_fish,clip_status,",
                    "fish_sex,carcass_condition,carc_tag_1,carc_tag_2,spawn_condition,length_measurement_cm,",
                    "cwt_detected,pittag_detected,pit_tag_number,cwt_label,dna_number,scale_card_number,",
                    "scale_card_position_number,encounter_comments,fish_condition,mark_status_1,mark_status_2,scan_cwt")
          
# Collapse vector of column names into a single string
form_fields <- paste(fields, collapse = ',')

# Pull data from iForm
r6_stream_surveys_deads = get_all_records("wdfw",417821,3363264,fields = "fields", limit = 1000, 0, access_token, form_fields,0)

#=================================================================
# r6_stream_surveys_lives
#=================================================================

fields = glue::glue("id,parent_record_id,parent_page_id,parent_element_id,created_date,created_by,created_location,",
                    "created_device_id,modified_date,modified_by,modified_location,modified_device_id,server_modified_date,",
                    "species_fish,run_type,live_type,unk_unknown,unknown_mark_unknown_sex_count,mark_sex_status_multiselect,",
                    "um_male,unmarked_male_count,um_female,unmarked_female_count,um_jack,unmarked_jack_count,um_unknown,",
                    "unmarked_unknown_sex_count,unk_male,unknown_mark_male_count,unk_female,unknown_mark_female_count,unk_jack,",
                    "unknown_mark_jack_count,ad_male,ad_male_count,ad_female,ad_female_count,ad_jack,ad_jack_count,ad_unknown,ad_mark_unknown_sex_count")

# Collapse vector of column names into a single string
form_fields <- paste(fields, collapse = ',')

# Pull data from iForm
r6_stream_surveys_lives = get_all_records("wdfw",417821,3363258,fields = "fields", limit = 1000, 0, access_token, form_fields,0)

#=================================================================
# r6_stream_surveys_other_observa
#=================================================================

fields = glue::glue("id,parent_record_id,parent_page_id,parent_element_id,created_date,created_by,created_location,",
                    "created_device_id,modified_date,modified_by,modified_location,modified_device_id,server_modified_date,",
                    "observation_type,miscellaneous_observation_name,known_total_barrier,barrier_descrip,barrier_ht,",
                    "barrier_ht_meas_est,pp_depth_of_barrier,barrier_pp_depth_meas_est,observation_details,observation_location,",
                    "observation_gps_warning_select,garmin_waypoint,garmin_waypoint_accuracy,gps_latitude,gps_longitude,",
                    "gps_accuracy,gps_elevation,comments,headerid_fkey,sgs_observationid,observation_row,observation_name,",
                    "poor_accuracy_counter,poor_accuracy_counter_2")

# Collapse vector of column names into a single string
form_fields <- paste(fields, collapse = ',')

# Pull data from iForm
r6_stream_surveys_other_observa = get_all_records("wdfw",417821,3363267,fields = "fields", limit = 1000, 0, access_token, form_fields,0)

#=================================================================
# r6_stream_surveys_other_pics
#=================================================================

fields = glue::glue("id,parent_record_id,parent_page_id,parent_element_id,created_date,created_by,created_location,",
                    "created_device_id,modified_date,modified_by,modified_location,modified_device_id,server_modified_date,",
                    "pics,comments")

# Collapse vector of column names into a single string
form_fields <- paste(fields, collapse = ',')

# Pull data from iForm
r6_stream_surveys_other_pics = get_all_records("wdfw",417821,3363270,fields = "fields", limit = 1000, 0, access_token, form_fields,0)

#=================================================================
# r6_stream_surveys_redds
#=================================================================

fields = glue::glue("id,parent_record_id,parent_page_id,parent_element_id,created_date,created_by,created_location,",
                    "created_device_id,modified_date,modified_by,modified_location,modified_device_id,server_modified_date,",
                    "redd_type,species_redd,redd_count,new_redd_name,transitory_redd_list,other_redd_name,previous_redd_name,",
                    "sgs_redd_name,redd_location,redd_loc_in_river,river_location_text,redd_length,redd_width,redd_degraded,",
                    "dewatered,dewater_text,si_redd,percent_si,si_by,si_text,sgs_redd_status,redd_latitude,redd_longitude,",
                    "fish_on_redd,live_unknown_mark_unknown_sex_count,live_unmarked_unknown_sex_count,status_marksex,",
                    "live_unmarked_female_count,live_unmarked_male_count,live_ad_clipped_female_count,live_ad_clipped_male_count,",
                    "live_ad_clipped_unknown_sex_count,live_unknown_mark_female_count,live_unknown_mark_male_count,",
                    "live_unmarked_jack_count,live_unknown_mark_jack_count,live_ad_clipped_jack_count,stat_week,species_code,",
                    "new_redd_count,redd_number_generator,sch_redd_passing,fch_redd_passing,coho_redd_passing,",
                    "steelhead_redd_passing,pacific_lamprey_redd_passing,wb_lamprey_redd_passing,encounter_comments,",
                    "prev_redd_grade,prev_redd_location,prev_redd_si,prev_redd_dewatered,prev_live_redd,prev_comments,",
                    "my_element_55,total_fish_on_redd,redd_status,redd_loc_accuracy,sgs_species,sgs_run,redd_orientation,",
                    "redd_channel_type,redd_time_stamp,prev_species_code,prev_si_by")

# Collapse vector of column names into a single string
form_fields <- paste(fields, collapse = ',')

# Pull data from iForm
r6_stream_surveys_redds = get_all_records("wdfw",417821,3363261,fields = "fields", limit = 1000, 0, access_token, form_fields,0)

#=================================================================
# r6_tagged_carcass_recoveries
#=================================================================

fields = glue::glue("id,parent_record_id,parent_page_id,parent_element_id,created_date,created_by,created_location,created_device_id,",
                    "modified_date,modified_by,modified_location,modified_device_id,server_modified_date,select_recovery_field,",
                    "recovery_tag_2,recovery_tag_1,recap_species")

# Collapse vector of column names into a single string
form_fields <- paste(fields, collapse = ',')

# Pull data from iForm
r6_tagged_carcass_recoveries = get_all_records("wdfw",417821,3363156,fields = "fields", limit = 1000, 0, access_token, form_fields,0)

#=================================================================
# Delete current contents of data tables in Access Database
#=================================================================
con <- odbcConnectAccess2007(dbPath)
sqlQuery(con, as.is = TRUE, glue::glue(paste("DELETE * FROM r6_stream_surveys;")))
sqlQuery(con, as.is = TRUE, glue::glue(paste("DELETE * FROM r6_stream_surveys_deads;")))
sqlQuery(con, as.is = TRUE, glue::glue(paste("DELETE * FROM r6_stream_surveys_lives;")))
sqlQuery(con, as.is = TRUE, glue::glue(paste("DELETE * FROM r6_stream_surveys_other_observa;")))
sqlQuery(con, as.is = TRUE, glue::glue(paste("DELETE * FROM r6_stream_surveys_other_pics;")))
sqlQuery(con, as.is = TRUE, glue::glue(paste("DELETE * FROM r6_stream_surveys_redds;")))
sqlQuery(con, as.is = TRUE, glue::glue(paste("DELETE * FROM r6_tagged_carcass_recoveries;")))
close(con)

#=================================================================
# Append or load new tables to Access Database
#=================================================================
con <- odbcConnectAccess2007(dbPath)
sqlSave(con, r6_stream_surveys,rownames = FALSE, append = TRUE)
sqlSave(con, r6_stream_surveys_deads,rownames = FALSE, append = TRUE)
sqlSave(con, r6_stream_surveys_lives,rownames = FALSE, append = TRUE)
sqlSave(con, r6_stream_surveys_other_observa,rownames = FALSE, append = TRUE)
sqlSave(con, r6_stream_surveys_other_pics,rownames = FALSE, append = TRUE)
sqlSave(con, r6_stream_surveys_redds,rownames = FALSE, append = TRUE)
sqlSave(con, r6_tagged_carcass_recoveries,rownames = FALSE, append = TRUE)
close(con)

# USe this to make a new table for testing
#con <- odbcConnectAccess2007(dbPath)
#sqlSave(con, r6_stream_surveys_redds, table = "r6_stream_surveys_redds_TEST",rownames = FALSE, append = FALSE)
#close(con)

#End of script
