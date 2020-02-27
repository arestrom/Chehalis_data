# Main survey event query
get_survey_event = function(survey_id) {
  qry = glue("select se.survey_event_id, se.species_id, sp.common_name as species, ",
             "sd.survey_design_type_code as survey_design, ",
             "cw.detection_method_description as cwt_detect_method, ",
             "rn.run_short_description as run, se.run_year, ",
             "se.estimated_percent_fish_seen as pct_fish_seen, ",
             "se.comment_text as species_comment, ",
             "datetime(se.created_datetime, 'localtime') as created_date, se.created_by, ",
             "datetime(se.modified_datetime, 'localtime') as modified_date, se.modified_by ",
             "from survey_event as se ",
             "left join species_lut as sp on se.species_id = sp.species_id ",
             "left join survey_design_type_lut as sd on se.survey_design_type_id = sd.survey_design_type_id ",
             "left join cwt_detection_method_lut as cw on se.cwt_detection_method_id = cw.cwt_detection_method_id ",
             "left join run_lut as rn on se.run_id = rn.run_id ",
             "where se.survey_id = '{survey_id}'")
  con = poolCheckout(pool)
  survey_events = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  survey_events = survey_events %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(survey_event_id, species_id, species, survey_design, cwt_detect_method,
           run, run_year, pct_fish_seen, species_comment, created_date,
           created_dt, created_by, modified_date, modified_dt, modified_by) %>%
    arrange(created_date)
  return(survey_events)
}

#==========================================================================
# Get generic lut input values...data_source, etc.
#==========================================================================

# Species
get_event_species = function() {
  qry = glue("select species_id, common_name as species ",
             "from species_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  species_list = DBI::dbGetQuery(con, qry) %>%
    arrange(species) %>%
    select(species_id, species)
  poolReturn(con)
  return(species_list)
}

# Survey design
get_survey_design = function() {
  qry = glue("select survey_design_type_id, survey_design_type_code as survey_design ",
             "from survey_design_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  survey_design_list = DBI::dbGetQuery(con, qry) %>%
    arrange(survey_design) %>%
    select(survey_design_type_id, survey_design)
  poolReturn(con)
  return(survey_design_list)
}

# Survey design
get_cwt_detect_method = function() {
  qry = glue("select cwt_detection_method_id, detection_method_description as cwt_detect_method ",
             "from cwt_detection_method_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  cwt_detect_method_list = DBI::dbGetQuery(con, qry) %>%
    arrange(cwt_detect_method) %>%
    select(cwt_detection_method_id, cwt_detect_method)
  poolReturn(con)
  return(cwt_detect_method_list)
}

# Run type
get_run = function() {
  qry = glue("select run_id, run_short_description as run ",
             "from run_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  run_list = DBI::dbGetQuery(con, qry) %>%
    arrange(run) %>%
    select(run_id, run)
  poolReturn(con)
  return(run_list)
}

#==========================================================================
# Validate survey_event insert operations
#==========================================================================

# Check for existing duplicate survey_event prior to survey_event insert operation
dup_survey_event = function(new_survey_event_vals, existing_survey_event_vals) {
  new_survey_event_vals = new_survey_event_vals %>%
    select(species, survey_design, run, run_year)
  matching_rows = new_survey_event_vals %>%
    inner_join(existing_survey_event_vals,
               by = c("species", "survey_design", "run", "run_year"))
  if (nrow(matching_rows) > 0 ) {
    dup_flag = TRUE
  } else {
    dup_flag = FALSE
  }
  return(dup_flag)
}

#========================================================
# Insert callback
#========================================================

# Define the insert callback
survey_event_insert = function(new_event_values) {
  new_event_values = new_event_values
  survey_event_id = remisc::get_uuid(1L)
  # Pull out data
  survey_id = new_event_values$survey_id
  species_id = new_event_values$species_id
  survey_design_type_id =  new_event_values$survey_design_type_id
  cwt_detection_method_id = new_event_values$cwt_detection_method_id
  run_id = new_event_values$run_id
  run_year = new_event_values$run_year
  estimated_percent_fish_seen = new_event_values$pct_fish_seen
  comment_text = new_event_values$species_comment
  if (is.na(comment_text) | comment_text == "") { comment_text = NA }
  created_by = new_event_values$created_by
  # Checkout a connection
  con = poolCheckout(pool)
  insert_result = dbSendStatement(
    con, glue_sql("INSERT INTO survey_event (",
                  "survey_event_id, ",
                  "survey_id, ",
                  "species_id, ",
                  "survey_design_type_id, ",
                  "cwt_detection_method_id, ",
                  "run_id, ",
                  "run_year, ",
                  "estimated_percent_fish_seen, ",
                  "comment_text, ",
                  "created_by) ",
                  "VALUES (",
                  "?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
  dbBind(insert_result, list(survey_event_id, survey_id, species_id, survey_design_type_id,
                             cwt_detection_method_id, run_id, run_year,
                             estimated_percent_fish_seen, comment_text,
                             created_by))
  dbGetRowsAffected(insert_result)
  dbClearResult(insert_result)
  poolReturn(con)
}

#========================================================
# Edit update callback
#========================================================

# Define update callback
survey_event_update = function(survey_event_edit_values) {
  edit_values = survey_event_edit_values
  # Pull out data
  survey_event_id = edit_values$survey_event_id
  species_id = edit_values$species_id
  survey_design_type_id =  edit_values$survey_design_type_id
  cwt_detection_method_id = edit_values$cwt_detection_method_id
  run_id = edit_values$run_id
  run_year = edit_values$run_year
  estimated_percent_fish_seen = edit_values$pct_fish_seen
  comment_text = edit_values$species_comment
  if (is.na(comment_text) | comment_text == "") { comment_text = NA }
  mod_dt = format(lubridate::with_tz(Sys.time(), "UTC"))
  mod_by = Sys.getenv("USERNAME")
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE survey_event SET ",
                  "species_id = ?, ",
                  "survey_design_type_id = ?, ",
                  "cwt_detection_method_id = ?, ",
                  "run_id = ?, ",
                  "run_year = ?, ",
                  "estimated_percent_fish_seen = ?, ",
                  "comment_text = ?, ",
                  "modified_datetime = ?, ",
                  "modified_by = ? ",
                  "where survey_event_id = ?"))
  dbBind(update_result, list(species_id, survey_design_type_id,
                             cwt_detection_method_id, run_id, run_year,
                             estimated_percent_fish_seen, comment_text,
                             mod_dt, mod_by, survey_event_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  poolReturn(con)
}

#========================================================
# Identify species dependencies prior to delete
#========================================================

# Identify survey dependencies prior to delete....do the same for survey_event
get_survey_event_dependencies = function(survey_event_id) {
  qry = glue("select ",
             "count(fe.fish_encounter_id) as fish_encounter, ",
             "count(rd.redd_encounter_id) as redd_encounter ",
             "from survey_event as se ",
             "left join fish_encounter as fe on se.survey_event_id = fe.survey_event_id ",
             "left join redd_encounter as rd on se.survey_event_id = rd.survey_event_id ",
             "where se.survey_event_id = '{survey_event_id}'")
  con = poolCheckout(pool)
  survey_event_dependents = DBI::dbGetQuery(pool, qry)
  poolReturn(con)
  has_entries = function(x) any(x > 0L)
  survey_event_dependents = survey_event_dependents %>%
    select_if(has_entries)
  return(survey_event_dependents)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
survey_event_delete = function(delete_values) {
  survey_event_id = delete_values$survey_event_id
  con = poolCheckout(pool)
  delete_result = dbSendStatement(
    con, glue_sql("DELETE FROM survey_event WHERE survey_event_id = ?"))
  dbBind(delete_result, list(survey_event_id))
  dbGetRowsAffected(delete_result)
  dbClearResult(delete_result)
  poolReturn(con)
}












