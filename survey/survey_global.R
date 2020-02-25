# Function to get header data...use multiselect for year
get_surveys = function(waterbody_id, survey_year) {
  qry = glue("select s.survey_id, s.survey_datetime as survey_date, ",
             "ds.data_source_code, ",
             "data_source_code || ': ' || data_source_name as data_source, ",
             "du.data_source_unit_name as data_source_unit, ",
             "sm.survey_method_code as survey_method, ",
             "dr.data_review_status_description as data_review, ",
             "locu.river_mile_measure as upper_rm, ",
             "locl.river_mile_measure as lower_rm, ",
             "locu.location_id as upper_location_id, ",
             "locl.location_id as lower_location_id, ",
             "sct.completion_status_description as completion, ",
             "ics.incomplete_survey_description as incomplete_type, ",
             "s.survey_start_datetime as start_time, ",
             "s.survey_end_datetime as end_time, ",
             "s.observer_last_name as observer, ",
             "s.data_submitter_last_name as submitter, ",
             "s.created_datetime as created_date, ",
             "s.created_by, s.modified_datetime as modified_date, ",
             "s.modified_by ",
             "from survey as s ",
             "inner join data_source_lut as ds on s.data_source_id = ds.data_source_id ",
             "inner join data_source_unit_lut as du on s.data_source_unit_id = du.data_source_unit_id ",
             "inner join survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
             "inner join data_review_status_lut as dr on s.data_review_status_id = dr.data_review_status_id ",
             "inner join location as locu on s.upper_end_point_id = locu.location_id ",
             "inner join location as locl on s.lower_end_point_id = locl.location_id ",
             "left join survey_completion_status_lut as sct on s.survey_completion_status_id = sct.survey_completion_status_id ",
             "inner join incomplete_survey_type_lut as ics on s.incomplete_survey_type_id = ics.incomplete_survey_type_id ",
             "where strftime('%Y', s.survey_datetime) = '{survey_year}' ",
             "and (locu.waterbody_id = '{waterbody_id}' or locl.waterbody_id = '{waterbody_id}')")
  con = poolCheckout(pool)
  surveys = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  surveys = surveys %>%
    mutate(survey_date = with_tz(as.POSIXct(survey_date), tzone = "America/Los_Angeles")) %>%
    mutate(survey_date_dt = format(survey_date, "%m/%d/%Y")) %>%
    mutate(start_time = with_tz(as.POSIXct(start_time), tzone = "America/Los_Angeles")) %>%
    mutate(start_time_dt = format(start_time, "%H:%M")) %>%
    mutate(end_time = with_tz(as.POSIXct(end_time), tzone = "America/Los_Angeles")) %>%
    mutate(end_time_dt = format(end_time, "%H:%M")) %>%
    mutate(created_date = with_tz(as.POSIXct(created_date), tzone = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = with_tz(as.POSIXct(modified_date), tzone = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    mutate(survey_date = as.Date(survey_date)) %>%
    select(survey_id, survey_date, survey_date_dt, survey_method, up_rm = upper_rm,
           lo_rm = lower_rm, start_time, start_time_dt, end_time, end_time_dt,
           observer, submitter, data_source_code, data_source, data_source_unit,
           data_review, completion, incomplete_type, created_date, created_dt,
           created_by, modified_date, modified_dt, modified_by) %>%
    arrange(survey_date, start_time, end_time, created_date)
  return(surveys)
}

#
#
# get_surveys = function(waterbody_id, survey_years) {
#   qry = glue("select s.survey_id, s.survey_datetime as survey_date, ",
#              "ds.data_source_code, ",
#              "data_source_code || ': ' || data_source_name as data_source, ",
#              "du.data_source_unit_name as data_source_unit, ",
#              "sm.survey_method_code as survey_method, ",
#              "dr.data_review_status_description as data_review, ",
#              "locu.river_mile_measure as upper_rm, ",
#              "locl.river_mile_measure as lower_rm, ",
#              "locu.location_id as upper_location_id, ",
#              "locl.location_id as lower_location_id, ",
#              "sct.completion_status_description as completion, ",
#              "ics.incomplete_survey_description as incomplete_type, ",
#              "s.survey_start_datetime as start_time, ",
#              "s.survey_end_datetime as end_time, ",
#              "s.observer_last_name as observer, ",
#              "s.data_submitter_last_name as submitter, ",
#              "s.created_datetime as created_date, ",
#              "s.created_by, s.modified_datetime as modified_date, ",
#              "s.modified_by ",
#              "from survey as s ",
#              "inner join data_source_lut as ds on s.data_source_id = ds.data_source_id ",
#              "inner join data_source_unit_lut as du on s.data_source_unit_id = du.data_source_unit_id ",
#              "inner join survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
#              "inner join data_review_status_lut as dr on s.data_review_status_id = dr.data_review_status_id ",
#              "inner join location as locu on s.upper_end_point_id = locu.location_id ",
#              "inner join location as locl on s.lower_end_point_id = locl.location_id ",
#              "left join survey_completion_status_lut as sct on s.survey_completion_status_id = sct.survey_completion_status_id ",
#              "inner join incomplete_survey_type_lut as ics on s.incomplete_survey_type_id = ics.incomplete_survey_type_id ",
#              "where strftime('%Y', s.survey_datetime) in ({survey_years}) ",
#              "and (locu.waterbody_id = '{waterbody_id}' or locl.waterbody_id = '{waterbody_id}')")
#   con = poolCheckout(pool)
#   surveys = DBI::dbGetQuery(con, qry)
#   poolReturn(con)
#   surveys = surveys %>%
#     mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
#     mutate(survey_date_dt = format(survey_date, "%m/%d/%Y")) %>%
#     mutate(start_time = with_tz(start_time, tzone = "America/Los_Angeles")) %>%
#     mutate(start_time_dt = format(start_time, "%H:%M")) %>%
#     mutate(end_time = with_tz(end_time, tzone = "America/Los_Angeles")) %>%
#     mutate(end_time_dt = format(end_time, "%H:%M")) %>%
#     mutate(created_date = with_tz(created_date, tzone = "America/Los_Angeles")) %>%
#     mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
#     mutate(modified_date = with_tz(modified_date, tzone = "America/Los_Angeles")) %>%
#     mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
#     mutate(survey_date = as.Date(survey_date)) %>%
#     select(survey_id, survey_date, survey_date_dt, survey_method, up_rm = upper_rm,
#            lo_rm = lower_rm, start_time, start_time_dt, end_time, end_time_dt,
#            observer, submitter, data_source_code, data_source, data_source_unit,
#            data_review, completion, incomplete_type, created_date, created_dt,
#            created_by, modified_date, modified_dt, modified_by) %>%
#     arrange(survey_date, start_time, end_time, created_date)
#   return(surveys)
# }

#==========================================================================
# Get generic lut input values...data_source, etc.
#==========================================================================

# Data source
get_data_source = function() {
  qry = glue("select data_source_id, data_source_code || ': ' || data_source_name as data_source ",
             "from data_source_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  data_source_list = DBI::dbGetQuery(con, qry) %>%
    arrange(data_source) %>%
    select(data_source_id, data_source)
  poolReturn(con)
  return(data_source_list)
}

# Data source unit
get_data_source_unit = function() {
  qry = glue("select data_source_unit_id, data_source_unit_name as data_source_unit ",
             "from data_source_unit_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  data_source_unit_list = DBI::dbGetQuery(con, qry) %>%
    arrange(data_source_unit) %>%
    select(data_source_unit_id, data_source_unit)
  poolReturn(con)
  return(data_source_unit_list)
}

# Survey method
get_survey_method = function() {
  qry = glue("select survey_method_id, survey_method_code as survey_method ",
             "from survey_method_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  survey_method_list = DBI::dbGetQuery(con, qry) %>%
    arrange(survey_method) %>%
    select(survey_method_id, survey_method)
  poolReturn(con)
  return(survey_method_list)
}

# Data review
get_data_review = function() {
  qry = glue("select data_review_status_id, data_review_status_description as data_review ",
             "from data_review_status_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  data_review_list = DBI::dbGetQuery(con, qry) %>%
    arrange(data_review) %>%
    select(data_review_status_id, data_review)
  poolReturn(con)
  return(data_review_list)
}

# Completion status
get_completion_status = function() {
  qry = glue("select survey_completion_status_id, completion_status_description as completion ",
             "from survey_completion_status_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  completion_list = DBI::dbGetQuery(con, qry) %>%
    arrange(completion) %>%
    select(survey_completion_status_id, completion)
  poolReturn(con)
  return(completion_list)
}

# Incomplete type
get_incomplete_type = function() {
  qry = glue("select incomplete_survey_type_id, incomplete_survey_description as incomplete_type ",
             "from incomplete_survey_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  incomplete_type_list = DBI::dbGetQuery(con, qry) %>%
    arrange(incomplete_type) %>%
    select(incomplete_survey_type_id, incomplete_type)
  poolReturn(con)
  return(incomplete_type_list)
}

#==========================================================================
# Validate survey create operations
#==========================================================================

# Check for existing surveys prior to survey insert operation
dup_survey = function(new_survey_vals, existing_survey_vals) {
  new_survey_vals = new_survey_vals %>%
    select(survey_dt, survey_method, up_rm, lo_rm, observer, data_source)
  matching_rows = new_survey_vals %>%
    inner_join(existing_survey_vals,
               by = c("survey_dt", "survey_method", "up_rm", "lo_rm",
                      "observer", "data_source"))
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
survey_insert = function(new_values) {
  # Pull out data
  survey_datetime = new_values$survey_dt
  data_source_id = new_values$data_source_id
  data_source_unit_id = new_values$data_source_unit_id
  survey_method_id = new_values$survey_method_id
  data_review_status_id = new_values$data_review_status_id
  upper_end_point_id = new_values$upper_end_point_id
  lower_end_point_id = new_values$lower_end_point_id
  survey_completion_status_id = new_values$survey_completion_status_id
  incomplete_survey_type_id = new_values$incomplete_survey_type_id
  survey_start_datetime = new_values$survey_start_datetime
  survey_end_datetime = new_values$survey_end_datetime
  observer_last_name = new_values$observer_last_name
  if (is.na(observer_last_name) | observer_last_name == "") { observer_last_name = NA }
  data_submitter_last_name = new_values$data_submitter_last_name
  if (is.na(data_submitter_last_name) | data_submitter_last_name == "") { data_submitter_last_name = NA }
  created_by = new_values$created_by

  # Checkout a connection
  con = poolCheckout(pool)
  insert_result = dbSendStatement(
    con, glue_sql("INSERT INTO survey (",
                  "survey_datetime, ",
                  "data_source_id, ",
                  "data_source_unit_id, ",
                  "survey_method_id, ",
                  "data_review_status_id, ",
                  "upper_end_point_id, ",
                  "lower_end_point_id, ",
                  "survey_completion_status_id, ",
                  "incomplete_survey_type_id, ",
                  "survey_start_datetime, ",
                  "survey_end_datetime, ",
                  "observer_last_name, ",
                  "data_submitter_last_name, ",
                  "created_by) ",
                  "VALUES (",
                  "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"))
  dbBind(insert_result, list(survey_datetime, data_source_id, data_source_unit_id,
                             survey_method_id, data_review_status_id, upper_end_point_id,
                             lower_end_point_id, survey_completion_status_id,
                             incomplete_survey_type_id, survey_start_datetime,
                             survey_end_datetime, observer_last_name,
                             data_submitter_last_name, created_by))
  dbGetRowsAffected(insert_result)
  dbClearResult(insert_result)
  poolReturn(con)
}

#========================================================
# Edit update callback
#========================================================

# Define update callback
survey_update = function(edit_values) {
  # Pull out data
  survey_datetime = edit_values$survey_datetime
  data_source_id = edit_values$data_source_id
  data_source_unit_id = edit_values$data_source_unit_id
  survey_method_id = edit_values$survey_method_id
  data_review_status_id = edit_values$data_review_status_id
  upper_end_point_id = edit_values$upper_end_point_id
  lower_end_point_id = edit_values$lower_end_point_id
  survey_completion_status_id = edit_values$survey_completion_status_id
  incomplete_survey_type_id = edit_values$incomplete_survey_type_id
  survey_start_datetime = edit_values$survey_start_datetime
  survey_end_datetime = edit_values$survey_end_datetime
  observer_last_name = edit_values$observer
  if (is.na(observer_last_name) | observer_last_name == "") { observer_last_name = NA }
  data_submitter_last_name = edit_values$submitter
  if (is.na(data_submitter_last_name) | data_submitter_last_name == "") { data_submitter_last_name = NA }
  mod_dt = lubridate::with_tz(Sys.time(), "UTC")
  mod_by = Sys.getenv("USERNAME")
  survey_id = edit_values$survey_id
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE survey SET ",
                  "survey_datetime = $1, ",
                  "data_source_id = $2, ",
                  "data_source_unit_id = $3, ",
                  "survey_method_id = $4, ",
                  "data_review_status_id = $5, ",
                  "upper_end_point_id = $6, ",
                  "lower_end_point_id = $7, ",
                  "survey_completion_status_id = $8, ",
                  "incomplete_survey_type_id = $9, ",
                  "survey_start_datetime = $10, ",
                  "survey_end_datetime = $11, ",
                  "observer_last_name = $12, ",
                  "data_submitter_last_name = $13, ",
                  "modified_datetime = $14, ",
                  "modified_by = $15 ",
                  "where survey_id = $16"))
  dbBind(update_result, list(survey_datetime, data_source_id, data_source_unit_id,
                             survey_method_id, data_review_status_id, upper_end_point_id,
                             lower_end_point_id, survey_completion_status_id,
                             incomplete_survey_type_id, survey_start_datetime,
                             survey_end_datetime, observer_last_name,
                             data_submitter_last_name, mod_dt, mod_by, survey_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  poolReturn(con)
}

#========================================================
# Identify dependencies prior to delete
#========================================================

# Identify survey dependencies prior to delete....do the same for survey_event
get_survey_dependencies = function(survey_id) {
  qry = glue("select count(fb.fish_barrier_id) as fish_barrier, ",
             "count(fc.fish_capture_id) as fish_capture, ",
             "count(fs.fish_species_presence_id) as fish_species_presence, ",
             "count(ms.mobile_survey_form_id) as mobile_survey_form, ",
             "count(ob.other_observation_id) as other_observation, ",
             "count(sc.survey_comment_id) as survey_comment, ",
             "count(se.survey_event_id) as survey_event, ",
             "count(si.survey_intent_id) as survey_intent, ",
             "count(wm.waterbody_measurement_id) as waterbody_measurement ",
             "from survey as s ",
             "left join fish_barrier as fb on s.survey_id = fb.survey_id ",
             "left join fish_capture as fc on s.survey_id = fc.survey_id ",
             "left join fish_species_presence as fs on s.survey_id = fs.survey_id ",
             "left join mobile_survey_form as ms on s.survey_id = ms.survey_id ",
             "left join other_observation as ob on s.survey_id = ob.survey_id ",
             "left join survey_comment as sc on s.survey_id = sc.survey_id ",
             "left join survey_event as se on s.survey_id = se.survey_id ",
             "left join survey_intent as si on s.survey_id = si.survey_id ",
             "left join waterbody_measurement as wm on s.survey_id = si.survey_id ",
             "where s.survey_id = '{survey_id}'")
  con = poolCheckout(pool)
  survey_dependents = DBI::dbGetQuery(pool, qry)
  has_entries = function(x) any(x > 0L)
  survey_dependents = survey_dependents %>%
    select_if(has_entries)
  return(survey_dependents)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
survey_delete = function(delete_values) {
  survey_id = delete_values$survey_id
  con = poolCheckout(pool)
  delete_result = dbSendStatement(
    con, glue_sql("DELETE FROM survey WHERE survey_id = $1"))
  dbBind(delete_result, list(survey_id))
  dbGetRowsAffected(delete_result)
  dbClearResult(delete_result)
  poolReturn(con)
}

