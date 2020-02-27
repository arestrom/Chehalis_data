
# Main redd_encounter query
get_redd_encounter = function(survey_event_id) {
  qry = glue("select rd.redd_encounter_id, datetime(rd.redd_encounter_datetime, 'localtime') as redd_encounter_time, ",
             "rd.redd_count, rs.redd_status_short_description as redd_status, ",
             "rd.redd_location_id, loc.location_name as redd_name, rd.comment_text as redd_comment, ",
             "datetime(rd.created_datetime, 'localtime') as created_date, rd.created_by, ",
             "datetime(rd.modified_datetime, 'localtime') as modified_date, rd.modified_by ",
             "from redd_encounter as rd ",
             "inner join redd_status_lut as rs on rd.redd_status_id = rs.redd_status_id ",
             "left join location as loc on rd.redd_location_id = loc.location_id ",
             "where rd.survey_event_id = '{survey_event_id}'")
  con = poolCheckout(pool)
  redd_encounters = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  redd_encounters = redd_encounters %>%
    mutate(redd_encounter_time = as.POSIXct(redd_encounter_time, tz = "America/Los_Angeles")) %>%
    mutate(redd_encounter_dt = format(redd_encounter_time, "%H:%M")) %>%
    mutate(redd_status = if_else(redd_status == "Combined visible redds",
                                 "Visible new and old redds combined", redd_status)) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(redd_encounter_id, redd_encounter_time, redd_encounter_dt, redd_count,
           redd_status, redd_location_id, redd_name, redd_comment, created_date,
           created_dt, created_by, modified_date, modified_dt, modified_by) %>%
    arrange(created_date)
  return(redd_encounters)
}

#==========================================================================
# Get generic lut input values
#==========================================================================

# Redd status
get_redd_status = function() {
  qry = glue("select redd_status_id, redd_status_short_description as redd_status ",
             "from redd_status_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  redd_status_list = DBI::dbGetQuery(con, qry) %>%
    mutate(redd_status = if_else(redd_status == "Combined visible redds",
                                 "Visible new and old redds combined", redd_status)) %>%
    arrange(redd_status) %>%
    select(redd_status_id, redd_status)
  poolReturn(con)
  return(redd_status_list)
}

#========================================================
# Insert callback
#========================================================

# Define the insert callback
redd_encounter_insert = function(new_redd_encounter_values) {
  new_insert_values = new_redd_encounter_values
  redd_encounter_id = remisc::get_uuid(1L)
  # Pull out data
  survey_event_id = new_insert_values$survey_event_id
  redd_location_id = new_insert_values$redd_location_id
  redd_status_id = new_insert_values$redd_status_id
  redd_encounter_datetime = format(new_insert_values$redd_encounter_datetime)
  redd_count = new_insert_values$redd_count
  comment_text = new_insert_values$comment_text
  if (is.na(comment_text) | comment_text == "") { comment_text = NA }
  created_by = new_insert_values$created_by
  # Checkout a connection
  con = poolCheckout(pool)
  insert_result = dbSendStatement(
    con, glue_sql("INSERT INTO redd_encounter (",
                  "redd_encounter_id, ",
                  "survey_event_id, ",
                  "redd_location_id, ",
                  "redd_status_id, ",
                  "redd_encounter_datetime, ",
                  "redd_count, ",
                  "comment_text, ",
                  "created_by) ",
                  "VALUES (",
                  "?, ?, ?, ?, ?, ?, ?, ?)"))
  dbBind(insert_result, list(redd_encounter_id, survey_event_id, redd_location_id,
                             redd_status_id, redd_encounter_datetime,
                             redd_count, comment_text, created_by))
  dbGetRowsAffected(insert_result)
  dbClearResult(insert_result)
  poolReturn(con)
}

#========================================================
# Edit update callback
#========================================================

# Define update callback
redd_encounter_update = function(redd_encounter_edit_values) {
  edit_values = redd_encounter_edit_values
  # Pull out data
  redd_encounter_id = edit_values$redd_encounter_id
  redd_location_id = edit_values$redd_location_id
  redd_status_id = edit_values$redd_status_id
  redd_encounter_datetime = format(edit_values$redd_encounter_time)
  redd_count = edit_values$redd_count
  comment_text = edit_values$redd_comment
  if (is.na(comment_text) | comment_text == "") { comment_text = NA }
  mod_dt = format(lubridate::with_tz(Sys.time(), "UTC"))
  mod_by = Sys.getenv("USERNAME")
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE redd_encounter SET ",
                  "redd_location_id = ?, ",
                  "redd_status_id = ?, ",
                  "redd_encounter_datetime = ?, ",
                  "redd_count = ?, ",
                  "comment_text = ?, ",
                  "modified_datetime = ?, ",
                  "modified_by = ? ",
                  "where redd_encounter_id = ?"))
  dbBind(update_result, list(redd_location_id, redd_status_id,
                             redd_encounter_datetime, redd_count,
                             comment_text, mod_dt, mod_by,
                             redd_encounter_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  poolReturn(con)
}

#========================================================
# Identify redd encounter dependencies prior to delete
#========================================================

# Identify fish_encounter dependencies prior to delete
get_redd_encounter_dependencies = function(redd_encounter_id) {
  qry = glue("select ",
             "count(ir.individual_redd_id) as individual_redd, ",
             "count(rc.redd_confidence_id) as redd_confidence, ",
             "count(rs.redd_substrate_id) as redd_substrate ",
             "from redd_encounter as rd ",
             "left join individual_redd as ir on rd.redd_encounter_id = ir.redd_encounter_id ",
             "left join redd_confidence as rc on rd.redd_encounter_id = rc.redd_encounter_id ",
             "left join redd_substrate as rs on rd.redd_encounter_id = rs.redd_encounter_id ",
             "where rd.redd_encounter_id = '{redd_encounter_id}'")
  con = poolCheckout(pool)
  redd_encounter_dependents = DBI::dbGetQuery(pool, qry)
  poolReturn(con)
  has_entries = function(x) any(x > 0L)
  redd_encounter_dependents = redd_encounter_dependents %>%
    select_if(has_entries)
  return(redd_encounter_dependents)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
redd_encounter_delete = function(delete_values) {
  redd_encounter_id = delete_values$redd_encounter_id
  con = poolCheckout(pool)
  delete_result = dbSendStatement(
    con, glue_sql("DELETE FROM redd_encounter WHERE redd_encounter_id = ?"))
  dbBind(delete_result, list(redd_encounter_id))
  dbGetRowsAffected(delete_result)
  dbClearResult(delete_result)
  poolReturn(con)
}












