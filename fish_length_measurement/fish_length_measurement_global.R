
# Main fish_encounter query
get_length_measurements = function(individual_fish_id) {
  qry = glue("select flm.fish_length_measurement_id, ",
             "mt.length_type_description as length_type, ",
             "flm.length_measurement_centimeter as length_cm, ",
             "datetime(flm.created_datetime, 'localtime') as created_date, flm.created_by, ",
             "datetime(flm.modified_datetime, 'localtime') as modified_date, flm.modified_by ",
             "from fish_length_measurement as flm ",
             "inner join fish_length_measurement_type_lut as mt on flm.fish_length_measurement_type_id = mt.fish_length_measurement_type_id ",
             "where flm.individual_fish_id = '{individual_fish_id}'")
  con = poolCheckout(pool)
  length_measurements = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  length_measurements = length_measurements %>%
    mutate(fish_length_measurement_id = tolower(fish_length_measurement_id)) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(fish_length_measurement_id, length_type, length_cm, created_date, created_dt, created_by,
           modified_date, modified_dt, modified_by) %>%
    arrange(created_date)
  return(length_measurements)
}

#==========================================================================
# Get generic lut input values
#==========================================================================

# Length type
get_length_type = function() {
  qry = glue("select fish_length_measurement_type_id, length_type_description as length_type ",
             "from fish_length_measurement_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  length_type_list = DBI::dbGetQuery(con, qry) %>%
    arrange(length_type) %>%
    select(fish_length_measurement_type_id, length_type)
  poolReturn(con)
  return(length_type_list)
}

#==========================================================================
# Validate create operations
#==========================================================================

# Check for existing duplicate survey_intent prior to survey_intent insert operation
dup_length_type = function(new_length_type_vals, existing_length_type_vals) {
  new_length_type_vals = new_length_type_vals %>%
    select(length_type)
  matching_rows = new_length_type_vals %>%
    select(length_type) %>%
    inner_join(existing_length_type_vals, by = "length_type")
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
length_measurement_insert = function(new_length_measurement_values) {
  new_insert_values = new_length_measurement_values
  fish_length_measurement_id = remisc::get_uuid(1L)
  # Pull out data
  individual_fish_id = new_insert_values$individual_fish_id
  fish_length_measurement_type_id = new_insert_values$fish_length_measurement_type_id
  length_measurement_centimeter =  new_insert_values$length_cm
  created_by = new_insert_values$created_by
  # Checkout a connection
  con = poolCheckout(pool)
  insert_result = dbSendStatement(
    con, glue_sql("INSERT INTO fish_length_measurement (",
                  "fish_length_measurement_id, ",
                  "individual_fish_id, ",
                  "fish_length_measurement_type_id, ",
                  "length_measurement_centimeter, ",
                  "created_by) ",
                  "VALUES (",
                  "?, ?, ?, ?, ?)"))
  dbBind(insert_result, list(fish_length_measurement_id, individual_fish_id, fish_length_measurement_type_id,
                             length_measurement_centimeter, created_by))
  dbGetRowsAffected(insert_result)
  dbClearResult(insert_result)
  poolReturn(con)
}

#========================================================
# Edit update callback
#========================================================

# Define update callback
length_measurement_update = function(length_measurement_edit_values) {
  edit_values = length_measurement_edit_values
  # Pull out data
  fish_length_measurement_id = edit_values$fish_length_measurement_id
  fish_length_measurement_type_id = edit_values$fish_length_measurement_type_id
  length_measurement_centimeter =  edit_values$length_cm
  mod_dt = format(lubridate::with_tz(Sys.time(), "UTC"))
  mod_by = Sys.getenv("USERNAME")
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE fish_length_measurement SET ",
                  "fish_length_measurement_type_id = ?, ",
                  "length_measurement_centimeter = ?, ",
                  "modified_datetime = ?, ",
                  "modified_by = ? ",
                  "where fish_length_measurement_id = ?"))
  dbBind(update_result, list(fish_length_measurement_type_id,
                             length_measurement_centimeter,
                             mod_dt, mod_by,
                             fish_length_measurement_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  poolReturn(con)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
length_measurement_delete = function(delete_values) {
  fish_length_measurement_id = delete_values$fish_length_measurement_id
  con = poolCheckout(pool)
  delete_result = dbSendStatement(
    con, glue_sql("DELETE FROM fish_length_measurement WHERE fish_length_measurement_id = ?"))
  dbBind(delete_result, list(fish_length_measurement_id))
  dbGetRowsAffected(delete_result)
  dbClearResult(delete_result)
  poolReturn(con)
}












