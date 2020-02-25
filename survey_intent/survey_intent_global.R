# Main survey intent query
get_survey_intent = function(survey_id) {
  qry = glue("select si.survey_intent_id, sp.common_name as species, ",
             "ct.count_type_code as count_type, si.created_datetime as created_date, ",
             "si.created_by, si.modified_datetime as modified_date, si.modified_by ",
             "from survey_intent as si ",
             "left join species_lut as sp on si.species_id = sp.species_id ",
             "left join count_type_lut as ct on si.count_type_id = ct.count_type_id ",
             "where si.survey_id = '{survey_id}'")
  con = poolCheckout(pool)
  survey_intents = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  survey_intents = survey_intents %>%
    mutate(created_date = with_tz(created_date, tzone = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = with_tz(modified_date, tzone = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(survey_intent_id, species, count_type, created_date, created_dt,
           created_by, modified_date, modified_dt, modified_by) %>%
    arrange(created_date)
  return(survey_intents)
}

#==========================================================================
# Get generic lut input values...data_source, etc.
#==========================================================================

# Area surveyed
get_intent_species = function() {
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

# Abundance
get_count_type = function() {
  qry = glue("select count_type_id, count_type_code as count_type ",
             "from count_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  count_type_list = DBI::dbGetQuery(con, qry) %>%
    arrange(count_type) %>%
    select(count_type_id, count_type)
  poolReturn(con)
  return(count_type_list)
}

#==========================================================================
# Validate survey_intent create operations
#==========================================================================

# Check for existing duplicate survey_intent prior to survey_intent insert operation
dup_survey_intent = function(new_survey_intent_vals, existing_survey_intent_vals) {
  new_survey_intent_vals = new_survey_intent_vals %>%
    select(species, count_type)
  matching_rows = new_survey_intent_vals %>%
    inner_join(existing_survey_intent_vals,
               by = c("species", "count_type"))
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
survey_intent_insert = function(new_intent_values) {
  new_intent_values = new_intent_values
  # Pull out data
  survey_id = new_intent_values$survey_id
  species_id = new_intent_values$species_id
  count_type_id =  new_intent_values$count_type_id
  created_by = new_intent_values$created_by
  # Checkout a connection
  con = poolCheckout(pool)
  insert_result = dbSendStatement(
    con, glue_sql("INSERT INTO survey_intent (",
                  "survey_id, ",
                  "species_id, ",
                  "count_type_id, ",
                  "created_by) ",
                  "VALUES (",
                  "$1, $2, $3, $4)"))
  dbBind(insert_result, list(survey_id, species_id, count_type_id, created_by))
  dbGetRowsAffected(insert_result)
  dbClearResult(insert_result)
  poolReturn(con)
}

#========================================================
# Edit update callback
#========================================================

# Define update callback
survey_intent_update = function(intent_edit_values) {
  edit_values = intent_edit_values
  # Pull out data
  survey_intent_id = edit_values$survey_intent_id
  species_id = edit_values$species_id
  count_type_id = edit_values$count_type_id
  mod_dt = lubridate::with_tz(Sys.time(), "UTC")
  mod_by = Sys.getenv("USERNAME")
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE survey_intent SET ",
                  "species_id = $1, ",
                  "count_type_id = $2, ",
                  "modified_datetime = $3, ",
                  "modified_by = $4 ",
                  "where survey_intent_id = $5"))
  dbBind(update_result, list(species_id, count_type_id, mod_dt, mod_by,
                             survey_intent_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  poolReturn(con)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
survey_intent_delete = function(delete_values) {
  survey_intent_id = delete_values$survey_intent_id
  con = poolCheckout(pool)
  delete_result = dbSendStatement(
    con, glue_sql("DELETE FROM survey_intent WHERE survey_intent_id = $1"))
  dbBind(delete_result, list(survey_intent_id))
  dbGetRowsAffected(delete_result)
  dbClearResult(delete_result)
  poolReturn(con)
}












