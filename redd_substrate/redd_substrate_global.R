
# Main individual_redd query
get_redd_substrate = function(redd_encounter_id) {
  qry = glue("select rs.redd_substrate_id, sl.substrate_level_short_description as substrate_level, ",
             "st.substrate_type_description as substrate_type, ",
             "rs.substrate_percent as substrate_pct, ",
             "rs.created_datetime as created_date, rs.created_by, ",
             "rs.modified_datetime as modified_date, rs.modified_by ",
             "from redd_substrate as rs ",
             "left join substrate_level_lut as sl on rs.substrate_level_id = sl.substrate_level_id ",
             "left join substrate_type_lut as st on rs.substrate_type_id = st.substrate_type_id ",
             "where rs.redd_encounter_id = '{redd_encounter_id}'")
  con = poolCheckout(pool)
  redd_substrate = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  redd_substrate = redd_substrate %>%
    mutate(created_date = with_tz(created_date, tzone = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = with_tz(modified_date, tzone = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(redd_substrate_id, substrate_level, substrate_type, substrate_pct,
           created_date, created_dt, created_by, modified_date, modified_dt,
           modified_by) %>%
    arrange(created_date)
  return(redd_substrate)
}

#==========================================================================
# Get generic lut input values
#==========================================================================

# Redd shape
get_substrate_level = function() {
  qry = glue("select substrate_level_id, substrate_level_short_description as substrate_level ",
             "from substrate_level_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  substrate_level_list = DBI::dbGetQuery(con, qry) %>%
    arrange(factor(substrate_level, levels = c("Primary", "Secondary", "Tertiary", "Quaternary"))) %>%
    select(substrate_level_id, substrate_level)
  poolReturn(con)
  return(substrate_level_list)
}

# Substrate type
get_substrate_type = function() {
  qry = glue("select substrate_type_id, substrate_type_description as substrate_type ",
             "from substrate_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  substrate_type_list = DBI::dbGetQuery(con, qry) %>%
    arrange(substrate_type) %>%
    select(substrate_type_id, substrate_type)
  poolReturn(con)
  return(substrate_type_list)
}

#========================================================
# Insert callback
#========================================================

# Define the insert callback
redd_substrate_insert = function(new_redd_substrate_values) {
  new_insert_values = new_redd_substrate_values
  # Pull out data
  redd_encounter_id = new_insert_values$redd_encounter_id
  substrate_level_id = new_insert_values$substrate_level_id
  substrate_type_id = new_insert_values$substrate_type_id
  substrate_percent = new_insert_values$substrate_pct
  created_by = new_insert_values$created_by
  # Checkout a connection
  con = poolCheckout(pool)
  insert_result = dbSendStatement(
    con, glue_sql("INSERT INTO redd_substrate (",
                  "redd_encounter_id, ",
                  "substrate_level_id, ",
                  "substrate_type_id, ",
                  "substrate_percent, ",
                  "created_by) ",
                  "VALUES (",
                  "$1, $2, $3, $4, $5)"))
  dbBind(insert_result, list(redd_encounter_id, substrate_level_id, substrate_type_id,
                             substrate_percent, created_by))
  dbGetRowsAffected(insert_result)
  dbClearResult(insert_result)
  poolReturn(con)
}

#========================================================
# Edit update callback
#========================================================

# Define update callback
redd_substrate_update = function(redd_substrate_edit_values) {
  edit_values = redd_substrate_edit_values
  # Pull out data
  redd_substrate_id = edit_values$redd_substrate_id
  substrate_level_id = edit_values$substrate_level_id
  substrate_type_id = edit_values$substrate_type_id
  substrate_percent = edit_values$substrate_pct
  mod_dt = lubridate::with_tz(Sys.time(), "UTC")
  mod_by = Sys.getenv("USERNAME")
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE redd_substrate SET ",
                  "substrate_level_id = $1, ",
                  "substrate_type_id = $2, ",
                  "substrate_percent = $3, ",
                  "modified_datetime = $4, ",
                  "modified_by = $5 ",
                  "where redd_substrate_id = $6"))
  dbBind(update_result, list(substrate_level_id, substrate_type_id,
                             substrate_percent, mod_dt, mod_by,
                             redd_substrate_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  poolReturn(con)
}


#========================================================
# Delete callback
#========================================================

# Define delete callback
redd_substrate_delete = function(delete_values) {
  redd_substrate_id = delete_values$redd_substrate_id
  con = poolCheckout(pool)
  delete_result = dbSendStatement(
    con, glue_sql("DELETE FROM redd_substrate WHERE redd_substrate_id = $1"))
  dbBind(delete_result, list(redd_substrate_id))
  dbGetRowsAffected(delete_result)
  dbClearResult(delete_result)
  poolReturn(con)
}

