
# Main waterbody measurements query
get_waterbody_meas = function(survey_id) {
  qry = glue("select wb.waterbody_measurement_id, wc.clarity_type_short_description as clarity_type, ",
             "wb.water_clarity_meter as clarity_meter, wb.stream_flow_measurement_cfs as flow_cfs, ",
             "datetime(s.survey_datetime, 'localtime') as survey_date, wb.start_water_temperature_celsius as start_temperature, ",
             "datetime(wb.start_water_temperature_datetime, 'localtime') as start_tmp_time, ",
             "wb.end_water_temperature_celsius as end_temperature, ",
             "datetime(wb.end_water_temperature_datetime, 'localtime') as end_tmp_time, ",
             "wb.waterbody_ph as water_ph, datetime(wb.created_datetime, 'localtime') as created_date, ",
             "wb.created_by, datetime(wb.modified_datetime, 'localtime') as modified_date, wb.modified_by ",
             "from waterbody_measurement as wb ",
             "inner join survey as s on wb.survey_id = s.survey_id ",
             "left join water_clarity_type_lut as wc on wb.water_clarity_type_id = wc.water_clarity_type_id ",
             "where wb.survey_id = '{survey_id}'")
  con = poolCheckout(pool)
  waterbody_measure = DBI::dbGetQuery(pool, qry)
  poolReturn(con)
  waterbody_measure = waterbody_measure %>%
    mutate(survey_date = as.POSIXct(survey_date, tz = "America/Los_Angeles")) %>%
    mutate(start_tmp_time = as.POSIXct(start_tmp_time, tz = "America/Los_Angeles")) %>%
    mutate(start_tmp_dt = format(start_tmp_time, "%H:%M")) %>%
    mutate(end_tmp_time = as.POSIXct(end_tmp_time, tz = "America/Los_Angeles")) %>%
    mutate(end_tmp_dt = format(end_tmp_time, "%H:%M")) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(waterbody_measurement_id, clarity_type, clarity_meter, flow_cfs, survey_date,
           start_temperature, start_tmp_time, start_tmp_dt, end_temperature,
           end_tmp_time, end_tmp_dt, water_ph, created_date, created_dt,
           created_by, modified_date, modified_dt, modified_by) %>%
    arrange(created_date)
  return(waterbody_measure)
}

#==========================================================================
# Get generic lut input values
#==========================================================================

# Water clarity type
get_clarity_type = function() {
  qry = glue("select water_clarity_type_id, clarity_type_short_description as clarity_type ",
             "from water_clarity_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  clarity_type_list = DBI::dbGetQuery(con, qry) %>%
    arrange(clarity_type) %>%
    select(water_clarity_type_id, clarity_type)
  poolReturn(con)
  return(clarity_type_list)
}

#========================================================
# Insert callback
#========================================================

# Define the insert callback
waterbody_meas_insert = function(new_wbm_values) {
  new_wbm_values = new_wbm_values
  waterbody_measurement_id = remisc::get_uuid(1L)
  # Pull out data
  survey_id = new_wbm_values$survey_id
  water_clarity_type_id = new_wbm_values$water_clarity_type_id
  water_clarity_meter =  new_wbm_values$clarity_meter
  stream_flow_measurement_cfs = new_wbm_values$flow_cfs
  start_water_temperature_datetime = format(new_wbm_values$start_temperature_time)
  start_water_temperature_celsius = new_wbm_values$start_temperature
  end_water_temperature_datetime = format(new_wbm_values$end_temperature_time)
  end_water_temperature_celsius = new_wbm_values$end_temperature
  waterbody_ph = new_wbm_values$water_ph
  created_by = new_wbm_values$created_by
  # Checkout a connection
  con = poolCheckout(pool)
  insert_result = dbSendStatement(
    con, glue_sql("INSERT INTO waterbody_measurement (",
                  "waterbody_measurement_id, ",
                  "survey_id, ",
                  "water_clarity_type_id, ",
                  "water_clarity_meter, ",
                  "stream_flow_measurement_cfs, ",
                  "start_water_temperature_datetime, ",
                  "start_water_temperature_celsius, ",
                  "end_water_temperature_datetime, ",
                  "end_water_temperature_celsius, ",
                  "waterbody_ph, ",
                  "created_by) ",
                  "VALUES (",
                  "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
  dbBind(insert_result, list(waterbody_measurement_id, survey_id, water_clarity_type_id, water_clarity_meter,
                             stream_flow_measurement_cfs, start_water_temperature_datetime,
                             start_water_temperature_celsius, end_water_temperature_datetime,
                             end_water_temperature_celsius, waterbody_ph, created_by))
  dbGetRowsAffected(insert_result)
  dbClearResult(insert_result)
  poolReturn(con)
}

#========================================================
# Edit update callback
#========================================================

# Define update callback
waterbody_meas_update = function(waterbody_meas_edit_values) {
  edit_values = waterbody_meas_edit_values
  # Pull out data
  waterbody_measurement_id = edit_values$waterbody_measurement_id
  water_clarity_type_id = edit_values$water_clarity_type_id
  water_clarity_meter = edit_values$clarity_meter
  stream_flow_measurement_cfs = edit_values$flow_cfs
  start_water_temperature_datetime = format(edit_values$start_temperature_time)
  start_water_temperature_celsius = edit_values$start_temperature
  end_water_temperature_datetime = format(edit_values$end_temperature_time)
  end_water_temperature_celsius = edit_values$end_temperature
  waterbody_ph = edit_values$water_ph
  mod_dt = format(lubridate::with_tz(Sys.time(), "UTC"))
  mod_by = Sys.getenv("USERNAME")
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE waterbody_measurement SET ",
                  "water_clarity_type_id = ?, ",
                  "water_clarity_meter = ?, ",
                  "stream_flow_measurement_cfs = ?, ",
                  "start_water_temperature_datetime = ?, ",
                  "start_water_temperature_celsius = ?, ",
                  "end_water_temperature_datetime = ?, ",
                  "end_water_temperature_celsius = ?, ",
                  "waterbody_ph = ?, ",
                  "modified_datetime = ?, ",
                  "modified_by = ? ",
                  "where waterbody_measurement_id = ?"))
  dbBind(update_result, list(water_clarity_type_id, water_clarity_meter,
                             stream_flow_measurement_cfs, start_water_temperature_datetime,
                             start_water_temperature_celsius, end_water_temperature_datetime,
                             end_water_temperature_celsius, waterbody_ph,
                             mod_dt, mod_by, waterbody_measurement_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  poolReturn(con)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
waterbody_meas_delete = function(delete_values) {
  waterbody_measurement_id = delete_values$waterbody_measurement_id
  con = poolCheckout(pool)
  delete_result = dbSendStatement(
    con, glue_sql("DELETE FROM waterbody_measurement WHERE waterbody_measurement_id = ?"))
  dbBind(delete_result, list(waterbody_measurement_id))
  dbGetRowsAffected(delete_result)
  dbClearResult(delete_result)
  poolReturn(con)
}
