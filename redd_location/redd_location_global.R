
# Main redd_location query
# Need stream, RMs, species, survey_date, time-span (four months prior)
get_redd_locations = function(waterbody_id, up_rm, lo_rm, survey_date, species_id) {
  # Define query for new redd locations...no attached surveys yet...finds new entries or orphan entries
  qry = glue("select rloc.location_id as redd_location_id, ",
             "rloc.location_name as redd_name, ",
             "lc.location_coordinates_id, ",
             "st_x(st_transform(lc.geom, 4326)) as longitude, ",
             "st_y(st_transform(lc.geom, 4326)) as latitude, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "sc.channel_type_description as channel_type, ",
             "lo.orientation_type_description as orientation_type, ",
             "rloc.location_description, ",
             "rloc.created_datetime as created_date, rloc.created_by, ",
             "rloc.modified_datetime as modified_date, rloc.modified_by ",
             "from location as rloc ",
             "inner join location_type_lut as lt on rloc.location_type_id = lt.location_type_id ",
             "left join location_coordinates as lc on rloc.location_id = lc.location_id ",
             "inner join stream_channel_type_lut as sc on rloc.stream_channel_type_id = sc.stream_channel_type_id ",
             "inner join location_orientation_type_lut as lo on rloc.location_orientation_type_id = lo.location_orientation_type_id ",
             "where lt.location_type_description = 'Redd encounter' ",
             "and rloc.waterbody_id = '{waterbody_id}'")
  con = poolCheckout(pool)
  redd_loc_one = DBI::dbGetQuery(con, qry)
  # Pull out location_ids for second query
  loc_ids = paste0(paste0("'", unique(redd_loc_one$redd_location_id), "'"), collapse = ", ")
  # Define query for redd locations already tied to surveys
  qry = glue("select s.survey_datetime as redd_survey_date, se.species_id as db_species_id, ",
             "sp.common_name as species, uploc.river_mile_measure as up_rm, ",
             "loloc.river_mile_measure as lo_rm, rloc.location_id as redd_location_id, ",
             "rloc.location_name as redd_name, rs.redd_status_short_description as redd_status, ",
             "lc.location_coordinates_id, ",
             "st_x(st_transform(lc.geom, 4326)) as longitude, ",
             "st_y(st_transform(lc.geom, 4326)) as latitude, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "sc.channel_type_description as channel_type, ",
             "lo.orientation_type_description as orientation_type, ",
             "rloc.location_description, ",
             "rloc.created_datetime as created_date, rloc.created_by, ",
             "rloc.modified_datetime as modified_date, rloc.modified_by ",
             "from survey as s ",
             "inner join location as uploc on s.upper_end_point_id = uploc.location_id ",
             "inner join location as loloc on s.lower_end_point_id = loloc.location_id ",
             "inner join survey_event as se on s.survey_id = se.survey_id ",
             "inner join species_lut as sp on se.species_id = sp.species_id ",
             "inner join redd_encounter as rd on se.survey_event_id = rd.survey_event_id ",
             "inner join redd_status_lut as rs on rd.redd_status_id = rs.redd_status_id ",
             "inner join location as rloc on rd.redd_location_id = rloc.location_id ",
             "left join location_coordinates as lc on rloc.location_id = lc.location_id ",
             "inner join stream_channel_type_lut as sc on rloc.stream_channel_type_id = sc.stream_channel_type_id ",
             "inner join location_orientation_type_lut as lo on rloc.location_orientation_type_id = lo.location_orientation_type_id ",
             "where rd.redd_location_id in ({loc_ids}) ",
             "and uploc.river_mile_measure <= {up_rm} ",
             "and loloc.river_mile_measure >= {lo_rm} ",
             "and not rs.redd_status_short_description in ('Previous redd, not visible')")
  redd_loc_two = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  # Dump entries in fish_loc_one that have surveys attached
  redd_loc_one = redd_loc_one %>%
    anti_join(redd_loc_two, by = "redd_location_id")
  redd_locations = bind_rows(redd_loc_one, redd_loc_two) %>%
    filter(is.na(db_species_id) | db_species_id == species_id) %>%
    mutate(latitude = round(latitude, 7)) %>%
    mutate(longitude = round(longitude, 7)) %>%
    mutate(redd_survey_date = with_tz(redd_survey_date, tzone = "America/Los_Angeles")) %>%
    mutate(survey_dt = format(redd_survey_date, "%m/%d/%Y")) %>%
    filter( is.na(redd_survey_date) | redd_survey_date >= (as.Date(survey_date) - months(4)) ) %>%
    filter( is.na(redd_survey_date) | redd_survey_date <= as.Date(survey_date) ) %>%
    mutate(created_date = with_tz(created_date, tzone = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = with_tz(modified_date, tzone = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(redd_location_id, location_coordinates_id,
           survey_date = redd_survey_date, survey_dt, species,
           redd_name, redd_status, latitude, longitude, horiz_accuracy,
           channel_type, orientation_type, location_description,
           created_date, created_dt, created_by, modified_date,
           modified_dt, modified_by) %>%
    arrange(created_date)
  return(redd_locations)
}

# Redd_coordinates query...for setting redd_marker when redd_location row is selected
get_redd_coordinates = function(redd_location_id) {
  qry = glue("select loc.location_id, lc.location_coordinates_id, ",
             "st_x(st_transform(lc.geom, 4326)) as longitude, ",
             "st_y(st_transform(lc.geom, 4326)) as latitude, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "lc.created_datetime as created_date, lc.created_by, ",
             "lc.modified_datetime as modified_date, lc.modified_by ",
             "from location as loc ",
             "inner join location_coordinates as lc on loc.location_id = lc.location_id ",
             "where loc.location_id = '{redd_location_id}'")
  con = poolCheckout(pool)
  redd_coordinates = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  redd_coordinates = redd_coordinates %>%
    mutate(latitude = round(latitude, 7)) %>%
    mutate(longitude = round(longitude, 7)) %>%
    mutate(created_date = with_tz(created_date, tzone = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = with_tz(modified_date, tzone = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(redd_location_id = location_id, location_coordinates_id,
           latitude, longitude, horiz_accuracy, created_date,
           created_dt, created_by, modified_date, modified_dt,
           modified_by) %>%
    arrange(created_date)
  return(redd_coordinates)
}

#==========================================================================
# Get generic lut input values
#==========================================================================

# Redd status
get_channel_type = function() {
  qry = glue("select stream_channel_type_id, channel_type_description as channel_type ",
             "from stream_channel_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  channel_type_list = DBI::dbGetQuery(con, qry) %>%
    arrange(channel_type) %>%
    select(stream_channel_type_id, channel_type)
  poolReturn(con)
  return(channel_type_list)
}

# Redd status
get_orientation_type = function() {
  qry = glue("select location_orientation_type_id, orientation_type_description as orientation_type ",
             "from location_orientation_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  orientation_type_list = DBI::dbGetQuery(con, qry) %>%
    arrange(orientation_type) %>%
    select(location_orientation_type_id, orientation_type)
  poolReturn(con)
  return(orientation_type_list)
}

#==========================================================================
# Get centroid of waterbody to use in interactive redd_map
#==========================================================================

get_stream_centroid = function(waterbody_id) {
  qry = glue("select DISTINCT waterbody_id, ",
             "ST_X(ST_Transform(ST_Centroid(geom), 4326)) as center_lon, ",
             "ST_Y(ST_Transform(ST_Centroid(geom), 4326)) as center_lat ",
             "from stream ",
             "where waterbody_id = '{waterbody_id}'")
  con = poolCheckout(pool)
  stream_centroid = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  return(stream_centroid)
}

#========================================================
# Insert callback
#========================================================

# Define the insert callback
redd_location_insert = function(new_redd_location_values) {
  new_insert_values = new_redd_location_values
  # Generate location_id
  location_id = remisc::get_uuid(1L)
  created_by = new_insert_values$created_by
  # Pull out location_coordinates table data
  horizontal_accuracy = as.numeric(new_insert_values$horiz_accuracy)
  latitude = new_insert_values$latitude
  longitude = new_insert_values$longitude
  # Pull out location table data
  waterbody_id = new_insert_values$waterbody_id
  wria_id = new_insert_values$wria_id
  location_type_id = new_insert_values$location_type_id
  stream_channel_type_id = new_insert_values$stream_channel_type_id
  location_orientation_type_id = new_insert_values$location_orientation_type_id
  location_name = new_insert_values$redd_name
  location_description = new_insert_values$location_description
  if (is.na(location_name) | location_name == "") { location_name = NA }
  if (is.na(location_description) | location_description == "") { location_description = NA }
  mod_dt = lubridate::with_tz(Sys.time(), "UTC")
  mod_by = Sys.getenv("USERNAME")
  # Insert to location table
  con = poolCheckout(pool)
  insert_loc_result = dbSendStatement(
    con, glue_sql("INSERT INTO location (",
                  "location_id, ",
                  "waterbody_id, ",
                  "wria_id, ",
                  "location_type_id, ",
                  "stream_channel_type_id, ",
                  "location_orientation_type_id, ",
                  "location_name, ",
                  "location_description, ",
                  "created_by) ",
                  "VALUES (",
                  "$1, $2, $3, $4, $5, $6, $7, $8, $9)"))
  dbBind(insert_loc_result, list(location_id, waterbody_id, wria_id,
                                 location_type_id, stream_channel_type_id,
                                 location_orientation_type_id, location_name,
                                 location_description, created_by))
  dbGetRowsAffected(insert_loc_result)
  dbClearResult(insert_loc_result)
  if (!is.na(latitude) & !is.na(longitude) ) {
    # Insert coordinates to location_coordinates
    qry = glue_sql("INSERT INTO location_coordinates ",
                   "(location_id, horizontal_accuracy, geom, created_by) ",
                   "VALUES ({location_id}, {horizontal_accuracy}, ",
                   "ST_Transform(ST_GeomFromText('POINT({longitude} {latitude})', 4326), 2927), ",
                   "{created_by}) ",
                   .con = con)
    DBI::dbExecute(con, qry)
  }
  poolReturn(con)
}

#==============================================================
# Identify redd location surveys prior to update or delete
#==============================================================

# Identify redd_encounter dependencies prior to delete
get_redd_location_surveys = function(redd_location_id) {
  qry = glue("select s.survey_datetime as survey_date, ",
             "s.observer_last_name as observer, ",
             "re.redd_count, mt.media_type_code as media_type, ",
             "ot.observation_type_name as other_observation_type ",
             "from location as loc ",
             "left join redd_encounter as re on loc.location_id = re.redd_location_id ",
             "left join survey_event as se on re.survey_event_id = se.survey_event_id ",
             "left join survey as s on se.survey_id = s.survey_id ",
             "left join media_location as ml on loc.location_id = ml.location_id ",
             "left join media_type_lut as mt on ml.media_type_id = mt.media_type_id ",
             "left join other_observation as oo on loc.location_id = oo.observation_location_id ",
             "left join observation_type_lut as ot on oo.observation_type_id = ot.observation_type_id ",
             "where re.redd_location_id is not null and loc.location_id = '{redd_location_id}'")
  con = poolCheckout(pool)
  redd_loc_surveys = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  redd_loc_surveys = redd_loc_surveys %>%
    mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
    mutate(survey_date = format(survey_date, "%m/%d/%Y"))
  return(redd_loc_surveys)
}

# Define update callback
redd_location_update = function(redd_location_edit_values, selected_redd_location_data) {
  edit_values = redd_location_edit_values
  # Pull out data for location table
  location_id = edit_values$redd_location_id
  stream_channel_type_id = edit_values$stream_channel_type_id
  location_orientation_type_id = edit_values$location_orientation_type_id
  location_name = edit_values$redd_name
  location_description = edit_values$location_description
  if (is.na(location_name) | location_name == "") { location_name = NA }
  if (is.na(location_description) | location_description == "") { location_description = NA }
  mod_dt = lubridate::with_tz(Sys.time(), "UTC")
  mod_by = Sys.getenv("USERNAME")
  created_by = mod_by
  # Pull out data for location_coordinates table
  horizontal_accuracy = edit_values$horiz_accuracy
  latitude = edit_values$latitude
  longitude = edit_values$longitude
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE location SET ",
                  "stream_channel_type_id = $1, ",
                  "location_orientation_type_id = $2, ",
                  "location_name = $3, ",
                  "location_description = $4, ",
                  "modified_datetime = $5, ",
                  "modified_by = $6 ",
                  "where location_id = $7"))
  dbBind(update_result, list(stream_channel_type_id,
                             location_orientation_type_id,
                             location_name, location_description,
                             mod_dt, mod_by,
                             location_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  # Insert coordinates to location_coordinates if previous entry does not exist
  if ( is.na(selected_redd_location_data$latitude) & is.na(selected_redd_location_data$longitude) ) {
    if ( !is.na(latitude) & !is.na(longitude) ) {
      # Insert coordinates to location_coordinates
      qry = glue_sql("INSERT INTO location_coordinates ",
                     "(location_id, horizontal_accuracy, geom, created_by) ",
                     "VALUES ({location_id}, {horizontal_accuracy}, ",
                     "ST_Transform(ST_GeomFromText('POINT({longitude} {latitude})', 4326), 2927), ",
                     "{created_by}) ",
                     .con = con)
      DBI::dbExecute(con, qry)
    }
  # Otherwise update coordinates if previous entry does exist
  } else if (!is.na(selected_redd_location_data$latitude) & !is.na(selected_redd_location_data$longitude) ) {
    if ( !is.na(latitude) & !is.na(longitude) ) {
      qry = glue_sql("UPDATE location_coordinates ",
                     "SET horizontal_accuracy = {horizontal_accuracy}, ",
                     "geom = ST_Transform(ST_GeomFromText('POINT({longitude} {latitude})', 4326), 2927), ",
                     "modified_datetime = {mod_dt}, modified_by = {mod_by} ",
                     "WHERE location_id = {location_id} ",
                     .con = con)
      DBI::dbExecute(con, qry)
    }
  }
  poolReturn(con)
}

#==============================================================
# Identify redd location dependencies prior to delete
#==============================================================

# Identify redd location dependencies prior to delete
get_redd_location_dependencies = function(redd_location_id) {
  qry = glue("select rd.redd_encounter_id, rd.redd_encounter_datetime as redd_encounter_time, ",
             "rd.redd_count, rs.redd_status_short_description as redd_status, ",
             "rd.redd_location_id, loc.location_name as redd_name, rd.comment_text as redd_comment, ",
             "rd.created_datetime as created_date, rd.created_by, ",
             "rd.modified_datetime as modified_date, rd.modified_by ",
             "from redd_encounter as rd ",
             "inner join redd_status_lut as rs on rd.redd_status_id = rs.redd_status_id ",
             "left join location as loc on rd.redd_location_id = loc.location_id ",
             "where rd.redd_location_id is not null and rd.redd_location_id = '{redd_location_id}'")
  con = poolCheckout(pool)
  redd_encounters = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  redd_encounters = redd_encounters %>%
    mutate(redd_encounter_time = with_tz(redd_encounter_time, tzone = "America/Los_Angeles")) %>%
    mutate(redd_encounter_date = format(redd_encounter_time, "%m/%d/%Y")) %>%
    mutate(redd_encounter_time = format(redd_encounter_time, "%H:%M")) %>%
    mutate(redd_status = if_else(redd_status == "Combined visible redds",
                                 "Visible new and old redds combined", redd_status)) %>%
    mutate(created_date = with_tz(created_date, tzone = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = with_tz(modified_date, tzone = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(redd_encounter_id, redd_encounter_date, redd_encounter_time, redd_count,
           redd_status, redd_location_id, redd_name, redd_comment, created_date,
           created_dt, created_by, modified_date, modified_dt, modified_by) %>%
    arrange(created_date)
  return(redd_encounters)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
redd_location_delete = function(delete_values) {
  redd_location_id = delete_values$redd_location_id
  con = poolCheckout(pool)
  # New function...delete only after all dependencies are removed
  delete_result_one = dbSendStatement(
    con, glue_sql("DELETE FROM location_coordinates WHERE location_id = $1"))
  dbBind(delete_result_one, list(redd_location_id))
  dbGetRowsAffected(delete_result_one)
  dbClearResult(delete_result_one)
  delete_result_two = dbSendStatement(
    con, glue_sql("DELETE FROM location WHERE location_id = $1"))
  dbBind(delete_result_two, list(redd_location_id))
  dbGetRowsAffected(delete_result_two)
  dbClearResult(delete_result_two)
  poolReturn(con)
}
