
# Query for newly entered redd_locations
get_new_redd_location = function(waterbody_id) {
  # Define query for new redd locations...no attached surveys yet...finds new entries
  qry = glue("select rloc.location_id as redd_location_id, ",
             "rloc.location_name as redd_name, ",
             "lc.location_coordinates_id, lc.geom as geometry, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "sc.channel_type_description as channel_type, ",
             "lo.orientation_type_description as orientation_type, ",
             "rloc.location_description, ",
             "datetime(rloc.created_datetime, 'localtime') as created_date, ",
             "datetime(rloc.modified_datetime, 'localtime') as modified_date, ",
             "rloc.created_by, rloc.modified_by ",
             "from location as rloc ",
             "inner join location_type_lut as lt on rloc.location_type_id = lt.location_type_id ",
             "left join location_coordinates as lc on rloc.location_id = lc.location_id ",
             "left join redd_encounter as rd on rloc.location_id = rd.redd_location_id ",
             "inner join stream_channel_type_lut as sc on rloc.stream_channel_type_id = sc.stream_channel_type_id ",
             "inner join location_orientation_type_lut as lo on rloc.location_orientation_type_id = lo.location_orientation_type_id ",
             "where date(rloc.created_datetime) > date('now', '-1 day') ",
             "and rd.redd_encounter_id is null ",
             "and rloc.waterbody_id = '{waterbody_id}' ",
             "and lt.location_type_description = 'Redd encounter'")
  con = poolCheckout(pool)
  new_loc = sf::st_read(con, query = qry, crs = 2927)
  poolReturn(con)
  return(new_loc)
}

# Query for previously entered redd_locations
get_previous_redd_location = function(waterbody_id, up_rm, lo_rm, survey_date, species_id) {
  # Define query for existing redd location entries with surveys attached
  qry = glue("select datetime(s.survey_datetime, 'localtime') as redd_survey_date, ",
             "se.species_id as db_species_id, ",
             "sp.common_name as species, uploc.river_mile_measure as up_rm, ",
             "loloc.river_mile_measure as lo_rm, rloc.location_id as redd_location_id, ",
             "rloc.location_name as redd_name, rs.redd_status_short_description as redd_status, ",
             "lc.location_coordinates_id, lc.geom as geometry, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "sc.channel_type_description as channel_type, ",
             "lo.orientation_type_description as orientation_type, ",
             "rloc.location_description, ",
             "datetime(rloc.created_datetime, 'localtime') as created_date, ",
             "datetime(rloc.modified_datetime, 'localtime') as modified_date,  ",
             "rloc.created_by, rloc.modified_by ",
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
             "where date(s.survey_datetime) between date('{survey_date}', '-3 month') and date('{survey_date}') ",
             "and rloc.waterbody_id = '{waterbody_id}' ",
             "and uploc.river_mile_measure <= {up_rm} ",
             "and loloc.river_mile_measure >= {lo_rm} ",
             "and se.species_id = '{species_id}' ",
             "and not rs.redd_status_short_description in ('Previous redd, not visible')")
  con = poolCheckout(pool)
  prev_loc = sf::st_read(con, query = qry, crs = 2927)
  poolReturn(con)
  return(prev_loc)
}

# Main redd_location query...includes redds within time-span four months prior
get_redd_locations = function(waterbody_id, up_rm, lo_rm, survey_date, species_id) {
  new_redd_loc = get_new_redd_location(waterbody_id)
  # st_coordinates does not work with missing coordinates, so parse out separately
  new_coords = new_redd_loc %>%
    filter(!is.na(location_coordinates_id)) %>%
    mutate(geometry = st_transform(geometry, 4326)) %>%
    mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
    mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
    st_drop_geometry()
  new_no_coords = new_redd_loc %>%
    filter(is.na(location_coordinates_id)) %>%
    st_drop_geometry()
  # Combine
  new_locs = bind_rows(new_no_coords, new_coords) %>%
    mutate(created_date = as.character(created_date)) %>%
    mutate(modified_date = as.character(modified_date))
  # Get pre-existing redd locations
  prev_redd_loc = get_previous_redd_location(waterbody_id, up_rm, lo_rm, survey_date, species_id)
  # st_coordinates does not work with missing coordinates, so parse out separately
  prev_coords = prev_redd_loc %>%
    filter(!is.na(location_coordinates_id)) %>%
    mutate(geometry = st_transform(geometry, 4326)) %>%
    mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
    mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
    st_drop_geometry()
  prev_no_coords = prev_redd_loc %>%
    filter(is.na(location_coordinates_id)) %>%
    st_drop_geometry()
  # Combine
  prev_locs = bind_rows(prev_no_coords, prev_coords) %>%
    mutate(created_date = as.character(created_date)) %>%
    mutate(modified_date = as.character(modified_date))
  redd_locations = bind_rows(new_locs, prev_locs) %>%
    mutate(latitude = round(latitude, 7)) %>%
    mutate(longitude = round(longitude, 7)) %>%
    mutate(redd_survey_date = as.POSIXct(redd_survey_date, tz = "America/Los_Angeles")) %>%
    mutate(survey_dt = format(redd_survey_date, "%m/%d/%Y")) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tzone = "America/Los_Angeles")) %>%
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
             "lc.geom as geometry, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "datetime(lc.created_datetime, 'localtime') as created_date, lc.created_by, ",
             "datetime(lc.modified_datetime, 'localtime') as modified_date, lc.modified_by ",
             "from location as loc ",
             "inner join location_coordinates as lc on loc.location_id = lc.location_id ",
             "where loc.location_id = '{redd_location_id}'")
  con = poolCheckout(pool)
  redd_coordinates = sf::st_read(con, query = qry, crs = 2927)
  poolReturn(con)
  # Only do the rest if nrows > 0
  if (nrow(redd_coordinates) > 0 ) {
    redd_coordinates = redd_coordinates %>%
      st_transform(., 4326) %>%
      mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
      mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
      st_drop_geometry() %>%
      mutate(latitude = round(latitude, 7)) %>%
      mutate(longitude = round(longitude, 7)) %>%
      mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
      mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
      mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
      mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
      select(redd_location_id = location_id, location_coordinates_id,
             latitude, longitude, horiz_accuracy, created_date,
             created_dt, created_by, modified_date, modified_dt,
             modified_by) %>%
      arrange(created_date)
  }
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
  # Insert to location table
  con = poolCheckout(pool)
  DBI::dbWithTransaction(con, {
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
                    "?, ?, ?, ?, ?, ?, ?, ?, ?)"))
    dbBind(insert_loc_result, list(location_id, waterbody_id, wria_id,
                                   location_type_id, stream_channel_type_id,
                                   location_orientation_type_id, location_name,
                                   location_description, created_by))
    dbGetRowsAffected(insert_loc_result)
    dbClearResult(insert_loc_result)
    # Insert coordinates to location_coordinates
    if (!is.na(latitude) & !is.na(longitude) ) {
      # Create location_coordinates_id
      location_coordinates_id = remisc::get_uuid(1L)
      # Create a point in hex binary
      geom = st_point(c(longitude, latitude)) %>%
        st_sfc(., crs = 4326) %>%
        st_transform(., 2927) %>%
        st_as_binary(., hex = TRUE)
      insert_lc_result = dbSendStatement(
        con, glue_sql("INSERT INTO location_coordinates (",
                      "location_coordinates_id, ",
                      "location_id, ",
                      "horizontal_accuracy, ",
                      "geom, ",
                      "created_by) ",
                      "VALUES (",
                      "?, ?, ?, ?, ?)"))
      dbBind(insert_lc_result, list(location_coordinates_id, location_id,
                                    horizontal_accuracy, geom, created_by))
      dbGetRowsAffected(insert_lc_result)
      dbClearResult(insert_lc_result)
    }
  })
  poolReturn(con)
}

#==============================================================
# Identify redd location surveys prior to update or delete
#==============================================================

# Identify redd_encounter dependencies prior to delete
get_redd_location_surveys = function(redd_location_id) {
  qry = glue("select datetime(s.survey_datetime, 'localtime') as survey_date, ",
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
    mutate(survey_date = as.POSIXct(survey_date, tz = "America/Los_Angeles")) %>%
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
  mod_dt = format(lubridate::with_tz(Sys.time(), "UTC"))
  mod_by = Sys.getenv("USERNAME")
  created_by = mod_by
  # Pull out data for location_coordinates table
  horizontal_accuracy = edit_values$horiz_accuracy
  latitude = edit_values$latitude
  longitude = edit_values$longitude
  # Checkout a connection
  con = poolCheckout(pool)
  DBI::dbWithTransaction(con, {
    update_result = dbSendStatement(
      con, glue_sql("UPDATE location SET ",
                    "stream_channel_type_id = ?, ",
                    "location_orientation_type_id = ?, ",
                    "location_name = ?, ",
                    "location_description = ?, ",
                    "modified_datetime = ?, ",
                    "modified_by = ? ",
                    "where location_id = ?"))
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
        # Create location_coordinates_id
        location_coordinates_id = remisc::get_uuid(1L)
        # Create a point in hex binary
        geom = st_point(c(longitude, latitude)) %>%
          st_sfc(., crs = 4326) %>%
          st_transform(., 2927) %>%
          st_as_binary(., hex = TRUE)
        insert_lc_result = dbSendStatement(
          con, glue_sql("INSERT INTO location_coordinates (",
                        "location_coordinates_id, ",
                        "location_id, ",
                        "horizontal_accuracy, ",
                        "geom, ",
                        "created_by) ",
                        "VALUES (",
                        "?, ?, ?, ?, ?)"))
        dbBind(insert_lc_result, list(location_coordinates_id, location_id,
                                      horizontal_accuracy, geom, created_by))
        dbGetRowsAffected(insert_lc_result)
        dbClearResult(insert_lc_result)
      }
      # Otherwise update coordinates if previous entry does exist
    } else if (!is.na(selected_redd_location_data$latitude) & !is.na(selected_redd_location_data$longitude) ) {
      if ( !is.na(latitude) & !is.na(longitude) ) {
        # Create a point in hex binary
        geom = st_point(c(longitude, latitude)) %>%
          st_sfc(., crs = 4326) %>%
          st_transform(., 2927) %>%
          st_as_binary(., hex = TRUE)
        update_lc_result = dbSendStatement(
          con, glue_sql("UPDATE location_coordinates SET ",
                        "horizontal_accuracy = ?, ",
                        "geom = ?, ",
                        "modified_datetime = ?, ",
                        "modified_by = ? ",
                        "where location_id = ?"))
        dbBind(update_lc_result, list(horizontal_accuracy, geom,
                                      mod_dt, mod_by, location_id))
        dbGetRowsAffected(update_lc_result)
        dbClearResult(update_lc_result)
      }
    }
  })
  poolReturn(con)
}

#==============================================================
# Identify redd location dependencies prior to delete
#==============================================================

# Identify redd location dependencies prior to delete
get_redd_location_dependencies = function(redd_location_id) {
  qry = glue("select rd.redd_encounter_id, datetime(rd.redd_encounter_datetime, 'localtime') as redd_encounter_time, ",
             "rd.redd_count, rs.redd_status_short_description as redd_status, ",
             "rd.redd_location_id, loc.location_name as redd_name, rd.comment_text as redd_comment, ",
             "datetime(rd.created_datetime, 'localtime') as created_date, rd.created_by, ",
             "datetime(rd.modified_datetime, 'localtime') as modified_date, rd.modified_by ",
             "from redd_encounter as rd ",
             "inner join redd_status_lut as rs on rd.redd_status_id = rs.redd_status_id ",
             "left join location as loc on rd.redd_location_id = loc.location_id ",
             "where rd.redd_location_id is not null and rd.redd_location_id = '{redd_location_id}'")
  con = poolCheckout(pool)
  redd_encounters = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  redd_encounters = redd_encounters %>%
    mutate(redd_encounter_time = as.POSIXct(redd_encounter_time, tz = "America/Los_Angeles")) %>%
    mutate(redd_encounter_date = format(redd_encounter_time, "%m/%d/%Y")) %>%
    mutate(redd_encounter_time = format(redd_encounter_time, "%H:%M")) %>%
    mutate(redd_status = if_else(redd_status == "Combined visible redds",
                                 "Visible new and old redds combined", redd_status)) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
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
  DBI::dbWithTransaction(con, {
    # New function...delete only after all dependencies are removed
    delete_result_one = dbSendStatement(
      con, glue_sql("DELETE FROM location_coordinates WHERE location_id = ?"))
    dbBind(delete_result_one, list(redd_location_id))
    dbGetRowsAffected(delete_result_one)
    dbClearResult(delete_result_one)
    delete_result_two = dbSendStatement(
      con, glue_sql("DELETE FROM location WHERE location_id = ?"))
    dbBind(delete_result_two, list(redd_location_id))
    dbGetRowsAffected(delete_result_two)
    dbClearResult(delete_result_two)
  })
  poolReturn(con)
}
