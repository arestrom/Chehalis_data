
# Main fish_location query
# Two part query
get_fish_locations = function(waterbody_id, up_rm, lo_rm, survey_date, species_id) {
  # Define query for new fish locations...no attached surveys yet...finds new entries or orphan entries
  qry_one = glue("select floc.location_id as fish_location_id, ",
                 "floc.location_name as fish_name, ",
                 "lc.location_coordinates_id, ",
                 "lc.geom as geometry, ",
                 "lc.horizontal_accuracy as horiz_accuracy, ",
                 "sc.channel_type_description as channel_type, ",
                 "lo.orientation_type_description as orientation_type, ",
                 "floc.location_description, ",
                 "datetime(floc.created_datetime, 'localtime') as created_date, ",
                 "datetime(floc.modified_datetime, 'localtime') as modified_date, ",
                 "floc.created_by, floc.modified_by ",
                 "from location as floc ",
                 "inner join location_type_lut as lt on floc.location_type_id = lt.location_type_id ",
                 "left join location_coordinates as lc on floc.location_id = lc.location_id ",
                 "inner join stream_channel_type_lut as sc on floc.stream_channel_type_id = sc.stream_channel_type_id ",
                 "inner join location_orientation_type_lut as lo on floc.location_orientation_type_id = lo.location_orientation_type_id ",
                 "where lt.location_type_description = 'Fish encounter' ",
                 "and floc.waterbody_id = '{waterbody_id}'")
  # Checkout connection
  con = poolCheckout(pool)
  fish_loc_one = sf::st_read(con, query = qry_one, crs = 2927)
  # st_coordinates does not work with missing coordinates, so parse out separately
  fish_loc_one_coords = fish_loc_one %>%
    filter(!is.na(location_coordinates_id)) %>%
    st_transform(., 4326) %>%
    mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
    mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
    st_drop_geometry()
  fish_loc_one_no_coords = fish_loc_one %>%
    filter(is.na(location_coordinates_id)) %>%
    st_drop_geometry()
  # Combine
  fish_loc_one = bind_rows(fish_loc_one_no_coords, fish_loc_one_coords)
  # Pull out location_ids for second query
  loc_ids = paste0(paste0("'", unique(fish_loc_one$fish_location_id), "'"), collapse = ", ")
  # Define query for fish locations already tied to surveys
  qry_two = glue("select datetime(s.survey_datetime, 'localtime') as fish_survey_date, ",
                 "se.species_id as db_species_id, ",
                 "sp.common_name as species, uploc.river_mile_measure as up_rm, ",
                 "loloc.river_mile_measure as lo_rm, floc.location_id as fish_location_id, ",
                 "floc.location_name as fish_name, fs.fish_status_description as fish_status, ",
                 "lc.location_coordinates_id, ",
                 "lc.geom as geometry, ",
                 "lc.horizontal_accuracy as horiz_accuracy, ",
                 "sc.channel_type_description as channel_type, ",
                 "lo.orientation_type_description as orientation_type, ",
                 "floc.location_description, ",
                 "datetime(floc.created_datetime, 'localtime') as created_date, ",
                 "datetime(floc.modified_datetime, 'localtime') as modified_date,  ",
                 "floc.created_by, floc.modified_by ",
                 "from survey as s ",
                 "inner join location as uploc on s.upper_end_point_id = uploc.location_id ",
                 "inner join location as loloc on s.lower_end_point_id = loloc.location_id ",
                 "inner join survey_event as se on s.survey_id = se.survey_id ",
                 "inner join species_lut as sp on se.species_id = sp.species_id ",
                 "inner join fish_encounter as fe on se.survey_event_id = fe.survey_event_id ",
                 "inner join fish_status_lut as fs on fe.fish_status_id = fs.fish_status_id ",
                 "inner join location as floc on fe.fish_location_id = floc.location_id ",
                 "left join location_coordinates as lc on floc.location_id = lc.location_id ",
                 "inner join stream_channel_type_lut as sc on floc.stream_channel_type_id = sc.stream_channel_type_id ",
                 "inner join location_orientation_type_lut as lo on floc.location_orientation_type_id = lo.location_orientation_type_id ",
                 "where fe.fish_location_id in ({loc_ids}) ",
                 "and uploc.river_mile_measure <= {up_rm} ",
                 "and loloc.river_mile_measure >= {lo_rm} ",
                 "and fs.fish_status_description = 'Dead'")
  fish_loc_two = sf::st_read(con, query = qry_two, crs = 2927)
  poolReturn(con)
  # st_coordinates does not work with missing coordinates, so parse out separately
  fish_loc_two_coords = fish_loc_two %>%
    filter(!is.na(location_coordinates_id)) %>%
    st_transform(., 4326) %>%
    mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
    mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
    st_drop_geometry()
  fish_loc_two_no_coords = fish_loc_two %>%
    filter(is.na(location_coordinates_id)) %>%
    st_drop_geometry()
  # Combine
  fish_loc_two = bind_rows(fish_loc_two_no_coords, fish_loc_two_coords)
  # Dump entries in fish_loc_one that have surveys attached
  fish_loc_one = fish_loc_one %>%
    anti_join(fish_loc_two, by = "fish_location_id")
  fish_locations = bind_rows(fish_loc_one, fish_loc_two) %>%
    filter(is.na(db_species_id) | db_species_id == species_id) %>%
    mutate(latitude = round(latitude, 7)) %>%
    mutate(longitude = round(longitude, 7)) %>%
    mutate(fish_survey_date = as.POSIXct(fish_survey_date, tz = "America/Los_Angeles")) %>%
    mutate(survey_dt = format(fish_survey_date, "%m/%d/%Y")) %>%
    filter( is.na(fish_survey_date) | fish_survey_date >= (as.Date(survey_date) - months(3)) ) %>%
    filter( is.na(fish_survey_date) | fish_survey_date <= as.Date(survey_date) ) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(fish_location_id, location_coordinates_id,
           survey_date = fish_survey_date, survey_dt, species,
           fish_name, fish_status, latitude, longitude, horiz_accuracy,
           channel_type, orientation_type, location_description,
           created_date, created_dt, created_by, modified_date,
           modified_dt, modified_by) %>%
    arrange(created_date)
  return(fish_locations)
}

#==========================================================================
# Get just the fish_coordinates
#==========================================================================

# fish_coordinates query
get_fish_coordinates = function(fish_location_id) {
  qry = glue("select loc.location_id, lc.location_coordinates_id, ",
             "lc.geom as geometry, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "datetime(lc.created_datetime, 'localtime') as created_date, lc.created_by, ",
             "datetime(lc.modified_datetime, 'localtime') as modified_date, lc.modified_by ",
             "from location as loc ",
             "inner join location_coordinates as lc on loc.location_id = lc.location_id ",
             "where loc.location_id = '{fish_location_id}'")
  con = poolCheckout(pool)
  fish_coordinates = sf::st_read(con, query = qry, crs = 2927)
  poolReturn(con)
  # Only do the rest if nrows > 0
  if (nrow(fish_coordinates) > 0 ) {
    fish_coordinates = fish_coordinates %>%
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
      select(fish_location_id = location_id, location_coordinates_id,
             latitude, longitude, horiz_accuracy, created_date,
             created_dt, created_by, modified_date, modified_dt,
             modified_by) %>%
      arrange(created_date)
  }
  return(fish_coordinates)
}

#==========================================================================
# Get generic lut input values
#==========================================================================

# Redd status
get_fish_channel_type = function() {
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
get_fish_orientation_type = function() {
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
fish_location_insert = function(new_fish_location_values) {
  new_insert_values = new_fish_location_values
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
  location_name = new_insert_values$fish_name
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
# Identify fish location surveys prior to update or delete
#==============================================================

# Identify fish_encounter dependencies prior to delete
get_fish_location_surveys = function(fish_location_id) {
  qry = glue("select datetime(s.survey_datetime, 'localtime') as survey_date, ",
             "s.observer_last_name as observer, loc.location_name as fish_name, ",
             "fe.fish_count, mt.media_type_code as media_type, ",
             "ot.observation_type_name as other_observation_type ",
             "from location as loc ",
             "left join fish_encounter as fe on loc.location_id = fe.fish_location_id ",
             "left join survey_event as se on fe.survey_event_id = se.survey_event_id ",
             "left join survey as s on se.survey_id = s.survey_id ",
             "left join media_location as ml on loc.location_id = ml.location_id ",
             "left join media_type_lut as mt on ml.media_type_id = mt.media_type_id ",
             "left join other_observation as oo on loc.location_id = oo.observation_location_id ",
             "left join observation_type_lut as ot on oo.observation_type_id = ot.observation_type_id ",
             "where fe.fish_location_id is not null and loc.location_id = '{fish_location_id}'")
  con = poolCheckout(pool)
  fish_loc_surveys = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  fish_loc_surveys = fish_loc_surveys %>%
    mutate(survey_date = as.POSIXct(survey_date, tz = "America/Los_Angeles")) %>%
    mutate(survey_date = format(survey_date, "%m/%d/%Y"))
  return(fish_loc_surveys)
}

#========================================================
# Edit location callback
#========================================================

# Define update callback
fish_location_update = function(fish_location_edit_values, selected_fish_location_data) {
  edit_values = fish_location_edit_values
  # Pull out data for location table
  location_id = edit_values$fish_location_id
  stream_channel_type_id = edit_values$stream_channel_type_id
  location_orientation_type_id = edit_values$location_orientation_type_id
  location_name = edit_values$fish_name
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
    if ( is.na(selected_fish_location_data$latitude) & is.na(selected_fish_location_data$longitude) ) {
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
    } else if (!is.na(selected_fish_location_data$latitude) & !is.na(selected_fish_location_data$longitude) ) {
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

#========================================================
# Identify fish location dependencies prior to delete
#========================================================

# Identify fish_encounter dependencies prior to delete
get_fish_location_dependencies = function(fish_location_id) {
  qry = glue("select fd.fish_encounter_id, datetime(fd.fish_encounter_datetime, 'localtime') as fish_encounter_time, ",
             "fd.fish_count, fs.fish_status_description as fish_status, ",
             "fd.fish_location_id, loc.location_name as fish_name, ",
             "datetime(fd.created_datetime, 'localtime') as created_date, fd.created_by, ",
             "datetime(fd.modified_datetime, 'localtime') as modified_date, fd.modified_by ",
             "from fish_encounter as fd ",
             "inner join fish_status_lut as fs on fd.fish_status_id = fs.fish_status_id ",
             "left join location as loc on fd.fish_location_id = loc.location_id ",
             "where fd.fish_location_id is not null and fd.fish_location_id = '{fish_location_id}'")
  con = poolCheckout(pool)
  fish_encounters = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  fish_encounters = fish_encounters %>%
    mutate(fish_encounter_time = as.POSIXct(fish_encounter_time, tz = "America/Los_Angeles")) %>%
    mutate(fish_encounter_date = format(fish_encounter_time, "%m/%d/%Y")) %>%
    mutate(fish_encounter_time = format(fish_encounter_time, "%H:%M")) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(fish_encounter_id, fish_encounter_date, fish_encounter_time, fish_count,
           fish_status, fish_location_id, fish_name, created_date,
           created_dt, created_by, modified_date, modified_dt, modified_by) %>%
    arrange(created_date)
  return(fish_encounters)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
fish_location_delete = function(delete_values) {
  fish_location_id = delete_values$fish_location_id
  con = poolCheckout(pool)
  DBI::dbWithTransaction(con, {
    # New function...delete only after all dependencies are removed
    delete_result_one = dbSendStatement(
      con, glue_sql("DELETE FROM location_coordinates WHERE location_id = ?"))
    dbBind(delete_result_one, list(fish_location_id))
    dbGetRowsAffected(delete_result_one)
    dbClearResult(delete_result_one)
    delete_result_two = dbSendStatement(
      con, glue_sql("DELETE FROM location WHERE location_id = ?"))
    dbBind(delete_result_two, list(fish_location_id))
    dbGetRowsAffected(delete_result_two)
    dbClearResult(delete_result_two)
  })
  poolReturn(con)
}
