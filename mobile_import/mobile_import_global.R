
# Main reach_point query
get_new_surveys = function(parent_form_id) {
  qry = glue("select loc.location_id, ",
             "lc.location_coordinates_id, ",
             "loc.river_mile_measure as river_mile, ",
             "loc.location_code as reach_point_code, ",
             "loc.location_name as reach_point_name, ",
             "lt.location_type_description as reach_point_type, ",
             "lc.geom as geometry, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "loc.location_description as reach_point_description, ",
             "datetime(loc.created_datetime, 'localtime') as created_date, loc.created_by, ",
             "datetime(loc.modified_datetime, 'localtime') as modified_date, loc.modified_by ",
             "from location as loc ",
             "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
             "left join location_coordinates as lc on loc.location_id = lc.location_id ",
             "where loc.waterbody_id = '{waterbody_id}' ",
             "and lt.location_type_description in ('Reach boundary point', 'Section break point')")
  con = poolCheckout(pool)
  reach_points = sf::st_read(con, query = qry, crs = 2927)
  poolReturn(con)
  # st_coordinates does not work with missing coordinates, so parse out separately
  reach_point_coords = reach_points %>%
    filter(!is.na(location_coordinates_id)) %>%
    st_transform(., 4326) %>%
    mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
    mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
    st_drop_geometry()
  reach_point_no_coords = reach_points %>%
    filter(is.na(location_coordinates_id)) %>%
    st_drop_geometry()
  # Combine
  reach_points = bind_rows(reach_point_no_coords, reach_point_coords) %>%
    mutate(river_mile = round(river_mile, 2)) %>%
    mutate(latitude = round(latitude, 7)) %>%
    mutate(longitude = round(longitude, 7)) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(location_id, location_coordinates_id, reach_point_code,
           river_mile, reach_point_name, reach_point_type,
           latitude, longitude, horiz_accuracy, reach_point_description,
           created_date, created_dt, created_by, modified_date, modified_dt,
           modified_by) %>%
    arrange(river_mile)
  return(reach_points)
}

# #========================================================
# # Insert callback
# #========================================================
#
# # Define the insert callback
# mobile_import_insert = function(new_reach_point_values) {
#   new_insert_values = new_reach_point_values
#   # Generate location_id
#   location_id = remisc::get_uuid(1L)
#   created_by = new_insert_values$created_by
#   # Pull out location_coordinates table data
#   horizontal_accuracy = as.numeric(new_insert_values$horiz_accuracy)
#   latitude = new_insert_values$latitude
#   longitude = new_insert_values$longitude
#   # Pull out location table data
#   waterbody_id = new_insert_values$waterbody_id
#   wria_id = new_insert_values$wria_id
#   location_type_id = new_insert_values$location_type_id
#   stream_channel_type_id = new_insert_values$stream_channel_type_id
#   location_orientation_type_id = new_insert_values$location_orientation_type_id
#   river_mile_measure = new_insert_values$river_mile
#   location_code = new_insert_values$reach_point_code
#   location_name = new_insert_values$reach_point_name
#   location_description = new_insert_values$reach_point_description
#   if (is.na(location_code) | location_code == "") { location_code = NA }
#   if (is.na(location_name) | location_name == "") { location_name = NA }
#   if (is.na(location_description) | location_description == "") { location_description = NA }
#   # Insert to location table
#   con = poolCheckout(pool)
#   insert_rp_result = dbSendStatement(
#     con, glue_sql("INSERT INTO location (",
#                   "location_id, ",
#                   "waterbody_id, ",
#                   "wria_id, ",
#                   "location_type_id, ",
#                   "stream_channel_type_id, ",
#                   "location_orientation_type_id, ",
#                   "river_mile_measure, ",
#                   "location_code, ",
#                   "location_name, ",
#                   "location_description, ",
#                   "created_by) ",
#                   "VALUES (",
#                   "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
#   dbBind(insert_rp_result, list(location_id, waterbody_id, wria_id,
#                                 location_type_id, stream_channel_type_id,
#                                 location_orientation_type_id, river_mile_measure,
#                                 location_code, location_name,
#                                 location_description, created_by))
#   dbGetRowsAffected(insert_rp_result)
#   dbClearResult(insert_rp_result)
#   # Insert coordinates to location_coordinates
#   if (!is.na(latitude) & !is.na(longitude) ) {
#     # Create location_coordinates_id
#     location_coordinates_id = remisc::get_uuid(1L)
#     # Create a point in hex binary
#     geom = st_point(c(longitude, latitude)) %>%
#       st_sfc(., crs = 4326) %>%
#       st_transform(., 2927) %>%
#       st_as_binary(., hex = TRUE)
#     insert_lc_result = dbSendStatement(
#       con, glue_sql("INSERT INTO location_coordinates (",
#                     "location_coordinates_id, ",
#                     "location_id, ",
#                     "horizontal_accuracy, ",
#                     "geom, ",
#                     "created_by) ",
#                     "VALUES (",
#                     "?, ?, ?, ?, ?)"))
#     dbBind(insert_lc_result, list(location_coordinates_id, location_id,
#                                   horizontal_accuracy, geom, created_by))
#     dbGetRowsAffected(insert_lc_result)
#     dbClearResult(insert_lc_result)
#   }
#   poolReturn(con)
# }

