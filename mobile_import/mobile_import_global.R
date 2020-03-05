
# Get counts and date-range of surveys ready to download
count_new_surveys = function(profile_id, parent_form_page_id, access_token) {
  # Define query for new mobile surveys
  qry = glue("select distinct s.survey_id ",
             "from survey as s")
  # Checkout connection
  con = DBI::dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  existing_surveys = DBI::dbGetQuery(con, qry)
  DBI::dbDisconnect(con)
  #poolReturn(con)
  # Define fields...just parent_id and survey_id this time
  fields = paste0("id, headerid, survey_date, stream_name, ",
                  "stream_name_text, new_stream_name, reach, ",
                  "reach_text, new_reach, gps_loc_lower, ",
                  "gps_loc_upper")
  start_id = 0L
  # Get list of all survey_ids and parent_form iform ids on server
  field_string <- paste0("id:<(>\"", start_id, "\"),", fields)
  # Loop through all survey records
  parent_ids = iformr::get_all_records(
    server_name = "wdfw",
    profile_id = profile_id,
    page_id = parent_form_page_id,
    fields = "fields",
    limit = 1000,
    offset = 0,
    access_token = access_token,
    field_string = field_string,
    since_id = start_id)
  # Keep only records where headerid is not in survey_id
  new_survey_data = parent_ids %>%
    select(parent_form_survey_id = id, survey_id = headerid, survey_date,
           stream_name, stream_name_text, new_stream_name, reach, reach_text,
           new_reach, gps_loc_lower, gps_loc_upper) %>%
    distinct() %>%
    anti_join(existing_surveys, by = "survey_id") %>%
    arrange(as.Date(survey_date))
  return(new_survey_data)
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

