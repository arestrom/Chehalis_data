#===============================================================================
# Verify queries work
#
# AS 2020-02-25
#===============================================================================

# Load libraries
library(DBI)
library(RSQLite)
library(tibble)
library(sf)
library(glue)

# Set data for query
# Absher Cr
waterbody_id = 'de2dabed-c195-481a-aaf5-c50d47d37073'
# Alder Cr
# waterbody_id = '3fd8a43f-505c-4e1b-9474-94046099af62'

# Main reach_point query
get_reach_point = function(waterbody_id) {
  # First check if any geometry exists
  qry_one = glue("select loc.location_id, ",
                 "lc.location_coordinates_id, ",
                 "loc.river_mile_measure as river_mile, ",
                 "loc.location_code as reach_point_code, ",
                 "loc.location_name as reach_point_name, ",
                 "lt.location_type_description as reach_point_type, ",
                 "loc.location_description as reach_point_description, ",
                 "datetime(loc.created_datetime, 'localtime') as created_date, loc.created_by, ",
                 "datetime(loc.modified_datetime, 'localtime') as modified_date, loc.modified_by ",
                 "from location as loc ",
                 "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
                 "left join location_coordinates as lc on loc.location_id = lc.location_id ",
                 "where loc.waterbody_id = '{waterbody_id}' ",
                 "and lt.location_type_description in ('Reach boundary point', 'Section break point')")
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  reach_points_one = DBI::dbGetQuery(con, qry_one)
  dbDisconnect(con)
  # Pull out any lc_ids
  lc_id = reach_points_one %>%
    filter(!is.na(location_coordinates_id)) %>%
    pull(location_coordinates_id)
  # Only get coordinates if they exist....avoids sfc error
  if ( length(lc_id) > 0 ) {
    location_coordinates_id = paste0(paste0("'", lc_id, "'"), collapse = ", ")
    qry_two = glue("select loc.location_id, ",
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
                   "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
                   "and lc.location_coordinates_id in ({location_coordinates_id})")
    con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
    #con = poolCheckout(pool)
    reach_points_two = sf::st_read(con, query = qry_two, crs = 2927)
    dbDisconnect(con)
    #poolReturn(con)
    # st_coordinates does not work with missing coordinates, so parse out separately
    reach_point_coords = reach_points_two %>%
      st_transform(., 4326) %>%
      mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
      mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
      mutate(latitude = round(latitude, 7)) %>%
      mutate(longitude = round(longitude, 7)) %>%
      st_drop_geometry()
  }
  # Pull out no_coords data
  reach_point_no_coords = reach_points_one %>%
    filter(is.na(location_coordinates_id)) %>%
    mutate(longitude = NA_real_) %>%
    mutate(latitude = NA_real_) %>%
    mutate(horiz_accuracy = NA_real_)
  # Combine
  if ( length(lc_id) > 0 ) {
    reach_points = bind_rows(reach_point_no_coords, reach_point_coords)
  } else {
    reach_points = reach_point_no_coords
  }
  reach_points = reach_points %>%
    mutate(river_mile = round(river_mile, 2)) %>%
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

# Run
reach_point_coords = get_reach_point(waterbody_id)










