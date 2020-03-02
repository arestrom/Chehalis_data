#===============================================================================
# Verify queries work
#
# AS 2020-02-25
#===============================================================================

# Load libraries
library(DBI)
library(RSQLite)
library(tibble)
library(glue)

# Set data for query
# Parent form id
parent_form_id = 3363255L

get_new_surveys = function(parent_form_id) {
  # Define query for new mobile surveys
  qry = glue("select distinct s.survey_id, ",
             "datetime(s.survey_date, 'localtime') ",
             "from survey as s")
  # Checkout connection
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  existing_surveys = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  #poolReturn(con)

  dbDisconnect(con)
  # st_coordinates does not work with missing coordinates, so parse out separately
  fish_loc_two_coords = fish_loc_two %>%
    filter(!is.na(location_coordinates_id)) %>%
    mutate(geometry = st_transform(geometry, 4326)) %>%
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

# Test
strt = Sys.time()
fish_locs = get_fish_locations(waterbody_id, up_rm, lo_rm, survey_date, species_id)
nd = Sys.time(); nd - strt

# fish_coordinates query
fish_location_id = fish_locs$fish_location_id[[5]]
get_fish_coordinates = function(fish_location_id) {
  qry = glue("select loc.location_id, lc.location_coordinates_id, ",
             "lc.geom as geometry, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "datetime(lc.created_datetime, 'localtime') as created_date, lc.created_by, ",
             "datetime(lc.modified_datetime, 'localtime') as modified_date, lc.modified_by ",
             "from location as loc ",
             "inner join location_coordinates as lc on loc.location_id = lc.location_id ",
             "where loc.location_id = '{fish_location_id}'")
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  fish_coordinates = sf::st_read(con, query = qry, crs = 2927)
  dbDisconnect(con)
  #poolReturn(con)
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

# Test
strt = Sys.time()
fish_coords = get_fish_coordinates(fish_location_id)
nd = Sys.time(); nd - strt

# Stream centroid query
get_stream_centroid = function(waterbody_id) {
  qry = glue("select DISTINCT st.waterbody_id, ",
             "st.geom as geometry ",
             "from stream as st ",
             "where st.waterbody_id = '{waterbody_id}'")
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  stream_centroid = sf::st_read(con, query = qry) %>%
    mutate(stream_center = st_centroid(geometry)) %>%
    mutate(stream_center = st_transform(stream_center, 4326)) %>%
    mutate(center_lon = as.numeric(st_coordinates(stream_center)[,1])) %>%
    mutate(center_lat = as.numeric(st_coordinates(stream_center)[,2])) %>%
    st_drop_geometry() %>%
    select(waterbody_id, center_lon, center_lat)
  dbDisconnect(con)
  #poolReturn(con)
  return(stream_centroid)
}

# Test
strt = Sys.time()
stream_center_pt = get_stream_centroid(waterbody_id)
nd = Sys.time(); nd - strt




# Create beach polygon centroids
bch_point_st = bch_info_st %>%
  mutate(beach_center = st_centroid(geometry)) %>%
  select(bidn, beach_name, beach_center)

# Convert to lat-lon
bch_point_st = st_transform(bch_point_st, 4326)

# Pull out lat-lons
bch_point_st = bch_point_st %>%
  mutate(lon = as.numeric(st_coordinates(beach_center)[,1])) %>%
  mutate(lat = as.numeric(st_coordinates(beach_center)[,2]))












