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
library(iformr)

# Set data for query
# Profile ID
profile_id = 417821L
# Parent form id
parent_form_page_id = 3363255L

count_new_surveys = function(profile_id, parent_form_page_id) {
  # Define query for new mobile surveys
  qry = glue("select distinct s.survey_id ",
             "from survey as s")
  # Checkout connection
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  existing_surveys = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  #poolReturn(con)
  # Define fields...just parent_id and survey_id this time
  fields = paste("id, headerid, survey_date")
  start_id = 0L
  # Get list of all survey_ids and parent_form iform ids on server
  field_string <- paste0("id:<(>\"", start_id, "\"),", fields)
  # Loop through all survey records
  parent_ids = get_all_records(
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
    select(parent_form_survey_id = id, survey_id = headerid, survey_date) %>%
    distinct() %>%
    anti_join(existing_surveys, by = "survey_id") %>%
    arrange(as.Date(survey_date)) %>%
    mutate(n_surveys = n()) %>%
    mutate(first_id = min(parent_form_survey_id)) %>%
    mutate(last_id = max(parent_form_survey_id)) %>%
    mutate(start_date = min(as.Date(survey_date))) %>%
    mutate(end_date = max(as.Date(survey_date))) %>%
    select(n_surveys, first_id, start_date, last_id, end_date) %>%
    distinct()
  return(new_survey_data)
}

# Test
strt = Sys.time()
new_surveys = count_new_surveys(profile_id, parent_form_page_id)
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












