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
waterbody_id = '05a031e6-62b7-411f-9e3c-b5d6efbda33b'
# Alder Cr
# waterbody_id = '3fd8a43f-505c-4e1b-9474-94046099af62'
up_rm = 0.50
#up_rm = "null"
lo_rm = 0.00
#lo_rm = "null"
survey_date = "2019-12-04"
# survey_date = "null"
# # Chinook
# species_id = "e42aa0fc-c591-4fab-8481-55b0df38dcb1"
# Coho
species_id = "a0f5b3af-fa07-449c-9f02-14c5368ab304"
#created_by = Sys.getenv("USERNAME")

get_redd_locations = function(waterbody_id, up_rm, lo_rm, survey_date, species_id) {
  # Define query for new redd locations...no attached surveys yet...finds new entries or orphan entries
  qry_one = glue("select rloc.location_id as redd_location_id, ",
                 "rloc.location_name as redd_name, ",
                 "lc.location_coordinates_id, ",
                 "lc.geom as geometry, ",
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
                 "inner join stream_channel_type_lut as sc on rloc.stream_channel_type_id = sc.stream_channel_type_id ",
                 "inner join location_orientation_type_lut as lo on rloc.location_orientation_type_id = lo.location_orientation_type_id ",
                 "where lt.location_type_description = 'Redd encounter' ",
                 "and rloc.waterbody_id = '{waterbody_id}'")
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  redd_loc_one = sf::st_read(con, query = qry_one, crs = 2927)
  # st_coordinates does not work with missing coordinates, so parse out separately
  redd_loc_one_coords = redd_loc_one %>%
    filter(!is.na(location_coordinates_id)) %>%
    mutate(geometry = st_transform(geometry, 4326)) %>%
    mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
    mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
    st_drop_geometry()
  redd_loc_one_no_coords = redd_loc_one %>%
    filter(is.na(location_coordinates_id)) %>%
    st_drop_geometry()
  # Combine
  redd_loc_one = bind_rows(redd_loc_one_no_coords, redd_loc_one_coords)
  # Pull out location_ids for second query
  loc_ids = paste0(paste0("'", unique(redd_loc_one$redd_location_id), "'"), collapse = ", ")
  # Define query for redd locations already tied to surveys
  qry_two = glue("select datetime(s.survey_datetime, 'localtime') as redd_survey_date, se.species_id as db_species_id, ",
                 "sp.common_name as species, uploc.river_mile_measure as up_rm, ",
                 "loloc.river_mile_measure as lo_rm, rloc.location_id as redd_location_id, ",
                 "rloc.location_name as redd_name, rs.redd_status_short_description as redd_status, ",
                 "lc.location_coordinates_id, ",
                 "lc.geom as geometry, ",
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
                 "where rd.redd_location_id in ({loc_ids}) ",
                 "and uploc.river_mile_measure <= {up_rm} ",
                 "and loloc.river_mile_measure >= {lo_rm} ",
                 "and not rs.redd_status_short_description in ('Previous redd, not visible')")
  redd_loc_two = sf::st_read(con, query = qry_two, crs = 2927)
  dbDisconnect(con)
  #poolReturn(con)
  # st_coordinates does not work with missing coordinates, so parse out separately
  redd_loc_two_coords = redd_loc_two %>%
    filter(!is.na(location_coordinates_id)) %>%
    mutate(geometry = st_transform(geometry, 4326)) %>%
    mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
    mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
    st_drop_geometry()
  redd_loc_two_no_coords = redd_loc_two %>%
    filter(is.na(location_coordinates_id)) %>%
    st_drop_geometry()
  # Combine
  redd_loc_two = bind_rows(redd_loc_two_no_coords, redd_loc_two_coords)
  # Dump entries in fish_loc_one that have surveys attached
  redd_loc_one = redd_loc_one %>%
    anti_join(redd_loc_two, by = "redd_location_id")
  redd_locations = bind_rows(redd_loc_one, redd_loc_two) %>%
    filter(is.na(db_species_id) | db_species_id == species_id) %>%
    mutate(latitude = round(latitude, 7)) %>%
    mutate(longitude = round(longitude, 7)) %>%
    mutate(redd_survey_date = as.POSIXct(redd_survey_date, tz = "America/Los_Angeles")) %>%
    mutate(survey_dt = format(redd_survey_date, "%m/%d/%Y")) %>%
    filter( is.na(redd_survey_date) | redd_survey_date >= (as.Date(survey_date) - months(4)) ) %>%
    filter( is.na(redd_survey_date) | redd_survey_date <= as.Date(survey_date) ) %>%
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

# Test
strt = Sys.time()
redd_locs = get_redd_locations(waterbody_id, up_rm, lo_rm, survey_date, species_id)
nd = Sys.time(); nd - strt

# Redd_coordinates query...for setting redd_marker when redd_location row is selected
redd_location_id = redd_locs$redd_location_id[[1]]
get_redd_coordinates = function(redd_location_id) {
  qry = glue("select loc.location_id, lc.location_coordinates_id, ",
             "lc.geom as geometry, ",
             "lc.horizontal_accuracy as horiz_accuracy, ",
             "datetime(lc.created_datetime, 'localtime') as created_date, lc.created_by, ",
             "datetime(lc.modified_datetime, 'localtime') as modified_date, lc.modified_by ",
             "from location as loc ",
             "inner join location_coordinates as lc on loc.location_id = lc.location_id ",
             "where loc.location_id = '{redd_location_id}'")
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  #con = poolCheckout(pool)
  redd_coordinates = sf::st_read(con, query = qry, crs = 2927)
  dbDisconnect(con)
  #poolReturn(con)
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

# Test
strt = Sys.time()
redd_coords = get_redd_coordinates(redd_location_id)
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













