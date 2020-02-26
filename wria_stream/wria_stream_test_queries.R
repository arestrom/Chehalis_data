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

# Get wrias
get_wrias = function() {
  qry = glue("select distinct wr.wria_code || ' ' || wr.wria_description as wria_name ",
             "from wria_lut as wr ",
             "order by wria_name")
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  wria_list = DBI::dbGetQuery(con, qry) %>%
    pull(wria_name)
  dbDisconnect(con)
  return(wria_list)
}

# Test
strt = Sys.time()
(wrias = get_wrias())
nd = Sys.time(); nd - strt

# # Get streams Method 1....works
# chosen_wria = wrias[[1]]
# get_streams_one = function(chosen_wria) {
#   qry = glue("select distinct wb.waterbody_id, wb.waterbody_name as stream_name, ",
#              "wb.waterbody_name, wb.latitude_longitude_id as llid, ",
#              "wb.stream_catalog_code as cat_code, wr.wria_id, st.stream_id, ",
#              "wr.wria_code || ' ' || wr.wria_description as wria_name, st.geom as geometry ",
#              "from waterbody_lut as wb ",
#              "inner join stream as st on wb.waterbody_id = st.waterbody_id ",
#              "inner join location as loc on wb.waterbody_id = loc.waterbody_id ",
#              "inner join wria_lut as wr on loc.wria_id = wr.wria_id ",
#              "order by stream_name")
#   con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
#   streams_st = sf::st_read(con, query = qry)
#   dbDisconnect(con)
#   streams_st = streams_st %>%
#     filter(wria_name %in% chosen_wria)
#   return(streams_st)
# }
#
# # Test
# strt = Sys.time()
# streams = get_streams_one(chosen_wria)
# nd = Sys.time(); nd - strt

# Get streams....Method 2....much faster, from 2.61 seconds to 0.42 seconds (6.2 times as fast)
chosen_wria = wrias[[2]]
get_streams = function(chosen_wria) {
  qry = glue("select distinct wb.waterbody_id, wb.waterbody_name as stream_name, ",
             "wb.waterbody_name, wb.latitude_longitude_id as llid, ",
             "wb.stream_catalog_code as cat_code, wr.wria_id, st.stream_id, ",
             "wr.wria_code || ' ' || wr.wria_description as wria_name, st.geom as geometry ",
             "from waterbody_lut as wb ",
             "inner join stream as st on wb.waterbody_id = st.waterbody_id ",
             "inner join location as loc on wb.waterbody_id = loc.waterbody_id ",
             "inner join wria_lut as wr on loc.wria_id = wr.wria_id ",
             "where wria_name = '{chosen_wria}' ",
             "order by stream_name")
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  streams_st = sf::st_read(con, query = qry)
  dbDisconnect(con)
  return(streams_st)
}

# Test
strt = Sys.time()
streams = get_streams(chosen_wria)
nd = Sys.time(); nd - strt

# Get years
waterbody_id = streams$waterbody_id[[1]]
get_data_years = function(waterbody_id) {
  qry = glue("select distinct strftime('%Y', datetime(s.survey_datetime, 'localtime')) as data_year ",
             "from survey as s ",
             "inner join location as up_loc on s.upper_end_point_id = up_loc.location_id ",
             "inner join location as lo_loc on s.lower_end_point_id = lo_loc.location_id ",
             "where up_loc.waterbody_id = '{waterbody_id}' ",
             "or lo_loc.waterbody_id = '{waterbody_id}' ",
             "order by data_year desc")
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  year_list = DBI::dbGetQuery(con, qry) %>%
    pull(data_year)
  dbDisconnect(con)
  return(year_list)
}

# Test
strt = Sys.time()
years = get_data_years(waterbody_id)
nd = Sys.time(); nd - strt

