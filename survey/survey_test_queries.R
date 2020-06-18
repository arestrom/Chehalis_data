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
  #con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
  wria_list = DBI::dbGetQuery(con, qry) %>%
    pull(wria_name)
  dbDisconnect(con)
  return(wria_list)
}

# Test
strt = Sys.time()
(wrias = get_wrias())
nd = Sys.time(); nd - strt

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
  #con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
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
  qry = glue("select distinct strftime('%Y', s.survey_datetime) as data_year ",
             "from survey as s ",
             "inner join location as up_loc on s.upper_end_point_id = up_loc.location_id ",
             "inner join location as lo_loc on s.lower_end_point_id = lo_loc.location_id ",
             "where up_loc.waterbody_id = '{waterbody_id}' ",
             "or lo_loc.waterbody_id = '{waterbody_id}' ",
             "order by data_year desc")
  #con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  con = dbConnect(RSQLite::SQLite(), dbname = 'database/spawning_ground_lite.sqlite')
  year_list = DBI::dbGetQuery(con, qry) %>%
    pull(data_year)
  dbDisconnect(con)
  return(year_list)
}

# Test
strt = Sys.time()
survey_years = get_data_years(waterbody_id)
nd = Sys.time(); nd - strt

# Function to get header data...use multiselect for year
survey_year = survey_years[[1]]
get_surveys = function(waterbody_id, survey_year) {
  qry = glue("select s.survey_id, datetime(s.survey_datetime, 'localtime') as survey_date, ",
             "ds.data_source_code, ",
             "data_source_code || ': ' || data_source_name as data_source, ",
             "du.data_source_unit_name as data_source_unit, ",
             "sm.survey_method_code as survey_method, ",
             "dr.data_review_status_description as data_review, ",
             "locu.river_mile_measure as upper_rm, ",
             "locl.river_mile_measure as lower_rm, ",
             "locu.location_id as upper_location_id, ",
             "locl.location_id as lower_location_id, ",
             "sct.completion_status_description as completion, ",
             "ics.incomplete_survey_description as incomplete_type, ",
             "datetime(s.survey_start_datetime, 'localtime') as start_time, ",
             "datetime(s.survey_end_datetime, 'localtime') as end_time, ",
             "s.observer_last_name as observer, ",
             "s.data_submitter_last_name as submitter, ",
             "datetime(s.created_datetime, 'localtime') as created_date, ",
             "s.created_by, datetime(s.modified_datetime, 'localtime') as modified_date, ",
             "s.modified_by ",
             "from survey as s ",
             "inner join data_source_lut as ds on s.data_source_id = ds.data_source_id ",
             "inner join data_source_unit_lut as du on s.data_source_unit_id = du.data_source_unit_id ",
             "inner join survey_method_lut as sm on s.survey_method_id = sm.survey_method_id ",
             "inner join data_review_status_lut as dr on s.data_review_status_id = dr.data_review_status_id ",
             "inner join location as locu on s.upper_end_point_id = locu.location_id ",
             "inner join location as locl on s.lower_end_point_id = locl.location_id ",
             "left join survey_completion_status_lut as sct on s.survey_completion_status_id = sct.survey_completion_status_id ",
             "inner join incomplete_survey_type_lut as ics on s.incomplete_survey_type_id = ics.incomplete_survey_type_id ",
             "where strftime('%Y', datetime(s.survey_datetime, 'localtime')) = '{survey_year}' ",
             "and (locu.waterbody_id = '{waterbody_id}' or locl.waterbody_id = '{waterbody_id}')")
  con = dbConnect(RSQLite::SQLite(), dbname = 'data/sg_lite.sqlite')
  surveys = DBI::dbGetQuery(con, qry)
  dbDisconnect(con)
  surveys = surveys %>%
    mutate(survey_date = as.POSIXct(survey_date, tz = "America/Los_Angeles")) %>%
    mutate(survey_date_dt = format(survey_date, "%m/%d/%Y")) %>%
    mutate(start_time = as.POSIXct(start_time, tz = "America/Los_Angeles")) %>%
    mutate(start_time_dt = format(start_time, "%H:%M")) %>%
    mutate(end_time = as.POSIXct(end_time, tz = "America/Los_Angeles")) %>%
    mutate(end_time_dt = format(end_time, "%H:%M")) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    mutate(survey_date = as.Date(survey_date)) %>%
    select(survey_id, survey_date, survey_date_dt, survey_method, up_rm = upper_rm,
           lo_rm = lower_rm, start_time, start_time_dt, end_time, end_time_dt,
           observer, submitter, data_source_code, data_source, data_source_unit,
           data_review, completion, incomplete_type, created_date, created_dt,
           created_by, modified_date, modified_dt, modified_by) %>%
    arrange(survey_date, start_time, end_time, created_date)
  return(surveys)
}

# Test
strt = Sys.time()
survey_data = get_surveys(waterbody_id, survey_year)
nd = Sys.time(); nd - strt

















