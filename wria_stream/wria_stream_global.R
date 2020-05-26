
get_wrias = function() {
  qry = glue("select distinct wr.wria_code || ' ' || wr.wria_description as wria_name ",
             "from wria_lut as wr ",
             "order by wria_name")
  con = poolCheckout(pool)
  wria_list = DBI::dbGetQuery(con, qry) %>%
    pull(wria_name)
  poolReturn(con)
  return(wria_list)
}

# Since no spatial join ability in sqlite need two queries, then join using sf
get_streams = function(chosen_wria) {
  # First query
  qry_one = glue("select distinct wb.waterbody_id, wb.waterbody_name as stream_name, ",
                 "wb.waterbody_name, wb.latitude_longitude_id as llid, ",
                 "wb.stream_catalog_code as cat_code, st.stream_id, st.geom as geometry ",
                 "from waterbody_lut as wb ",
                 "inner join stream as st on wb.waterbody_id = st.waterbody_id ",
                 "order by stream_name")
  # Second query
  qry_two = glue("select distinct wria_id, wria_code || ' ' || wria_description as wria_name, ",
                 "wria_code, geom as geometry ",
                 "from wria_lut")
  con = poolCheckout(pool)
  streams_st = sf::st_read(con, query = qry_one, crs = 2927)
  wria_st = sf::st_read(con, query = qry_two, crs = 2927)
  poolReturn(con)
  # Add wria_code, then filter using sf
  streams_st = streams_st %>%
    st_join(wria_st) %>%
    filter(wria_code == chosen_wria) %>%
    select(waterbody_id, stream_name, waterbody_name, llid,
           cat_code, wria_id, stream_id, wria_name)
  return(streams_st)
}

# get_streams = function(chosen_wria) {
#   qry = glue("select distinct wb.waterbody_id, wb.waterbody_name as stream_name, ",
#              "wb.waterbody_name, wb.latitude_longitude_id as llid, ",
#              "wb.stream_catalog_code as cat_code, wr.wria_id, st.stream_id, ",
#              "wr.wria_code || ' ' || wr.wria_description as wria_name, st.geom as geometry ",
#              "from waterbody_lut as wb ",
#              "inner join stream as st on wb.waterbody_id = st.waterbody_id ",
#              "inner join location as loc on wb.waterbody_id = loc.waterbody_id ",
#              "inner join wria_lut as wr on loc.wria_id = wr.wria_id ",
#              "where wr.wria_code = '{chosen_wria}' ",
#              "order by stream_name")
#   con = poolCheckout(pool)
#   streams_st = sf::st_read(con, query = qry, crs = 2927)
#   poolReturn(con)
#   return(streams_st)
# }

get_data_years = function(waterbody_id) {
  qry = glue("select distinct strftime('%Y', datetime(s.survey_datetime, 'localtime')) as data_year ",
             "from survey as s ",
             "inner join location as up_loc on s.upper_end_point_id = up_loc.location_id ",
             "inner join location as lo_loc on s.lower_end_point_id = lo_loc.location_id ",
             "where up_loc.waterbody_id = '{waterbody_id}' ",
             "or lo_loc.waterbody_id = '{waterbody_id}' ",
             "order by data_year desc")
  con = poolCheckout(pool)
  year_list = DBI::dbGetQuery(con, qry) %>%
    pull(data_year)
  poolReturn(con)
  return(year_list)
}

get_end_points = function(waterbody_id) {
  qry = glue("select distinct loc.location_id, loc.river_mile_measure as river_mile, ",
             "loc.location_description as rm_desc ",
             "from location as loc ",
             "inner join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
             "where location_type_description in ('Reach boundary point', 'Section break point') ",
             "and waterbody_id = '{waterbody_id}'")
  con = poolCheckout(pool)
  end_points = DBI::dbGetQuery(con, qry)
  poolReturn(con)
  end_points = end_points %>%
    arrange(river_mile) %>%
    mutate(rm_label = if_else(is.na(rm_desc), as.character(river_mile),
                              paste0(river_mile, " ", rm_desc))) %>%
    select(location_id, rm_label)
  return(end_points)
}
