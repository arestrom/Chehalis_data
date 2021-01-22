#===========================================================================
# Output WRIA 22 and 23 reach_points for Lea
#
# Notes:
#  1.
#
#  Completed: 2020-04-02
#
# AS 2020-04-02
#===========================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Load libraries
library(DBI)
library(RPostgres)
library(dplyr)
library(remisc)
library(tidyr)
library(sf)
library(stringi)
library(lubridate)
library(glue)
library(odbc)
library(units)
library(openxlsx)

# Set options
options(digits=14)

# Keep connections pane from opening
options("connectionObserver" = NULL)

#=====================================================================================
# Function to get user for database
pg_user <- function(user_label) {
  Sys.getenv(user_label)
}

# Function to get pw for database
pg_pw <- function(pwd_label) {
  Sys.getenv(pwd_label)
}

# Function to get pw for database
pg_host <- function(host_label) {
  Sys.getenv(host_label)
}

# Function to connect to postgres
pg_con_prod = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = pg_host("pg_host_prod"),
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_prod"),
    port = port)
  con
}

#============================================================================
# Import all wria 22 reach points from sg
#============================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name as stream_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, st.stream_id, loc.location_id, loc.river_mile_measure as river_mile, ",
           "loc.location_name, loc.location_description, lc.gid, lc.geom as geometry ",
           "from spawning_ground.waterbody_lut as wb ",
           "left join spawning_ground.stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join spawning_ground.wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "left join spawning_ground.location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join spawning_ground.location_coordinates as lc on loc.location_id = lc.location_id ",
           "left join spawning_ground.location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where wr.wria_code in ('22') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by stream_name")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
sg_points_22 = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Pull out data with geometry for sg_points
sg_all_22 = sg_points_22
sg_points_22 = sg_points_22 %>%
  filter(!is.na(gid))

# Check crs
st_crs(sg_points_22)$epsg

# Organize
sg_points_22 = sg_points_22 %>%
  select(waterbody_id, stream_name,
         llid, cat_code, wria_code, stream_id, location_id,
         river_mile, location_name, location_description,
         gid, geometry)

# Check for duplicated location IDs
any(duplicated(sg_points_22$location_id))
any(is.na(sg_points_22$location_id))

# Create string for query
loc_22_ids = paste0(paste0("'", sg_points_22$location_id, "'"), collapse = ", ")

# Find the most recent year and the survey_types for all location_ids in wria 22
qry = glue("select distinct date_part('year', s.survey_datetime) as survey_year, ",
           "s.upper_end_point_id, s.lower_end_point_id, ",
           "sd.survey_design_type_code as survey_type ",
           "from spawning_ground.survey as s ",
           "inner join spawning_ground.survey_event as se on s.survey_id = se.survey_id ",
           "inner join spawning_ground.survey_design_type_lut as sd on ",
           "se.survey_design_type_id = sd.survey_design_type_id ",
           "where s.upper_end_point_id in ({loc_22_ids}) or ",
           "s.lower_end_point_id in ({loc_22_ids})")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
sd_22 = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Stack and pivot
sd_22_up = sd_22 %>%
  select(survey_year, location_id = upper_end_point_id, survey_type)

sd_22_lo = sd_22 %>%
  select(survey_year, location_id = lower_end_point_id, survey_type)

sd_22_stack = rbind(sd_22_up, sd_22_lo) %>%
  distinct() %>%
  group_by(location_id) %>%
  mutate(last_survey_year = max(survey_year)) %>%
  ungroup() %>%
  select(location_id, last_survey_year, survey_type) %>%
  distinct()

sd_22_pivot = sd_22_stack %>%
  pivot_wider(names_from = survey_type, values_from = last_survey_year)

# Add survey types to sg_points
sg_points_22_st = sg_points_22 %>%
  left_join(sd_22_pivot, by = "location_id")

#============================================================================
# Import all wria 23 reach points from sg
#============================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name as stream_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, st.stream_id, loc.location_id, loc.river_mile_measure as river_mile, ",
           "loc.location_name, loc.location_description, lc.gid, lc.geom as geometry ",
           "from spawning_ground.waterbody_lut as wb ",
           "left join spawning_ground.stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join spawning_ground.wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "left join spawning_ground.location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join spawning_ground.location_coordinates as lc on loc.location_id = lc.location_id ",
           "left join spawning_ground.location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where wr.wria_code in ('23') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by stream_name")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
sg_points_23 = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Pull out data with geometry for sg_points
sg_all_23 = sg_points_23
sg_points_23 = sg_points_23 %>%
  filter(!is.na(gid))

# Check crs
st_crs(sg_points_23)$epsg

# Organize
sg_points_23 = sg_points_23 %>%
  select(waterbody_id, stream_name,
         llid, cat_code, wria_code, stream_id, location_id,
         river_mile, location_name, location_description,
         gid, geometry)

# Check for duplicated location IDs
any(duplicated(sg_points_23$location_id))
any(is.na(sg_points_23$location_id))

# Create string for query
loc_23_ids = paste0(paste0("'", sg_points_23$location_id, "'"), collapse = ", ")

# Find the most recent year and the survey_types for all location_ids in wria 23
qry = glue("select distinct date_part('year', s.survey_datetime) as survey_year, ",
           "s.upper_end_point_id, s.lower_end_point_id, ",
           "sd.survey_design_type_code as survey_type ",
           "from spawning_ground.survey as s ",
           "inner join spawning_ground.survey_event as se on s.survey_id = se.survey_id ",
           "inner join spawning_ground.survey_design_type_lut as sd on ",
           "se.survey_design_type_id = sd.survey_design_type_id ",
           "where s.upper_end_point_id in ({loc_23_ids}) or ",
           "s.lower_end_point_id in ({loc_23_ids})")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
sd_23 = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Stack and pivot
sd_23_up = sd_23 %>%
  select(survey_year, location_id = upper_end_point_id, survey_type)

sd_23_lo = sd_23 %>%
  select(survey_year, location_id = lower_end_point_id, survey_type)

sd_23_stack = rbind(sd_23_up, sd_23_lo) %>%
  distinct() %>%
  group_by(location_id) %>%
  mutate(last_survey_year = max(survey_year)) %>%
  ungroup() %>%
  select(location_id, last_survey_year, survey_type) %>%
  distinct()

sd_23_pivot = sd_23_stack %>%
  pivot_wider(names_from = survey_type, values_from = last_survey_year)

# Add survey types to sg_points
sg_points_23_st = sg_points_23 %>%
  left_join(sd_23_pivot, by = "location_id")

#============================================================================
# Output to shapefiles
#============================================================================

# Output as shapefile...this is the only format Arc can open it seems.
write_sf(sg_points_22_st, dsn = "data/shapefiles/sg_wria22_points.shp", delete_layer = TRUE)
write_sf(sg_points_23_st, dsn = "data/shapefiles/sg_wria23_points.shp", delete_layer = TRUE)

#============================================================================
# Output to Excel....to simplify do coordinate conversions in sql
#============================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name as stream_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, st.stream_id, loc.location_id, loc.river_mile_measure as river_mile, ",
           "loc.location_name, loc.location_description, ",
           "ST_Y(ST_Transform(ST_Centroid(lc.geom), 4326)) as latitude, ",
           "ST_X(ST_Transform(ST_Centroid(lc.geom), 4326)) as longitude, ",
           "lc.gid ",
           "from spawning_ground.waterbody_lut as wb ",
           "left join spawning_ground.stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join spawning_ground.wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "left join spawning_ground.location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join spawning_ground.location_coordinates as lc on loc.location_id = lc.location_id ",
           "left join spawning_ground.location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where wr.wria_code in ('22') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by stream_name")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
sg_coords_22 = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name as stream_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, st.stream_id, loc.location_id, loc.river_mile_measure as river_mile, ",
           "loc.location_name, loc.location_description, ",
           "ST_Y(ST_Transform(ST_Centroid(lc.geom), 4326)) as latitude, ",
           "ST_X(ST_Transform(ST_Centroid(lc.geom), 4326)) as longitude, ",
           "lc.gid ",
           "from spawning_ground.waterbody_lut as wb ",
           "left join spawning_ground.stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join spawning_ground.wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "left join spawning_ground.location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join spawning_ground.location_coordinates as lc on loc.location_id = lc.location_id ",
           "left join spawning_ground.location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where wr.wria_code in ('23') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by stream_name")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
sg_coords_23 = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Add survey_types
sg_coords_22 = sg_coords_22 %>%
  left_join(sd_22_pivot, by = "location_id")

# Add survey_types
sg_coords_23 = sg_coords_23 %>%
  left_join(sd_23_pivot, by = "location_id")

# Output with styling
num_cols = ncol(sg_coords_22)
out_name = paste0("data/sg_coords22_scharpf.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "SGCoords", gridLines = TRUE)
writeData(wb, sheet = 1, sg_coords_22, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)

# Output with styling
num_cols = ncol(sg_coords_23)
out_name = paste0("data/sg_coords23_scharpf.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "SGCoords", gridLines = TRUE)
writeData(wb, sheet = 1, sg_coords_23, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)





