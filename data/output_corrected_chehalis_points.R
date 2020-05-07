#===========================================================================
# Correct WRIA 22 and 23 reach_points from Lea. After adding and editing
# noticed that some points, especially on SF Newaukum were duplicated.
# Checking that only one point per stream is present
#
# Notes:
#  1.
#
#  Completed: 2020-04-
#
# AS 2020-04-29
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
pg_con_local = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

#============================================================================
# Import WRIA 22 and 23 points
#============================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "loc.river_mile_measure as river_mile, loc.location_id, ",
           "loc.location_name, loc.location_description, loc.created_datetime, ",
           "loc.created_by, st.stream_id, lc.gid, lc.geom as geometry ",
           "from location as loc ",
           "left join location_coordinates as lc on loc.location_id = lc.location_id ",
           "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "where wr.wria_code in ('22', '23') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by wb.waterbody_display_name, loc.river_mile_measure")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
sg_points = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Check crs
st_crs(sg_points)$epsg

# Split into two datasets
sg_one = sg_points %>%
  filter(!is.na(gid)) %>%
  st_transform(., 4326) %>%
  mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
  mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
  st_drop_geometry()

# Pull out second set without geometry
sg_two = sg_points %>%
  filter(is.na(gid)) %>%
  st_drop_geometry() %>%
  mutate(longitude = NA_real_) %>%
  mutate(latitude = NA_real_)

# Combine to include all for export to excel
sg_coords = rbind(sg_one, sg_two) %>%
  arrange(waterbody_display_name, river_mile) %>%
  select(waterbody_id, location_id, stream_id, gid, cat_code,
         waterbody_name, waterbody_display_name,
         llid, river_mile, location_name, location_description,
         latitude, longitude)

# Pull out just data with geometry for export to geopackage
sg_points = sg_points %>%
  filter(!is.na(gid)) %>%
  st_transform(., 4326) %>%
  select(waterbody_id, location_id, stream_id, gid, cat_code,
         waterbody_name, waterbody_display_name,
         llid, river_mile, location_name, location_description)

# # Output as a geopackage
# write_sf(sg_points, "data/sg_points_4326_2020-05-07.gpkg")

# # Output with styling
# num_cols = ncol(sg_coords)
# current_date = format(Sys.Date())
# out_name = paste0("data/SGCoords_", current_date, ".xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "SGCoords", gridLines = TRUE)
# writeData(wb, sheet = 1, sg_coords, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

#============================================================================================
# Should Shaeffer Slough be updated? Verify
#============================================================================================



