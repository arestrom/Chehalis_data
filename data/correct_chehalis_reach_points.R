#===========================================================================
# Correct edited WRIA 22 and 23 reach_points from Lea. Some LLIDs and
# waterbodies have been added or changed. So now need to make sure
# points are assigned to the correct waterbody_id
#
# Notes:
#  1.
#
#  Completed: 2020-04-
#
# AS 2020-04-14
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
# Import all stream data from sg that are in WRIAs 22 or 23
#============================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, st.stream_id, st.gid, st.geom as geometry ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "where stream_id is not null and wr.wria_code in ('22', '23') ",
           "order by waterbody_name")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
sg_streams = st_read(db_con, query = qry)
dbDisconnect(db_con)

#============================================================================
# Import all reach data from points file corrected by Lea
#============================================================================

# Get Lea's corrected point data
reach_edits = read_sf("data/SG_Lea_Point_Edits.gdb", layer = "sg_points_lea_edits")

# Pull out coordinates then rename to the default geometry...also takes care of the bogus z value
reach_edits = reach_edits %>%
  mutate(lon = as.numeric(st_coordinates(Shape)[,1])) %>%
  mutate(lat = as.numeric(st_coordinates(Shape)[,2])) %>%
  st_drop_geometry() %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
  select(fid, wbid = waterbody_id, wb_name = waterbody_name, wb_display_name = waterbody_display_name,
         rc_llid = llid, rc_cat_code = cat_code, stid = stream_id, location_id, river_mile, location_name,
         location_description, comments) %>%
  st_transform(2927)

#==========================================================================
# Try to join by nearest_feature
#==========================================================================

# Identify the nearest stream to the reach_edit points
nearest_stream = reach_edits %>%
  mutate(nst = try(st_nearest_feature(reach_edits, sg_streams))) %>%
  st_drop_geometry()

# Create nst for join in streams
stream = sg_streams %>%
  st_drop_geometry() %>%
  mutate(nst = seq(1, nrow(sg_streams)))

# Join stream to nearest_stream by nst
nearest_stream = nearest_stream %>%
  inner_join(stream, by = "nst") %>%
  select(fid, wbid, waterbody_id, wb_name, waterbody_name, wb_display_name,
         waterbody_display_name, rc_llid, llid, rc_cat_code, cat_code,
         stid, stream_id, gid, location_id, river_mile, location_name,
         location_description, comments, nst)

# Pull out the cases where wbid match and check more carefully
waterbody_matches = nearest_stream %>%
  filter(wbid == waterbody_id)

# Pull out cases where wbid does not match
waterbody_no_match = nearest_stream %>%
  filter(!wbid == waterbody_id)





















# Pull out data with geometry for sg_points
sg_all = sg_points
sg_points = sg_points %>%
  filter(!is.na(gid))

# Check crs
st_crs(sg_points)$epsg

# Set gid to sequential
sg_points = sg_points %>%
  select(waterbody_id, waterbody_name, waterbody_display_name,
         llid, cat_code, wria_code, stream_id, location_id,
         river_mile, location_name, location_description,
         gid, geometry)

# Output as shapefile...this is the only format Arc can open it seems.
# write_sf(sg_points, dsn = "data/sg_points_2020-04-02.shp", delete_layer = TRUE)

# Set gid to sequential
sg_points = sg_points %>%
  mutate(fid = seq(1, nrow(sg_points))) %>%
  select(fid, waterbody_id, waterbody_name, waterbody_display_name,
         llid, cat_code, wria_code, stream_id, location_id,
         river_mile, location_name, location_description, geometry) %>%
  st_transform(., 4326)

# Pull out lat lons
sg_coords = sg_points %>%
  mutate(longitude = as.numeric(st_coordinates(geometry)[,1])) %>%
  mutate(latitude = as.numeric(st_coordinates(geometry)[,2])) %>%
  st_drop_geometry() %>%
  select(fid, waterbody_id, waterbody_name, waterbody_display_name,
         llid, cat_code, wria_code, stream_id, location_id,
         river_mile, location_name, location_description,
         longitude, latitude)

# # Output as a geopackage
# write_sf(sg_points, "data/sg_points_4326_2020-04-02.gpkg")

# Trim to needed columns
sg_points_no_coords = sg_all %>%
  st_drop_geometry() %>%
  filter(is.na(gid)) %>%
  select(waterbody_id, waterbody_name, waterbody_display_name,
         llid, cat_code, wria_code, stream_id, location_id,
         river_mile, location_name, location_description) %>%
  distinct() %>%
  arrange(waterbody_name, river_mile)

# # Output with styling
# num_cols = ncol(sg_points_no_coords)
# current_date = format(Sys.Date())
# out_name = paste0("data/SGPointsNoCoords_", current_date, ".xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "SGPointsNoCoords", gridLines = TRUE)
# writeData(wb, sheet = 1, sg_points_no_coords, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

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
