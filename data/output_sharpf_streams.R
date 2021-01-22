#===========================================================================
# Output stream geometry to shapefile for Mike Scharpf and Dale Gombert
#
# Notes:
#  1.
#
#  Completed: 2021-01-21
#
# AS 2021-01-21
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

# # Get table of valid units
# units_table = valid_udunits(quiet = TRUE)

#=========================================================================
# Get stream data from SG that has defined geometry
#=========================================================================

# Define query to get wria 22 streams
qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name as stream_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, st.stream_id, st.gid, st.geom ",
           "from spawning_ground.waterbody_lut as wb ",
           "left join spawning_ground.stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join spawning_ground.location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join spawning_ground.wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where st.stream_id is not null and wr.wria_code in ('22') ",
           "order by stream_name")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
streams_22 = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Pull out cases where LLID is duplicated
dup_streams = streams_22 %>%
  st_drop_geometry() %>%
  group_by(llid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(llid) %>%
  distinct() %>%
  inner_join(streams_22, by = "llid")

# Define query to get wria 23 streams
qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name as stream_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, st.stream_id, st.gid, st.geom ",
           "from spawning_ground.waterbody_lut as wb ",
           "left join spawning_ground.stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join spawning_ground.location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join spawning_ground.wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where st.stream_id is not null and wr.wria_code in ('23') ",
           "order by stream_name")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
streams_23 = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Pull out cases where LLID is duplicated
dup_streams = streams_23 %>%
  st_drop_geometry() %>%
  group_by(llid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(llid) %>%
  distinct() %>%
  inner_join(streams_23, by = "llid")


#=========================================================================
# Output stream data
#=========================================================================

# Check epsg
st_crs(streams_22)$epsg

# # Output sg_streams as a shape file
write_sf(streams_22, dsn = "data/shapefiles/sg_wria22_streams.shp", delete_layer = TRUE)
write_sf(streams_23, dsn = "data/shapefiles/sg_wria23_streams.shp", delete_layer = TRUE)

#=========================================================================
# Get all waterbody data
#=========================================================================

#=========================================================================
# Get stream data from SG that does not have defined geometry
#=========================================================================

# Define query to get wria 22 data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name as stream_name, ",
           "wr.wria_code, wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code ",
           "from spawning_ground.waterbody_lut as wb ",
           "left join spawning_ground.stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join spawning_ground.location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join spawning_ground.wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where st.stream_id is null and wr.wria_code in ('22') ",
           "order by stream_name")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
streams_22_no_geo = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out cases where LLID is duplicated
dup_streams = streams_22_no_geo %>%
  group_by(waterbody_id) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(waterbody_id) %>%
  distinct() %>%
  inner_join(streams_22_no_geo, by = "waterbody_id")

# Define query to get wria 23 data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_display_name as stream_name, ",
           "wr.wria_code, wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code ",
           "from spawning_ground.waterbody_lut as wb ",
           "left join spawning_ground.stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join spawning_ground.location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join spawning_ground.wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where st.stream_id is null and wr.wria_code in ('23') ",
           "order by stream_name")

# Get values from source
db_con = pg_con_prod(dbname = "FISH")
streams_23_no_geo = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out cases where LLID is duplicated
dup_streams = streams_23_no_geo %>%
  group_by(waterbody_id) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(waterbody_id) %>%
  distinct() %>%
  inner_join(streams_23_no_geo, by = "waterbody_id")


# Output with styling
num_cols = ncol(streams_22_no_geo)
current_date = format(Sys.Date())
out_name = paste0("data/WRIA_22_Streams_No_Geometry.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "NoLLID", gridLines = TRUE)
writeData(wb, sheet = 1, streams_22_no_geo, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)

# Output with styling
num_cols = ncol(streams_23_no_geo)
current_date = format(Sys.Date())
out_name = paste0("data/WRIA_23_Streams_No_Geometry.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "NoLLID", gridLines = TRUE)
writeData(wb, sheet = 1, streams_23_no_geo, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)


