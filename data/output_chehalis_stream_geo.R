#===========================================================================
# Output stream geometry for Lea's team
#
# Notes:
#  1. To get the latest LLID data from geolib...open geolib LLID_ROUTES in qgis. Then
#     export as a geopackage. Set CRS to 2927 first. Default settings will
#     work. Takes a few minutes to download. Then use code below to trim
#     to WRIAs 22 and 23.
#  2. Some substantial changes occurred on 2020-03-12. One of the most dramatic was
#     Sherwood Creek, which was essentially split into Hensen Creek for the lower
#     portion and Sherwood Creek for part of the upper portion. So RMs will no
#     longer be meaningful and should be replaced by coordinates and point descriptors.
#
#  Completed: 2020-03-12
#
# AS 2020-03-12
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

# # Get table of valid units
# units_table = valid_udunits(quiet = TRUE)

#=========================================================================
# Get stream data from SG
#=========================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wr.wria_code, wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "st.stream_id, st.gid, st.geom ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where stream_id is not null and wria_code in ('22', '23') ",
           "order by waterbody_name")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
streams_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Pull out cases where LLID is duplicated
dup_streams = streams_st %>%
  st_drop_geometry() %>%
  group_by(llid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(llid) %>%
  distinct() %>%
  inner_join(streams_st, by = "llid")

# Pull out only first instance of chehalis dup streams
streams_st = streams_st %>%
  group_by(llid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq == 1L) %>%
  select(-n_seq)

#=========================================================================
# Output for Lea
#=========================================================================

# # Output
# st_crs(streams_st)$epsg
# # Write chehalis subset to local
# # unique(streams_st$wria_code)
# write_sf(streams_st, "data/chehalis_geo_2020-03-16.gpkg")
# write_sf(streams_st, "data/chehalis_geo_2020-03-16.gdb") # Does not work....driver issue...No go from QGIS either...Exported .shp

#=========================================================================
# Get all waterbody data
#=========================================================================

#=========================================================================
# Get stream data from SG
#=========================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wr.wria_code, wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where stream_id is null and wria_code in ('22', '23') ",
           "order by waterbody_name")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
waterbody = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Pull out cases where LLID is duplicated
dup_streams = waterbody %>%
  group_by(waterbody_id) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(waterbody_id) %>%
  distinct() %>%
  inner_join(waterbody, by = "waterbody_id")

# # Output with styling
# num_cols = ncol(waterbody)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "Chehalis_NoLLID.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "NoLLID", gridLines = TRUE)
# writeData(wb, sheet = 1, waterbody, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)


