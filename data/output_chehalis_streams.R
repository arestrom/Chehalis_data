#===========================================================================
# Output WRIA 22 and 23 streams for Lea
#
# Notes:
#  1.
#
#  Completed: 2020-03-11
#
# AS 2020-03-11
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

# # Output as a geopackage
# write_sf(sg_streams, "data/sg_streams_2020-04-02.gpkg")

# # Output sg_streams as a shape file
# write_sf(sg_streams, dsn = "data/shapefiles/sg_streams_2020-06-11.shp", delete_layer = TRUE)

# Trim to needed columns
streams = streams_st %>%
  st_drop_geometry() %>%
  select(waterbody_id, waterbody_name, waterbody_display_name,
         llid, cat_code) %>%
  distinct()

# # Output with styling
# num_cols = ncol(streams)
# current_date = format(Sys.Date())
# out_name = paste0("data/", current_date, "_", "ChehalisStreams.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "ChehalisStreams", gridLines = TRUE)
# writeData(wb, sheet = 1, streams, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)
