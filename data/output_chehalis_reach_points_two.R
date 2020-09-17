#===========================================================================
# Output WRIA 22 and 23 reach_points for Lea
#
# Notes:
#  1.
#
#  Completed: 2020-09-14
#
# AS 2020-09-14
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
           "wr.wria_code, st.stream_id, loc.location_id, loc.river_mile_measure as river_mile, ",
           "loc.location_name, loc.location_description, ",
           "ST_Y(ST_Transform(ST_Centroid(lc.geom), 4326)) as latitude, ",
           "ST_X(ST_Transform(ST_Centroid(lc.geom), 4326)) as longitude ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "inner join wria_lut as wr on st_intersects(st.geom, wr.geom) ",
           "left join location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join location_coordinates as lc on loc.location_id = lc.location_id ",
           "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where stream_id is not null and wr.wria_code in ('22', '23') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by waterbody_name")

# Get values from source
db_con = pg_con_local(dbname = "spawning_ground")
sg_points = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Trim to needed columns
sg_points = sg_points %>%
  select(waterbody_id, waterbody_name, waterbody_display_name,
         llid, cat_code, wria_code, stream_id, location_id,
         river_mile, location_name, location_description,
         latitude, longitude) %>%
  distinct() %>%
  arrange(waterbody_name, river_mile)

# Output with styling
num_cols = ncol(sg_points)
current_date = format(Sys.Date())
out_name = paste0("data/SGPoints_", current_date, ".xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "SGPoints", gridLines = TRUE)
writeData(wb, sheet = 1, sg_points, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)


