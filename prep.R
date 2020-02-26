#================================================================
# Prep data for salmon_data shiny interface
#
# AS 2019-06-11
#================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

library(DBI)
library(odbc)
library(pool)
library(dplyr)
library(sf)
library(glue)
library(rmapshaper)
library(lubridate)

# Set options
#options(digits=14)

# Keep connections pane from opening
options("connectionObserver" = NULL)

#=============================================================================
# Get wria data..including geometry (for map)
#=============================================================================

# Query to get wria data
qry = glue("select wria_code, wria_description as wria_name, geom as geometry ",
           "from wria_lut")

# Run the query
db_con = dbConnect(odbc::odbc(), timezone = "UTC", dsn = "local_spawn")
wria_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Filter to needed wrias
wria_st = wria_st %>%
  filter(wria_code %in% c("22", "23"))

# Check size of object
object.size(wria_st)

# Pull out values for drop-down
wria_list = wria_st %>%
  select(wria_code, wria_name) %>%
  st_drop_geometry() %>%
  arrange(as.integer(wria_code)) %>%
  mutate(wria_name = paste0(wria_code, " ", wria_name)) %>%
  select(wria_name)

# # Output wria_list to rds
# saveRDS(wria_list, "www/wria_list.rds")

# Create wa_beaches with tide corrections and stations
wria_polys = wria_st %>%
  st_transform(4326) %>%
  ms_simplify() %>%
  mutate(wria_name = paste0(wria_code, " ", wria_name)) %>%
  select(wria_name, geometry)

# Check size of object
object.size(wria_polys)

# # Output wria_polys to rds
# saveRDS(wria_polys, "www/wria_polys.rds")

