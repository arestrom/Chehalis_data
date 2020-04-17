#=================================================================
# Generate and LLID_ROUTES copy with .prj file
#
# AS 2020-04-16
#=================================================================

# Libraries
library(remisc)
library(dplyr)
library(glue)
library(sf)

#========================================================
# Get the data
#========================================================

# Read in zero coordinates for each BIDN in flight file
llid_routes = read_sf("C:/data/RStudio/chehalis_data/data/LLID_ROUTES.shp", crs = 2927)
st_crs(llid_routes)

# # Output flt_obs with missing bidns to shapefile
st_write(llid_routes, "C:/data/RStudio/chehalis_data/data/geolib_20200415_LLID.shp")

