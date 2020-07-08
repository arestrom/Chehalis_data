#==============================================================
# Application to edit data for spawning_ground database
#
# Notes for Lea...form related
#  1. Should change logins for consistency, so created_by can be pulled from start portion.
#     Could be underscore or . If data entry is listed as "real_time" can I just use observer?
#     It looks like observer is always just one person....better than using VAR...
#  2. Ask if first and last names should be used for observers. I prefer just
#     last name.
#  3. Why is run year defined at header level? Need to fix run_year assignments.
#  4. Can data_source always be assigned to WDFW? Is a data_source_unit needed?
#  5. If required data are missing...may want to just assign a default value,
#     upload, then output a file with problematic data?
#  6. Can I just use observers for data_submitter...I am for now.
#  7. What is Clarity Pool in other_observations?
#     Answer: Standardized location where pool clarity is measured...from Region 5
#  8. What is new_survey_bottom and new_survey_top?
#     Answer: Used by Region 5 to designate new upper and lower end-points.
#             Ask Reg 5 if this is needed in database...where to store?
#
# Notes on install R 4.0.2
#  1. Only needed to do remotes::install_github("arestrom/iformr")
#     and               remotes::install_github("arestrom/remisc")
#     otherwise all packages installed and updated correcly from
#     RStudio prompt when opening project global.R. Everything
#     appears to work correctly...including leaflet.extras.
#     Should also install Rtools4 and add path:
#     PATH="${RTOOLS40_HOME}\usr\bin;${PATH}"
#
# Notes:
#  1. Very strange error using the pool and dbplyr query for
#     get_beaches(). I had to wrap output with as.data.frame().
#     I did not get this with the original standard odbc query.
#     Each row of the datatable held all rows. Probably related
#     to difference in tibble behavior. See:
#     https://onunicornsandgenes.blog/2019/10/06/using-r-when-weird-errors-occur-in-packages-that-used-to-work-check-that-youre-not-feeding-them-a-tibble/
#  2. dbplyr does not recognize geometry columns. Need standard
#     text query. Can use odbc package + sf and pull out lat lons.
#  3. For DT updates: https://stackoverflow.com/questions/56879672/how-to-replacedata-in-dt-rendered-in-r-shiny-using-the-datatable-function
#     https://dev.to/awwsmm/reactive-datatables-in-r-with-persistent-filters-l26
#  4. Can not use reactives for pulling data for use in DTs
#     They do not fire properly to update tables. Just use
#     query functions from global directly. I tested by just
#     putting a reactive between functions in beach_data and
#     got failures right away. Probably should do thorough test
#     of eventReactives.
#  5. Do not use rownames = FALSE in DT. Data will not reload
#     when using replaceData() function.
#  6. For sourcing server code: https://r-bar.net/organize-r-shiny-list-source/
#                               https://shiny.rstudio.com/articles/scoping.html
#  7. For wria_stream code to work...needed to use eventReactive(), Solved problem
#     with weird firing of querys and leaked pool.
#  8. See: https://www.endpoint.com/blog/2015/08/12/bucardo-postgres-replication-pgbench
#     for possible replication scenarios.
#  9. To prune merged git branches:
#     a, Check which branches have been merged:       git branch --merged
#     b, Once branch has been deleted from remote:    git fetch -p            # To prune those no longer on remote
#     c, To delete branch from local:                 git branch -d <branch>
#
#
# ToDo:
#  1. Add animation to buttons as in dt_editor example.
#  2. Add validate and need functions to eliminate crashes
#  3. Make sure users are set up with permissions and dsn's.
#  4. Allow map modals to be resizable and draggable.
#     Try the shinyjqui or sortable package:
#     https://github.com/nanxstats/awesome-shiny-extensions
#  5. Set data_source order using number of surveys in category.
#     Can do a query of data to arrange by n, then name.
#  6. Add modal screen to validate clarity type is chosen along
#     with clarity_meter
#  7. For survey, survey intent and waterbody meas, the modal for
#     no row selected does not fire. Only survey comment works.
#     Adding req to selected_survey_comment_data reactive kills
#     the modal response. But removing req from others causes
#     errors to occur in reactives below.
#  8. Need to dump and reload all lakes data...using new layer
#     Dale is creating for me. Look for intersecting polygons
#     before uploading and dump any duplicates. Dale's layer
#     is omitting the marshland.
#  9. Need to scan all existing stream geometry for overlapping
#     segments...then dump those and reset sequences. Need to
#     add code to look for overlapping segments in scripts to
#     upload all geometry. Join line segments to one line per llid.
# 10. Add input$delete observers to all shinyjs disable code
#     See example in redd_substrate_srv code.
# 11. Check that select inputs are ordered optimally. Use
#     example code in redd_substrate_global as example to
#     order by levels.
# 12. Use descriptions in Data Source drop-down. Codes too cryptic.
# 13. Need to add code to edit modals to make sure all
#     required fields have values entered. See fish_location.
#     Or use validate...need?
# 14. Wrap all database operations in transactions before
#     porting to the fish.spawning_ground schema.
#     See examples in dbWithTransactions() DBI docs.
# 15. In survey code...add incomplete_survey_type and
#     data_source_unit lut values as selects
# 16. Add code to limit the number of possible length
#     measurements to the number of items in length_type
#     lut list.
# 17. Verify all screens...especially edit, do not remove
#     required values by backspacing and updating. !!!!!
# 18. Change select for year in wria_stream_ui.R to
#     shinywidgets pickerSelect multiple.
# 19. Need css for radius on textInput boxes
# 20. Get rid of extra channel and orientation lut function
#     for fish in fish_location_global.R. Can reuse
#     function from redd_location.
# 21. Find css to narrow sidebar in header boxplus.
# 22. Add spinners or progress bar when loading locations.
# 23. See example code "current_redd_locations" in redd_encounter_srv.R
#     as example of how to possibly simplify a bunch of repeat
#     invocations of get_xxx global functions.
# 24. Try to use two year location queries for all carcass locations,
#     WRIA and stream. Then filter in memory using reactives? To
#     pre-run location queries for fish and redds, print stats
#     on n-surveys, n-redds, n-carcasses on front-page
# 25. Consider adding theme selector to set background
#     colors, themes, etc. Look at bootstraplib package.
# 26. Consider using shinyjs to add class "required_field" directly
#     to each required element. Then just use one css entry for all.
# 27. FUNCTIONS get_wrias() and get_streams() in wria_stream....CAN AND SHOULD BE OPTIMIZED !!!!!!
# 28. Add raster tile coverage for full offline capability... !!!
# 29. Eventually test with lidar and raytracer mapping.
# 30. All datetime values be stored in sqlite as UTC. Dates stored as text after review
#     of options. Updated globals to convert to local in sql rather than lubridate.
#     Sqlite datetime('localtime') function was tested and is dst enabled.
# 31. Update all names in inputs and DT columns to more readable format.
# 32. Add code to unselect row in parent table whenever a row is deleted in child table.
#     Use example from survey_comment_srv code.
# 33. Consider using fish_location_insert code from here in salmon_data (parameterized...not postgis sql)
# 34. Check all st_read arguments to make sure they include crs = 2927!!!!!!!!!!!!!
# 35. For update of sqlite data to main pg DB, need to include any new locations
#     or streams entered to local.
# 36. Change stream drop-down code to use display_name not full waterbody_name
# 37. Change required fields in reach_point to omit river_mile. We should start
#     weaning off RMs and go with codes, descriptors and coords instead.
# 38. Wrap "add_end_points" reactive code in mobile_import_srv.R in try-catch...with toastr?.
# 39. Need to add an other_observations and passage_feature accordians at the survey level...along
#     with interface to add or edit location.
# 40. Need script to sync central DB location data with local.
# 41. Need interface for media. Copy code from mykos.
# 42.
#
# AS 2020-06-24
#==============================================================

# Load libraries
library(shiny)
library(shinydashboard)
library(shinydashboardPlus)
library(shinyTime)
library(shinyjs)
library(tippy)
library(RSQLite)
library(glue)
library(tibble)
library(DBI)
library(pool)
library(dplyr)
library(DT)
library(tibble)
library(leaflet)
library(mapedit)
library(leaflet.extras)
library(sf)
library(lubridate)
library(remisc)
library(shinytoastr)
library(stringi)
#library(reactlog)

# Keep connections pane from opening
options("connectionObserver" = NULL)
#options(shiny.reactlog = TRUE)
#reactlogShow()

# # Read .rds data
wria_polys = readRDS("www/wria_polys.rds")

# Read content definitions of data-entry screens
source("dashboard/dash_header.R")
source("dashboard/dash_leftsidebar.R")
source("wria_stream/wria_stream_ui.R")
source("wria_stream/wria_stream_global.R")
source("survey/survey_ui.R")
source("survey/survey_global.R")
source("survey_comment/survey_comment_ui.R")
source("survey_comment/survey_comment_global.R")
source("survey_intent/survey_intent_ui.R")
source("survey_intent/survey_intent_global.R")
source("waterbody_meas/waterbody_meas_ui.R")
source("waterbody_meas/waterbody_meas_global.R")
source("survey_event/survey_event_ui.R")
source("survey_event/survey_event_global.R")
source("fish_location/fish_location_ui.R")
source("fish_location/fish_location_global.R")
source("fish_encounter/fish_encounter_ui.R")
source("fish_encounter/fish_encounter_global.R")
source("individual_fish/individual_fish_ui.R")
source("individual_fish/individual_fish_global.R")
source("fish_length_measurement/fish_length_measurement_ui.R")
source("fish_length_measurement/fish_length_measurement_global.R")
source("redd_location/redd_location_ui.R")
source("redd_location/redd_location_global.R")
source("redd_encounter/redd_encounter_ui.R")
source("redd_encounter/redd_encounter_global.R")
source("individual_redd/individual_redd_ui.R")
source("individual_redd/individual_redd_global.R")
source("redd_substrate/redd_substrate_ui.R")
source("redd_substrate/redd_substrate_global.R")
source("reach_point/reach_point_ui.R")
source("reach_point/reach_point_global.R")
source("mobile_import/mobile_import_ui.R")
source("mobile_import/mobile_import_global.R")

# Define globals ================================================================

# # Switch to RPostgres....works, but need to change placeholders in separate branch...then do PR.
# pool = pool::dbPool(RSQLite::SQLite(), dbname = "database/spawning_ground_lite.sqlite", host = "localhost")

# Test using onedrive
chehalis_lite_path = "C:/Users/stromas/OneDrive - Washington State Executive Branch Agencies/chehalis_lite/spawning_ground_lite.sqlite"
pool = pool::dbPool(RSQLite::SQLite(), dbname = chehalis_lite_path, host = "localhost")

# Define functions =============================================================

#==========================================================================
# Get centroid of waterbody to use in interactive maps
#==========================================================================

# Stream centroid query
get_stream_centroid = function(waterbody_id) {
  qry = glue("select DISTINCT st.waterbody_id, ",
             "st.geom as geometry ",
             "from stream as st ",
             "where st.waterbody_id = '{waterbody_id}'")
  con = poolCheckout(pool)
  stream_centroid = sf::st_read(con, query = qry, crs = 2927) %>%
    mutate(stream_center = st_centroid(geometry)) %>%
    mutate(stream_center = st_transform(stream_center, 4326)) %>%
    mutate(center_lon = as.numeric(st_coordinates(stream_center)[,1])) %>%
    mutate(center_lat = as.numeric(st_coordinates(stream_center)[,2])) %>%
    st_drop_geometry() %>%
    select(waterbody_id, center_lon, center_lat)
  poolReturn(con)
  return(stream_centroid)
}

# Stream centroid query
get_stream_bounds = function(waterbody_id) {
  qry = glue("select DISTINCT st.waterbody_id, ",
             "st.geom as geometry ",
             "from stream as st ",
             "where st.waterbody_id = '{waterbody_id}'")
  con = poolCheckout(pool)
  stream_bounds = sf::st_read(con, query = qry, crs = 2927)
  poolReturn(con)
  stream_bounds = stream_bounds %>%
    st_transform(., 4326) %>%
    st_cast(., "POINT", warn = FALSE) %>%
    mutate(lat = as.numeric(st_coordinates(geometry)[,2])) %>%
    mutate(lon = as.numeric(st_coordinates(geometry)[,1])) %>%
    st_drop_geometry() %>%
    mutate(min_lat = min(lat),
           min_lon = min(lon),
           max_lat = max(lat),
           max_lon = max(lon)) %>%
    select(waterbody_id, min_lat, min_lon, max_lat, max_lon) %>%
    distinct()
  return(stream_bounds)
}

# Function to close pool
onStop(function() {
  poolClose(pool)
})


