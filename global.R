#==============================================================
# Application to edit data for spawning_ground database
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
# 21. Update all DT column names to more readable versions?
# 22. For editing reach points...only allow editing coordinates
#     for one year previous. Otherwise send request to data manager.
#     Done....Can also suggest adding new point with one decimal
#     difference in river_mile. Then leave historical data alone.
# 23. Find css to narrow sidebar in header boxplus.
# 24. Add spinners or progress bar when loading locations.
# 26. Zoom to stream extent when setting locations via map.
#     Right now marker is sometimes off the screen.
# 28. See example code "current_redd_locations" in redd_encounter_srv.R
#     as example of how to possibly simplify a bunch of repeat
#     invocations of get_xxx global functions.
# 29. Try to use two year location queries for all carcass locations,
#     WRIA and stream. Then filter in memory using reactives? To
#     pre-run location queries for fish and redds, print stats
#     on n-surveys, n-redds, n-carcasses on front-page
# 31. Consider adding theme selector to set background
#     colors, themes, etc. Look at bootstraplib package.
# 32. Consider using shinyjs to add class "required_field" directly
#     to each required element. Then just use one css entry for all.
# 33. FUNCTIONS get_wrias() and get_streams() in wria_stream....CAN AND SHOULD BE OPTIMIZED !!!!!!
# 34. Add raster tile coverage for full offline capability... !!!
# 35. Eventually test with lidar and raytracer mapping.
# 36. Using get_uuid() for all insert code....spatialite CreateUUID() did not always fire.
#     Got rid of spatialite dll's and cleaned up create script to not include default uuid.
#     Got rid of load extension code in global.R. Added get_uuid() to globals only.
# 37. All datetime values be stored in sqlite as UTC. Dates stored as text after review
#     of options. Updated globals to convert to local in sql rather than lubridate.
#     Sqlite datetime('localtime') function was tested and is dst enabled.
# 38. Update all names in inputs and DT columns to more readable format.
# 39. Add code to unselect row in parent table whenever a row is deleted in child table.
#     Use example from survey_comment_srv code.
# 40. Consider using fish_location_insert code from here in salmon_data (parameterized...not postgis sql)
# 41. Check all st_read arguments to make sure they include crs = 2927!!!!!!!!!!!!!
#
# AS 2020-02-26
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
library(sf)
library(lubridate)
library(remisc)
library(leaflet.extras)
library(shinytoastr)
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
# source("individual_fish/individual_fish_ui.R")
# source("individual_fish/individual_fish_global.R")
# source("fish_length_measurement/fish_length_measurement_ui.R")
# source("fish_length_measurement/fish_length_measurement_global.R")
# source("redd_location/redd_location_ui.R")
# source("redd_location/redd_location_global.R")
# source("redd_encounter/redd_encounter_ui.R")
# source("redd_encounter/redd_encounter_global.R")
# source("individual_redd/individual_redd_ui.R")
# source("individual_redd/individual_redd_global.R")
# source("redd_substrate/redd_substrate_ui.R")
# source("redd_substrate/redd_substrate_global.R")
# source("reach_point/reach_point_ui.R")
# source("reach_point/reach_point_global.R")

# Define globals ================================================================

# Switch to RPostgres....works, but need to change placeholders in separate branch...then do PR.
pool = pool::dbPool(RSQLite::SQLite(), dbname = "data/sg_lite.sqlite", host = "localhost")

# Define functions =============================================================

# Function to close pool
onStop(function() {
  poolClose(pool)
})


