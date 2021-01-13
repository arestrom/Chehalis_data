#=====================================================================================
# Repopulates Previous Redd name option lists for TWS iForm spawning ground surveys
# Code by Danny Warren - WDFW
#
# Edited and tested by AS 2020-09-15
#=====================================================================================

# Always load library and package
#devtools::install_github("arestrom/iformr")

library(iformr)
library(glue)
library(odbc)
library(DBI)
library(jsonlite)
library(dplyr)
library(RODBC)
library(curl)

# Utility Clears the workspace
rm(list = ls(all.names = TRUE))

# Set significant digitss to 14 for lat/lon under the options function
options(digits = 14)

# Set global values....verify values are correct!!!!
# Profile ID
profile_id = 417821L
# Option List ID
option_list_id = 4447515

# Define the Path to Access database where source data resides
dbPath <- "S:\\Reg6\\FP\\Lea Ronne\\R6_Spawning_Ground_Surveys.accdb"

# Get access token
access_token = NULL
while (is.null(access_token)) {
  access_token = iformr::get_iform_access_token(
    server_name = "wdfw",
    client_key_name = "r6production_key",
    client_secret_name = "r6production_secret")
}

# Get the element ids
element_ids <- get_option_list_element_ids(
  server_name = "wdfw",
  profile_id = profile_id,
  optionlist_id = option_list_id,
  element = "key_value",
  access_token = access_token)

# Concatenate id field into list
id_values = data_frame(id = c(element_ids$id))

# Convert list to json
id_values_json = jsonlite::toJSON(id_values, auto_unbox = TRUE)

# Inspect json to make sure list creation worked
# id_values_json

# Delete specified elements from option list
deleted_ids <- delete_options_in_list(
  server_name = "wdfw",
  profile_id = profile_id,
  optionlist_id = option_list_id,
  id_values = id_values_json,
  access_token = access_token)

# Inspect the first five deleted ids
# head(deleted_ids, 5)

# Pull new list values from Access
con <- odbcConnectAccess2007(dbPath)
new_list_values = sqlQuery(con, as.is = TRUE, "SELECT * FROM previous_redds_option_list ORDER BY sort_order")
close(con)

# Covert to json
new_list_values_json <- jsonlite::toJSON(new_list_values, auto_unbox = TRUE)

# Add option elements from locations dataset to the new option list
option_ids <- add_options_to_list(
  server_name = "wdfw",
  profile_id = profile_id,
  optionlist_id = option_list_id,
  option_values = new_list_values_json,
  access_token = access_token)

# Inspect the first five new option list element ids.
# head(option_ids, 5)


#Redd Update Process Complete




