# Repopulates Previous Redd name option lists for TWS iForm spawning ground surveys
# Code by Danny Warren - WDFW...

# Always load library and package
#devtools::install_github("arestrom/iformr")

library(iformr)
library(glue)
library(odbc)
library(DBI)
pacman ::p_load(jsonlite, dplyr, RODBC, curl)

#Temporarily hardcode API keys so they are recognized
Sys.setenv(r6production_key = "37b9489a27608cd70c412a15a200b07530db55bd")
Sys.setenv(r6production_secret = "53e08f8e72cb473e03fcae8726fe0a8bab1c37c6")

# Utility Clears the workspace
rm(list=ls(all=TRUE))

# Set significant digitss to 14 for lat/lon under the options function
options(digits=14)

# Define the Path to Access database where source data resides
dbPath <- "S:\\Reg6\\FP\\Lea Ronne\\R6_Spawning_Ground_Surveys.accdb"

# Get access_token
access_token = get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "r6production_key",
  client_secret_name = "r6production_secret")


# Get the element ids
element_ids <- get_option_list_element_ids(
  server_name = "wdfw",
  profile_id = 417821,
  optionlist_id = 4447515,
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
  profile_id = 417821,
  optionlist_id = 4447515,
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
  profile_id = 417821,
  optionlist_id = 4447515,
  option_values = new_list_values_json,
  access_token = access_token)

# Inspect the first five new option list element ids.
# head(option_ids, 5)


#Redd Update Process Complete




