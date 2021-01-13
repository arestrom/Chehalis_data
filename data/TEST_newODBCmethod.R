#==================================================================
# Test Connection
#==================================================================

# Load libraries
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

# Set significant digits to 14 for lat/lon under the options function
options(digits = 14)

# Production db
 #dbPath <- "S:\\Reg6\\FP\\Lea Ronne\\R6_Spawning_Ground_Surveys.accdb"
 dbPath = "C:\\data\\RStudio\\chehalis_data\\data\\R6_Spawning_Ground_SurveysCopy.accdb"

 # Access Driver Path
 accessDriver <- "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ="

#con <- odbcConnectAccess2007(dbPath)
con <- odbcDriverConnect(paste0(accessDriver, dbPath))
test_data = sqlQuery(con, as.is = TRUE, glue::glue(paste("SELECT * FROM Sex_LUT")))
close(con)
