#=================================================================
# Load SGS with data from Chehalis SG 
# 
# NOTES: 
#  1. 
#
# ToDo:
#  1. 
#
# THIS UPLOAD: 
#  1. 
#
# ABANDONED....IT DOES NOT MAKE SENSE TO PUSH DATA BACK TO SGS GIVEN
#              CHANGES IN STREAMS AND REACHES. 
#  
# All combined, 379 records were appended. 83 in Survey
#
# AS 2020-01-13
#=================================================================

# Clear workspace
rm(list=ls(all.names = TRUE))

library(remisc)
library(odbc)
library(RODBC)
library(DBI)
library(lubridate)
library(dplyr)
library(formattable)
library(stringi)
library(tidyr)
library(glue)
library(RPostgres)
library(tibble)
library(lubridate)

# Set globals
create_dt = substr(format(Sys.time()), 1, 10)
since_id = 0L
wdfw_reg = as.integer(4)
login = 'stromas'

# Keep connections pane from opening
options("connectionObserver" = NULL)

# SET THE DATABASE CONNECTION
#SGS_CON = 'R_SGS_WinAuth'
#SGS_CON = 'R_To_SGSB'

#============================================================================
# Define functions
#============================================================================

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
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

nAppend = function() {
  nsrv = if(exists("survey")) nrow(survey) else 0
  nsrvd = if(exists("survey_detail")) nrow(survey_detail) else 0
  nlive = if(exists("live")) nrow(live) else 0
  ndead = if(exists("dead")) nrow(dead) else 0
  nredd = if(exists("fish_redd")) nrow(fish_redd) else 0
  ncarc = if(exists("ind_carcass")) nrow(ind_carcass) else 0
  ncmark = if(exists("carcass_marks")) nrow(carcass_marks) else 0
  nvals = c(nsrv, nsrvd, nlive, ndead, nredd, ncarc, ncmark)
  nrec = tibble(SGS_Table = c('SURVEY', 'SURVEY_DETAIL', 'LIVE_FISH',
                                  'DEAD_FISH', 'FISH_REDDS', 'INDIVIDUAL_CARCASS',
                                  'INDIVIDUAL_CARCASS_MARK'),
                    Records_to_Append = nvals)
  nrec
}

# Function to get survey_id
get_survey_id = function(create_dt, create_login, dsn = SGS_CON) {
  qry = glue("SELECT SURVEY_Id AS survey_id, tempid ",
             "FROM SURVEY ",
             "WHERE tempid > 0 AND Create_DT = '{create_dt}' ",
             "AND Create_WDFWLogin_Id = '{create_login}'")
  con = odbcConnect(dsn)
  dat = sqlQuery(con, qry)
  close(con)
  dat
}

# Function to get survey_detail_id
get_survey_detail_id = function(create_dt, create_login, dsn = SGS_CON) {
  qry = glue("SELECT SURVEY_DETAIL_Id AS survey_detail_id, tempid ",
             "FROM SURVEY_DETAIL ",
             "WHERE tempid > 0 AND Create_DT = '{create_dt}' ",
             "AND Create_WDFWLogin_Id = '{create_login}'")
  con = odbcConnect(dsn)
  dat = sqlQuery(con, qry)
  close(con)
  dat
}

# Function to get live_id
get_live_id = function(create_dt, create_login, dsn = SGS_CON) {
  qry = glue("SELECT LIVE_FISH_Id AS live_id, SURVEY_DETAIL_Id as survey_detail_id ",
             "FROM LIVE_FISH ",
             "WHERE Create_DT = '{create_dt}' ",
             "AND Create_WDFWLogin_Id = '{create_login}'")
  con = odbcConnect(dsn)
  dat = sqlQuery(con, qry)
  close(con)
  dat
}

# Function to get dead_id
get_dead_id = function(create_dt, create_login, dsn = SGS_CON) {
  qry = glue("SELECT DEAD_FISH_Id AS dead_id, SURVEY_DETAIL_Id as survey_detail_id ",
             "FROM DEAD_FISH ",
             "WHERE Create_DT = '{create_dt}' ",
             "AND Create_WDFWLogin_Id = '{create_login}'")
  con = odbcConnect(dsn)
  dat = sqlQuery(con, qry)
  close(con)
  dat
}

# Function to get dead_id
get_redd_id = function(create_dt, create_login, dsn = SGS_CON) {
  qry = glue("SELECT FISH_REDDS_Id AS redd_id, SURVEY_DETAIL_Id as survey_detail_id ",
             "FROM FISH_REDDS ",
             "WHERE Create_DT = '{create_dt}' ",
             "AND Create_WDFWLogin_Id = '{create_login}'")
  con = odbcConnect(dsn)
  dat = sqlQuery(con, qry)
  close(con)
  dat
}

# Function to get dead_id
get_carcass_id = function(create_dt, create_login, dsn = SGS_CON) {
  qry = glue("SELECT INDIVIDUAL_CARCASS_Id AS carcass_id, SURVEY_DETAIL_Id as survey_detail_id, ",
             "Universal_Biological_Id ",
             "FROM INDIVIDUAL_CARCASS ",
             "WHERE Create_DT = '{create_dt}' ",
             "AND Create_WDFWLogin_Id = '{create_login}'")
  con = odbcConnect(dsn)
  dat = sqlQuery(con, qry, as.is = TRUE)
  close(con)
  dat
}

# Function to get dead_id
get_carcass_mark_id = function(create_dt, create_login, dsn = SGS_CON) {
  qry = glue("SELECT INDIVIDUAL_CARCASS_MARK_Id AS carcass_mark_id, INDIVIDUAL_CARCASS_Id as carcass_id ",
             "FROM INDIVIDUAL_CARCASS_MARK ",
             "WHERE Create_DT = '{create_dt}' ",
             "AND Create_WDFWLogin_Id = '{create_login}'")
  con = odbcConnect(dsn)
  dat = sqlQuery(con, qry, as.is = TRUE)
  close(con)
  dat
}

# #===================================================
# # APPEND NEWLY ADDED STREAMS IN SGS TO SGSB IF NEEDED
# # COMMENT OUT OTHERWISE
# #===================================================
# 
# # # If a new stream was added to SGS during first run of script with R_SGS_WinAuth use this code to add to SGSB
# # # Get max STREAM_LUT_Id from SGSB
# # con = odbcConnect("R_To_SGSB")
# # maxst_sgsb = sqlQuery(con, as.is = TRUE,
# #                       paste(sep='',
# #                             "SELECT MAX(STREAM_LUT_Id) ",
# #                             "FROM STREAM_LUT"))
# # close(con)
# # 
# # # Get max STREAM_LUT_Id from SGS
# # con = odbcConnect("R_SGS_WinAuth")
# # new_st = sqlQuery(con, as.is = TRUE,
# #                   paste(sep='',
# #                         "SELECT * ",
# #                         "FROM STREAM_LUT ",
# #                         "WHERE STREAM_LUT_Id >= ", maxst_sgsb))
# # close(con)
# # 
# # # Inspect
# # # new_st
# # maxst_sgsb
# # 
# # # Get rid of last row in SGSB
# # STREAM_LUT = subset(new_st, !STREAM_LUT_Id == as.integer(maxst_sgsb))
# # 
# # # RUN APPEND TO SGS REACH TABLE
# # # Set identity insert statement
# # con = odbcConnect("R_To_SGSB")
# # sqlSave(con, STREAM_LUT, rownames=FALSE, append=TRUE, verbose=TRUE, fast=FALSE)
# # close(con)
# # 
# # # CHECK THE APPENDED DATA TO VERIFY ALL RECORDS WERE APPENDED
# # #=============================================================
# # 
# # con = odbcConnect("R_To_SGSB")
# # appended = sqlQuery(con,as.is=TRUE,
# #                     paste(sep='',
# #                           "SELECT * ",
# #                           "FROM STREAM_LUT ",
# #                           "WHERE STREAM_LUT_Id > ", maxst_sgsb," ",
# #                           "ORDER BY STREAM_LUT_Id"))
# # close(con)
# # 
# # if(!identical(dim(appended)[1],dim(STREAM_LUT)[1])) {
# #   cat("\nWarning: Check the output. Incorrect number of records were appended\n\n")
# # } else {
# #   cat("\nAll streams were successfully appended!\n\n",sep='')
# # }
# 
# # ===================================================
# # APPEND NEWLY ADDED REACHES IN SGS TO SGSB IF NEEDED
# # COMMENT OUT OTHERWISE
# # ===================================================
# 
# # If a new reach was added to SGS during first run of script with R_SGS_WinAuth use this code to add to SGSB
# # Get max STREAM_REACH_Id from SGSB
# con = odbcConnect("R_To_SGSB")
# maxrc_sgsb = sqlQuery(con, as.is = TRUE,
#                       paste(sep='',
#                             "SELECT MAX(STREAM_REACH_Id) ",
#                             "FROM STREAM_REACH"))
# close(con)
# 
# # Get max STREAM_REACH_Id from SGS
# con = odbcConnect("R_SGS_WinAuth")
# new_rc = sqlQuery(con, as.is = TRUE,
#                   paste(sep='',
#                         "SELECT * ",
#                         "FROM STREAM_REACH ",
#                         "WHERE STREAM_REACH_Id >= ", maxrc_sgsb))
# close(con)
# 
# # Inspect
# # new_rc
# maxrc_sgsb
# 
# # Get rid of last row in SGSB
# STREAM_REACH = subset(new_rc, !STREAM_REACH_Id == as.integer(maxrc_sgsb))
# 
# # RUN APPEND TO SGS REACH TABLE
# # Set identity insert statement
# con = odbcConnect("R_To_SGSB")
# sqlQuery(con,'SET IDENTITY_INSERT STREAM_REACH ON')
# sqlSave(con, STREAM_REACH, rownames=FALSE,append=TRUE,varTypes=vtype_rc,
#         verbose=TRUE, fast=FALSE)
# sqlQuery(con,'SET IDENTITY_INSERT STREAM_REACH OFF')
# close(con)
# 
# # CHECK THE APPENDED DATA TO VERIFY ALL RECORDS WERE APPENDED
# # =============================================================
# 
# con = odbcConnect("R_To_SGSB")
# appended = sqlQuery(con,as.is=TRUE,
#                     paste(sep='',
#                           "SELECT * ",
#                           "FROM STREAM_REACH ",
#                           "WHERE STREAM_REACH_Id > ", maxrc_sgsb," ",
#                           "ORDER BY STREAM_REACH_Id"))
# close(con)
# 
# if(!identical(dim(appended)[1],dim(STREAM_REACH)[1])) {
#   cat("\nWarning: Check the output. Incorrect number of records were appended\n\n")
# } else {
#   cat("\nAll reaches were successfully appended!\n\n",sep='')
# }

#================================================================================
# GET page_id of parent form. Needed to extract records from form.
#================================================================================

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Run function to get ID of a specific page ID given the page name. 
# Edit arguments as needed
page_id <- get_page_id(
  server_name = "wdfw", 
  profile_id = 417672, 
  page_name = "sgs_d13_form",
  limit = 100, 
  offset = 0,
  access_token = access_token)

# Inspect the page_id
page_id

#================================================================================
# GET selected records from the parent form
#================================================================================

# Get since_id from mobile_archive
db_con_local = pg_con(
  host_label = "pg_host_local",
  dbname = "mobile_archive",
  user_label = "pg_user",
  pw_label = "pg_pwd_local",
  port = "5432")

# Connect to database and retrive a count of records in a table
max_parent_id = DBI::dbGetQuery(db_con_local, "select max(parent_record_id) from d13_iform_base_v2")
DBI::dbDisconnect(db_con_local)

# FIRST RUN WITH THIS FORM...SET TO ZERO
# max_parent_id = 0L

# Get start_id as either 0 (since_id) or last parent_record_id uploaded
start_id = max_parent_id + 1

# From stored filter
fields = paste("id, survey_uuid, created_date, created_by, created_location, created_device_id, ",
               "survey_date, fish_stat_week, survey_date_text, stream, stream_name_text, ",
               "lower_river_score, lower_river_mile_waypoint, lower_river_mile_location_description, ",
               "upper_river_score, upper_river_mile_waypoint, upper_river_mile_location_description, ",
               "data_source, observers_text, data_submitter, survey_method")

# Set id to ascending order and pull only records greater than the last_id
field_string <- paste0("id:<(>\"", start_id, "\"),", fields)

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Define function to loop through 1000 record retrievals  (29 records at 2:03 pm)
parent_records = get_all_records(
  server_name = "wdfw",
  profile_id = 417672,
  page_id = page_id,
  fields = "fields",
  limit = 1000,
  offset = 0,
  access_token = access_token,
  field_string = field_string,
  since_id = since_id)

#===================================================================
# Process the parent form records
#===================================================================

# Rename id to parent_record_id for more explicit joins to subform data
parent_records <- parent_records %>% 
  rename(parent_record_id = id)

# Convert date values
parent_records = parent_records %>% 
  mutate(created_date = iformr::idate_time(created_date)) %>% 
  mutate(created_time = remisc::get_text_item(created_date, item = 2)) %>% 
  mutate(created_date = substr(created_date, 1, 10)) %>% 
  select(parent_record_id, survey_uuid, created_date, created_time, created_by, 
         created_location, created_device_id, survey_start_date = survey_date, fish_stat_week,
         stream_id = stream, stream_name = stream_name_text, lo_rm = lower_river_score, 
         lo_rm_gps = lower_river_mile_waypoint, lo_rm_desc = lower_river_mile_location_description, 
         up_rm = upper_river_score, up_rm_gps = upper_river_mile_waypoint, 
         up_rm_desc = upper_river_mile_location_description, data_source, 
         observers = observers_text, data_submitter, survey_method)

# Join reach_id, get stream ids
st_ids = paste0(unique(parent_records$stream_id), collapse = ", ")

# Get all stream reaches for streams in st_ids
qry = glue::glue("SELECT STREAM_LUT_Id AS stream_id, STREAM_REACH_ID AS reach_id, ",
                 "Upper_River_Mile_Meas AS up_rm, Lower_River_Mile_Meas as lo_rm ",
                 "FROM STREAM_REACH ",
                 "WHERE STREAM_LUT_Id IN ({st_ids})")
# Run query
con <- dbConnect(odbc::odbc(), SGS_CON)
rc = dbGetQuery(con, qry)
dbDisconnect(con)

# Check for reversed rms
chk_rm = parent_records %>% 
  filter(lo_rm > up_rm)

# ADD if block to reverse if it ever becomes an issue
if (nrow(chk_rm) > 0) {
  cat("\nWARNING: Some RMs reversed. Do not pass go!\n\n")
}

# Check for duplicated reaches
chk_dup_rc = rc %>% 
  group_by(stream_id, up_rm, lo_rm) %>% 
  mutate(nseq = row_number(stream_id)) %>% 
  ungroup() %>% 
  filter(nseq > 1) %>% 
  left_join(rc, by = c("stream_id", "up_rm", "lo_rm"))

if (nrow(chk_dup_rc) > 0) {
  cat("\nWARNING: Duplicate reaches. Do not pass go!\n\n")
  dup_rc = paste0(chk_dup_rc$reach_id.y, collapse = ", ")
  dup_rc
}

# # Check for existing survey in duplicate reaches
# con = odbcConnect(SGS_CON)
# xsurv = sqlQuery(con, glue::glue("SELECT * FROM SURVEY ",
#                                  "WHERE STREAM_REACH_Id IN ({dup_rc})"))
# close(con)
# 
# # Update survey table prior to deleting duplicate reaches
# con = odbcConnect(SGS_CON)
# sqlQuery(con,
#          paste(sep='',
#                "UPDATE SURVEY ",
#                "SET STREAM_REACH_Id = 37312 ",
#                "WHERE STREAM_REACH_Id = 41305"))
# close(con)
# 
# # Delete the reach no longer needed
# con = odbcConnect(SGS_CON)
# sqlQuery(con,as.is=TRUE,
#          paste(sep='',
#                "DELETE STREAM_REACH ",
#                "WHERE STREAM_REACH_Id = 41305"))
# close(con)

# Add to parent_records
np1 = nrow(parent_records)
parent_records = parent_records %>% 
  mutate(stream_id = as.integer(stream_id)) %>% 
  mutate(up_rm = as.numeric(up_rm)) %>% 
  mutate(lo_rm = as.numeric(lo_rm)) %>% 
  left_join(rc, by = c("stream_id", "up_rm", "lo_rm"))

# # THIS TIME ONLY !!!!!!!!!!!!!!!!!!!!!!!!!
# parent_records = parent_records %>% 
#   mutate(lo_rm = if_else(is.na(lo_rm), 4.2, lo_rm))

# Check if any rows added
np2 = nrow(parent_records)
if (!identical(np1, np2)) {
  cat("\nWARNING: Extra rows added. Do not pass go!\n\n")
}

# Pull out reaches with missing RMs
new_rc = parent_records %>% 
  filter(is.na(reach_id)) %>% 
  select(stream_id, stream_name, up_rm, lo_rm, up_rm_gps, up_rm_desc,
         lo_rm_gps, lo_rm_desc) %>% 
  distinct() 

# Convert empty strings to na
new_rc[3:8] = lapply(new_rc[3:8], set_na)

# Warn if new reaches needed
if (nrow(new_rc) > 0) {
  cat("\nWARNING: New reaches needed. Use code below.\n\n")
} else {
  cat("\nNo new reaches needed. Ok to proceed\n\n")
}

#============================================================================
# Sequentially add in descriptions and coordinates
#============================================================================

# Process location info
if (nrow(new_rc) > 0) {
  # Pull out reaches with missing RMs
  if (any(is.na(new_rc$up_rm_gps))) {
    cat("\nNo coordinates. Skip to next section. Verify next time.\n\n")
  } else {
    new_rc = new_rc %>% 
      mutate(up_lat = get_text_item(up_rm_gps, 1, ",")) %>% 
      mutate(up_lon = get_text_item(up_rm_gps, 2, ",")) %>%
      mutate(up_acc = get_text_item(up_rm_gps, 5, ",")) %>%
      mutate(up_lat = as.numeric(get_text_item(up_lat, 2, ":"))) %>% 
      mutate(up_lon = as.numeric(get_text_item(up_lon, 2, ":"))) %>%
      mutate(up_acc = as.numeric(get_text_item(up_acc, 2, ":"))) %>%
      mutate(lo_lat = get_text_item(lo_rm_gps, 1, ",")) %>% 
      mutate(lo_lon = get_text_item(lo_rm_gps, 2, ",")) %>%
      mutate(lo_acc = get_text_item(lo_rm_gps, 5, ",")) %>%
      mutate(lo_lat = as.numeric(get_text_item(lo_lat, 2, ":"))) %>% 
      mutate(lo_lon = as.numeric(get_text_item(lo_lon, 2, ":"))) %>%
      mutate(lo_acc = as.numeric(get_text_item(lo_acc, 2, ":"))) %>%
      select(-c(up_rm_gps, lo_rm_gps)) %>% 
      arrange(stream_id, lo_rm, up_rm) 
  }
  
  # Pull out core fields
  nrc = new_rc %>% 
    select(stream_id, stream_name, up_rm, lo_rm) %>% 
    distinct()
  
  # Pull out lower descriptions and get the best if more than one
  rc_desc_lo = new_rc %>% 
    select(stream_id, up_rm, lo_rm, lo_rm_desc) %>% 
    filter(!is.na(lo_rm_desc)) %>% 
    arrange(stream_id, up_rm, lo_rm, desc(lo_rm_desc)) %>% 
    group_by(stream_id, up_rm, lo_rm) %>% 
    mutate(nseq = row_number(lo_rm_desc)) %>% 
    ungroup() %>% 
    filter(nseq == 1) %>% 
    select(-nseq)
  
  # Add to nrc
  nrc = nrc %>% 
    left_join(rc_desc_lo, by = c("stream_id", "up_rm", "lo_rm"))
  
  # Pull out upper descriptions and get the best if more than one
  rc_desc_up = new_rc %>% 
    select(stream_id, up_rm, lo_rm, up_rm_desc) %>% 
    filter(!is.na(up_rm_desc)) %>% 
    arrange(stream_id, up_rm, lo_rm, desc(up_rm_desc)) %>% 
    group_by(stream_id, up_rm, lo_rm) %>% 
    mutate(nseq = row_number(up_rm_desc)) %>% 
    ungroup() %>% 
    filter(nseq == 1) %>% 
    select(-nseq)
  
  # Add to nrc
  nrc = nrc %>% 
    left_join(rc_desc_up, by = c("stream_id", "up_rm", "lo_rm"))
  
  if (any(is.na(new_rc$up_rm_gps))) {
    cat("\nNo coordinates. Skip to next section. Verify next time.\n\n")
    # Create RM description if missing
    nrc = nrc %>% 
      mutate(up_lat = NA_real_) %>% 
      mutate(lo_lat = NA_real_) %>% 
      mutate(up_lon = NA_real_) %>% 
      mutate(lo_lon = NA_real_) %>% 
      mutate(rm_desc = NA_character_) %>% 
      select(-c(lo_rm_desc, up_rm_desc))
  } else {
    # Pull out upper coordinates and get the best set if more than one
    rc_up_coords = new_rc %>% 
      select(stream_id, up_rm, lo_rm, up_lat, up_lon, up_acc) %>% 
      filter(!is.na(up_acc)) %>% 
      arrange(stream_id, up_rm, lo_rm, up_acc) %>% 
      group_by(stream_id, up_rm, lo_rm) %>% 
      mutate(nseq = row_number(up_acc)) %>% 
      ungroup() %>% 
      filter(nseq == 1) %>% 
      select(-nseq)
    
    # Add to nrc
    nrc = nrc %>% 
      left_join(rc_up_coords, by = c("stream_id", "up_rm", "lo_rm"))
    
    # Pull out lower coordinates and get the best set if more than one
    rc_lo_coords = new_rc %>% 
      select(stream_id, up_rm, lo_rm, lo_lat, lo_lon, lo_acc) %>% 
      filter(!is.na(lo_acc)) %>% 
      arrange(stream_id, up_rm, lo_rm, lo_acc) %>% 
      group_by(stream_id, up_rm, lo_rm) %>% 
      mutate(nseq = row_number(lo_acc)) %>% 
      ungroup() %>% 
      filter(nseq == 1) %>% 
      select(-nseq)
    
    # Add to nrc
    # Naming convention for Dist 13 is lower to upper RM. Verify with other Districts !!!!!!!!!!!
    nrc = nrc %>% 
      left_join(rc_lo_coords, by = c("stream_id", "up_rm", "lo_rm")) %>% 
      mutate(rm_desc = paste0(lo_rm_desc, " to ", up_rm_desc)) %>% 
      select(-c(lo_rm_desc, up_rm_desc))
  }
  
  #============================================================================
  # Upload new reaches to SGS
  #============================================================================
  
  # Get max STREAM_REACH_Id from SGS
  qry = glue::glue("SELECT MAX(STREAM_REACH_Id) ",
                   "FROM STREAM_REACH")
  con <- dbConnect(odbc::odbc(), SGS_CON)
  maxrc_sgs = dbGetQuery(con, qry)
  maxrc = as.integer(maxrc_sgs)
  dbDisconnect(con)
  
  # Add needed variables
  sp_rc = nrc %>%
    mutate(STREAM_REACH_Id = as.integer(seq(maxrc + 1L, maxrc + nrow(nrc)))) %>%
    mutate(STREAM_REACH_Code = NA_character_) %>%
    mutate(Upper_River_Latitude_WGS84_Meas = as.numeric(up_lat)) %>%
    mutate(Lower_River_Latitude_WGS84_Meas = as.numeric(lo_lat)) %>%
    mutate(Upper_River_Longitude_WGS84_Meas = as.numeric(up_lon)) %>%
    mutate(Lower_River_Longitude_WGS84_Meas = as.numeric(lo_lon)) %>%
    mutate(rm_desc = if_else(rm_desc == "NA to NA", NA_character_, rm_desc)) %>% 
    mutate(Obsolete_Flag = as.integer(0)) %>%
    mutate(Obsolete_Date = NA_character_) %>%
    select(STREAM_REACH_Id, STREAM_REACH_Code, STREAM_LUT_Id = stream_id,
           Upper_River_Mile_Meas = up_rm, Lower_River_Mile_Meas = lo_rm,
           Upper_River_Latitude_WGS84_Meas, Upper_River_Longitude_WGS84_Meas,
           Lower_River_Latitude_WGS84_Meas, Lower_River_Longitude_WGS84_Meas,
           Location_Txt = rm_desc, Obsolete_Flag, Obsolete_Date)
  
  # Check to make sure no STREAM_REACH_Code entries > 20 char
  n_code = ifelse(all(is.na(sp_rc$STREAM_REACH_Code)), 0, 
                  max(nchar(sp_rc$STREAM_REACH_Code), na.rm = TRUE))
  if(n_code > 20) {
    cat("\nWARNING: At least one ReachCode exceeds the allowed 20 character limit\n\n",sep='')
  }
  
  # Make sure no NAs in required fields
  any(is.na(sp_rc$STREAM_REACH_Id))
  any(is.na(sp_rc$STREAM_LUT_Id))
  any(is.na(sp_rc$Upper_River_Mile_Meas))
  any(is.na(sp_rc$Lower_River_Mile_Meas))
  any(is.na(sp_rc$Obsolete_Flag))
  
  #=====================================================================================
  # APPEND NEW REACHES
  #=====================================================================================
  
  # # Append
  # con = odbcConnect(SGS_CON)
  # sqlQuery(con,'SET IDENTITY_INSERT STREAM_REACH ON')
  # sqlSave(con, sp_rc, tablename = "STREAM_REACH", rownames = FALSE,
  #         append = TRUE, verbose = FALSE, fast = TRUE)
  # sqlQuery(con,'SET IDENTITY_INSERT STREAM_REACH OFF')
  # close(con)
  
  #=====================================================================================
  # CHECK THE APPENDED DATA TO VERIFY ALL RECORDS WERE APPENDED
  #=====================================================================================
  
  # Define qry
  qry = glue::glue("SELECT * ",
                   "FROM STREAM_REACH ",
                   "WHERE STREAM_REACH_Id > {maxrc} ",
                   "ORDER BY STREAM_REACH_Id")
  
  # Run query
  con <- dbConnect(odbc::odbc(), SGS_CON)
  appended = dbGetQuery(con, qry)
  dbDisconnect(con)
  
  if(!identical(nrow(appended), nrow(sp_rc))) {
    cat("\nWarning: Check the output. Incorrect number of records were appended\n\n")
  } else {
    cat("\nAll reaches were appended. OK to proceed.\n\n", sep = '')
  }
  
  # Pull out subset to join to parent_records
  rcs = sp_rc %>% 
    select(new_reach_id = STREAM_REACH_Id, stream_id = STREAM_LUT_Id,
           up_rm = Upper_River_Mile_Meas, lo_rm = Lower_River_Mile_Meas) %>% 
    distinct()
  
  # Add new reaches to parent_records
  parent_records = parent_records %>% 
    left_join(rcs, by = c("stream_id", "up_rm", "lo_rm")) %>% 
    mutate(reach_id = if_else(is.na(reach_id) & !is.na(new_reach_id),
                              new_reach_id, reach_id)) %>% 
    select(parent_record_id, survey_uuid, created_date, created_time, created_by, 
           created_location, created_device_id, survey_start_date, fish_stat_week,
           stream_id, stream_name, reach_id, data_source, observers, data_submitter, 
           survey_method)
  
  # Verify there are no missing reach_ids
  if (any(is.na(parent_records$reach_id))) {
    cat("\nWARNING: There are still missing reach_ids. Do not pass go!\n\n")
  }
  # rm(list = c())  # Clean up some files here !!!!!!!!!!!!!!
}

#============================================================================
# Process final columns in the parent form
# If new reaches were added then rerun to here to get needed values
#============================================================================

# Convert data_source
unique(parent_records$data_source)
parent_records = parent_records %>% 
  mutate(data_source = case_when(
    data_source == "wdfw" ~ "63",
    data_source == "tulalip tribe" ~ "54",
    data_source == "snohomish county" ~ "45",
    data_source == "king_county" ~ "78",
    data_source == "snopud" ~ "82",
    is.na(data_source) ~ NA_character_)) %>% 
  mutate(data_source = as.integer(data_source))
unique(parent_records$data_source)

# Pull out wria from stream_name
# Look at: https://stackoverflow.com/questions/45595272/in-r-remove-all-dots-from-string-apart-from-the-last
# gsub("\\.(?=[^.]*\\.)", "", wria, perl=TRUE)
parent_records = parent_records %>% 
  mutate(wria = gsub("[^0-9.]", "", stream_name)) %>% 
  mutate(wria = gsub("\\.(?=[^.]*\\.)", "", wria, perl=TRUE)) %>% 
  mutate(wria = get_text_item(wria, 1, "\\.")) %>% 
  mutate(wria = as.integer(wria))
unique(parent_records$wria)

# Convert survey_method to id values
unique(parent_records$survey_method)
parent_records = parent_records %>% 
  mutate(survey_method = recode(survey_method,
                                "foot" = "3",
                                "raft" = "4",
                                "heli" = "7",
                                "trap" = "10",
                                "weir" = "12",
                                "hook_and_line" = "14",
                                "boat" = "5")) %>% 
  mutate(survey_method = as.integer(survey_method))
unique(parent_records$survey_method)

# Process observers
unique(parent_records$observers)
parent_records = parent_records %>% 
  mutate(observers = gsub(", ", ";", observers))
unique(parent_records$observers)

# Check stat week
chk_stat_week = parent_records %>% 
  mutate(stat_week = fish_stat_week(survey_start_date)) %>% 
  filter(!fish_stat_week == stat_week)

if (nrow(chk_stat_week) == 0) {
  cat("\nAll stat_week values agree between R and js functions.\n\n")
  rm(chk_stat_week)
} else {
  cat("\nInspect chk_stat_week, possible error in stat_week functions.\n\n")
}

# Clean up
rm(list = c("chk_rm", "new_rc", "rc"))

#================================================================================
# GET page_id for the the survey_comments subform
#================================================================================

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Run function to get ID of a specific page ID given the page name. 
# Edit arguments as needed
page_id <- get_page_id(
  server_name = "wdfw", 
  profile_id = 417672, 
  page_name = "survey_comments_1_1",
  limit = 100, 
  offset = 0,
  access_token = access_token)

# Inspect the page_id
page_id

#================================================================================
# GET selected records from the comment form
#================================================================================

# From stored filter
fields = paste("parent_record_id, id, weather, stream_flow, cfs, visibility, ",
               "general_survey_comments, survey_photos, waypoint")

# Set start_id as the minimum parent_record_id minus one
start_id = min(parent_records$parent_record_id) - 1

# Set id to ascending order and pull only records greater than the last parent_record_id
field_string <- paste0("parent_record_id:<(>\"", start_id, "\"),", fields)

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Define function to loop through 1000 record retrievals
comment_records = get_all_records(
  server_name = "wdfw",
  profile_id = 417672,
  page_id = page_id,
  fields = "fields",
  limit = 1000,
  offset = 0,
  access_token = access_token,
  field_string = field_string,
  since_id = since_id)

#===================================================================
# Process the comment form records
#===================================================================

# Rename id to comment_id for more explicit joins to subform data
comment_records <- comment_records %>% 
  rename(comment_id = id)

# Convert weather to id values. 
# Now a pick list rather than previous multi-select
unique(comment_records$weather)
comment_records = comment_records %>% 
  mutate(weather_id = case_when(
    weather == "sunny" ~ 1L,
    weather == "cloudy" ~ 2L,
    weather == "showers" ~ 3L,
    weather == "raining" ~ 4L,
    weather == "snowing" ~ 5L,
    weather == "windy" ~ 6L,
    is.na(weather) ~ 7L))
unique(comment_records$weather_id)

# Convert steam flow to id values
unique(comment_records$stream_flow)
comment_records = comment_records %>% 
  mutate(stream_flow_id = case_when(
    stream_flow == "low" ~ 2L,
    stream_flow == "medium_low" ~ 6L,
    stream_flow == "medium" ~ 3L,
    stream_flow == "medium_high" ~ 7L,
    stream_flow == "high" ~ 4L,
    stream_flow == "flooding" ~ 5L,
    stream_flow == "dry" ~ 1L,
    stream_flow == "partly_frozen" ~ 8L,
    stream_flow == "frozen" ~ 9L,
    is.na(stream_flow) ~ NA_integer_))
unique(comment_records$stream_flow_id)

# Convert visibility to id values
comment_records = comment_records %>% 
  mutate(visibility_id = case_when(
    visibility == "excellent" ~ 1L,
    visibility == "very_good" ~ 2L,
    visibility == "good" ~ 3L,
    visibility == "fair" ~ 4L,
    visibility == "poor" ~ 5L,
    visibility == "not_surveyable" ~ 6L,
    is.na(visibility) ~ NA_integer_))
unique(comment_records$visibility_id)

# Check cfs
unique(comment_records$cfs)

# Check for entries in survey_photos field
if(any(!is.na(comment_records$survey_photos))) {
  cat("\nWARNING: Need code to handle survey_photos! Ok to ignore for now.\n\n")
} else {
  cat("\nThere were no survey_photos. Ok to proceed.\n\n")
}

# Check for entries in waypoint field
if(any(!is.na(comment_records$waypoint))) {
  cat("\nWARNING: Need code to handle photo waypoint! Ok to ignore for now.\n\n")
} else {
  cat("\nThere were no photo waypoints. Ok to proceed.\n\n")
}

# Organize
comment_records = comment_records %>% 
  select(comment_id, parent_record_id, weather_id, stream_flow_id,
         visibility_id, cfs, general_survey_comments, survey_photos,
         photo_waypoint = waypoint)

#===================================================================
# Combine comment records with parent_records...no more photo form
#===================================================================

# Join comment and parent records
parent_records = parent_records %>% 
  left_join(comment_records, by = "parent_record_id")

# Clean up
rm(list = c("comment_records", "chk_dup_rc"))

#================================================================================
# GET page_id for the species subform
#================================================================================

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Run function to get ID of a specific page ID given the page name. 
# Edit arguments as needed
page_id <- get_page_id(
  server_name = "wdfw", 
  profile_id = 417672, 
  page_name = "species_data_1_1",
  limit = 100, 
  offset = 0,
  access_token = access_token)

# Inspect the page_id
page_id

#================================================================================
# GET selected records from the species form
#================================================================================

# Set start_id as the minimum parent_record_id minus one
start_id = min(parent_records$parent_record_id) - 1

# From stored filter
fields = paste("parent_record_id, id, species, run_type, run_year, predominant_origin, ",
               "survey_type, percent_seen, cwt_detection_method, new_redds, old_redds, ",
               "combined_redds")

# Set id to ascending order and pull only records greater than start_id
field_string <- paste0("parent_record_id:<(>\"", start_id, "\"),", fields)

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Define function to loop through 1000 record retrievals
species_records = get_all_records(
  server_name = "wdfw",
  profile_id = 417672,
  page_id = page_id,
  fields = "fields",
  limit = 1000,
  offset = 0,
  access_token = access_token,
  field_string = field_string,
  since_id = since_id)

#===================================================================
# Process the species form records
#===================================================================

# Reset IDs for species
unique(species_records$species)
species_records = species_records %>% 
  mutate(species_id = case_when(
    species == "chinook" ~ 1L,
    species == "pink" ~ 3L, 
    species == "chum" ~ 2L,
    species == "coho" ~ 4L,
    species == "sockeye" ~ 5L,
    species == "atlantic" ~ 8L,
    species == "unknown_salmon" ~ 0L, 
    species == "steelhead" ~ 6L,
    species == "rainbow_trout" ~ 16L,
    species == "cutthroat" ~ 11L,
    species == "unknown_trout" ~ 28L,
    species == "bull_trout_dolly_varden" ~ 10L,
    species == "pacific_lamprey" ~ 15L,
    species == "western_brook_lamprey" ~ 18L,
    species == "unknown_lamprey" ~ 27L,
    species == "sculpin" ~ 17L,
    species == "mountain_whitefish" ~ 14L,
    species == "northern_pikeminnow" ~ 25L,
    species == "largescale_sucker" ~ 22L,
    species == "peamouth" ~ 23L))
unique(species_records$species_id)

# Convert run_type to id
unique(species_records$run_type)
species_records = species_records %>% 
  mutate(run_type_id = case_when(
    run_type == "fall" ~ 1L,
    run_type == "fall_early" ~ 3L,
    run_type == "winter" ~ 7L,
    run_type == "spring" ~ 5L,
    run_type == "summer" ~ 6L,
    run_type == "unknown" ~ 9L,
    is.na(run_type) ~ 9L))
unique(species_records$run_type_id)

# Convert origin
unique(species_records$predominant_origin)
species_records = species_records %>%
  mutate(origin_id = case_when(
    predominant_origin == "unknown" ~ 4L,
    predominant_origin == "mixed" ~ 3L,
    predominant_origin == "hatchery" ~ 1L,
    predominant_origin == "natural" ~ 2L,
    is.na(predominant_origin) ~ 4L)) 
unique(species_records$origin_id)

# Convert survey_type
unique(species_records$survey_type)
species_records = species_records %>%
  mutate(survey_type_id = case_when(
    survey_type == "index" ~ 1L,
    survey_type == "supp" ~ 2L,
    survey_type == "spot" ~ 3L,
    survey_type == "exploratory" ~ 9L,
    survey_type == "carcass" ~ 5L,
    survey_type == "partial" ~ 6L)) 
unique(species_records$survey_type_id)

# Convert cwt detection
species_records = species_records %>% 
  mutate(cwt_detection_method = as.character(cwt_detection_method)) %>% 
  mutate(cwt_detection_method_id = case_when(
    cwt_detection_method == "electronic" ~ 2L, 
    cwt_detection_method == "visual" ~ 3L,
    cwt_detection_method == "other" ~ 4L,
    is.na(cwt_detection_method) ~ NA_integer_)) %>% 
  select(species_record_id  = id, parent_record_id, species_id, 
         run_type_id, run_year, origin_id, survey_type_id,
         cwt_detection_method_id, percent_seen, new_redds, 
         old_redds, combined_redds)

# Verify no parent_ids in species_records are missing from parent_records
# Can be due to data added after most recent upload..and upload included parent records
# Then needed parent_records might not be available for join
if (!all(species_records$parent_record_id %in% parent_records$parent_record_id)) {
  cat("\nWARNING: Not all needed IDs present in species_records table. Do not pass go!\n\n")
}

# Check which parent_record_ids are missing
par_ids = species_records %>% 
  select(parent_record_id, species_record_id) %>% 
  anti_join(parent_records, by = "parent_record_id")

# Record sum of redds
sum_new_redd = sum(species_records$new_redds, na.rm = TRUE)
sum_old_redd = sum(species_records$old_redds, na.rm = TRUE)

#================================================================================
# GET page_id for the live subform
#================================================================================

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Run function to get ID of a specific page ID given the page name. 
# Edit arguments as needed
page_id <- get_page_id(
  server_name = "wdfw", 
  profile_id = 417672, 
  page_name = "sgs_live_count_1_1",
  limit = 100, 
  offset = 0,
  access_token = access_token)

# Inspect the page_id
page_id

#================================================================================
# GET selected records from the live form
#================================================================================

# Set start_id as the minimum parent_record_id minus one
start_id = min(species_records$species_record_id) - 1

# From stored filter
fields = paste("parent_record_id, id, live, sex, maturity, holding_or_spawning")

# Set id to ascending order and pull only records greater than start_id
field_string <- paste0("parent_record_id:<(>\"", start_id, "\"),", fields)

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Define function to loop through 1000 record retrievals
live_records = get_all_records(
  server_name = "wdfw",
  profile_id = 417672,
  page_id = page_id,
  fields = "fields",
  limit = 1000,
  offset = 0,
  access_token = access_token,
  field_string = field_string,
  since_id = since_id)

#===================================================================
# Process the live form records
#===================================================================

# Reset IDs for sex
unique(live_records$sex)
live_records = live_records %>% 
  dplyr::rename(live_record_id = id,
         species_record_id = parent_record_id) %>% 
  mutate(sex = as.character(sex)) %>% 
  mutate(sex_id = case_when(
    sex == "female" ~ 2L,
    sex == "male" ~ 1L, 
    sex == "not_applicable" ~ 0L,
    sex == "unknown" ~ 3L, 
    is.na(sex) ~ NA_integer_)) 
unique(live_records$sex_id)

# Reset IDs for maturity
unique(live_records$maturity)
live_records = live_records %>% 
  mutate(maturity = as.character(maturity)) %>% 
  mutate(maturity_id = case_when(
    maturity == "adult" ~ 3L,
    maturity == "jack" ~ 2L, 
    maturity == "na" ~ 0L,
    is.na(maturity) ~ NA_integer_)) 
unique(live_records$maturity_id)

# Reset live-type IDs
live_records = live_records %>%
  mutate(holding_or_spawning = as.character(holding_or_spawning)) %>% 
  mutate(live_type_id = case_when(
    holding_or_spawning == "holding" ~ 1L,
    holding_or_spawning == "spawning" ~ 2L, 
    holding_or_spawning == "not_applicable" ~ 0L,
    is.na(holding_or_spawning) ~ NA_integer_)) %>%
  select(live_record_id, species_record_id, live_count = live, lv_sex = sex, 
         lv_sex_id = sex_id, lv_maturity = maturity, lv_maturity_id =maturity_id, 
         live_type = holding_or_spawning, live_type_id) 
unique(live_records$live_type_id)

# Verify no species_record_ids in live_records are missing from speces_records
# Can be due to data added after most recent upload..and upload included parent records
# Then needed parent_records might not be available for join
if (!all(live_records$species_record_id %in% species_records$species_record_id)) {
  cat("\nWARNING: Not all needed IDs present in live_records table. Do not pass go!\n\n")
}

# Check which parent_record_ids are missing
missing_live_ids = live_records %>% 
  select(species_record_id, live_record_id) %>% 
  anti_join(species_records, by = "species_record_id")

# Strip out live record with no match
no_live_id = missing_live_ids$live_record_id
live_records = live_records %>% 
  filter(!live_record_id %in% no_live_id)

# Record sum of live
sum_live = sum(live_records$live_count, na.rm = TRUE)

#================================================================================
# GET page_id for the dead subform
#================================================================================

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Run function to get ID of a specific page ID given the page name. 
# Edit arguments as needed
page_id <- get_page_id(
  server_name = "wdfw", 
  profile_id = 417672, 
  page_name = "sgs_dead_count_1_1",
  limit = 100, 
  offset = 0,
  access_token = access_token)

# Inspect the page_id
page_id

#================================================================================
# GET selected records from the dead form
#================================================================================

# Set start_id as the minimum parent_record_id minus one
start_id = min(species_records$species_record_id) - 1

# From stored filter
fields = paste("parent_record_id, id, dead, sex, maturity, ad_clipped, cwt_detected")

# Set id to ascending order and pull only records greater than start_id
field_string <- paste0("parent_record_id:<(>\"", start_id, "\"),", fields)

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Define function to loop through 1000 record retrievals
dead_records = get_all_records(
  server_name = "wdfw",
  profile_id = 417672,
  page_id = page_id,
  fields = "fields",
  limit = 1000,
  offset = 0,
  access_token = access_token,
  field_string = field_string,
  since_id = since_id)

#===================================================================
# Process the dead form records
#===================================================================

# Reset IDs
dead_records = dead_records %>% 
  rename(dead_record_id = id,
         species_record_id = parent_record_id,
         dead_count = dead) 

# Reset sex IDs
unique(dead_records$sex)
dead_records = dead_records %>% 
  mutate(sex = as.character(sex)) %>% 
  mutate(sex_id = case_when(
    sex == "female" ~ 2L,
    sex == "male" ~ 1L, 
    sex == "not_applicable" ~ 0L,
    sex == "unknown" ~ 3L, 
    is.na(sex) ~ NA_integer_)) 
unique(dead_records$sex_id)

# Reset IDs for maturity
unique(dead_records$maturity)
dead_records = dead_records %>% 
  mutate(maturity = as.character(maturity)) %>% 
  mutate(maturity_id = case_when(
    maturity == "adult" ~ 3L,
    maturity == "jack" ~ 2L, 
    maturity == "na" ~ 0L,
    is.na(maturity) ~ NA_integer_)) 
unique(dead_records$maturity_id)

# Reset IDs for clip status
unique(dead_records$ad_clipped)
dead_records = dead_records %>% 
  mutate(ad_clipped = as.character(ad_clipped)) %>% 
  mutate(ad_clip_id = case_when(
    ad_clipped == "04_did_not_check" ~ 5L,
    ad_clipped == "01_clipped" ~ 1L,
    ad_clipped == "02_not_clipped" ~ 2L,
    ad_clipped == "03_unknown_decayed" ~ 3L,
    ad_clipped == "05_undeterminable" ~ 3L,
    ad_clipped == "06 _not_applicable" ~ 0L,
    is.na(ad_clipped) ~ NA_integer_)) 
unique(dead_records$ad_clip_id)

# Reset IDs for cwt_detected
unique(dead_records$cwt_detected)
dead_records = dead_records %>%
  mutate(cwt_detected = as.character(cwt_detected)) %>% 
  mutate(cwt_detected_id = case_when(
    cwt_detected == "00_no_check" ~ 0L,
    cwt_detected == "01_beep" ~ 1L,
    cwt_detected == "02_no_beep" ~ 2L,
    cwt_detected == "03_no_head" ~ 3L,
    cwt_detected == "04_not_applicable" ~ 0L,
    is.na(cwt_detected) ~ 0L)) %>%
  select(dead_record_id, species_record_id, dead_count, dd_sex = sex, 
         dd_sex_id = sex_id, dd_maturity = maturity, dd_maturity_id = maturity_id, 
         dd_ad_clip = ad_clipped, dd_ad_clip_id = ad_clip_id,
         dd_cwt_detected = cwt_detected, dd_cwt_detected_id = cwt_detected_id)

# Verify no species_record_ids in dead_records are missing from speces_records
# Can be due to data added after most recent upload..and upload included parent records
# Then needed parent_records might not be available for join
if (!all(dead_records$species_record_id %in% species_records$species_record_id)) {
  cat("\nWARNING: Not all needed IDs present in dead_records table. Do not pass go!\n\n")
}

# Check which parent_record_ids are missing
missing_dead_ids = dead_records %>% 
  select(species_record_id, dead_record_id) %>% 
  anti_join(species_records, by = "species_record_id")

# Strip out live record with no match
no_dead_id = missing_dead_ids$dead_record_id
dead_records = dead_records %>% 
  filter(!dead_record_id %in% no_dead_id)

# Record sum of dead
sum_dead = sum(dead_records$dead_count, na.rm = TRUE)

#================================================================================
# GET page_id for the individual carcass subform
#================================================================================

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Run function to get ID of a specific page ID given the page name. 
# Edit arguments as needed
page_id <- get_page_id(
  server_name = "wdfw", 
  profile_id = 417672, 
  page_name = "sgs_individual_carcass_scale_card_data_1_1",
  limit = 100, 
  offset = 0,
  access_token = access_token)

# Inspect the page_id
page_id

#================================================================================
# GET selected records from the individual carcass
#================================================================================

# Set start_id as the minimum parent_record_id minus one
start_id = min(species_records$species_record_id) - 1

# From stored filter
fields = paste("parent_record_id, id, carcass_uuid, sex, maturity, fork_length_cm, ",
               "poh_cm, egg_retention, scale_sample_number, cwt_snout_sample_number, ",
               "final_otolith_sample_number, final_genetic_sample_number, comments, ",
               "ud_clip_, gill_condition, marks_and_tags")

# Set id to ascending order and pull only records greater than start_id
field_string <- paste0("parent_record_id:<(>\"", start_id, "\"),", fields)

# Get access_token
access_token <- get_iform_access_token(
  server_name = "wdfw",
  client_key_name = "RappsD13ClientKey", 
  client_secret_name = "RappsD13ClientSecret")

# Define function to loop through 1000 record retrievals
idead_records = get_all_records(
  server_name = "wdfw",
  profile_id = 417672,
  page_id = page_id,
  fields = "fields",
  limit = 1000,
  offset = 0,
  access_token = access_token,
  field_string = field_string,
  since_id = since_id)

#===================================================================
# Process the idead form records
# Split off carcass marks after all other tables have been created
# and all table IDs assigned
#===================================================================

# New section to handle case of no idead_records
if (nrow(idead_records) > 0 ) {
  # Reset IDs ect.
  idead_records = idead_records %>% 
    dplyr::rename(idead_record_id = id,
                  species_record_id = parent_record_id,
                  marks = marks_and_tags,
                  bio_uuid = carcass_uuid) %>% 
    mutate(fork_length_cm = as.numeric(fork_length_cm)) %>% 
    mutate(fork_length_cm = if_else(fork_length_cm == 0, NA_real_, fork_length_cm)) %>%
    mutate(poh_cm = as.numeric(poh_cm)) %>% 
    mutate(poh_cm = if_else(poh_cm == 0, NA_real_, poh_cm)) %>%
    mutate(gill_condition = as.character(gill_condition)) %>% 
    mutate(gill_condition = case_when(
      gill_condition == "01_4-4" ~ "4/4",
      gill_condition == "02_3-4" ~ "3/4",
      gill_condition == "03_2-4" ~ "2/4",
      gill_condition == "04_1-4" ~ "1/4",
      gill_condition == "05_0-4" ~ "0/4",
      is.na(gill_condition) ~ NA_character_)) %>% 
    mutate(comments = as.character(comments)) %>% 
    mutate(comments = if_else(comments == "", NA_character_, comments)) %>% 
    mutate(icomments = if_else(!is.na(comments) & !is.na(gill_condition), 
                               paste0(comments, "; Gill condition: ", gill_condition),
                               if_else(is.na(comments) & !is.na(gill_condition),
                                       paste0("Gill condition: ", gill_condition),
                                       if_else(!is.na(comments) & is.na(gill_condition),
                                               comments, NA_character_)))) %>% 
    mutate(otolith_number = as.character(final_otolith_sample_number)) %>% 
    mutate(otolith_number = if_else(otolith_number %in% c("no", ""), 
                                    NA_character_, otolith_number)) %>%
    mutate(genetic_number = as.character(final_genetic_sample_number)) %>% 
    mutate(genetic_number = if_else(genetic_number %in% c("no", ""), 
                                    NA_character_, genetic_number)) %>% 
    mutate(marks = as.character(marks)) %>% 
    mutate(ad_clip = if_else(!is.na(marks) & stri_detect_fixed(marks, "ad_clip"), "01_clipped", "02_not_clipped")) %>% 
    mutate(ad_clip = if_else(!is.na(ud_clip_) & stri_detect_fixed(ud_clip_, "yes"), "05_undeterminable", ad_clip)) %>%
    mutate(cwt_detected = if_else(!is.na(marks) & stri_detect_fixed(marks, "coded_wire_tag"), "01_beep", NA_character_)) %>%
    mutate(cwt_detected = if_else(!is.na(cwt_snout_sample_number) & !cwt_snout_sample_number == "", "01_beep", cwt_detected)) %>%
    select(idead_record_id, species_record_id, bio_uuid, sex, maturity, fork_length_cm, poh_cm, 
           egg_retention, scale_sample_number, cwt_snout_sample_number, comments, gill_condition,
           icomments, marks, bio_uuid, otolith_number, genetic_number, iad_clip = ad_clip,
           ud_clip = ud_clip_, icwt_detected = cwt_detected) 
  
  # Verify that all species_record_ids in idead_records are also in dead_records
  idead_ids = unique(idead_records$species_record_id)
  dead_ids = unique(dead_records$species_record_id)
  all(idead_ids %in% dead_ids)
  if (!all(idead_ids %in% dead_ids)) {
    cat("\nWARNING: Not all idead_ids in dead_ids. Investigate!\n\n")
  } else {
    cat("\nAll ids present. Ok to proceed.\n\n")
  }
  
  # # Check missing dead_ids: Result one fish, hook sampled for DNA, then released
  # missing_dead_ids = idead_ids[!idead_ids %in% dead_ids]
  # chk_missing_dead_ids = idead_records %>% 
  #   filter(species_record_id == missing_dead_ids)
  
  # Set sex IDs
  unique(idead_records$sex)
  idead_records = idead_records %>%
    mutate(sex = as.character(sex)) %>% 
    mutate(isex_id = case_when(
      sex == "female" ~ 2L,
      sex == "male" ~ 1L, 
      sex == "not_applicable" ~ 0L,
      sex == "unknown" ~ 3L, 
      is.na(sex) ~ NA_integer_)) 
  unique(idead_records$isex_id)
  
  # Reset IDs for maturity
  unique(idead_records$maturity)
  idead_records = idead_records %>% 
    mutate(maturity = as.character(maturity)) %>% 
    mutate(imaturity_id = case_when(
      maturity == "adult" ~ 3L,
      maturity == "jack" ~ 2L, 
      maturity == "na" ~ 0L,
      is.na(maturity) ~ NA_integer_)) 
  unique(idead_records$imaturity_id)
  
  # Reset IDs for clip status
  unique(idead_records$iad_clip)
  idead_records = idead_records %>% 
    mutate(iad_clip = as.character(iad_clip)) %>% 
    mutate(iad_clip_id = case_when(
      iad_clip == "04_did_not_check" ~ 5L,
      iad_clip == "01_clipped" ~ 1L,
      iad_clip == "02_not_clipped" ~ 2L,
      iad_clip == "03_unknown_decayed" ~ 3L,
      iad_clip == "05_undeterminable" ~ 4L,
      iad_clip == "06 _not_applicable" ~ 0L,
      is.na(iad_clip) ~ 0L)) 
  unique(idead_records$iad_clip_id)
  
  # Process comments
  idead_records = idead_records %>% 
    mutate(icomments = if_else(!is.na(icomments) & !iad_clip_id == 4L, icomments,
                               if_else(!is.na(icomments) & iad_clip_id == 4L,
                                       paste0("UD clip; ", icomments),
                                       if_else(is.na(icomments) & iad_clip_id == 4L, "UD clip",
                                               icomments))))
  
  # Reset IDs for cwt_detected
  unique(idead_records$icwt_detected)
  idead_records = idead_records %>%
    mutate(icwt_detected = as.character(icwt_detected)) %>% 
    mutate(icwt_detected_id = case_when(
      icwt_detected == "00_no_check" ~ 0L,
      icwt_detected == "01_beep" ~ 1L,
      icwt_detected == "02_no_beep" ~ 2L,
      icwt_detected == "03_no_head" ~ 3L,
      icwt_detected == "04_not_applicable" ~ 0L,
      is.na(icwt_detected) ~ 2L))
  
  # Process comments
  idead_records = idead_records %>% 
    mutate(icomments = if_else(!is.na(icomments) & is.na(poh_cm), icomments,
                               if_else(!is.na(icomments) & !is.na(poh_cm),
                                       paste0("POH: ", poh_cm, " cm; ", icomments),
                                       if_else(is.na(icomments) & !is.na(poh_cm),
                                               paste0("POH: ", poh_cm, " cm"),
                                               icomments))))
  # Process rest
  idead_records = idead_records %>% 
    mutate(marks = as.character(marks)) %>% 
    mutate(marks = if_else(marks == "", NA_character_, marks)) %>%
    mutate(carc_num = NA_character_) %>%
    mutate(length = fork_length_cm) %>%
    mutate(length_type_id = 3L) %>%
    mutate(egg_pct = as.numeric(egg_retention)) %>%
    mutate(cwt_code = NA_character_) %>%
    mutate(idead_count = 1L) %>% 
    select(idead_record_id, species_record_id, idead_count, ubid = bio_uuid, carc_num, isex_id, imaturity_id, 
           iad_clip, fork_length_cm, length_type_id, egg_pct, scale_sample_number, cwt_snout_sample_number,
           cwt_code, genetic_number, otolith_number, icomments, marks) %>%
    distinct()
  
  # Verify no species_record_ids in idead_records are missing from species_records
  # Can be due to data added after most recent upload..and upload included parent records
  # Then needed parent_records might not be available for join
  if (!all(idead_records$species_record_id %in% species_records$species_record_id)) {
    cat("\nWARNING: Not all needed IDs present in idead_records table. Do not pass go!\n\n")
  }
  
  # Check which parent_record_ids are missing
  missing_idead_ids = idead_records %>% 
    select(species_record_id, idead_record_id) %>% 
    anti_join(species_records, by = "species_record_id")
  
  # Strip out idead record with no match
  no_idead_id = missing_idead_ids$idead_record_id
  idead_records = idead_records %>% 
    filter(!idead_record_id %in% no_idead_id)
}

#===================================================================
# Add species data to parent_records
#===================================================================

# Check if any species_record_ids are duplicated
if (any(duplicated(species_records$species_record_id))) {
  cat("\nWARNING: Some duplicated species_record IDs. Do not pass go.\n\n")
}

# Check if any parent_record_ids are missing
if (!all(species_records$parent_record_id %in% parent_records$parent_record_id)) {
  cat("\nWARNING: Not all needed IDs present in parent_records table. Do not pass go!\n\n")
}

# Join species_records to parent_records
sp = parent_records %>% 
  left_join(species_records, by = "parent_record_id") %>% 
  distinct()

# Check for duplicated species_record_ids
chk_sp_dup = sp %>% 
  group_by(species_record_id) %>% 
  mutate(nseq = row_number(species_record_id)) %>% 
  ungroup() %>% 
  filter(nseq > 1) %>% 
  select(species_record_id) %>% 
  left_join(sp, by = "species_record_id")

if (nrow(chk_sp_dup) > 0) {
  cat("\nWARNING: Some duplicated rows. Probabably Ok...due to photos. Inspect photo_id and species.\n\n")
}

#===================================================================
# Add live data to sp
#===================================================================

# Check if any live_ids are duplicated
if (any(duplicated(live_records$live_record_id))) {
  cat("\nWARNING: Some duplicated live_record IDs. Do not pass go.\n\n")
}

# Check if any parent_record_ids are missing
if (!all(live_records$species_record_id %in% sp$species_record_id)) {
  cat("\nWARNING: Not all needed IDs present in sp table. Do not pass go!\n\n")
}

# Check which live_records$species_record_ids are missing from sp
lv_ids = live_records %>% 
  select(live_record_id, species_record_id) %>% 
  anti_join(sp, by = "species_record_id")

# Join species_records to parent_records
nrec = nrow(sp)
sp = sp %>% 
  left_join(live_records, by = "species_record_id") %>% 
  distinct()
nrec2 = nrow(sp)
if (!nrec == nrec2) {
  cat("\nWARNING: Rows were added. Inspect chk_dup_live below!\n\n")
}

# Check for duplicated live_record_ids
chk_live_dup = sp %>% 
  group_by(live_record_id) %>% 
  mutate(nseq = row_number(live_record_id)) %>% 
  ungroup() %>% 
  filter(nseq > 1) %>% 
  select(live_record_id) %>% 
  left_join(sp, by = "live_record_id")

if (nrow(chk_live_dup) > 0) {
  cat("\nWARNING: Some duplicated rows. Inspect.\n\n")
}

#===================================================================
# Add dead data to sp
#===================================================================

# Check if any live_ids are duplicated
if (any(duplicated(dead_records$dead_record_id))) {
  cat("\nWARNING: Some duplicated dead_record IDs. Do not pass go.\n\n")
}

# Check if any parent_record_ids are missing
if (!all(dead_records$species_record_id %in% sp$species_record_id)) {
  cat("\nWARNING: Not all needed IDs present in sp table. Do not pass go!\n\n")
}

# Join dead_records to sp
nrec = nrow(sp)
sp = sp %>% 
  left_join(dead_records, by = "species_record_id") %>% 
  distinct()
nrec2 = nrow(sp)
if (!nrec == nrec2) {
  cat("\nWARNING: Rows were added. Inspect chk_dup_dead below!\n\n")
}

# Check for duplicated dead_record_ids
chk_dead_dup = sp %>% 
  group_by(dead_record_id) %>% 
  mutate(nseq = row_number(dead_record_id)) %>% 
  ungroup() %>% 
  filter(nseq > 1) %>% 
  select(dead_record_id) %>% 
  left_join(sp, by = "dead_record_id")

if (nrow(chk_dead_dup) > 0) {
  cat("\nWARNING: Some duplicated rows. Inspect.\n\n")
}

#===================================================================
# Generate unique IDs for Survey table data
#===================================================================

# Verify there are no missing values in grouping columns below
any(is.na(sp$survey_start_date))
any(is.na(sp$reach_id))
any(is.na(sp$observers))
any(is.na(sp$data_source))
any(is.na(sp$data_submitter))
any(is.na(sp$survey_method))

# Make sure character values in grp columns are trimmed, or types are ints
sp$survey_start_date = trimws(sp$survey_start_date)
sp$observers = trimws(sp$observers)
sp$data_submitter = trimws(sp$data_submitter)
typeof(sp$reach_id)
typeof(sp$data_source)
typeof(sp$survey_method)

# Add survey_guid
sp = sp %>% 
  group_by(survey_start_date, reach_id, observers, data_source,
           data_submitter, survey_method, weather_id) %>% 
  mutate(survey_guid = remisc::get_uuid(1L)) %>% 
  ungroup()

# Verify survey_ids
chk_survey_ids = sp[c("survey_guid", "survey_start_date", "reach_id", "survey_method",
                      "observers", "data_source")]

# Check without survey_guid in arrange
chk_survey_ids = chk_survey_ids %>%
  arrange_at(c("survey_start_date", "reach_id", "survey_method",
               "observers", "data_source"))

# Add needed variables for survey table
sp = sp %>%
  mutate(tempid = NA_integer_) %>%
  mutate(WDFW_REGION_LUT_ID = wdfw_reg) %>%
  mutate(CNTY_Code = NA_integer_) %>%
  mutate(Start_Time = NA_character_) %>%
  mutate(Stop_Time = NA_character_) %>%
  mutate(Cubic_Feet_Per_Second_Flow_Meas = cfs) %>%
  mutate(Water_Visibility_Feet_Meas = NA_real_) %>%
  mutate(Water_Temperature_Celsius_Meas = NA_real_) %>%
  mutate(Create_DT = create_dt) %>%
  mutate(Create_WDFWLogin_Id = login) %>%
  mutate(Modify_DT = NA_character_) %>%
  mutate(Modify_WDFWLogin_Id = NA_character_)

# Check survey table required values. Must all be FALSE.
any(is.na(sp$survey_start_date))
any(is.na(sp$wria))
any(is.na(sp$reach_id))
any(is.na(sp$survey_method))
any(is.na(sp$Create_DT))
any(is.na(sp$Create_WDFWLogin_Id))

# Pull out survey table
survey = sp %>%
  select(survey_guid, tempid, SURVEY_Date = survey_start_date,
         WDFW_REGION_LUT_ID, WRIA_LUT_Id = wria, CNTY_Code, Start_Time,
         Stop_Time, STREAM_REACH_Id = reach_id, Observer_Name_Txt = observers,
         DATA_SOURCE_LUT_Id = data_source, Data_Submitter_Last_Name = data_submitter,
         SURVEY_METHOD_LUT_Id = survey_method, WEATHER_TYPE_LUT_Id = weather_id,
         STREAM_FLOW_TYPE_LUT_Id = stream_flow_id, SURVEY_VISIBILITY_TYPE_LUT_Id = visibility_id,
         Cubic_Feet_Per_Second_Flow_Meas, Water_Visibility_Feet_Meas,
         Water_Temperature_Celsius_Meas, Comment_Txt = general_survey_comments, 
         Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>%
  distinct()

# Check for duplicated surveys
chk_survey_dup = survey %>%
  group_by(SURVEY_Date, STREAM_REACH_Id, SURVEY_METHOD_LUT_Id) %>%
  select(SURVEY_Date, STREAM_REACH_Id, SURVEY_METHOD_LUT_Id) %>%
  mutate(nseq = row_number(SURVEY_Date)) %>%
  filter(nseq > 1) %>%
  inner_join(survey, by = c("SURVEY_Date", "STREAM_REACH_Id", "SURVEY_METHOD_LUT_Id"))

# Raise warning if any dups present
if(nrow(chk_survey_dup) > 0) {
  cat("\nWARNING: Some duplicate surveys in xlsx. Investigate!\n\n")
}

if (any(duplicated(survey$survey_guid))) {
  cat("\nWARNING: Some duplicated survey_guids. Do not pass go!\n\n")
}

# # Trim one comment
# survey$Comment_Txt[survey$Comment_Txt == "Chinook counts:\n\nBelow rack: 5\nCedar hole: 34\nMay creek: 81\nBear creek: 100\nHighway 2: 70\nIndividuals: 21\nTOTAL: 311\n\nOne guy fishing bellow startup on the Wallace \n2 boats (6 anglers) between Wallace and sultan \n5 anglers at mouth of sultan "] =
#   "Chinook: Below rack:5; Cedar hole:34; May creek:81; Bear creek:100; Highway 2:70; Individuals:21; TOTAL:311; One angler below Startup; 6 anglers between Wallace and Sultan; 5 anglers mouth of Sultan"

# Check survey for overly long comments
chk_comment = survey %>% 
  filter(!is.na(Comment_Txt)) %>% 
  select(survey_guid, Comment_Txt) %>% 
  mutate(n_chars = nchar(Comment_Txt)) %>% 
  filter(n_chars > 200)

if (nrow(chk_comment) > 0) {
  cat("\nWARNING: Some comments too long. Investigate!\n\n")
  chk_comment$Comment_Txt
}

# Verify there are no missing required fields. ALL MUST BE FALSE
any(is.na(survey$survey_guid))
any(is.na(survey$SURVEY_Date))
any(is.na(survey$WRIA_LUT_Id))
any(is.na(survey$STREAM_REACH_Id))
any(is.na(survey$DATA_SOURCE_LUT_Id))
any(is.na(survey$SURVEY_METHOD_LUT_Id))
any(is.na(survey$Create_DT))
any(is.na(survey$Create_WDFWLogin_Id))

# Get rid of survey columns from sp
sp = sp %>%
  select(-c(WDFW_REGION_LUT_ID, wria, CNTY_Code, Start_Time, Stop_Time, 
            Water_Visibility_Feet_Meas, Water_Temperature_Celsius_Meas))

#============================================================================
# Pull out survey detail table
#============================================================================

# Verify that all species_record_ids in idead_records are also now in sp
if (nrow(idead_records) > 0 ) {
  idead_ids = unique(idead_records$species_record_id)
  any(is.na(idead_ids))
  sp_ids = unique(sp$species_record_id)
  any(is.na(sp_ids))
  all(idead_ids %in% sp_ids)
}

# Add some missing values to sp, get rid of records with no species entries
sp = sp %>%
  filter(!is.na(species_record_id)) %>% 
  mutate(SASI_STOCK_Id = NA_integer_) %>%
  mutate(Steelhead_Redd_Grouping_Cnt = NA_integer_) %>%
  mutate(Species_Comment_Txt = NA_character_)

# Reverify species_ids
sp_ids = unique(sp$species_record_id)
any(is.na(sp_ids))
if (nrow(idead_records) > 0 ) {
  all(idead_ids %in% sp_ids)
}

# Inspect cases of species unknown
chk_unkn = sp %>%
  filter(species_id == 0L)

# Verify there are no missing required values or grouping values
any(is.na(sp$survey_guid))
any(is.na(sp$species_id))
any(is.na(sp$run_type_id))
any(is.na(sp$run_year))
any(is.na(sp$origin_id))
any(is.na(sp$survey_type_id))
any(is.na(sp$cwt_detection_method_id))

# # FILL IN MISSING SURVEY_TYPE SHOULD NOT BE POSSIBLE
# sp = sp %>% 
#   mutate(survey_type = if_else(is.na(survey_type), "01_index", survey_type)) %>% 
#   mutate(survey_type_id = if_else(is.na(survey_type_id), 1L, survey_type_id))

# Make sure character values in grp columns are trimmed, or types are all ints
typeof(sp$survey_guid)
typeof(sp$species_id)
typeof(sp$run_type_id)
typeof(sp$run_year)
typeof(sp$origin_id)
typeof(sp$survey_type_id)
typeof(sp$cwt_detection_method_id)  # Many NAs

# Add survey_detail_guid
sp = sp %>% 
  group_by(survey_guid, species_id, run_type_id, run_year, origin_id,
           survey_type_id, cwt_detection_method_id) %>% 
  mutate(survey_detail_guid = remisc::get_uuid(1L)) %>% 
  ungroup()

# Get rid of bogus cwt-detection method value.
unique(sp$cwt_detection_method_id)
# sp = sp %>%
#   mutate(cwt_detection_method_id = if_else(cwt_detection_method_id == 1L, 
#                                            0L, cwt_detection_method_id))

# Check without survey_detail_id in arrange
chk_survey_detail_ids = sp %>%
  arrange_at(c("survey_guid", "species_id", "run_type_id", "run_year", "origin_id",
               "survey_type_id", "cwt_detection_method_id")) %>%
  select(survey_guid, survey_detail_guid, species_id, run_type_id, run_year, origin_id,
         survey_type_id, cwt_detection_method_id)

# Pull out survey detail table
survey_detail = sp %>%
  select(survey_detail_guid, tempid, survey_guid,
         SGS_SPECIES_LUT_Id = species_id, SGS_RUN_LUT_Id = run_type_id, 
         Run_Yr = run_year, ORIGIN_LUT_Id = origin_id, SASI_STOCK_Id, 
         SURVEY_TYPE_LUT_Id = survey_type_id, Estimated_Seen_Fish_Pct = percent_seen, 
         SGS_TAG_DETECTION_METHOD_LUT_Id = cwt_detection_method_id,
         Steelhead_Redd_Grouping_Cnt, Comment_Txt = Species_Comment_Txt,
         Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>%
  distinct()

# Get rid of some unneeded columns from sp
sp = sp %>%
  select(-c(SASI_STOCK_Id, Steelhead_Redd_Grouping_Cnt,
            Species_Comment_Txt, tempid))

# Check survey_detail for overly long comments
chk_comment = survey_detail %>% 
  filter(!is.na(Comment_Txt)) %>% 
  select(survey_detail_guid, Comment_Txt) %>% 
  mutate(n_chars = nchar(Comment_Txt)) %>% 
  filter(n_chars > 200)

#============================================================================
# Pull out live table...Are zero's being added for all target species !!!!!!!
#============================================================================

# Get live data
live = sp %>%
  select(survey_detail_guid, species_id, lv_sex, lv_sex_id, lv_maturity, 
         lv_maturity_id, live_type_id, live_count, Create_DT, 
         Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id, 
         live_record_id) %>%
  filter(!is.na(live_record_id)) %>% 
  arrange(live_record_id, survey_detail_guid) %>% 
  distinct()

# live$live_record_id[duplicated(live$live_record_id)]

# Check to see if counts match
if (nrow(live) == nrow(live_records)) {
  cat("\nNumber of live records are as expected. Ok to proceed.\n\n")
} else {
  cat("\nWARNING: More live than live records. Probably Ok.\n\n")
}

# Calculate sex, maturity, and clip status
live = live %>%
  mutate(lv_sex_id = if_else(live_count == 0L, 0L, lv_sex_id)) %>%
  mutate(lv_maturity_id = if_else(live_count == 0L, 0L, lv_maturity_id)) %>%
  mutate(live_type_id = if_else(live_count == 0L, 0L, live_type_id)) %>%
  mutate(live_type_id = if_else(is.na(live_type_id), 0L, live_type_id)) %>% 
  mutate(cwt_detected_id = 0L) %>% 
  mutate(ad_clip_id = 0L) %>% 
  select(survey_detail_guid, lv_sex_id, lv_maturity_id, live_type_id,
         cwt_detected_id, ad_clip_id, live_count, Create_DT, 
         Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>%
  arrange(survey_detail_guid, lv_sex_id, lv_maturity_id, live_count)

# Check for missing values for sex and maturity
any(is.na(live$lv_sex_id)) 
any(is.na(live$lv_maturity_id))

# A few cases of sex and maturity not filled...replace with values as needed
live = live %>% 
  mutate(lv_sex_id = if_else(is.na(lv_sex_id) & live_count > 0L, 3L, lv_sex_id)) %>%
  mutate(lv_maturity_id = if_else(is.na(lv_maturity_id) & live_count > 0, 3L, lv_maturity_id)) 

# Add summary count column, and collapse to one value by group
live = live %>%
  mutate(live_count = as.integer(live_count)) %>%
  group_by(survey_detail_guid, lv_sex_id, lv_maturity_id, live_type_id) %>%
  mutate(sum_live_count = sum(live_count)) %>%
  ungroup() %>%
  select(survey_detail_guid, lv_sex_id, lv_maturity_id, live_type_id,
         cwt_detected_id, ad_clip_id, sum_live_count, live_count, 
         Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>%
  distinct()

# Check if summary needed
chk_sum = live %>% 
  filter(!sum_live_count == live_count)

if (nrow(chk_sum) > 0) {
  cat("\nCounts needed to be summarized!\n\n") 
} else {
  cat("\nNo summarized counts needed. One row per survey_detail_id.\n\n")
}

# Get rid of dead_count column
live = live %>% 
  select(survey_detail_guid, lv_sex_id, lv_maturity_id, live_type_id,
         cwt_detected_id, ad_clip_id, live_count = sum_live_count, 
         Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>% 
  distinct()

# Verify there's only one detail_id per record
# Was ok this time...no true duplicates, just different spawn-hold types.
if (any(duplicated(live$survey_detail_guid))) {
  cat("\nWARNING: Some duplicated survey_detail_ids. Add more grouping columns below!\n\n")
} else {
  cat("\nNo duplicated survey_detail_guids. Ok to proceed with only survey_detail_guid as grouping column.\n\n")
}

# Verify there are no missing values in grouping columns or required values
# No duplicated sd_id's so use sd_id as grouping variable
any(is.na(live$survey_detail_guid))
any(is.na(live$lv_sex_id))
any(is.na(live$lv_maturity_id))
any(is.na(live$live_type_id))
any(is.na(live$cwt_detected_id))
any(is.na(live$ad_clip_id))
any(is.na(live$live_count))

# Make sure character values in grp columns are trimmed, or types are all ints
typeof(live$survey_detail_guid)
typeof(live$lv_sex_id)
typeof(live$lv_maturity_id)
typeof(live$live_type_id)
typeof(live$cwt_detected_id)
typeof(live$ad_clip_id)
typeof(live$live_count)

# Add live_guid
live = live %>% 
  group_by(survey_detail_guid, live_type_id, lv_maturity_id) %>% 
  mutate(live_guid = remisc::get_uuid(1L)) %>% 
  ungroup()

# Pull out final columns
live = live %>% 
  select(live_guid, survey_detail_guid,
         FISH_SEX_LUT_Id = lv_sex_id, MATURITY_LUT_Id = lv_maturity_id,
         SGS_LIVE_FISH_TYPE_LUT_Id = live_type_id, CWT_DETECTED_LUT_Id = cwt_detected_id,
         ADIPOSE_CLIP_STATUS_LUT_Id = ad_clip_id, LIVE_FISH_Cnt = live_count,
         Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id)

# Check for duplicated live_fish_id
any(duplicated(live$live_guid))

#============================================================================
# Pull out dead table...Are zero's being added for all target species !!!!!!!
#============================================================================

# Get dead data
dead = sp %>%
  select(survey_detail_guid, species_id, dd_sex, dd_sex_id, dd_maturity, 
         dd_maturity_id, dd_cwt_detected, dd_cwt_detected_id, 
         dd_ad_clip, dd_ad_clip_id, dead_count, Create_DT, 
         Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id, 
         dead_record_id) %>%
  filter(!is.na(dead_record_id)) %>% 
  distinct()

# Check to see if counts match
if (nrow(dead) == nrow(dead_records)) {
  cat("\nNumber of dead records are as expected. Ok to proceed.\n\n")
} else {
  cat("\nWARNING: More dead than dead_records. Probably Ok.\n\n")
}

# Calculate sex, maturity, and clip status
dead = dead %>%
  mutate(dd_sex_id = if_else(dead_count == 0L, 0L, dd_sex_id)) %>%
  mutate(dd_sex_id = if_else(dead_count > 0L & is.na(dd_sex_id), 3L, dd_sex_id)) %>%
  mutate(dd_maturity_id = if_else(dead_count == 0L, 0L, dd_maturity_id)) %>%
  mutate(dd_maturity_id = if_else(dead_count > 0L & is.na(dd_maturity_id), 3L, dd_maturity_id)) %>%
  mutate(dd_cwt_detected_id = if_else(dead_count == 0L, 0L, dd_cwt_detected_id)) %>%
  mutate(dd_cwt_detected_id = if_else(dead_count > 0L & is.na(dd_cwt_detected_id) & 
                                        species_id %in% c(1L, 4L), 3L, dd_cwt_detected_id)) %>%
  mutate(dd_cwt_detected_id = if_else(dead_count > 0L & is.na(dd_cwt_detected_id) & 
                                        !species_id %in% c(1L, 4L), 0L, dd_cwt_detected_id)) %>%
  mutate(dd_ad_clip_id = if_else(dead_count == 0L, 0L, dd_ad_clip_id)) %>%
  mutate(dd_ad_clip_id = if_else(dead_count > 0L & is.na(dd_ad_clip_id) &
                                   species_id %in% c(1L, 4L), 5L, dd_ad_clip_id)) %>%
  mutate(dd_ad_clip_id = if_else(dead_count > 0L & is.na(dd_ad_clip_id) &
                                   !species_id %in% c(1L, 4L), 0L, dd_ad_clip_id)) %>%
  mutate(prev_count = 0L) %>% 
  select(survey_detail_guid, dd_sex_id, dd_maturity_id, dd_cwt_detected_id, 
         dd_ad_clip_id, dead_count, prev_count, Create_DT, Create_WDFWLogin_Id, 
         Modify_DT, Modify_WDFWLogin_Id) %>%
  arrange(survey_detail_guid, dd_sex_id, dd_maturity_id, dead_count)

# Add summary count column, and collapse to one value by group
dead = dead %>%
  mutate(dead_count = as.integer(dead_count)) %>%
  group_by(survey_detail_guid, dd_sex_id, dd_maturity_id, dd_cwt_detected_id,
           dd_ad_clip_id) %>%
  mutate(sum_dead_count = sum(dead_count)) %>%
  ungroup() %>%
  select(survey_detail_guid, dd_sex_id, dd_maturity_id, dd_cwt_detected_id, 
         dd_ad_clip_id, dead_count, sum_dead_count, prev_count, Create_DT, 
         Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>%
  distinct()

# Verify if summary needed
chk_sum = dead %>% 
  filter(!sum_dead_count == dead_count)

if (nrow(chk_sum) > 0) {
  cat("\nCounts needed to be summarized!\n\n") 
} else {
  cat("\nNo summarized counts needed. One row per survey_detail_id.\n\n")
}

# Get rid of dead_count column
dead = dead %>% 
  select(survey_detail_guid, dd_sex_id, dd_maturity_id, dd_cwt_detected_id, 
         dd_ad_clip_id, dead_count = sum_dead_count, prev_count, Create_DT, 
         Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>% 
  distinct()

# Verify there's only one detail_id per record
if (any(duplicated(dead$survey_detail_guid))) {
  cat("\nWARNING: Some duplicated survey_detail_guids. Add full set of grouping columns below!\n\n")
} else {
  cat("\nNo duplicated survey_detail_ids. Ok to proceed with only survey_detail_guid as grouping column.\n\n")
}

# Verify there are no missing values in grouping columns
# No duplicated sd_id's so use sd_id as grouping variable
any(is.na(dead$survey_detail_guid))
any(is.na(dead$dd_sex_id))
any(is.na(dead$dd_maturity_id))
any(is.na(dead$dd_cwt_detected_id)) 
any(is.na(dead$dd_ad_clip_id))
any(is.na(dead$dead_count))

# Make sure character values in grp columns are trimmed, or types are all ints
typeof(dead$survey_detail_guid)
typeof(dead$dd_sex_id)
typeof(dead$dd_maturity_id)
typeof(dead$dd_cwt_detected_id)
typeof(dead$dd_ad_clip_id)
typeof(dead$dead_count)

# Add dead_guid
dead = dead %>% 
  group_by(survey_detail_guid, dd_sex_id, dd_maturity_id, dd_cwt_detected_id, dd_ad_clip_id) %>% 
  mutate(dead_guid = remisc::get_uuid(1L)) %>% 
  ungroup()

# Check without dead_id in arrange
chk_dead_ids = dead %>%
  arrange_at(c("survey_detail_guid", "dd_sex_id", "dd_maturity_id", "dd_cwt_detected_id",
               "dd_ad_clip_id")) %>%
  select(survey_detail_guid, dead_guid, dd_sex_id, dd_maturity_id, dd_cwt_detected_id,
         dd_ad_clip_id, dead_count, prev_count)

# Pull out final columns
dead = dead %>% 
  select(dead_guid, survey_detail_guid,
         FISH_SEX_LUT_Id = dd_sex_id, MATURITY_LUT_Id = dd_maturity_id,
         CWT_DETECTED_LUT_Id = dd_cwt_detected_id, 
         ADIPOSE_CLIP_STATUS_LUT_Id = dd_ad_clip_id, DEAD_FISH_Cnt = dead_count,
         Previously_Counted_Ind = prev_count, Create_DT, Create_WDFWLogin_Id, 
         Modify_DT, Modify_WDFWLogin_Id)

# Check for duplicated dead_fish_id
any(duplicated(dead$dead_guid))

#============================================================================
# Pull out redd table...Are zero's being added for all target species !!!!!!!
#============================================================================

# Get redd data
redd = sp %>%
  mutate(new_redds = as.integer(new_redds)) %>% 
  mutate(old_redds = as.integer(old_redds)) %>% 
  select(species_record_id, survey_detail_guid, species_id, survey_type_id, new_redds, old_redds, Create_DT, 
         Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>%
  filter(!(is.na(new_redds) & is.na(old_redds))) %>% 
  distinct()

# Pull out redd data if any exist
if (nrow(redd) > 0) {
  # Get redd data
  redd = redd %>%
    mutate(new_redds = if_else(new_redds == 0 & !(species_id %in% c(6L, 1L) & survey_type_id == 1),
                               NA_integer_, new_redds)) %>% 
    mutate(old_redds = if_else(old_redds == 0 & !(species_id %in% c(6L, 1L) & survey_type_id == 1),
                               NA_integer_, old_redds)) %>% 
    select(survey_detail_guid, new_redds, old_redds, Create_DT, 
           Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>%
    distinct()
  
  # Pull out new_redd
  new_redd = redd %>% 
    select(survey_detail_guid, new_redds, Create_DT, Create_WDFWLogin_Id, 
           Modify_DT, Modify_WDFWLogin_Id) %>% 
    mutate(redd_count_type = 1L) %>% 
    select(survey_detail_guid, redd_count_type, redd_count = new_redds, 
           Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>% 
    filter(!is.na(redd_count)) %>% 
    distinct()
  
  # Pull out old_redd
  old_redd = redd %>% 
    select(survey_detail_guid, old_redds, Create_DT, Create_WDFWLogin_Id, 
           Modify_DT, Modify_WDFWLogin_Id) %>%
    mutate(redd_count_type = 2L) %>% 
    select(survey_detail_guid, redd_count_type, redd_count = old_redds, 
           Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>%
    filter(!is.na(redd_count)) %>% 
    distinct()
  
  # Combine
  fish_redd = rbind(new_redd, old_redd)
  fish_redd = fish_redd %>% 
    arrange(survey_detail_guid, redd_count_type)
  
  # Add summary count column, and collapse to one value by group
  fish_redd = fish_redd %>%
    mutate(redd_count = as.integer(redd_count)) %>%
    group_by(survey_detail_guid, redd_count_type) %>%
    mutate(sum_redd_count = sum(redd_count)) %>%
    ungroup() %>%
    select(survey_detail_guid, redd_count_type, redd_count, sum_redd_count, 
           Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>%
    distinct()
  
  # Verify if summary needed
  chk_sum = fish_redd %>% 
    filter(!sum_redd_count == redd_count)
  
  if (nrow(chk_sum) > 0) {
    cat("\nCounts needed to be summarized!\n\n") 
  } else {
    cat("\nNo summarized counts needed. One row per survey_detail_id.\n\n")
  }
  
  # Get rid of dead_count column
  fish_redd = fish_redd %>% 
    select(survey_detail_guid, redd_count_type, redd_count = sum_redd_count, 
           Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id) %>% 
    distinct()
  
  # Verify there's only one detail_id per record
  if (any(duplicated(fish_redd$survey_detail_guid))) {
    cat("\nWARNING: Some duplicated survey_detail_guids. Add full set of grouping columns below!\n\n")
  } else {
    cat("\nNo duplicated survey_detail_guids. Ok to proceed with only survey_detail_guid as grouping column.\n\n")
  }
  
  # Verify there are no missing values in grouping columns
  # No duplicated sd_id's so use sd_id as grouping variable
  any(is.na(fish_redd$survey_detail_guid))
  any(is.na(fish_redd$redd_count_type))
  any(is.na(fish_redd$redd_count))
  
  # Make sure character values in grp columns are trimmed, or types are all ints
  typeof(fish_redd$survey_detail_guid)
  typeof(fish_redd$redd_count_type)
  typeof(fish_redd$redd_count)
  
  # Add dead_guid
  fish_redd = fish_redd %>% 
    group_by(survey_detail_guid, redd_count_type) %>% 
    mutate(fish_redd_guid = remisc::get_uuid(1L)) %>% 
    ungroup()
  
  # Check without redd_guid in arrange
  chk_redd_ids = fish_redd %>%
    arrange_at(c("survey_detail_guid", "redd_count_type", "redd_count")) %>%
    select(fish_redd_guid, survey_detail_guid, redd_count_type, redd_count)
  
  # Pull out final columns
  fish_redd = fish_redd %>% 
    select(fish_redd_guid, survey_detail_guid,
           REDDS_COUNT_TYPE_LUT_Id = redd_count_type, REDDS_Cnt = redd_count,
           Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id)
} else {
  fish_redd = NULL
}

# Check if any fish_redd_ids are duplicated
if (any(duplicated(fish_redd$fish_redd_guid))) {
  cat("\nWARNING: Some duplicated fish_redd_guids. Do not pass go.\n\n")
} else {
  cat("\nNo duplicated fish_redd_guids. Ok to proceed.\n\n")
}

#===================================================================
# Add species data to individual_carcass data
#===================================================================

# Check if any species_record_ids are duplicated
if (any(duplicated(species_records$species_record_id))) {
  cat("\nWARNING: Some duplicated species_record IDs. Do not pass go.\n\n")
}

# Check if any parent_record_ids are missing
if (!all(species_records$parent_record_id %in% parent_records$parent_record_id)) {
  cat("\nWARNING: Not all needed IDs present in parent_records table. Do not pass go!\n\n")
}

# Verify no missing species_record_id in idead_records
if (nrow(idead_records) > 0 ) {
  if (any(is.na(idead_records$species_record_id))) {
    cat("\nWARNING: Not all needed IDs present in idead_records table. Do not pass go!\n\n")
  }
}

# Get an sp subset to add survey_detail_id
sp_sub = sp %>% 
  select(species_record_id, survey_detail_guid, species_id, Create_DT,
         Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id)

# Join sp subset to idead_records
if (nrow(idead_records) > 0 ) {
  idead_records = idead_records %>%
    left_join(sp_sub, by = "species_record_id") %>%
    distinct()
  
  # Verify no missing survey_detail_guids
  any(is.na(idead_records$survey_detail_guid))
  
  # Pull out ind_carcass data
  ind_carc = idead_records %>%
    select(survey_detail_guid, ubid, carc_num, isex_id, imaturity_id, fork_length_cm, length_type_id,
           egg_pct, scale_sample_number, cwt_snout_sample_number, cwt_code, genetic_number, otolith_number,
           iad_clip, icomments, marks, idead_record_id, Create_DT, Create_WDFWLogin_Id, Modify_DT,
           Modify_WDFWLogin_Id) %>%
    distinct()
  
  # Check for duplicated idead_record_id
  any(duplicated(ind_carc$idead_record_id))
  
  # Verify there are no missing values in grouping columns
  # No duplicated idead_record_id so use idead_record_id as grouping variable
  any(is.na(ind_carc$survey_detail_guid))
  any(is.na(ind_carc$idead_record_id))
  
  # Make sure character values in grp columns are trimmed, or types are all ints
  typeof(ind_carc$survey_detail_guid)
  typeof(ind_carc$idead_record_id)
  
  # Add individual_carcass_guid
  ind_carc = ind_carc %>% 
    group_by(idead_record_id) %>% 
    mutate(individual_carcass_guid = remisc::get_uuid(1L)) %>% 
    ungroup()
  
  # Verify no duplicate ids
  any(duplicated(ind_carc$individual_carcass_guid))
  
  # Verify no egg_pct > 100
  if (any(!is.na(ind_carc$egg_pct) & (ind_carc$egg_pct > 100))) {
    cat("\nWARNING: Some egg_pct > 100. Do not pass go!\n\n")
  } else {
    cat("\nEgg_Pct within range. Ok to proceed.\n\n")
  }
  
  # # Set egg_pct to NA_real if > 100
  # ind_carc$egg_pct[ind_carc$egg_pct > 100] = NA_real_
  
  # Pull out final columns
  ind_carcass = ind_carc %>%
    select(individual_carcass_guid, survey_detail_guid,
           Universal_Biological_Id = ubid, Carcass_Num = carc_num, FISH_SEX_LUT_Id = isex_id,
           MATURITY_LUT_Id = imaturity_id, Length_Centimeters_Meas = fork_length_cm,
           LENGTH_MEASURE_TYPE_LUT_Id = length_type_id, Egg_Retention_Pct = egg_pct,
           Scale_Sample_Num = scale_sample_number, CWT_Snout_Sample_Num = cwt_snout_sample_number,
           CWT_Code = cwt_code, Genetic_Sample_Num = genetic_number, Otolith_Sample_Num = otolith_number,
           Comment_Txt = icomments, Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id)
  
  # Check ind_carcass for overly long comments
  chk_comment = ind_carcass %>%
    filter(!is.na(Comment_Txt)) %>%
    select(individual_carcass_guid, Comment_Txt) %>%
    mutate(n_chars = nchar(Comment_Txt)) %>%
    filter(n_chars > 100)
  
  # # Correct one occurrance
  # ind_carcass$Comment_Txt[ind_carcass$Comment_Txt ==
  #   "Carcass was missing upper part of body, no Otos, not wAnded. DNA was taken from dorsal; Gill condition: 04_1-4"] =
  #   "Carcass missing upper body, no Otos, not wanded. DNA taken from dorsal; Gill condition: 04_1-4"
  
  #===================================================================
  # Create the individual_carcass_mark table
  #===================================================================
  
  # Pull out needed variables
  carc_mark = ind_carc %>%
    select(individual_carcass_guid, marks, Create_DT, Create_WDFWLogin_Id,
           Modify_DT, Modify_WDFWLogin_Id) %>%
    filter(!is.na(marks)) %>% 
    distinct()
  
  # Clean up
  if (nrow(carc_mark) > 0 ) {
    # Separate out the mark_types
    carc_marks = separate_rows(carc_mark, marks, sep = ",")
    carc_marks = carc_marks %>%
      mutate(marks = trimws(marks)) %>%
      filter(!is.na(marks) & !marks == "") %>%
      mutate(mark_type_id = case_when(
        marks == "ad_clip" ~ 1L,
        marks == "coded_wire_tag" ~ 4L,
        marks == "anal_fin_clip" ~ 2L,
        marks == "bottom_caudal_fin_clip" ~ 3L,
        marks == "jaw_tag" ~ 5L,
        marks == "marked_otolith" ~ 14L,
        marks == "left_opercle_punch" ~ 6L,
        marks == "left_pectoral_fin_clip" ~ 7L,
        marks == "left_ventral_fin_clip" ~ 8L,
        marks == "right_opercle_punch" ~ 9L,
        marks == "right_pectoral_fin_clip" ~ 10L,
        marks == "right_ventral_fin_clip" ~ 11L,
        marks == "spaghetti_tag" ~ 12L,
        marks == "top_caudal" ~ 13L)) %>%
      select(individual_carcass_guid, mark_type_id, Create_DT, Create_WDFWLogin_Id,
             Modify_DT, Modify_WDFWLogin_Id)
    
    # Verify there are no missing values in grouping columns
    # No duplicated sd_id's so use sd_id as grouping variable
    any(is.na(carc_marks$individual_carcass_guid))
    any(is.na(carc_marks$mark_type_id))
    
    # Make sure character values in grp columns are trimmed, or types are all ints
    typeof(carc_marks$individual_carcass_guid)
    typeof(carc_marks$mark_type_id)
    
    # Add guid
    carc_marks = carc_marks %>% 
      mutate(carcass_mark_guid = remisc::get_uuid(nrow(carc_marks)))
    
    # Check without carc_mark_id in arrange
    chk_mark_ids = carc_marks %>%
      arrange_at(c("carcass_mark_guid", "mark_type_id")) %>%
      select(carcass_mark_guid, individual_carcass_guid, mark_type_id)
    
    # Pull out final columns
    carcass_marks = carc_marks %>%
      select(carcass_mark_guid, individual_carcass_guid,
             MARK_TYPE_LUT_Id = mark_type_id, Create_DT, Create_WDFWLogin_Id)
  } else {
    carcass_marks = carc_mark
  }
} else {
  carcass_marks = idead_records
}

#============================================================================
# Some checks
#============================================================================

# Calculate sums in final tables
tsum_new_redd = sum(fish_redd$REDDS_Cnt[fish_redd$REDDS_COUNT_TYPE_LUT_Id == 1], na.rm = TRUE)
tsum_old_redd = sum(fish_redd$REDDS_Cnt[fish_redd$REDDS_COUNT_TYPE_LUT_Id == 2], na.rm = TRUE)
tsum_live = sum(live$LIVE_FISH_Cnt, na.rm = TRUE)
tsum_dead = sum(dead$DEAD_FISH_Cnt, na.rm = TRUE)

# Check that sums agree
sum_new_redd == tsum_new_redd
sum_old_redd == tsum_old_redd
sum_live == tsum_live
sum_dead == tsum_dead

# Remove fish_redd if null
if (is.null(fish_redd)) {
  rm(fish_redd)
}

# Check how many records to append
to_append = nAppend()
to_append
sum_records = sum(to_append$Records_to_Append)
sum_records

#============================================================================
# Append data
#============================================================================

# Add column if SGS_CON = "R_To_SGSB"
if (SGS_CON == "R_To_SGSB") {
  survey_detail = survey_detail %>%
    mutate(SURVEY_COUNT_TYPE_LUT_Id = 1L) %>%
    select(survey_detail_guid, tempid, survey_guid, SGS_SPECIES_LUT_Id, SGS_RUN_LUT_Id, Run_Yr,
           ORIGIN_LUT_Id, SASI_STOCK_Id, SURVEY_TYPE_LUT_Id, SURVEY_COUNT_TYPE_LUT_Id,
           Estimated_Seen_Fish_Pct, SGS_TAG_DETECTION_METHOD_LUT_Id, Steelhead_Redd_Grouping_Cnt,
           Comment_Txt, Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id)
}

# Verify no wrias are missing
if (any(is.na(survey$WRIA_LUT_Id))) {
  cat("\nWARNING: At least one wria_lut_id missing. Do not pass go!\n\n")
} else {
  cat("\nNo missing wria_ids. Ok to proceed.\n\n")
}

# Pull out copy of survey as survey_p for consistency with klacan code
survey_p = survey %>% 
  mutate(tempid = seq(1, nrow(survey))) %>%
  select(survey_guid, tempid, SURVEY_Date, WRIA_LUT_Id, STREAM_REACH_Id, Observer_Name_Txt,
         DATA_SOURCE_LUT_Id, Data_Submitter_Last_Name, SURVEY_METHOD_LUT_Id,
         WEATHER_TYPE_LUT_Id, STREAM_FLOW_TYPE_LUT_Id, Cubic_Feet_Per_Second_Flow_Meas,
         Water_Visibility_Feet_Meas, SURVEY_VISIBILITY_TYPE_LUT_Id, Comment_Txt,
         Create_DT, Create_WDFWLogin_Id)

# New append code using semi-transactions
if (sum_records ==379L) {
  
  # Define column list
  survey_list = c("tempid", "SURVEY_Date", "WRIA_LUT_Id", "STREAM_REACH_Id", "Observer_Name_Txt",
                  "DATA_SOURCE_LUT_Id", "Data_Submitter_Last_Name", "SURVEY_METHOD_LUT_Id",
                  "WEATHER_TYPE_LUT_Id", "STREAM_FLOW_TYPE_LUT_Id", "Cubic_Feet_Per_Second_Flow_Meas",
                  "Water_Visibility_Feet_Meas", "SURVEY_VISIBILITY_TYPE_LUT_Id", "Comment_Txt",
                  "Create_DT", "Create_WDFWLogin_Id")
  survey_list = paste0(survey_list, collapse = ", ")
  
  #=============================================================================================
  # Insert survey table with tempid
  #=============================================================================================
  
  # Write temp_data to SGS
  survey_temp = survey_p %>%
    select(-survey_guid)
  con = odbcConnect(SGS_CON)
  sqlSave(con, survey_temp, rownames = FALSE, fast = FALSE)
  close(con)
  
  # Semi-transaction
  con = odbcConnect(SGS_CON)
  sqlQuery(con,
           glue("INSERT INTO SURVEY ",
                "({survey_list}) ",
                "SELECT {survey_list} ",
                "FROM survey_temp"))
  # Clean up
  sqlDrop(con, "survey_temp")
  close(con)
  
  # Pull out survey_ids for count and later
  create_dt = survey_p$Create_DT[1]
  create_login = survey_p$Create_WDFWLogin_Id[1]
  survey_id = get_survey_id(create_dt, create_login, dsn = SGS_CON)
  
  # Clean out temp_id
  con = odbcConnect(SGS_CON)
  sqlQuery(con,
           paste0("UPDATE SURVEY ",
                  "SET tempid = NULL ",
                  "WHERE tempid IS NOT NULL"))
  close(con)
  
  # Verify count
  if (!nrow(survey_id) == nrow(survey_p)) {
    cat("\nWARNING: Not all data were written to SURVEY table. Investigate!\n\n")
  } else {
    cat("\nAll records were written. Ok to proceed.\n\n")
  }
  
  # Join to survey_p and pull out needed columns
  survey_id_cols = survey_p %>%
    left_join(survey_id, by = "tempid") %>%
    select(survey_guid, survey_id)
  
  #===============================================================
  # survey_detail table
  #===============================================================
  
  # Join survey_guid to survey_detail
  survey_detail_p = survey_detail %>%
    left_join(survey_id_cols, by = "survey_guid")
  
  # Warn if any survey_id values missing from survey_detail_p
  if (any(is.na(survey_detail_p$survey_id))) {
    cat("\nWARNING: Some missing survey_id's. Do not pass go!\n\n")
  } else {
    cat("\nNo missing survey_ids. Ok to proceed.\n\n")
  }
  
  # Format for insertion to SGS
  survey_detail_p = survey_detail_p %>%
    mutate(tempid = seq(1, nrow(survey_detail_p))) %>%
    mutate(create_dt = format(Sys.time())) %>%
    mutate(create_login = Sys.getenv("USERNAME")) %>%
    select(survey_detail_guid, tempid, SURVEY_Id = survey_id, SGS_SPECIES_LUT_Id,
           SGS_RUN_LUT_Id, Run_Yr, SURVEY_TYPE_LUT_Id, Estimated_Seen_Fish_Pct,
           SGS_TAG_DETECTION_METHOD_LUT_Id, Comment_Txt,
           Create_DT = create_dt, Create_WDFWLogin_Id = create_login) %>%
    distinct()
  
  # Define column list
  survey_detail_list = c("tempid", "SURVEY_Id", "SGS_SPECIES_LUT_Id", "SGS_RUN_LUT_Id", "Run_Yr",
                         "SURVEY_TYPE_LUT_Id", "Estimated_Seen_Fish_Pct", "SGS_TAG_DETECTION_METHOD_LUT_Id",
                         "Comment_Txt", "Create_DT", "Create_WDFWLogin_Id")
  survey_detail_list = paste0(survey_detail_list, collapse = ", ")
  
  # Write temp_data to SGSB
  survey_detail_temp = survey_detail_p %>%
    select(-survey_detail_guid)
  con = odbcConnect(SGS_CON)
  sqlSave(con, survey_detail_temp, rownames = FALSE, fast = FALSE)
  close(con)
  
  # Semi-transaction
  con = odbcConnect(SGS_CON)
  sqlQuery(con,
           glue("INSERT INTO SURVEY_DETAIL ",
                "({survey_detail_list}) ",
                "SELECT {survey_detail_list} ",
                "FROM survey_detail_temp"))
  # Clean up
  sqlDrop(con, "survey_detail_temp")
  close(con)
  
  # Pull out survey_ids for count and later
  create_dt = survey_detail_p$Create_DT[1]
  create_login = survey_detail_p$Create_WDFWLogin_Id[1]
  survey_detail_id = get_survey_detail_id(create_dt, create_login, dsn = SGS_CON)
  
  # Clean out temp_id
  con = odbcConnect(SGS_CON)
  sqlQuery(con,
           paste0("UPDATE SURVEY_DETAIL ",
                  "SET tempid = NULL ",
                  "WHERE tempid IS NOT NULL"))
  close(con)
  
  # Verify count
  if (!nrow(survey_detail_id) == nrow(survey_detail_p)) {
    cat("\nWARNING: Not all data were written to SURVEY_DETAIL table. Investigate!\n\n")
  } else {
    cat("\nAll records were written. Ok to proceed.\n\n")
  }
  
  # Join to survey_detail_p and pull out needed columns
  survey_detail_id_cols = survey_detail_p %>%
    left_join(survey_detail_id, by = "tempid") %>%
    select(survey_detail_guid, survey_detail_id)
  
  #===============================================================
  # live table
  #===============================================================
  
  # Join survey_detail_guid to live
  live = live %>%
    left_join(survey_detail_id_cols, by = "survey_detail_guid")
  
  # Warn if any survey_detail_id values missing
  if (any(is.na(live$survey_detail_id))) {
    cat("\nWARNING: Some missing survey_detail_id's. Do not pass go!\n\n")
  } else {
    cat("\nNo missing survey_detail_ids. Ok to proceed.\n\n")
  }
  
  # Get rid of any duplicated zero counts by survey_detail_id
  live_p = live %>%
    group_by(survey_detail_guid, FISH_SEX_LUT_Id, MATURITY_LUT_Id, SGS_LIVE_FISH_TYPE_LUT_Id,
             CWT_DETECTED_LUT_Id, ADIPOSE_CLIP_STATUS_LUT_Id, LIVE_FISH_Cnt) %>%
    mutate(n_seq = row_number(Create_DT)) %>%
    ungroup() %>%
    mutate(del_zero = if_else(LIVE_FISH_Cnt == 0 & n_seq > 1, "yes", "no")) %>%
    filter(del_zero == "no") %>%
    mutate(Create_DT = format(Sys.time())) %>%
    mutate(Create_WDFWLogin_Id = Sys.getenv("USERNAME")) %>%
    select(SURVEY_DETAIL_Id = survey_detail_id, FISH_SEX_LUT_Id, MATURITY_LUT_Id, SGS_LIVE_FISH_TYPE_LUT_Id,
           CWT_DETECTED_LUT_Id, ADIPOSE_CLIP_STATUS_LUT_Id, LIVE_FISH_Cnt, Create_DT, Create_WDFWLogin_Id,
           Modify_DT, Modify_WDFWLogin_Id) 
  
  # Define column list
  live_list = c("SURVEY_DETAIL_Id", "FISH_SEX_LUT_Id", "MATURITY_LUT_Id", "SGS_LIVE_FISH_TYPE_LUT_Id",
                "CWT_DETECTED_LUT_Id", "ADIPOSE_CLIP_STATUS_LUT_Id", "LIVE_FISH_Cnt", "Create_DT",
                "Create_WDFWLogin_Id")
  live_list = paste0(live_list, collapse = ", ")
  
  # Write temp_data to SGS
  live_temp = live_p
  con = odbcConnect(SGS_CON)
  sqlSave(con, live_temp, rownames = FALSE, fast = FALSE)
  close(con)
  
  # Somi-transaction
  con = odbcConnect(SGS_CON)
  sqlQuery(con,
           glue("INSERT INTO LIVE_FISH ",
                "({live_list}) ",
                "SELECT {live_list} ",
                "FROM live_temp"))
  # Clean up
  sqlDrop(con, "live_temp")
  close(con)
  
  # Pull out survey_ids for count and later
  create_dt = live_p$Create_DT[1]
  create_login = live_p$Create_WDFWLogin_Id[1]
  live_id = get_live_id(create_dt, create_login, dsn = SGS_CON)
  
  # Verify count
  if (!nrow(live_id) == nrow(live_p)) {
    cat("\nWARNING: Not all data were written to LIVE_FISH table. Investigate!\n\n")
  } else {
    cat("\nAll records were written. Ok to proceed.\n\n")
  }
  
  #===============================================================
  # dead table
  #===============================================================
  
  # Join survey_detail_guid to dead
  dead = dead %>%
    left_join(survey_detail_id_cols, by = "survey_detail_guid")
  
  # Warn if any survey_detail_id values missing
  if (any(is.na(dead$survey_detail_id))) {
    cat("\nWARNING: Some missing survey_detail_id's. Do not pass go!\n\n")
  } else {
    cat("\nNo missing survey_detail_ids. Ok to proceed.\n\n")
  }
  
  # Get rid of any duplicated zero counts by survey_detail_id
  dead_p = dead %>%
    group_by(survey_detail_id, FISH_SEX_LUT_Id, MATURITY_LUT_Id, CWT_DETECTED_LUT_Id,
             ADIPOSE_CLIP_STATUS_LUT_Id, DEAD_FISH_Cnt, Previously_Counted_Ind) %>%
    mutate(n_seq = row_number(Create_DT)) %>%
    ungroup() %>%
    mutate(del_zero = if_else(DEAD_FISH_Cnt == 0 & n_seq > 1, "yes", "no")) %>%
    filter(del_zero == "no") %>%
    mutate(Create_DT = format(Sys.time())) %>%
    mutate(Create_WDFWLogin_Id = Sys.getenv("USERNAME")) %>%
    select(SURVEY_DETAIL_Id = survey_detail_id, FISH_SEX_LUT_Id, MATURITY_LUT_Id, CWT_DETECTED_LUT_Id,
           ADIPOSE_CLIP_STATUS_LUT_Id, DEAD_FISH_Cnt, Previously_Counted_Ind, Create_DT, Create_WDFWLogin_Id,
           Modify_DT, Modify_WDFWLogin_Id)
  
  # Define column list
  dead_list = c("SURVEY_DETAIL_Id", "FISH_SEX_LUT_Id", "MATURITY_LUT_Id",
                "CWT_DETECTED_LUT_Id", "ADIPOSE_CLIP_STATUS_LUT_Id",
                "DEAD_FISH_Cnt", "Previously_Counted_Ind", "Create_DT",
                "Create_WDFWLogin_Id")
  dead_list = paste0(dead_list, collapse = ", ")
  
  # Write temp_data to SGS
  dead_temp = dead_p
  con = odbcConnect(SGS_CON)
  sqlSave(con, dead_temp, rownames = FALSE, fast = FALSE)
  close(con)
  
  # Semi-transaction
  con = odbcConnect(SGS_CON)
  sqlQuery(con,
           glue("INSERT INTO DEAD_FISH ",
                "({dead_list}) ",
                "SELECT {dead_list} ",
                "FROM dead_temp"))
  # Clean up
  sqlDrop(con, "dead_temp")
  close(con)
  
  # Pull out survey_ids for count and later
  create_dt = dead_p$Create_DT[1]
  create_login = dead_p$Create_WDFWLogin_Id[1]
  dead_id = get_dead_id(create_dt, create_login, dsn = SGS_CON)
  
  # Verify count
  if (!nrow(dead_id) == nrow(dead_p)) {
    cat("\nWARNING: Not all data were written to DEAD_FISH table. Investigate!\n\n")
  } else {
    cat("\nAll records were written. Ok to proceed.\n\n")
  }
  
  #===============================================================
  # redd table
  #===============================================================

  # Join survey_detail_guid to dead
  fish_redd = fish_redd %>%
    left_join(survey_detail_id_cols, by = "survey_detail_guid")

  # Warn if any survey_detail_id values missing
  if (any(is.na(fish_redd$survey_detail_id))) {
    cat("\nWARNING: Some missing survey_detail_id's. Do not pass go!\n\n")
  } else {
    cat("\nNo missing survey_detail_ids. Ok to proceed.\n\n")
  }

  # Get rid of any duplicated zero counts by survey_detail_id
  redd_p = fish_redd %>%
    group_by(survey_detail_id, REDDS_COUNT_TYPE_LUT_Id, REDDS_Cnt) %>%
    mutate(n_seq = row_number(Create_DT)) %>%
    ungroup() %>%
    mutate(del_zero = if_else(REDDS_Cnt == 0 & n_seq > 1, "yes", "no")) %>%
    filter(del_zero == "no") %>%
    mutate(Create_DT = format(Sys.time())) %>%
    mutate(Create_WDFWLogin_Id = Sys.getenv("USERNAME")) %>%
    select(SURVEY_DETAIL_Id = survey_detail_id, REDDS_COUNT_TYPE_LUT_Id, REDDS_Cnt,
           Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id)

  # Define column list
  redd_list = c("SURVEY_DETAIL_Id", "REDDS_COUNT_TYPE_LUT_Id",
                "REDDS_Cnt", "Create_DT", "Create_WDFWLogin_Id")
  redd_list = paste0(redd_list, collapse = ", ")

  # Write temp_data to SGS
  redd_temp = redd_p
  con = odbcConnect(SGS_CON)
  sqlSave(con, redd_temp, rownames = FALSE, fast = FALSE)
  close(con)

  # Semi-transaction
  con = odbcConnect(SGS_CON)
  sqlQuery(con,
           glue("INSERT INTO FISH_REDDS ",
                "({redd_list}) ",
                "SELECT {redd_list} ",
                "FROM redd_temp"))
  # Clean up
  sqlDrop(con, "redd_temp")
  close(con)

  # Pull out survey_ids for count and later
  create_dt = redd_p$Create_DT[1]
  create_login = redd_p$Create_WDFWLogin_Id[1]
  redd_id = get_redd_id(create_dt, create_login, dsn = SGS_CON)

  # Verify count
  if (!nrow(redd_id) == nrow(redd_p)) {
    cat("\nWARNING: Not all data were written to FISH_REDDS table. Investigate!\n\n")
  } else {
    cat("\nAll records were written. Ok to proceed.\n\n")
  }
  
  #===============================================================
  # ind_carcass table
  #===============================================================
  
  # Join survey_detail_guid to dead
  ind_carcass = ind_carcass %>%
    left_join(survey_detail_id_cols, by = "survey_detail_guid")
  
  # Warn if any survey_detail_id values missing
  if (any(is.na(ind_carcass$survey_detail_id))) {
    cat("\nWARNING: Some missing survey_detail_id's. Do not pass go!\n\n")
  } else {
    cat("\nNo missing survey_detail_ids. Ok to proceed.\n\n")
  }
  
  # # # THIS TIME ONLY....get rid of excessive egg_pct
  # # ind_carcass$Egg_Retention_Pct[ind_carcass$Egg_Retention_Pct == 393242] = NA_real_
  # unique(nchar(ind_carcass$Comment_Txt))
  # ind_carcass$Comment_Txt[ind_carcass$Comment_Txt == "POH: 65 cm; Only had 1 OTO, and it fell into the rocks. I was unable to find it. ; Gill condition: 2/4"] = "POH: 65 cm; Only had 1 OTO, fell into the rocks. Was unable to find; Gill condition: 2/4"
  # ind_carcass$Comment_Txt[ind_carcass$Comment_Txt == "POH: 70 cm; Had no snout. End of fish was torn so unable to distinguish a clip or not.; Gill condition: 1/4"] = "POH: 70 cm; No snout. Fish torn, unknown if clipped or not; Gill condition: 1/4"
  # unique(nchar(ind_carcass$Comment_Txt))
  
  # Get rid of any duplicated zero counts by survey_detail_id
  ind_carcass_p = ind_carcass %>%
    mutate(Create_DT = format(Sys.time())) %>%
    mutate(Create_WDFWLogin_Id = Sys.getenv("USERNAME")) %>%
    select(SURVEY_DETAIL_Id = survey_detail_id, Universal_Biological_Id, Carcass_Num, FISH_SEX_LUT_Id, 
           MATURITY_LUT_Id, Length_Centimeters_Meas, LENGTH_MEASURE_TYPE_LUT_Id, Egg_Retention_Pct,
           Scale_Sample_Num, CWT_Snout_Sample_Num, CWT_Code, Genetic_Sample_Num, Otolith_Sample_Num,
           Comment_Txt, Create_DT, Create_WDFWLogin_Id, Modify_DT, Modify_WDFWLogin_Id)
  
  # Define column list
  carc_list = c("SURVEY_DETAIL_Id", "Universal_Biological_Id", "Carcass_Num", "FISH_SEX_LUT_Id",
                "MATURITY_LUT_Id", "Length_Centimeters_Meas", "LENGTH_MEASURE_TYPE_LUT_Id",
                "Egg_Retention_Pct", "Scale_Sample_Num", "CWT_Snout_Sample_Num", "CWT_Code",
                "Genetic_Sample_Num", "Otolith_Sample_Num", "Comment_Txt", "Create_DT",
                "Create_WDFWLogin_Id")
  carc_list = paste0(carc_list, collapse = ", ")
  
  # Write temp_data to SGS
  carc_temp = ind_carcass_p
  con = odbcConnect(SGS_CON)
  sqlSave(con, carc_temp, rownames = FALSE, fast = FALSE)
  close(con)
  
  # Semi-transaction
  con = odbcConnect(SGS_CON)
  sqlQuery(con,
           glue("INSERT INTO INDIVIDUAL_CARCASS ",
                "({carc_list}) ",
                "SELECT {carc_list} ",
                "FROM carc_temp"))
  # Clean up
  sqlDrop(con, "carc_temp")
  close(con)
  
  # Pull out survey_ids for count and later
  create_dt = ind_carcass_p$Create_DT[1]
  create_login = ind_carcass_p$Create_WDFWLogin_Id[1]
  carcass_id = get_carcass_id(create_dt, create_login, dsn = SGS_CON)
  
  # Verify count
  if (!nrow(carcass_id) == nrow(ind_carcass_p)) {
    cat("\nWARNING: Not all data were written to INDIVIDUAL_CARCASS table. Investigate!\n\n")
  } else {
    cat("\nAll records were written. Ok to proceed.\n\n")
  }
  
  # Add IDs to ind_carc and pull out carcass_marks table
  ind_carcass_id = ind_carcass %>% 
    left_join(carcass_id, by = c("survey_detail_id", "Universal_Biological_Id"))
  
  # Verify count
  if (!nrow(carcass_id) == nrow(ind_carcass_id)) {
    cat("\nWARNING: New rows were created. Investigate!\n\n")
  } else {
    cat("\nNo new records created. Ok to proceed.\n\n")
  }
  
  #===================================================================
  # Create the individual_carcass_mark table
  #===================================================================
  
  # Pull out marks by universal_guid
  mark_id = ind_carc %>% 
    select(Universal_Biological_Id = ubid, marks)
  
  # Pull out needed variables
  carc_mark = ind_carcass_id %>%
    left_join(mark_id, by = "Universal_Biological_Id") %>% 
    select(carcass_id, marks, Create_DT, Create_WDFWLogin_Id,
           Modify_DT, Modify_WDFWLogin_Id) %>%
    filter(!is.na(marks)) %>% 
    distinct()
  
  # Clean up
  if (nrow(carc_mark) > 0 ) {
    # Separate out the mark_types
    carc_marks = separate_rows(carc_mark, marks, sep = ",")
    carc_marks = carc_marks %>%
      mutate(marks = trimws(marks)) %>%
      filter(!is.na(marks) & !marks == "") %>%
      mutate(mark_type_id = case_when(
        marks == "ad_clip" ~ 1L,
        marks == "coded_wire_tag" ~ 4L,
        marks == "anal_fin_clip" ~ 2L,
        marks == "bottom_caudal_fin_clip" ~ 3L,
        marks == "jaw_tag" ~ 5L,
        marks == "marked_otolith" ~ 14L,
        marks == "left_opercle_punch" ~ 6L,
        marks == "left_pectoral_fin_clip" ~ 7L,
        marks == "left_ventral_fin_clip" ~ 8L,
        marks == "right_opercle_punch" ~ 9L,
        marks == "right_pectoral_fin_clip" ~ 10L,
        marks == "right_ventral_fin_clip" ~ 11L,
        marks == "spaghetti_tag" ~ 12L,
        marks == "top_caudal" ~ 13L)) %>%
      select(carcass_id, mark_type_id, Create_DT, Create_WDFWLogin_Id,
             Modify_DT, Modify_WDFWLogin_Id)
    
    # Verify there are no missing values in grouping columns
    any(is.na(carc_marks$carcass_id))
    any(is.na(carc_marks$mark_type_id))
    
    # Pull out final columns
    carcass_marks = carc_marks %>%
      select(INDIVIDUAL_CARCASS_Id = carcass_id, MARK_TYPE_LUT_Id = mark_type_id, 
             Create_DT, Create_WDFWLogin_Id)
    
    # Define column list
    carc_mark_list = c("INDIVIDUAL_CARCASS_Id", "MARK_TYPE_LUT_Id", "Create_DT",
                       "Create_WDFWLogin_Id")
    carc_mark_list = paste0(carc_mark_list, collapse = ", ")
    
    # Write temp_data to SGS
    carc_mark_temp = carcass_marks
    con = odbcConnect(SGS_CON)
    sqlSave(con, carc_mark_temp, rownames = FALSE, fast = FALSE)
    close(con)
    
    # Semi-transaction
    con = odbcConnect(SGS_CON)
    sqlQuery(con,
             glue("INSERT INTO INDIVIDUAL_CARCASS_MARK ",
                  "({carc_mark_list}) ",
                  "SELECT {carc_mark_list} ",
                  "FROM carc_mark_temp"))
    # Clean up
    sqlDrop(con, "carc_mark_temp")
    close(con)
    
    # Pull out survey_ids for count and later
    create_dt = carcass_marks$Create_DT[1]
    create_login = carcass_marks$Create_WDFWLogin_Id[1]
    carcass_mark_id = get_carcass_mark_id(create_dt, create_login, dsn = SGS_CON)
    
    # Verify count
    if (!nrow(carcass_mark_id) == nrow(carcass_marks)) {
      cat("\nWARNING: Not all data were written to INDIVIDUAL_CARCASS_MARK table. Investigate!\n\n")
    } else {
      cat("\nAll records were written. Ok to proceed.\n\n")
    }
    
  } else {
    carcass_marks = carc_mark
  }
}

#===================================================================
# Upload sp and idead_records to pg iform_archive
# ONLY APPEND AFTER UPLOADING TO SGS, NOT SGSB
# MAKE SURE append = TRUE after first load.
#===================================================================

# Connect to database and retrive a count of records in the tables
db_con = pg_con_local(dbname = "mobile_archive")
base_count = DBI::dbGetQuery(db_con, "select count(*) from d13_iform_base_v2")
carcass_count = DBI::dbGetQuery(db_con, "select count(*) from d13_iform_carcass_v2")
DBI::dbDisconnect(db_con)

# # Initial load
# base_count = 0L
# carcass_count = 0L

# Print
base_count
carcass_count

# Check flow meas
unique(sp$Cubic_Feet_Per_Second_Flow_Meas)

# # Change sp$Cubic_Feet_Per_Second_Flow_Meas
# # Some last minute weirdness now fixed
# names(sp)[67] = "Cubic_Feet_Per_Second_Flow_Meas"
# sp[73] = NULL
#
# sp = sp %>%
#   mutate(Cubic_Feet_Per_Second_Flow_Meas = as.integer(round(Cubic_Feet_Per_Second_Flow_Meas, 0)))

# # Forgot to upload last time...get rid of surveys from 6-14 onwards
# sp_old = sp %>% 
#   filter(!survey_start_date %in% c("2018-06-14", "2018-06-15", "2018-06-19")) %>% 
#   rename(survey_id = survey_guid, survey_detail_id = survey_detail_guid) %>% 
#   mutate(survey_id = 1L) %>% 
#   mutate(survey_detail_id = 1L) %>% 
#   mutate(Cubic_Feet_Per_Second_Flow_Meas = as.integer(Cubic_Feet_Per_Second_Flow_Meas))

# Set CFS to integer
sp = sp %>%
  mutate(Cubic_Feet_Per_Second_Flow_Meas = as.integer(Cubic_Feet_Per_Second_Flow_Meas))

# # NEEDED TO UPDATE COLUMN TYPEs...PROBABLY WILL BE MORE LATER !!!!!!!!!!!!!!!!!!!!
# qry = glue("alter table d13_iform_base_v2 ",
#            "alter column lo_rm_desc type text, ",
#            "alter column up_rm_desc type text")
# 
# # Run query
# db_con = pg_con_local(dbname = "mobile_archive")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)

# # NEEDED TO UPDATE COLUMN TYPEs...PROBABLY WILL BE MORE LATER !!!!!!!!!!!!!!!!!!!!
# qry = glue("alter table d13_iform_base_v2 ",
#            "alter column up_rm_gps type text")
# 
# # Run query
# db_con = pg_con_local(dbname = "mobile_archive")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)

# Write to mobile_archive
db_con = pg_con_local(dbname = "mobile_archive")
DBI::dbWriteTable(db_con, "d13_iform_base_v2", sp, row.names = FALSE, append = TRUE)
DBI::dbWriteTable(db_con, "d13_iform_carcass_v2", idead_records, row.names = FALSE, append = TRUE)
DBI::dbDisconnect(db_con)

# Connect to database and retrieve a count of records in the tables
db_con = pg_con_local(dbname = "mobile_archive")
base_count_2 = DBI::dbGetQuery(db_con, "select count(*) from d13_iform_base_v2")
carcass_count_2 = DBI::dbGetQuery(db_con, "select count(*) from d13_iform_carcass_v2")
DBI::dbDisconnect(db_con)

# Print number of records written...All correct..compare with sp and idead_records
(base_count_2 - base_count) == nrow(sp)
(carcass_count_2 - carcass_count) == nrow(idead_records)

