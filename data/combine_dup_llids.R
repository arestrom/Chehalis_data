#===========================================================================
# Combine stream segments with duplicate LLIDs into one continuous segment
#
# Notes:
#  1.
#
#  Completed: 2020-03-10
#
# AS 2020-03-10
#===========================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Load libraries
#library(odbc)
library(DBI)
library(RPostgres)
library(dplyr)
library(remisc)
library(tidyr)
library(sf)
#library(openxlsx)
library(stringi)
library(lubridate)
library(glue)
library(iformr)

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

#=========================================================================
# Import all stream data from sg where two or more segments exist per llid
#=========================================================================

# Define query to get needed data
qry = glue("select distinct wb.waterbody_id, wb.waterbody_name, wb.waterbody_display_name, ",
           "wb.latitude_longitude_id as llid, wb.stream_catalog_code as cat_code, ",
           "wr.wria_code, st.stream_id, st.gid, st.geom as geometry ",
           "from waterbody_lut as wb ",
           "left join stream as st on wb.waterbody_id = st.waterbody_id ",
           "left join location as loc on wb.waterbody_id = loc.waterbody_id ",
           "left join wria_lut as wr on loc.wria_id = wr.wria_id ",
           "where stream_id is not null and wria_code in ('22', '23') ",
           "order by waterbody_name")

# Get values from source
pg_con = dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
                   port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
streams_st = wria_st = st_read(pg_con, query = qry)
dbDisconnect(pg_con)

# Pull out cases where LLID is duplicated
dup_streams = streams_st %>%
  st_drop_geometry() %>%
  group_by(llid) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1) %>%
  select(llid) %>%
  distinct() %>%
  inner_join(streams_st, by = "llid")

# # Get Arleta's latest llid data
# llid_st = read_sf("C:/data/RStudio/spawn_survey_data/data/llid_st.gpkg", layer = "llid_st", crs = 2927)
# # Write chehalis subset to local
# llid_chehalis = llid_st %>%
#   filter(wria_code %in% c("22", "23"))
# #unique(llid_chehalis$wria_code)
# write_sf(llid_chehalis, "data/llid_chehalis.gpkg")
# Get Arleta's latest llid data
llid_chehalis = read_sf("data/llid_chehalis.gpkg", layer = "llid_chehalis", crs = 2927)

# Get Leslie's cat_llid layer
cat_llid = read_sf("C:/data/RStudio/spawn_survey_data/data/cat_llid.gpkg", layer = "cat_llid.gpkg", crs = 2927)
# Write chehalis subset to local
cat_llid_chehalis = cat_llid %>%
  filter(wria_code %in% c("22", "23"))
#unique(cat_llid_chehalis$wria_code)
write_sf(cat_llid_chehalis, "data/cat_llid_chehalis.gpkg")
# Get Leslie's latest llid data
cat_llid_chehalis = read_sf("data/cat_llid_chehalis.gpkg", layer = "cat_llid_chehalis", crs = 2927)

#===========================================================================================
# Get all stream and RM option lists from Lea's profile
#===========================================================================================

# Chehalis is not in the option list...Ok to update Chehalis without regard to Lea's form.

#===========================================================================================
# Correct Chehalis duplicates
#===========================================================================================

# Chehalis River was entered twice in SGS, both as 22.0190 and 23.0190. 23.1090 was named Upper Chehalis.
# Need to delete 23.1090 (3852) from SGS and SG, and reassign all RMs for lower portion to 22.1090 (484).
# The LLID entered to SGS for 22.1090 is correct.
#
# To correct SG
# 1. Reassign all RM locations to 22.1090 version
# 2. Look for duplicate RMs...where found, reassign in survey to top dup row (sort for lat-long)
# 3. Delete unused RMs...coordinates first if any
# 4. Delete unused waterbody (23.1090 version)
# 5. Reassign upper RMs as appropriate to EF Chehalis.

# Get all RMs for both Chehalis streams...needed to identify duplicate RMs that should be reassigned
qry = glue("select loc.location_id, loc.waterbody_id, wb.waterbody_name, wria_id, ",
           "loc.river_mile_measure as rm, loc.location_code, loc.location_name, ",
           "loc.location_description, lt.location_type_description as loc_type ",
           "from location as loc ",
           "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where loc.waterbody_id in ('c3ccff86-1995-42c5-b417-394f09471ec3', '34d930e5-6178-492c-accd-4add290daef9') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by loc.river_mile_measure")

# Get values from source
pg_con = dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
                   port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
ms_rm = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# Get all RMs for EF Chehalis stream
qry = glue("select loc.location_id, loc.waterbody_id, wb.waterbody_name, wria_id, ",
           "loc.river_mile_measure as rm, loc.location_code, loc.location_name, ",
           "loc.location_description, lt.location_type_description as loc_type ",
           "from location as loc ",
           "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where loc.waterbody_id in ('3ce98c4d-34be-4ae0-9191-bccbeac086ef') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by loc.river_mile_measure")

# Get values from source
pg_con = dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
                   port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
ef_rm = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

#=======================================================================================================
# Step 1: Add some RMs to Chehalis River LLID that we are keeping...and that are not duplicated in both
#         We need RM entries for 64.0 and 101.8
#=======================================================================================================

# # Add two new locations
# new_rm = tibble(waterbody_id = rep("c3ccff86-1995-42c5-b417-394f09471ec3", 2),
#                 wria_id = rep("d398adc5-fc6f-4c58-9a91-9dc42030ef57", 2),
#                 location_type_id = rep("0caa52b7-dfd7-4bf6-bd99-effb17099fd3", 2),
#                 stream_channel_type_id = rep("540d2361-7598-46b0-88b0-895558760c52", 2),
#                 location_orientation_type_id = rep("ffdeeb40-11c8-4268-a2a5-bd73dedd8c25", 2),
#                 river_mile_measure = c(64.0, 101.8),
#                 location_code = rep(NA_character_, 2),
#                 location_name = rep(NA_character_, 2),
#                 location_description = rep(NA_character_, 2),
#                 waloc_id = rep(NA_integer_, 2))
#
# # Format
# new_rm = new_rm %>%
#   mutate(location_id = remisc::get_uuid(nrow(new_rm))) %>%
#   mutate(created_datetime = with_tz(Sys.time(), tzone = "UTC")) %>%
#   mutate(created_by = Sys.getenv("USERNAME")) %>%
#   mutate(modified_datetime = with_tz(as.POSIXct(NA), tzone = "UTC")) %>%
#   mutate(modified_by = NA_character_) %>%
#   select(location_id, waterbody_id, wria_id, location_type_id, stream_channel_type_id,
#          location_orientation_type_id, river_mile_measure, location_code, location_name,
#          location_description, waloc_id, created_datetime, created_by, modified_datetime,
#          modified_by)
#
# # Upload
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground_archive", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbWriteTable(pg_con, "location", new_rm, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(pg_con)
#
# # Upload
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbWriteTable(pg_con, "location", new_rm, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(pg_con)
#
# # Upload
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "FISH", host = pg_host("pg_host_prod"),
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_prod"))
# tbl = Id(schema = "spawning_ground", table = "location")
# DBI::dbWriteTable(pg_con, tbl, new_rm, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(pg_con)

#=======================================================================================================
# Step 2: Update all cases where RMs occur in both LLIDs to the preferred wb_id
#=======================================================================================================

# # # Pull out the location_ids where waterbody_id needs to be updated
# # up_loc_ids = ms_rm %>%
# #   filter(waterbody_id == "34d930e5-6178-492c-accd-4add290daef9") %>%
# #   pull(location_id)
# #
# # up_loc_ids = unique(up_loc_ids)
# #
# # # Format to string
# # up_loc_ids = paste0(paste0("'", up_loc_ids, "'"), collapse = ", ")
#
# # Update waterbody_ids for selected locations
# qry = glue("update location set waterbody_id = 'c3ccff86-1995-42c5-b417-394f09471ec3' ",
#            "where location_id in ({up_loc_ids})")
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
#
# # Update
# qry = glue("update location set waterbody_id = 'c3ccff86-1995-42c5-b417-394f09471ec3' ",
#            "where location_id in ({up_loc_ids})")
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground_archive", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
#
# # Update
# qry = glue("update spawning_ground.location set waterbody_id = 'c3ccff86-1995-42c5-b417-394f09471ec3' ",
#            "where location_id in ({up_loc_ids})")
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "FISH", host = pg_host("pg_host_prod"),
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_prod"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)

#=======================================================================================================
# Step 3: Look for cases where RMs are now duplicated...update to top one...check for coords
#=======================================================================================================

# # Pull out duplicated RMs
# ms_dup = ms_rm %>%
#   group_by(waterbody_id, rm) %>%
#   mutate(n_seq = row_number()) %>%
#   ungroup() %>%
#   filter(n_seq > 1) %>%
#   select(rm) %>%
#   left_join(ms_rm, by = "rm") %>%
#   select(location_id, waterbody_name, loc_type, rm) %>%
#   distinct()
#
# # Get coordinates to join, if any
# loc_id = ms_dup %>%
#   select(location_id) %>%
#   distinct() %>%
#   pull(location_id)
#
# # Create loc_id string
# loc_id = paste0(paste0("'", loc_id, "'"), collapse = ", ")
#
# # Get coordinates
# qry = glue("select location_id, ",
#            "st_y(st_transform(geom, 4326)) as lat, ",
#            "st_x(st_transform(geom, 4326)) as lon ",
#            "from location_coordinates where location_id in ({loc_id})")
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# coords = DBI::dbGetQuery(pg_con, qry)
# DBI::dbDisconnect(pg_con)
#
# # Join to ms_dup
# ms_dupc = ms_dup %>%
#   left_join(coords, by = "location_id") %>%
#   arrange(waterbody_name, rm, lat, lon) %>%
#   group_by(rm) %>%
#   mutate(n_seq = row_number()) %>%
#   ungroup()
#
# #=======================================================================================================
# # Step 4: Update all locations to best one, then delete the others...all are reach boundary points
# #=======================================================================================================
#
# # RM 20.2: Set keep and delete ids for each iteration
# keep_id = "'2290bd22-d376-43cc-b4ed-fe4bf6cdcadd'"
# delete_ids = c("0dcdebb3-2851-4207-8cd7-549dd28353b0")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 23.9: Set keep and delete ids for each iteration
# keep_id = "'b44ee690-83fd-4835-8ced-b8a4027053a8'"
# delete_ids = c("79f5fecf-8622-42cd-b850-27e0a8b40fa5")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 25.0: Set keep and delete ids for each iteration
# keep_id = "'f799253e-d6b4-4706-93f3-857bc67e7f76'"
# delete_ids = c("72eac99d-a857-4434-bc82-8042241f9f3f")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 25.2: Set keep and delete ids for each iteration
# keep_id = "'9752acd1-160e-4ebe-8b93-81b332c3768d'"
# delete_ids = c("3045c01a-cbd5-40fe-be45-1297597a4c7d", "0f26fb66-a624-498a-a1d7-fb0846e43147")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 33.3: Set keep and delete ids for each iteration
# keep_id = "'e9fab64e-969b-426e-988b-f5ed097892d4'"
# delete_ids = c("2cfe4d42-1138-4bd1-b622-2b5c1eee542c", "8fc11b3e-830d-445f-86d1-d959942faf1c")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 42.1: Set keep and delete ids for each iteration
# keep_id = "'598ef624-5e20-4d3b-b608-57aa1be50c25'"
# delete_ids = c("337bd19b-0e25-4e41-a422-02e6ab18ca73")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 44.1: Set keep and delete ids for each iteration
# keep_id = "'db8db144-c019-4de3-91d5-9dd93e6c4f41'"
# delete_ids = c("45e96be3-7295-4c98-853d-32a44a941087")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 47.0: Set keep and delete ids for each iteration
# keep_id = "'41d5fe1b-53b9-4a11-b17a-6a3dd0b92629'"
# delete_ids = c("25caeb2c-c776-48ba-9b1b-387fe4a281c1", "c9b32122-0ee7-4abe-b211-3d7a7ed12129")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 54.2: Set keep and delete ids for each iteration
# keep_id = "'3952d247-104c-4e47-aa6a-8a33216a3286'"
# delete_ids = c("76c3a558-5fb1-4830-978a-22cfe433e4df", "99933767-cd64-4082-b5ed-d16fae3c06cb")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 60.0: Set keep and delete ids for each iteration
# keep_id = "'f2b4d522-74ae-4a01-9182-790e20c34747'"
# delete_ids = c("d152feda-55cc-4f47-a428-dfe701951a69")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 64.0: Set keep and delete ids for each iteration
# keep_id = "'424face4-e1a9-43b6-ba5a-8701eb23982b'"
# delete_ids = c("c3747799-0cdd-49a4-ace3-91b8127a3d75")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 64.4: Set keep and delete ids for each iteration
# keep_id = "'5e14a36c-4420-4489-a83d-89e90cdbdd3d'"
# delete_ids = c("a9dcf29a-9852-4560-bcc8-2a35d90804bb", "795b5839-dda6-40d8-a80f-612781757040")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 67.0: Set keep and delete ids for each iteration
# keep_id = "'0b15d7e8-6716-41d4-9a02-e4ce301d0407'"
# delete_ids = c("584a96f5-93f2-4c40-864f-a9632000eb03", "634e9b30-78af-4dcc-9b22-17d0cb7bc098")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 74.9: Set keep and delete ids for each iteration
# keep_id = "'fadf8ab7-5eaa-4c74-b547-15f6fcc86a7d'"
# delete_ids = c("4f626b86-a463-4944-bd55-f88f1d5bd51c")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 75.4: Set keep and delete ids for each iteration
# keep_id = "'142f2eec-feae-4695-a5a8-31b055a22921'"
# delete_ids = c("1d8c5cb0-c220-4bf3-8227-0845d186ef7e", "1b130a38-f0bb-485e-9b16-3ecf0cabbd74")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 77.8: Set keep and delete ids for each iteration
# keep_id = "'a077980f-bd17-4755-931b-617a476855db'"
# delete_ids = c("18a71784-e60e-4d2d-9a4d-9d843ace80e4")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 78.1: Set keep and delete ids for each iteration
# keep_id = "'f53f9f35-14b1-47c6-a062-1dd4453ed244'"
# delete_ids = c("2e96cf51-a044-406e-b01f-8c612ca38ff8")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 81.2: Set keep and delete ids for each iteration
# keep_id = "'c1f6b90f-5b19-46fa-8a9e-8c9c6db54413'"
# delete_ids = c("da844c00-6d9d-457e-893c-d03d3b08b1c6", "7a10838d-78c7-4b64-ba1a-f61973eec018")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 88.3: Set keep and delete ids for each iteration
# keep_id = "'1e4b7d54-ddd0-4478-89e0-0bf38f72228f'"
# delete_ids = c("752dd021-0547-4ef9-ae49-228809db1f90", "647c68b4-4f56-454a-ac08-ca72f15c5856")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 90.3: Set keep and delete ids for each iteration
# keep_id = "'1411b292-c2a6-4817-b9ec-724b82e9aa1a'"
# delete_ids = c("d767c6c6-a9ed-4894-a638-e723353cedf9")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 91.7: Set keep and delete ids for each iteration
# keep_id = "'d865f18a-daa9-4cb2-95d2-89e5b31986c8'"
# delete_ids = c("953fad84-38eb-4587-a9b5-689119120aa1")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 94.0: Set keep and delete ids for each iteration
# keep_id = "'ec56edd0-4df8-4030-a747-704535f69e04'"
# delete_ids = c("4cf97fdc-167e-4e63-a094-6e013221aebc")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 94.4: Set keep and delete ids for each iteration
# keep_id = "'fea92124-19a8-43ef-b352-3727272dc10a'"
# delete_ids = c("059604df-0c37-4840-8c40-ff73d7606892")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 97.0: Set keep and delete ids for each iteration
# keep_id = "'002f9a69-a40d-40f6-a676-892fc628b2b1'"
# delete_ids = c("5e2d76d7-6f84-462f-bfb1-067ccae27d4c")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 97.9: Set keep and delete ids for each iteration
# keep_id = "'af1c11f7-555c-44b6-b2a0-6456e251a6b3'"
# delete_ids = c("11e80652-1a8a-4e32-a682-5bf033cee957")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 100.4: Set keep and delete ids for each iteration
# keep_id = "'86e87a10-cdd0-4682-8dbf-825f4c3f7dd0'"
# delete_ids = c("87af6a97-4c9a-402b-a185-d8ef9635ed09")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 101.8: Set keep and delete ids for each iteration
# keep_id = "'12d1dcae-2752-4e4f-bb78-710554c51484'"
# delete_ids = c("ee232e34-c1f7-44b6-8b9b-f2ed251ccf2f")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 102.7: Set keep and delete ids for each iteration
# keep_id = "'19653b0b-add3-4545-b291-4c9022acbed5'"
# delete_ids = c("8b8d7751-2f2c-4ff1-9f06-80652e74415e")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 103.7: Set keep and delete ids for each iteration
# keep_id = "'52e32937-767c-426c-b791-d024d9f717ed'"
# delete_ids = c("bfd8bb12-7a4d-48b1-a31e-2171056a3a9a", "2b99d2d1-b8d2-4a10-ac26-7ce8961318e3")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 106.2: Set keep and delete ids for each iteration
# keep_id = "'dd900986-15dd-4423-8c8d-a1c686593fa6'"
# delete_ids = c("80871614-fcc2-4bff-b79e-0b3ca609ff4a", "a0b47a49-737d-467b-b8b7-67b0fc1c2af2")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 108.2: Set keep and delete ids for each iteration
# keep_id = "'090aee5a-8003-4a1c-940e-6e9227a8d5b0'"
# delete_ids = c("b489f80b-515f-4687-92f9-18efb33e645b")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 108.5: Set keep and delete ids for each iteration
# keep_id = "'06975798-a3eb-4b19-8001-0f0a97c31fdd'"
# delete_ids = c("cb215cfd-80fb-4d22-ac97-676997e81588")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 108.7: Set keep and delete ids for each iteration
# keep_id = "'7b4bbfa2-2236-454d-b4a0-6314a76b24e9'"
# delete_ids = c("c324c2c9-c5f2-498c-87e2-5c80d90ef8ce", "55aedf55-efce-4ed9-be04-a17e99086d9a")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 110.2: Set keep and delete ids for each iteration
# keep_id = "'a3b745fa-8cc5-4b8c-a443-f84e0641a712'"
# delete_ids = c("209e9329-4c58-4851-bf30-e52ffc79a9a5")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 111.5: Set keep and delete ids for each iteration
# keep_id = "'54278c6e-606f-40cd-a2de-b80d6bf7a8df'"
# delete_ids = c("784fd15b-084f-4595-9ea4-f5e13e6ccfe1")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 112.6: Set keep and delete ids for each iteration
# keep_id = "'6535e36e-a4cb-4ec0-9f3f-88aaa1c7c2ba'"
# delete_ids = c("7eb33e68-d421-423c-80ad-54e0e9ea2d57")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 113.7: Set keep and delete ids for each iteration
# keep_id = "'6a82e405-561d-4fce-8d0c-04e35b124536'"
# delete_ids = c("cd5cd443-9875-4b8d-9bdb-c34b2462e45e")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 116.7: Set keep and delete ids for each iteration
# keep_id = "'f643fa43-68ff-44a8-b312-bc1ff0cd4d99'"
# delete_ids = c("d8894dfa-4cf3-4bf8-9d23-488599cf6819")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 117.5: Set keep and delete ids for each iteration
# keep_id = "'63721b64-23a0-486b-9f05-43c515986702'"
# delete_ids = c("3018b845-77d5-4ace-aeb6-7bdaa71d5710")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 118.1: Set keep and delete ids for each iteration
# keep_id = "'ce555245-0c50-42bf-bbdb-b35663330e2f'"
# delete_ids = c("3f9a7d80-7963-40e2-aaf3-72c0fe666bb8")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 120.1: Set keep and delete ids for each iteration
# keep_id = "'d4e52981-72f9-420f-a838-e3928f337013'"
# delete_ids = c("0612eebb-1553-4184-a3fe-4c664144372a")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 121.3: Set keep and delete ids for each iteration
# keep_id = "'aea98a8b-9d93-4310-9523-3f806e85490f'"
# delete_ids = c("4bd3ebd0-ec30-4165-8142-57fa9f300318")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 122.5: Set keep and delete ids for each iteration
# keep_id = "'9b341d65-6e05-44af-bc31-4b4ba26cfe5d'"
# delete_ids = c("fda753d8-4fcf-4449-b6c6-a456186442d5")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 123.3: Set keep and delete ids for each iteration
# keep_id = "'6bb2f6d9-1007-4dd1-aa0a-b23f0b34de67'"
# delete_ids = c("31106b7e-3ac8-4b74-938d-b0432dfd483e")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 123.5: Set keep and delete ids for each iteration
# keep_id = "'6e379996-cf9d-47fe-b6d8-7f9a6f1f89e6'"
# delete_ids = c("a559df20-caf7-42f9-8e1f-5c500fbf7846")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 124.0: Set keep and delete ids for each iteration
# keep_id = "'bfd68096-59ff-47fc-93f3-493b7190a154'"
# delete_ids = c("7eb8060c-fa5e-4670-8317-04710828d6b3")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 124.3: Set keep and delete ids for each iteration
# keep_id = "'a879acea-c715-4b56-a0f4-b52554a50b6c'"
# delete_ids = c("78755302-afbb-481e-b3b6-9a03121515b6")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 124.5: Set keep and delete ids for each iteration
# keep_id = "'bb2c74f6-ee33-45da-99ea-69a61c921ce5'"
# delete_ids = c("905e77db-a1d4-4158-830d-30187da9972a")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 125.4: Set keep and delete ids for each iteration
# keep_id = "'af04e230-2cf8-4acd-a888-d3ce53f6c4cd'"
# delete_ids = c("9642c572-87aa-4766-b102-428b1254ab38")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 126.4: Set keep and delete ids for each iteration
# keep_id = "'bd6484c9-4a0f-4c9b-a861-821caeb90184'"
# delete_ids = c("6459eca5-40fc-4332-aa9b-20f62784bc48")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 127.2: Set keep and delete ids for each iteration
# keep_id = "'f0bc85a1-b7cc-44de-9ae9-face31eb91e3'"
# delete_ids = c("8ccada1c-3773-439e-aa00-0ac849505b7a")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 127.7: Set keep and delete ids for each iteration
# keep_id = "'fd406650-f81b-440d-bff6-dbb39771c51a'"
# delete_ids = c("1a02001e-a6f4-4fa8-90f0-eaa444950912")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 128.2: Set keep and delete ids for each iteration
# keep_id = "'c2cf01a5-85ce-4c11-8606-d9665d007cf6'"
# delete_ids = c("b22bb453-72bb-408f-a9af-b2604758be3f")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 128.6: Set keep and delete ids for each iteration
# keep_id = "'f9732e83-fa97-4dc6-8205-4b6489dfbccf'"
# delete_ids = c("65c1a49c-14a7-4dae-943e-cd98e906a02b")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 129.3: Set keep and delete ids for each iteration
# keep_id = "'1675df65-1bf9-4cb9-988c-c4e4f95d5698'"
# delete_ids = c("9229e611-0101-4062-8d5f-9b684a04075b")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)
#
# # RM 129.7: Set keep and delete ids for each iteration
# keep_id = "'b5952ded-09aa-421c-81b1-01a2d49887a8'"
# delete_ids = c("533c540a-de24-481b-855e-aaae80cab930")
# delete_ids = paste0(paste0("'", delete_ids, "'"), collapse = ", ")
# # Run updates
# rc_update_local(db = "spawning_ground_archive", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_local(db = "spawning_ground", keep_id = keep_id, delete_ids = delete_ids)
# rc_update_prod(keep_id = keep_id, delete_ids = delete_ids)

#===========================================================================================
# Check if there is any data still tied to old Chehalis waterbody_id
#===========================================================================================

# Get any remaining data for old upper Chehalis streams...see if waterbody can be deleted: Result...none
qry = glue("select loc.location_id, loc.waterbody_id, wb.waterbody_name, wria_id, ",
           "loc.river_mile_measure as rm, loc.location_code, loc.location_name, ",
           "loc.location_description, lt.location_type_description as loc_type ",
           "from location as loc ",
           "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where loc.waterbody_id in ('34d930e5-6178-492c-accd-4add290daef9')")

# Get values from source
pg_con = dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
                   port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
old_locs = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

# # Delete the old Chehalis waterbody
# qry = glue("delete from waterbody_lut where waterbody_id ='34d930e5-6178-492c-accd-4add290daef9'")
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground_archive", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
#
# qry = glue("delete from waterbody_lut where waterbody_id ='34d930e5-6178-492c-accd-4add290daef9'")
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
#
# qry = glue("delete from spawning_ground.waterbody_lut where waterbody_id ='34d930e5-6178-492c-accd-4add290daef9'")
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "FISH", host = pg_host("pg_host_prod"),
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_prod"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)

#===========================================================================================
# Reassign any RMs for Chehalis that lie above 118.91 to EF Chehalis
#===========================================================================================

# Pull out location_ids for RMs above 118.9
ef_rms = ms_rm %>%
  filter(rm > 118.9) %>%
  select(location_id, waterbody_id, waterbody_name, rm) %>%
  distinct() %>%
  left_join(coords, by = "location_id") %>%
  arrange(rm)

# Pull out loc_ids that need to have stream updated to EF-Chehalis
ef_loc_ids = ef_rms %>%
  select(location_id) %>%
  distinct() %>%
  pull(location_id)

# Convert to string
ef_loc_ids = paste0(paste0("'", ef_loc_ids, "'"), collapse = ", ")

# # Update DBs
# qry = glue("update location set waterbody_id = '3ce98c4d-34be-4ae0-9191-bccbeac086ef' ",
#            "where location_id in ({ef_loc_ids})")
# # Run updates
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground_archive", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# # Run updates
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
#
# qry = glue("update spawning_ground.location set waterbody_id = '3ce98c4d-34be-4ae0-9191-bccbeac086ef' ",
#            "where location_id in ({ef_loc_ids})")
# # Run updates
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "FISH", host = pg_host("pg_host_prod"),
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_prod"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)

#===========================================================================================
# Check Chehalis and EF Chehalis RM assignements
#===========================================================================================

# Get all RMs for both Chehalis streams...needed to identify duplicate RMs that should be reassigned
qry = glue("select loc.location_id, loc.waterbody_id, wb.waterbody_name, wria_id, ",
           "loc.river_mile_measure as rm, loc.location_code, loc.location_name, ",
           "loc.location_description, lt.location_type_description as loc_type ",
           "from location as loc ",
           "left join waterbody_lut as wb on loc.waterbody_id = wb.waterbody_id ",
           "left join location_type_lut as lt on loc.location_type_id = lt.location_type_id ",
           "where loc.waterbody_id in ('c3ccff86-1995-42c5-b417-394f09471ec3', '3ce98c4d-34be-4ae0-9191-bccbeac086ef') ",
           "and lt.location_type_description in ('Reach boundary point', 'Section break point') ",
           "order by loc.river_mile_measure")

# Get values from source
pg_con = dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
                   port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
ef_ms_rm = dbGetQuery(pg_con, qry)
dbDisconnect(pg_con)

#===========================================================================================
# Update Chehalis info in waterbody_lut
#===========================================================================================

# # Query
# qry = glue("update waterbody_lut ",
#            "set waterbody_display_name = 'Chehalis R (22.0190)', ",
#            "stream_catalog_code = '22.0190' ",
#            "where waterbody_id = 'c3ccff86-1995-42c5-b417-394f09471ec3'")
# # Run updates
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground_archive", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# # Run updates
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "spawning_ground", host = "localhost",
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_local"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
#
# qry = glue("update spawning_ground.waterbody_lut ",
#            "set waterbody_display_name = 'Chehalis R (22.0190)', ",
#            "stream_catalog_code = '22.0190' ",
#            "where waterbody_id = 'c3ccff86-1995-42c5-b417-394f09471ec3'")
# # Run updates
# pg_con = DBI::dbConnect(RPostgres::Postgres(), dbname = "FISH", host = pg_host("pg_host_prod"),
#                         port = "5432", user = pg_user("pg_user"), password = pg_pw("pg_pwd_prod"))
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)

