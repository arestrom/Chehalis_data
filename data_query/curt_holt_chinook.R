#================================================================
# Export Chinook data to excel in Curt Holt format
#
# Notes: 
#  1. Added sort_order ...see below
#
# AS 2020-06-09
#================================================================

# Clear workspace
rm(list=ls(all=TRUE))

# Load any required libraries
library(RPostgres)
library(dplyr)
library(DBI)
library(glue)
library(tidyr)
library(remisc)
library(stringi)
library(openxlsx)

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
  con <- dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = "5432")
  con
}

#============================================================================
# Get data: Chinook 
#============================================================================

# Set selectable values
run_year = 2016
species = "Chin"
run = "Fall"      # Fall or Spring
#wria = "22.0360"

# Get header data...first function 
qry = glue("select up_wr.wria_code as up_wria, lo_wr.wria_code as lo_wria, ",
           "wb.stream_catalog_code as cat_code, wb.waterbody_display_name as stream_name, ",
           "sd.survey_design_type_code as type, s.survey_datetime as survey_date, ",
           "lo_loc.river_mile_measure as rml, up_loc.river_mile_measure as rmu, ",
           "sm.survey_method_code as method, sf.flow_type_short_description as flow, ",
           "vc.visibility_condition as rifflevis ",
           "from survey as s ",
           "inner join location as up_loc on s.upper_end_point_id = up_loc.location_id ",
           "inner join location as lo_loc on s.lower_end_point_id = lo_loc.location_id ",
           "inner join ")



# STOPPED HERE .... 



db_con = pg_con_local(dbname = "spawning_ground")
dat = dbGetQuery(con, qry)
close(con)

# Get CWT label
con = sgs_con()
ic = sqlQuery(con, as.is = TRUE,
               paste(sep='', 
                     "SELECT * FROM ChehalisCWT_View"))
close(con)

#============================================================================
# Format data
#============================================================================

# Format
table(dat$Type, useNA = "ifany")
dat$Type[dat$Type == "INDX"] = "Index"
dat$Type[dat$Type == "SPOT"] = "Spot"
dat$Type[dat$Type == "SUPP"] = "Supp"
dat = dat %>% 
  mutate(Date = substr(Date, 1, 10)) %>% 
  mutate(StatWeek = fish_stat_week(Date, start_day = "Sun")) %>%                
  mutate(RML = if_else(substr(RML, 1, 1) == ".", paste0("0", RML), RML)) %>%    
  mutate(RMU = if_else(substr(RMU, 1, 1) == ".", paste0("0", RMU), RMU)) %>%
  mutate(RML = if_else(substr(RML, 1, 3) == "00.", substr(RML, 2, nchar(RML)), RML)) %>%
  mutate(RMU = if_else(substr(RMU, 1, 3) == "00.", substr(RMU, 2, nchar(RMU)), RMU)) %>%  
  mutate(RMLn = as.numeric(RML)) %>% 
  mutate(RMUn = as.numeric(RMU)) %>%                                            
  mutate(Method = str_to_title(Method)) %>% 
  mutate(Species = str_to_title(Species))

# Fill in zeros where NA in summary rows
dat[,15:22] = lapply(dat[,15:22], set_na_zero)                                  
dat[,27:35] = lapply(dat[,27:35], set_na_zero)                                        

# Compute RVIS
dat = dat %>% 
  arrange(Type, WRIA, RMLn, RMUn, as.Date(Date)) %>% 
  mutate(sort_order = seq(1, nrow(dat))) %>% 
  mutate(Reach = paste0(Type, "_", WRIA, "_", RML, "_", RMU)) %>%
  mutate(RCOMB = comb_redds) %>% 
  mutate(RVIS = if_else(
    !is.na(RNEW) & !is.na(old_redds) & !is.na(comb_redds), RNEW + old_redds + comb_redds,
    if_else(!is.na(RNEW) & !is.na(old_redds) & is.na(comb_redds), RNEW + old_redds,
            if_else(!is.na(RNEW) & is.na(old_redds) & is.na(comb_redds), RNEW, 
                    if_else(is.na(RNEW) & is.na(old_redds) & !is.na(comb_redds), comb_redds, NA_integer_)))))

# Compute RCUM
dat = dat %>% 
  mutate(RNEW_comb = if_else(!is.na(RNEW) & !is.na(comb_redds), RNEW + comb_redds,
                             if_else(!is.na(RNEW) & is.na(comb_redds), RNEW, 
                                     if_else(is.na(RNEW) & !is.na(comb_redds), comb_redds, NA_integer_)))) %>% 
  group_by(Reach) %>% 
  mutate(RCUM = if_else(Type == "Supp", 
                        cumsum(RNEW_comb),
                        cumsum(RNEW))) %>%
  ungroup() %>% 
  select(Survey_Detail_Id, WRIA, StreamName, Type, Date, StatWeek, RML, RMU, 
         Method, Flow, RiffleVis, LM, LF, LSND, LJ, DM, DF, 
         DSND, DJ, RNEW, RVIS, RCUM, RCOMB, Comments, Reach, ADClippedBeep,        
         ADClippedNoBeep, ADClippedNoHead, UnMarkBeep, UnMarkNoBeep,
         UnMarkNoHead, UnknownMarkBeep, UnknownMarkNoBeep, 
         UnknownMarkNoHead, sort_order)

# Add CWTHeadLabels
ic = ic %>% 
  distinct() %>% 
  arrange(Survey_Detail_Id)

# Pivot out cwt_labels
icp = ic %>% 
  group_by(Survey_Detail_Id) %>% 
  mutate(key = row_number(CWTHeadLabels)) %>% 
  spread(key = key, value = CWTHeadLabels) %>% 
  ungroup() 

# Concatenate cwt_labels to one variable
icpc = icp %>% 
  mutate(cwts = apply(icp[,2:ncol(icp)], 1, paste0, collapse = ", ")) %>% 
  select(Survey_Detail_Id, cwts) %>% 
  mutate(CWTHeadLabels = stri_replace_all_fixed(cwts, pattern = ", NA", replacement = "")) %>% 
  select(Survey_Detail_Id, CWTHeadLabels)

# Add to dat
dat = dat %>% 
  left_join(icpc, by = "Survey_Detail_Id") %>% 
  select(-Survey_Detail_Id)

# Pull out unique set of Reaches to add blank rows
dat_empty = dat
dat_empty[,1:23] = ""
dat_empty[,25:35] = ""
dat_empty = unique(dat_empty)
dat = rbind(dat, dat_empty)

# Add sort order to all rows by reach
sort_dat = dat %>% 
  select(Reach, sort_order) %>% 
  filter(as.integer(sort_order) > 0) %>% 
  mutate(sort_order = as.integer(sort_order)) %>% 
  group_by(Reach) %>% 
  mutate(sort_seq = row_number(Reach)) %>% 
  ungroup() %>% 
  filter(sort_seq == 1) %>% 
  distinct() %>% 
  arrange(sort_order) %>% 
  select(Reach, sort_two = sort_order)

# Add sort_dat to dat
dat = dat %>%
  left_join(sort_dat, by = "Reach")

# Order as before
dat = dat %>% 
  arrange(sort_two, as.Date(Date)) %>% 
  mutate(Comments = if_else(is.na(Comments), "", Comments)) %>% 
  mutate(CWTHeadLabels = if_else(is.na(CWTHeadLabels), "", CWTHeadLabels)) %>% 
  select(-c(Reach, sort_order))

# Format date mm/dd/yyyy   
dat = dat %>% 
  mutate(Date = format(as.Date(Date), format = "%m/%d/%Y")) %>% 
  mutate(Date = if_else(is.na(Date), "", Date)) %>% 
  select(-sort_two)

#============================================================================
# Output to Excel
#============================================================================

# # Write directly to excel
# out_name = paste0(run_year, "_", run, "_", "Chinook.xlsx")
# write.xlsx(dat, file = out_name, colNames = TRUE, sheetName = "Chinook")

# Or fancier with styling
out_name = paste0(run_year, "_", run, "_", "Chinook.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "Chinook", gridLines = TRUE)
writeData(wb, sheet = 1, dat, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:33, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)




