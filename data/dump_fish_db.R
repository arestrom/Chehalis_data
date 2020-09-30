#===========================================================================
# Get rid of FISH db from local
#
# Notes:
#  1. First had to change name to fish then dropped.
#
# AS 2020-09-30
#===========================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Load libraries
library(DBI)
library(RPostgres)
library(glue)

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

# Function to connect to postgres
pg_con_local = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

#=========================================================================
# Import all stream data from sg for WRIAs 22 and 23
#=========================================================================

# Define query to get needed data
qry = glue("drop database FISH")

# Get values from source
db_con = pg_con_local(dbname = "postgres")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)
