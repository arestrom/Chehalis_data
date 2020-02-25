# Main survey comment query
get_survey_comment = function(survey_id) {
  qry = glue("select sc.survey_comment_id, ars.area_surveyed, ",
             "fa.fish_abundance_condition as abundance_condition, ",
             "stc.stream_condition, sf.flow_type_short_description as stream_flow, ",
             "cc.survey_count_condition as count_condition, ",
             "sd.survey_direction_description as survey_direction, ",
             "st.survey_timing, vc.visibility_condition, ",
             "vt.visibility_type_short_description as visibility_type, ",
             "wt.weather_type_description as weather_type, ",
             "sc.comment_text as survey_comment, ",
             "sc.created_datetime as created_date, ",
             "sc.created_by, sc.modified_datetime as modified_date, ",
             "sc.modified_by ",
             "from survey_comment as sc ",
             "left join area_surveyed_lut as ars on sc.area_surveyed_id = ars.area_surveyed_id ",
             "left join fish_abundance_condition_lut as fa on sc.fish_abundance_condition_id = fa.fish_abundance_condition_id ",
             "left join stream_condition_lut as stc on sc.stream_condition_id = stc.stream_condition_id ",
             "left join stream_flow_type_lut as sf on sc.stream_flow_type_id = sf.stream_flow_type_id ",
             "left join survey_count_condition_lut as cc on sc.survey_count_condition_id = cc.survey_count_condition_id ",
             "left join survey_direction_lut as sd on sc.survey_direction_id = sd.survey_direction_id ",
             "left join survey_timing_lut as st on sc.survey_timing_id = st.survey_timing_id ",
             "left join visibility_condition_lut as vc on sc.visibility_condition_id = vc.visibility_condition_id ",
             "left join visibility_type_lut as vt on sc.visibility_type_id = vt.visibility_type_id ",
             "left join weather_type_lut as wt on sc.weather_type_id = wt.weather_type_id ",
             "where sc.survey_id = '{survey_id}'")
  con = poolCheckout(pool)
  survey_comments = DBI::dbGetQuery(pool, qry)
  poolReturn(con)
  survey_comments = survey_comments %>%
    # mutate(survey_comment_id = tolower(survey_comment_id)) %>%
    mutate(created_date = with_tz(created_date, tzone = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = with_tz(modified_date, tzone = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(survey_comment_id, area_surveyed, abundance_condition, stream_condition,
           stream_flow, count_condition, survey_direction, survey_timing,
           visibility_condition, visibility_type, weather_type, survey_comment,
           created_date, created_dt, created_by, modified_date, modified_dt,
           modified_by) %>%
    arrange(created_date)
  return(survey_comments)
}

#==========================================================================
# Get generic lut input values...data_source, etc.
#==========================================================================

# Area surveyed
get_area_surveyed = function() {
  qry = glue("select area_surveyed_id, area_surveyed ",
             "from area_surveyed_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  area_surveyed_list = DBI::dbGetQuery(pool, qry) %>%
    arrange(area_surveyed) %>%
    select(area_surveyed_id, area_surveyed)
  poolReturn(con)
  return(area_surveyed_list)
}

# Abundance
get_abundance_condition = function() {
  qry = glue("select fish_abundance_condition_id, fish_abundance_condition as abundance_condition ",
             "from fish_abundance_condition_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  fish_abundance_list = DBI::dbGetQuery(con, qry) %>%
    arrange(abundance_condition) %>%
    select(fish_abundance_condition_id, abundance_condition)
  poolReturn(con)
  return(fish_abundance_list)
}

# Stream condition
get_stream_condition = function() {
  qry = glue("select stream_condition_id, stream_condition ",
             "from stream_condition_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  stream_condition_list = DBI::dbGetQuery(con, qry) %>%
    arrange(stream_condition) %>%
    select(stream_condition_id, stream_condition)
  poolReturn(con)
  return(stream_condition_list)
}

# Stream condition
get_stream_flow = function() {
  qry = glue("select stream_flow_type_id, flow_type_short_description as stream_flow ",
             "from stream_flow_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  stream_flow_list = DBI::dbGetQuery(con, qry) %>%
    arrange(stream_flow) %>%
    select(stream_flow_type_id, stream_flow)
  poolReturn(con)
  return(stream_flow_list)
}

# Count condition
get_count_condition = function() {
  qry = glue("select survey_count_condition_id, survey_count_condition as count_condition ",
             "from survey_count_condition_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  count_condition_list = DBI::dbGetQuery(con, qry) %>%
    arrange(count_condition) %>%
    select(survey_count_condition_id, count_condition)
  poolReturn(con)
  return(count_condition_list)
}

# Survey direction
get_survey_direction = function() {
  qry = glue("select survey_direction_id, survey_direction_description as survey_direction ",
             "from survey_direction_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  survey_direction_list = DBI::dbGetQuery(con, qry) %>%
    arrange(survey_direction) %>%
    select(survey_direction_id, survey_direction)
  poolReturn(con)
  return(survey_direction_list)
}

# Survey timing
get_survey_timing = function() {
  qry = glue("select survey_timing_id, survey_timing ",
             "from survey_timing_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  survey_timing_list = DBI::dbGetQuery(con, qry) %>%
    arrange(survey_timing) %>%
    select(survey_timing_id, survey_timing)
  poolReturn(con)
  return(survey_timing_list)
}

# Visibility condition
get_visibility_condition = function() {
  qry = glue("select visibility_condition_id, visibility_condition ",
             "from visibility_condition_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  visibility_condition_list = DBI::dbGetQuery(con, qry) %>%
    arrange(visibility_condition) %>%
    select(visibility_condition_id, visibility_condition)
  poolReturn(con)
  return(visibility_condition_list)
}

# Visibility type
get_visibility_type = function() {
  qry = glue("select visibility_type_id, visibility_type_short_description as visibility_type ",
             "from visibility_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  visibility_type_list = DBI::dbGetQuery(con, qry) %>%
    arrange(visibility_type) %>%
    select(visibility_type_id, visibility_type)
  poolReturn(con)
  return(visibility_type_list)
}

# Weather type
get_weather_type = function() {
  qry = glue("select weather_type_id, weather_type_description as weather_type ",
             "from weather_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  weather_type_list = DBI::dbGetQuery(con, qry) %>%
    arrange(weather_type) %>%
    select(weather_type_id, weather_type)
  poolReturn(con)
  return(weather_type_list)
}

#========================================================
# Insert callback
#========================================================

# Define the insert callback
survey_comment_insert = function(new_comment_values) {
  new_comment_values = new_comment_values
  # Pull out data
  survey_id = new_comment_values$survey_id
  area_surveyed_id = new_comment_values$area_surveyed_id
  fish_abundance_condition_id =  new_comment_values$fish_abundance_condition_id
  stream_condition_id = new_comment_values$stream_condition_id
  stream_flow_type_id = new_comment_values$stream_flow_type_id
  survey_count_condition_id = new_comment_values$survey_count_condition_id
  survey_direction_id = new_comment_values$survey_direction_id
  survey_timing_id =new_comment_values$survey_timing_id
  visibility_condition_id = new_comment_values$visibility_condition_id
  visibility_type_id = new_comment_values$visibility_type_id
  weather_type_id = new_comment_values$weather_type_id
  comment_text = new_comment_values$comment_text
  if (is.na(comment_text) | comment_text == "") { comment_text = NA }
  created_by = new_comment_values$created_by
  # Checkout a connection
  con = poolCheckout(pool)
  insert_result = dbSendStatement(
    con, glue_sql("INSERT INTO survey_comment (",
                  "survey_id, ",
                  "area_surveyed_id, ",
                  "fish_abundance_condition_id, ",
                  "stream_condition_id, ",
                  "stream_flow_type_id, ",
                  "survey_count_condition_id, ",
                  "survey_direction_id, ",
                  "survey_timing_id, ",
                  "visibility_condition_id, ",
                  "visibility_type_id, ",
                  "weather_type_id, ",
                  "comment_text, ",
                  "created_by) ",
                  "VALUES (",
                  "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"))
  dbBind(insert_result, list(survey_id, area_surveyed_id, fish_abundance_condition_id,
                             stream_condition_id, stream_flow_type_id,
                             survey_count_condition_id, survey_direction_id,
                             survey_timing_id, visibility_condition_id,
                             visibility_type_id, weather_type_id, comment_text,
                             created_by))
  dbGetRowsAffected(insert_result)
  dbClearResult(insert_result)
  poolReturn(con)
}

#========================================================
# Edit update callback
#========================================================

# Define update callback
survey_comment_update = function(comment_edit_values) {
  edit_values = comment_edit_values
  # Pull out data
  survey_comment_id = edit_values$survey_comment_id
  area_surveyed_id = edit_values$area_surveyed_id
  fish_abundance_condition_id = edit_values$fish_abundance_condition_id
  stream_condition_id = edit_values$stream_condition_id
  stream_flow_type_id = edit_values$stream_flow_type_id
  survey_count_condition_id = edit_values$survey_count_condition_id
  survey_direction_id = edit_values$survey_direction_id
  survey_timing_id = edit_values$survey_timing_id
  visibility_condition_id = edit_values$visibility_condition_id
  visibility_type_id = edit_values$visibility_type_id
  weather_type_id = edit_values$weather_type_id
  comment_text = edit_values$comment_text
  if (is.na(comment_text) | comment_text == "") { comment_text = NA }
  mod_dt = lubridate::with_tz(Sys.time(), "UTC")
  mod_by = Sys.getenv("USERNAME")
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE survey_comment SET ",
                  "area_surveyed_id = $1, ",
                  "fish_abundance_condition_id = $2, ",
                  "stream_condition_id = $3, ",
                  "stream_flow_type_id = $4, ",
                  "survey_count_condition_id = $5, ",
                  "survey_direction_id = $6, ",
                  "survey_timing_id = $7, ",
                  "visibility_condition_id = $8, ",
                  "visibility_type_id = $9, ",
                  "weather_type_id = $10, ",
                  "comment_text = $11, ",
                  "modified_datetime = $12, ",
                  "modified_by = $13 ",
                  "where survey_comment_id = $14"))
  dbBind(update_result, list(area_surveyed_id, fish_abundance_condition_id,
                             stream_condition_id, stream_flow_type_id,
                             survey_count_condition_id, survey_direction_id,
                             survey_timing_id, visibility_condition_id,
                             visibility_type_id, weather_type_id, comment_text,
                             mod_dt, mod_by, survey_comment_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  poolReturn(con)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
survey_comment_delete = function(delete_values) {
  survey_comment_id = delete_values$survey_comment_id
  con = poolCheckout(pool)
  delete_result = dbSendStatement(
    con, glue_sql("DELETE FROM survey_comment WHERE survey_comment_id = $1"))
  dbBind(delete_result, list(survey_comment_id))
  dbGetRowsAffected(delete_result)
  dbClearResult(delete_result)
  poolReturn(con)
}




