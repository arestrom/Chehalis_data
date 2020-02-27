
# Main fish_encounter query
get_individual_fish = function(fish_encounter_id) {
  qry = glue("select ind.individual_fish_id, ",
             "fc.fish_condition_short_description as fish_condition, ",
             "ft.trauma_type_short_description as fish_trauma, ",
             "gcn.gill_condition_type_description as gill_condition, ",
             "sc.spawn_condition_short_description as spawn_condition, ",
             "cr.cwt_result_type_short_description as cwt_result, ",
             "ac.european_age_code as age_code, gilbert_rich_age_code as gr, ",
             "ind.percent_eggs_retained as pct_eggs, ",
             "ind.eggs_retained_gram as eggs_gram, ",
             "ind.eggs_retained_number as eggs_number, ",
             "ind.fish_sample_number as fish_sample_num, ",
             "ind.scale_sample_card_number as scale_card_num, ",
             "ind.scale_sample_position_number as scale_position_num, ",
             "ind.cwt_snout_sample_number as snout_sample_num, ",
             "ind.cwt_tag_code, ind.genetic_sample_number as genetic_sample_num, ",
             "ind.otolith_sample_number as otolith_sample_num, ",
             "ind.comment_text as fish_comment, ",
             "datetime(ind.created_datetime, 'localtime') as created_date, ind.created_by, ",
             "datetime(ind.modified_datetime, 'localtime') as modified_date, ind.modified_by ",
             "from individual_fish as ind ",
             "left join fish_condition_type_lut as fc on ind.fish_condition_type_id = fc.fish_condition_type_id ",
             "left join fish_trauma_type_lut as ft on ind.fish_trauma_type_id = ft.fish_trauma_type_id ",
             "left join gill_condition_type_lut as gcn on ind.gill_condition_type_id = gcn.gill_condition_type_id ",
             "left join spawn_condition_type_lut as sc on ind.spawn_condition_type_id = sc.spawn_condition_type_id ",
             "left join cwt_result_type_lut as cr on ind.cwt_result_type_id = cr.cwt_result_type_id ",
             "left join age_code_lut as ac on ind.age_code_id = ac.age_code_id ",
             "where ind.fish_encounter_id = '{fish_encounter_id}'")
  con = poolCheckout(pool)
  individual_fish = DBI::dbGetQuery(pool, qry)
  poolReturn(con)
  individual_fish = individual_fish %>%
    mutate(age_code = if_else(!is.na(age_code), paste0("eu: ", age_code), age_code)) %>%
    mutate(age_code = if_else(!is.na(age_code) & !is.na(gr), paste0(age_code, "; gr: ", gr), age_code)) %>%
    mutate(created_date = as.POSIXct(created_date, tz = "America/Los_Angeles")) %>%
    mutate(created_dt = format(created_date, "%m/%d/%Y %H:%M")) %>%
    mutate(modified_date = as.POSIXct(modified_date, tz = "America/Los_Angeles")) %>%
    mutate(modified_dt = format(modified_date, "%m/%d/%Y %H:%M")) %>%
    select(individual_fish_id, fish_condition, fish_trauma, gill_condition, spawn_condition,
           fish_sample_num, scale_card_num, scale_position_num, age_code, snout_sample_num,
           cwt_tag_code, cwt_result, genetic_sample_num, otolith_sample_num, pct_eggs,
           eggs_gram, eggs_number, fish_comment, created_date, created_dt, created_by,
           modified_date, modified_dt, modified_by) %>%
    arrange(created_date)
  return(individual_fish)
}

#==========================================================================
# Get generic lut input values
#==========================================================================

# Fish condition
get_fish_condition = function() {
  qry = glue("select fish_condition_type_id, fish_condition_short_description as fish_condition ",
             "from fish_condition_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  fish_condition_list = DBI::dbGetQuery(con, qry) %>%
    arrange(fish_condition) %>%
    select(fish_condition_type_id, fish_condition)
  poolReturn(con)
  return(fish_condition_list)
}

# Fish trauma
get_fish_trauma = function() {
  qry = glue("select fish_trauma_type_id, trauma_type_short_description as fish_trauma ",
             "from fish_trauma_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  fish_trauma_list = DBI::dbGetQuery(con, qry) %>%
    arrange(fish_trauma) %>%
    select(fish_trauma_type_id, fish_trauma)
  poolReturn(con)
  return(fish_trauma_list)
}

# Gill conditon
get_gill_condition = function() {
  qry = glue("select gill_condition_type_id, gill_condition_type_description as gill_condition ",
             "from gill_condition_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  gill_condition_list = DBI::dbGetQuery(con, qry) %>%
    arrange(gill_condition) %>%
    select(gill_condition_type_id, gill_condition)
  poolReturn(con)
  return(gill_condition_list)
}

# Spawn conditon
get_spawn_condition = function() {
  qry = glue("select spawn_condition_type_id, spawn_condition_short_description as spawn_condition ",
             "from spawn_condition_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  spawn_condition_list = DBI::dbGetQuery(con, qry) %>%
    arrange(spawn_condition) %>%
    select(spawn_condition_type_id, spawn_condition)
  poolReturn(con)
  return(spawn_condition_list)
}

# Age code
get_age_code = function() {
  qry = glue("select age_code_id, european_age_code as age_code, gilbert_rich_age_code as gr ",
             "from age_code_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  age_code_list = DBI::dbGetQuery(con, qry) %>%
    mutate(age_code = paste0("eu: ", age_code)) %>%
    mutate(age_code = if_else(!is.na(gr), paste0(age_code, "; gr: ", gr), age_code)) %>%
    arrange(age_code) %>%
    select(age_code_id, age_code)
  poolReturn(con)
  return(age_code_list)
}

# CWT result
get_cwt_result = function() {
  qry = glue("select cwt_result_type_id, cwt_result_type_short_description as cwt_result ",
             "from cwt_result_type_lut ",
             "where obsolete_datetime is null")
  con = poolCheckout(pool)
  cwt_result_list = DBI::dbGetQuery(con, qry) %>%
    arrange(cwt_result) %>%
    select(cwt_result_type_id, cwt_result)
  poolReturn(con)
  return(cwt_result_list)
}

#========================================================
# Insert callback
#========================================================

# Define the insert callback
individual_fish_insert = function(new_individual_fish_values) {
  new_insert_values = new_individual_fish_values
  individual_fish_id = remisc::get_uuid(1L)
  # Pull out data
  fish_encounter_id = new_insert_values$fish_encounter_id
  fish_condition_type_id = new_insert_values$fish_condition_type_id
  fish_trauma_type_id =  new_insert_values$fish_trauma_type_id
  gill_condition_type_id = new_insert_values$gill_condition_type_id
  spawn_condition_type_id = new_insert_values$spawn_condition_type_id
  cwt_result_type_id = new_insert_values$cwt_result_type_id
  age_code_id = new_insert_values$age_code_id
  percent_eggs_retained = new_insert_values$pct_eggs
  eggs_retained_gram = new_insert_values$eggs_gram
  eggs_retained_number = new_insert_values$eggs_number
  fish_sample_number = new_insert_values$fish_sample_num
  scale_sample_card_number = new_insert_values$scale_card_num
  scale_sample_position_number = new_insert_values$scale_position_num
  cwt_snout_sample_number = new_insert_values$snout_sample_num
  cwt_tag_code = new_insert_values$cwt_tag_code
  genetic_sample_number = new_insert_values$genetic_sample_num
  otolith_sample_number = new_insert_values$otolith_sample_num
  comment_text = new_insert_values$fish_comment
  if (is.na(fish_sample_number) | fish_sample_number == "") { fish_sample_number = NA }
  if (is.na(scale_sample_card_number) | scale_sample_card_number == "") { scale_sample_card_number = NA }
  if (is.na(scale_sample_position_number) | scale_sample_position_number == "") { scale_sample_position_number = NA }
  if (is.na(cwt_snout_sample_number) | cwt_snout_sample_number == "") { cwt_snout_sample_number = NA }
  if (is.na(cwt_tag_code) | cwt_tag_code == "") { cwt_tag_code = NA }
  if (is.na(genetic_sample_number) | genetic_sample_number == "") { genetic_sample_number = NA }
  if (is.na(otolith_sample_number) | otolith_sample_number == "") { otolith_sample_number = NA }
  if (is.na(comment_text) | comment_text == "") { comment_text = NA }
  created_by = new_insert_values$created_by
  # Checkout a connection
  con = poolCheckout(pool)
  insert_result = dbSendStatement(
    con, glue_sql("INSERT INTO individual_fish (",
                  "individual_fish_id, ",
                  "fish_encounter_id, ",
                  "fish_condition_type_id, ",
                  "fish_trauma_type_id, ",
                  "gill_condition_type_id, ",
                  "spawn_condition_type_id, ",
                  "cwt_result_type_id, ",
                  "age_code_id, ",
                  "percent_eggs_retained, ",
                  "eggs_retained_gram, ",
                  "eggs_retained_number, ",
                  "fish_sample_number, ",
                  "scale_sample_card_number, ",
                  "scale_sample_position_number, ",
                  "cwt_snout_sample_number, ",
                  "cwt_tag_code, ",
                  "genetic_sample_number, ",
                  "otolith_sample_number, ",
                  "comment_text, ",
                  "created_by) ",
                  "VALUES (",
                  "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
  dbBind(insert_result, list(individual_fish_id, fish_encounter_id, fish_condition_type_id, fish_trauma_type_id,
                             gill_condition_type_id, spawn_condition_type_id, cwt_result_type_id,
                             age_code_id, percent_eggs_retained, eggs_retained_gram,
                             eggs_retained_number, fish_sample_number, scale_sample_card_number,
                             scale_sample_position_number, cwt_snout_sample_number, cwt_tag_code,
                             genetic_sample_number, otolith_sample_number, comment_text, created_by))
  dbGetRowsAffected(insert_result)
  dbClearResult(insert_result)
  poolReturn(con)
}

#========================================================
# Edit update callback
#========================================================

# Define update callback
individual_fish_update = function(individual_fish_edit_values) {
  edit_values = individual_fish_edit_values
  # Pull out data
  individual_fish_id = edit_values$individual_fish_id
  fish_condition_type_id = edit_values$fish_condition_type_id
  fish_trauma_type_id =  edit_values$fish_trauma_type_id
  gill_condition_type_id = edit_values$gill_condition_type_id
  spawn_condition_type_id = edit_values$spawn_condition_type_id
  cwt_result_type_id = edit_values$cwt_result_type_id
  age_code_id = edit_values$age_code_id
  percent_eggs_retained = edit_values$pct_eggs
  eggs_retained_gram = edit_values$eggs_gram
  eggs_retained_number = edit_values$eggs_number
  fish_sample_number = edit_values$fish_sample_num
  scale_sample_card_number = edit_values$scale_card_num
  scale_sample_position_number = edit_values$scale_position_num
  cwt_snout_sample_number = edit_values$snout_sample_num
  cwt_tag_code = edit_values$cwt_tag_code
  genetic_sample_number = edit_values$genetic_sample_num
  otolith_sample_number = edit_values$otolith_sample_num
  comment_text = edit_values$fish_comment
  if (is.na(fish_sample_number) | fish_sample_number == "") { fish_sample_number = NA }
  if (is.na(scale_sample_card_number) | scale_sample_card_number == "") { scale_sample_card_number = NA }
  if (is.na(scale_sample_position_number) | scale_sample_position_number == "") { scale_sample_position_number = NA }
  if (is.na(cwt_snout_sample_number) | cwt_snout_sample_number == "") { cwt_snout_sample_number = NA }
  if (is.na(cwt_tag_code) | cwt_tag_code == "") { cwt_tag_code = NA }
  if (is.na(genetic_sample_number) | genetic_sample_number == "") { genetic_sample_number = NA }
  if (is.na(otolith_sample_number) | otolith_sample_number == "") { otolith_sample_number = NA }
  if (is.na(comment_text) | comment_text == "") { comment_text = NA }
  mod_dt = format(lubridate::with_tz(Sys.time(), "UTC"))
  mod_by = Sys.getenv("USERNAME")
  # Checkout a connection
  con = poolCheckout(pool)
  update_result = dbSendStatement(
    con, glue_sql("UPDATE individual_fish SET ",
                  "fish_condition_type_id = ?, ",
                  "fish_trauma_type_id = ?, ",
                  "gill_condition_type_id = ?, ",
                  "spawn_condition_type_id = ?, ",
                  "cwt_result_type_id = ?, ",
                  "age_code_id = ?, ",
                  "percent_eggs_retained = ?, ",
                  "eggs_retained_gram = ?, ",
                  "eggs_retained_number = ?, ",
                  "fish_sample_number = ?, ",
                  "scale_sample_card_number = ?, ",
                  "scale_sample_position_number = ?, ",
                  "cwt_snout_sample_number = ?, ",
                  "cwt_tag_code = ?, ",
                  "genetic_sample_number = ?, ",
                  "otolith_sample_number = ?, ",
                  "comment_text = ?, ",
                  "modified_datetime = ?, ",
                  "modified_by = ? ",
                  "where individual_fish_id = ?"))
  dbBind(update_result, list(fish_condition_type_id, fish_trauma_type_id,
                             gill_condition_type_id, spawn_condition_type_id,
                             cwt_result_type_id, age_code_id, percent_eggs_retained,
                             eggs_retained_gram, eggs_retained_number,
                             fish_sample_number, scale_sample_card_number,
                             scale_sample_position_number, cwt_snout_sample_number,
                             cwt_tag_code, genetic_sample_number,
                             otolith_sample_number, comment_text,
                             mod_dt, mod_by, individual_fish_id))
  dbGetRowsAffected(update_result)
  dbClearResult(update_result)
  poolReturn(con)
}

#========================================================
# Identify individual fish dependencies prior to delete
#========================================================

# Identify fish_encounter dependencies prior to delete
get_individual_fish_dependencies = function(individual_fish_id) {
  qry = glue("select ",
             "count(fl.fish_length_measurement_id) as fish_length_measurement ",
             "from fish_length_measurement as fl ",
             "where fl.individual_fish_id = '{individual_fish_id}'")
  con = poolCheckout(pool)
  individual_fish_dependents = DBI::dbGetQuery(pool, qry)
  poolReturn(con)
  has_entries = function(x) any(x > 0L)
  individual_fish_dependents = individual_fish_dependents %>%
    select_if(has_entries)
  return(individual_fish_dependents)
}

#========================================================
# Delete callback
#========================================================

# Define delete callback
individual_fish_delete = function(delete_values) {
  individual_fish_id = delete_values$individual_fish_id
  con = poolCheckout(pool)
  delete_result = dbSendStatement(
    con, glue_sql("DELETE FROM individual_fish WHERE individual_fish_id = ?"))
  dbBind(delete_result, list(individual_fish_id))
  dbGetRowsAffected(delete_result)
  dbClearResult(delete_result)
  poolReturn(con)
}











