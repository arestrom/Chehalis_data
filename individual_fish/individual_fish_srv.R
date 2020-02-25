#========================================================
# Generate lut select ui's
#========================================================

output$fish_condition_select = renderUI({
  fish_condition_list = get_fish_condition()$fish_condition
  fish_condition_list = c("", fish_condition_list)
  selectizeInput("fish_condition_select", label = "fish_condition",
                 choices = fish_condition_list, selected = "No data",
                 width = "150px")
})

output$fish_trauma_select = renderUI({
  fish_trauma_list = get_fish_trauma()$fish_trauma
  fish_trauma_list = c("", fish_trauma_list)
  selectizeInput("fish_trauma_select", label = "fish_trauma",
                 choices = fish_trauma_list, selected = "No data",
                 width = "150px")
})

output$gill_condition_select = renderUI({
  gill_condition_list = get_gill_condition()$gill_condition
  gill_condition_list = c("", gill_condition_list)
  selectizeInput("gill_condition_select", label = "gill_condition",
                 choices = gill_condition_list, selected = "No data",
                 width = "200px")
})

output$spawn_condition_select = renderUI({
  spawn_condition_list = get_spawn_condition()$spawn_condition
  spawn_condition_list = c("", spawn_condition_list)
  selectizeInput("spawn_condition_select", label = "spawn_condition",
                 choices = spawn_condition_list, selected = "No data",
                 width = "200px")
})

output$age_code_select = renderUI({
  age_code_list = get_age_code()$age_code
  age_code_list = c("", age_code_list)
  selectizeInput("age_code_select", label = "age_code",
                 choices = age_code_list, selected = "No data",
                 width = "150px")
})

output$cwt_result_select = renderUI({
  cwt_result_list = get_cwt_result()$cwt_result
  cwt_result_list = c("", cwt_result_list)
  selectizeInput("cwt_result_select", label = "cwt_result",
                 choices = cwt_result_list, selected = "Not applicable",
                 width = "130px")
})

#========================================================
# Primary datatable for individual_fish
#========================================================

# Primary DT datatable for survey_intent
output$individual_fishes = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_encounters_rows_selected)
  req(!is.na(selected_fish_encounter_data()$fish_encounter_id))
  individual_fish_title = glue("{selected_survey_event_data()$species} data for {input$stream_select} on ",
                               "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                               "to {selected_survey_data()$lo_rm}")
  individual_fish_data = get_individual_fish(selected_fish_encounter_data()$fish_encounter_id) %>%
    select(fish_condition, fish_trauma, gill_condition, spawn_condition, fish_sample_num, scale_card_num,
           scale_position_num, age_code, snout_sample_num, cwt_tag_code, cwt_result, genetic_sample_num,
           otolith_sample_num, pct_eggs, eggs_gram, eggs_number, fish_comment, created_dt, created_by,
           modified_dt, modified_by)

  # Generate table
  datatable(individual_fish_data,
            selection = list(mode = 'single'),
            options = list(dom = 'ltp',
                           pageLength = 5,
                           lengthMenu = c(1, 5, 10, 20),
                           scrollX = T,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: black; width: auto;',
              htmltools::em(htmltools::strong(individual_fish_title))))
})

# Create surveys DT proxy object
individual_fish_dt_proxy = dataTableProxy(outputId = "individual_fishes")

#========================================================
# Collect encounter values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_individual_fish_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_encounters_rows_selected)
  req(input$individual_fishes_rows_selected)
  req(!is.na(selected_fish_encounter_data()$fish_encounter_id))
  individual_fish_data = get_individual_fish(selected_fish_encounter_data()$fish_encounter_id)
  individual_fish_row = input$individual_fishes_rows_selected
  selected_individual_fish = tibble(individual_fish_id = individual_fish_data$individual_fish_id[individual_fish_row],
                                    fish_condition = individual_fish_data$fish_condition[individual_fish_row],
                                    fish_trauma = individual_fish_data$fish_trauma[individual_fish_row],
                                    gill_condition = individual_fish_data$gill_condition[individual_fish_row],
                                    spawn_condition = individual_fish_data$spawn_condition[individual_fish_row],
                                    fish_sample_num = individual_fish_data$fish_sample_num[individual_fish_row],
                                    scale_card_num = individual_fish_data$scale_card_num[individual_fish_row],
                                    scale_position_num = individual_fish_data$scale_position_num[individual_fish_row],
                                    age_code = individual_fish_data$age_code[individual_fish_row],
                                    snout_sample_num = individual_fish_data$snout_sample_num[individual_fish_row],
                                    cwt_tag_code = individual_fish_data$cwt_tag_code[individual_fish_row],
                                    cwt_result = individual_fish_data$cwt_result[individual_fish_row],
                                    genetic_sample_num = individual_fish_data$genetic_sample_num[individual_fish_row],
                                    otolith_sample_num = individual_fish_data$otolith_sample_num[individual_fish_row],
                                    pct_eggs = individual_fish_data$pct_eggs[individual_fish_row],
                                    eggs_gram = individual_fish_data$eggs_gram[individual_fish_row],
                                    eggs_number = individual_fish_data$eggs_number[individual_fish_row],
                                    fish_comment = individual_fish_data$fish_comment[individual_fish_row],
                                    created_date = individual_fish_data$created_date[individual_fish_row],
                                    created_by = individual_fish_data$created_by[individual_fish_row],
                                    modified_date = individual_fish_data$modified_date[individual_fish_row],
                                    modified_by = individual_fish_data$modified_by[individual_fish_row])
  return(selected_individual_fish)
})

#========================================================
# Update event select inputs to values in selected row
#========================================================

# Update all survey input values to values in selected row
observeEvent(input$individual_fishes_rows_selected, {
  sindat = selected_individual_fish_data()
  updateSelectizeInput(session, "fish_condition_select", selected = sindat$fish_condition)
  updateSelectizeInput(session, "fish_trauma_select", selected = sindat$fish_trauma)
  updateSelectizeInput(session, "gill_condition_select", selected = sindat$gill_condition)
  updateSelectizeInput(session, "spawn_condition_select", selected = sindat$spawn_condition)
  updateTextInput(session, "fish_sample_num_input", value = sindat$fish_sample_num)
  updateTextInput(session, "scale_card_num_input", value = sindat$scale_card_num)
  updateTextInput(session, "scale_position_num_input", value = sindat$scale_position_num)
  updateSelectizeInput(session, "age_code_select", selected = sindat$age_code)
  updateTextInput(session, "snout_sample_num_input", value = sindat$snout_sample_num)
  updateTextInput(session, "cwt_tag_code_input", value = sindat$cwt_tag_code)
  updateSelectizeInput(session, "cwt_result_select", selected = sindat$cwt_result)
  updateTextInput(session, "genetic_sample_num_input", value = sindat$genetic_sample_num)
  updateTextInput(session, "otolith_sample_num_input", value = sindat$otolith_sample_num)
  updateNumericInput(session, "pct_eggs_input", value = sindat$pct_eggs)
  updateNumericInput(session, "eggs_gram_input", value = sindat$eggs_gram)
  updateNumericInput(session, "eggs_number_input", value = sindat$eggs_number)
  updateTextInput(session, "ind_fish_comment_input", value = sindat$fish_comment)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Disable "New" button if a row of comments already exists
observe({
  req(!is.na(selected_fish_encounter_data()$fish_encounter_id))
  input$insert_individual_fish
  ind_fish_data = get_individual_fish(selected_fish_encounter_data()$fish_encounter_id)
  if (nrow(ind_fish_data) >= 1L) {
    shinyjs::disable("ind_fish_add")
  } else {
    shinyjs::enable("ind_fish_add")
  }
})

# Create reactive to collect input values for insert actions
individual_fish_create = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_encounters_rows_selected)
  req(!is.na(selected_fish_encounter_data()$fish_encounter_id))
  # fish_encounter_id
  fish_encounter_id_input = selected_fish_encounter_data()$fish_encounter_id
  # Fish count
  fish_count_input = selected_fish_encounter_data()$fish_count
  # Fish condition
  fish_condition_input = input$fish_condition_select
  if ( fish_condition_input == "" ) {
    fish_condition_type_id = NA
  } else {
    fish_condition_vals = get_fish_condition()
    fish_condition_type_id = fish_condition_vals %>%
      filter(fish_condition == fish_condition_input) %>%
      pull(fish_condition_type_id)
  }
  # Fish trauma
  fish_trauma_input = input$fish_trauma_select
  if ( fish_trauma_input == "" ) {
    fish_trauma_type_id = NA
  } else {
    fish_trauma_vals = get_fish_trauma()
    fish_trauma_type_id = fish_trauma_vals %>%
      filter(fish_trauma == fish_trauma_input) %>%
      pull(fish_trauma_type_id)
  }
  # Gill condition
  gill_condition_input = input$gill_condition_select
  if ( gill_condition_input == "" ) {
    gill_condition_type_id = NA
  } else {
    gill_condition_vals = get_gill_condition()
    gill_condition_type_id = gill_condition_vals %>%
      filter(gill_condition == gill_condition_input) %>%
      pull(gill_condition_type_id)
  }
  # Spawn condition
  spawn_condition_input = input$spawn_condition_select
  if ( spawn_condition_input == "" ) {
    spawn_condition_type_id = NA
  } else {
    spawn_condition_vals = get_spawn_condition()
    spawn_condition_type_id = spawn_condition_vals %>%
      filter(spawn_condition == spawn_condition_input) %>%
      pull(spawn_condition_type_id)
  }
  # Age code
  age_code_input = input$age_code_select
  if ( age_code_input == "" ) {
    age_code_id = NA
  } else {
    age_code_vals = get_age_code()
    age_code_id = age_code_vals %>%
      filter(age_code == age_code_input) %>%
      pull(age_code_id)
  }
  # CWT result
  cwt_result_input = input$cwt_result_select
  if ( cwt_result_input == "" ) {
    cwt_result_type_id = NA
  } else {
    cwt_result_vals = get_cwt_result()
    cwt_result_type_id = cwt_result_vals %>%
      filter(cwt_result == cwt_result_input) %>%
      pull(cwt_result_type_id)
  }
  new_individual_fish = tibble(fish_encounter_id = fish_encounter_id_input,
                               fish_count = fish_count_input,
                               fish_condition = fish_condition_input,
                               fish_condition_type_id = fish_condition_type_id,
                               fish_trauma = fish_trauma_input,
                               fish_trauma_type_id = fish_trauma_type_id,
                               gill_condition = gill_condition_input,
                               gill_condition_type_id = gill_condition_type_id,
                               spawn_condition = spawn_condition_input,
                               spawn_condition_type_id = spawn_condition_type_id,
                               fish_sample_num = input$fish_sample_num_input,
                               scale_card_num = input$scale_card_num_input,
                               scale_position_num = input$scale_position_num_input,
                               age_code = age_code_input,
                               age_code_id = age_code_id,
                               snout_sample_num = input$snout_sample_num_input,
                               cwt_tag_code = input$cwt_tag_code_input,
                               cwt_result = cwt_result_input,
                               cwt_result_type_id = cwt_result_type_id,
                               genetic_sample_num = input$genetic_sample_num_input,
                               otolith_sample_num = input$otolith_sample_num_input,
                               pct_eggs = input$pct_eggs_input,
                               eggs_gram = input$eggs_gram_input,
                               eggs_number = input$eggs_number_input,
                               fish_comment = input$ind_fish_comment_input,
                               created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                               created_by = Sys.getenv("USERNAME"))
  return(new_individual_fish)
})

# Generate values to show in modal
output$individual_fish_modal_insert_vals = renderDT({
  individual_fish_modal_in_vals = individual_fish_create() %>%
    select(fish_condition, fish_trauma, gill_condition, spawn_condition, fish_sample_num, scale_card_num,
           scale_position_num, age_code, snout_sample_num, cwt_tag_code, cwt_result, genetic_sample_num,
           otolith_sample_num, pct_eggs, eggs_gram, eggs_number, fish_comment)
  # Generate table
  datatable(individual_fish_modal_in_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

# Modal for new intents. Need a dup flag, multiple rows possible
observeEvent(input$ind_fish_add, {
  new_individual_fish_vals = individual_fish_create()
  showModal(
    tags$div(id = "individual_fish_insert_modal",
             # Verify required fields have data...none can be blank
             if ( is.na(new_individual_fish_vals$fish_condition_type_id) |
                  is.na(new_individual_fish_vals$fish_trauma_type_id) |
                  is.na(new_individual_fish_vals$gill_condition_type_id) |
                  is.na(new_individual_fish_vals$spawn_condition_type_id) |
                  is.na(new_individual_fish_vals$cwt_result_type_id) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Values are required in the condition, trauma, and cwt_result fields"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify fish count is one
             } else if (!individual_fish_create()$fish_count == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Individual fish data can only be entered if the fish_count value equals 1"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert new sample data to the database?"),
                 fluidPage (
                   DT::DTOutput("individual_fish_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_individual_fish", "Insert sample data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
individual_fish_insert_vals = reactive({
  new_ind_fish_values = individual_fish_create() %>%
    select(fish_encounter_id, fish_condition_type_id, fish_trauma_type_id, gill_condition_type_id,
           spawn_condition_type_id, cwt_result_type_id, age_code_id, pct_eggs, eggs_gram, eggs_number,
           fish_sample_num, scale_card_num, scale_position_num, snout_sample_num, cwt_tag_code,
           genetic_sample_num, otolith_sample_num, fish_comment, created_by)
  return(new_ind_fish_values)
})

# Update DB and reload DT
observeEvent(input$insert_individual_fish, {
  tryCatch({
    individual_fish_insert(individual_fish_insert_vals())
    shinytoastr::toastr_success("New sample data was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_individual_fish_insert_vals = get_individual_fish(selected_fish_encounter_data()$fish_encounter_id) %>%
    select(fish_condition, fish_trauma, gill_condition, spawn_condition, fish_sample_num, scale_card_num,
           scale_position_num, age_code, snout_sample_num, cwt_tag_code, cwt_result, genetic_sample_num,
           otolith_sample_num, pct_eggs, eggs_gram, eggs_number, fish_comment, created_dt, created_by,
           modified_dt, modified_by)
  replaceData(individual_fish_dt_proxy, post_individual_fish_insert_vals)
})

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
individual_fish_edit = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_encounters_rows_selected)
  req(input$individual_fishes_rows_selected)
  req(!is.na(selected_individual_fish_data()$individual_fish_id))
  # Fish condition
  fish_condition_input = input$fish_condition_select
  if ( fish_condition_input == "" ) {
    fish_condition_type_id = NA
  } else {
    fish_condition_vals = get_fish_condition()
    fish_condition_type_id = fish_condition_vals %>%
      filter(fish_condition == fish_condition_input) %>%
      pull(fish_condition_type_id)
  }
  # Fish trauma
  fish_trauma_input = input$fish_trauma_select
  if ( fish_trauma_input == "" ) {
    fish_trauma_type_id = NA
  } else {
    fish_trauma_vals = get_fish_trauma()
    fish_trauma_type_id = fish_trauma_vals %>%
      filter(fish_trauma == fish_trauma_input) %>%
      pull(fish_trauma_type_id)
  }
  # Gill condition
  gill_condition_input = input$gill_condition_select
  if ( gill_condition_input == "" ) {
    gill_condition_type_id = NA
  } else {
    gill_condition_vals = get_gill_condition()
    gill_condition_type_id = gill_condition_vals %>%
      filter(gill_condition == gill_condition_input) %>%
      pull(gill_condition_type_id)
  }
  # Spawn condition
  spawn_condition_input = input$spawn_condition_select
  if ( spawn_condition_input == "" ) {
    spawn_condition_type_id = NA
  } else {
    spawn_condition_vals = get_spawn_condition()
    spawn_condition_type_id = spawn_condition_vals %>%
      filter(spawn_condition == spawn_condition_input) %>%
      pull(spawn_condition_type_id)
  }
  # Age code
  age_code_input = input$age_code_select
  if ( age_code_input == "" ) {
    age_code_id = NA
  } else {
    age_code_vals = get_age_code()
    age_code_id = age_code_vals %>%
      filter(age_code == age_code_input) %>%
      pull(age_code_id)
  }
  # CWT result
  cwt_result_input = input$cwt_result_select
  if ( cwt_result_input == "" ) {
    cwt_result_type_id = NA
  } else {
    cwt_result_vals = get_cwt_result()
    cwt_result_type_id = cwt_result_vals %>%
      filter(cwt_result == cwt_result_input) %>%
      pull(cwt_result_type_id)
  }
  edit_individual_fish = tibble(individual_fish_id = selected_individual_fish_data()$individual_fish_id,
                                fish_condition = fish_condition_input,
                                fish_condition_type_id = fish_condition_type_id,
                                fish_trauma = fish_trauma_input,
                                fish_trauma_type_id = fish_trauma_type_id,
                                gill_condition = gill_condition_input,
                                gill_condition_type_id = gill_condition_type_id,
                                spawn_condition = spawn_condition_input,
                                spawn_condition_type_id = spawn_condition_type_id,
                                fish_sample_num = input$fish_sample_num_input,
                                scale_card_num = input$scale_card_num_input,
                                scale_position_num = input$scale_position_num_input,
                                age_code = age_code_input,
                                age_code_id = age_code_id,
                                snout_sample_num = input$snout_sample_num_input,
                                cwt_tag_code = input$cwt_tag_code_input,
                                cwt_result = cwt_result_input,
                                cwt_result_type_id = cwt_result_type_id,
                                genetic_sample_num = input$genetic_sample_num_input,
                                otolith_sample_num = input$otolith_sample_num_input,
                                pct_eggs = input$pct_eggs_input,
                                eggs_gram = input$eggs_gram_input,
                                eggs_number = input$eggs_number_input,
                                fish_comment = input$ind_fish_comment_input,
                                created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                                created_by = Sys.getenv("USERNAME"))
  edit_individual_fish = edit_individual_fish %>%
    mutate(fish_sample_num = if_else(is.na(fish_sample_num) | fish_sample_num == "", NA_character_, fish_sample_num)) %>%
    mutate(scale_card_num = if_else(is.na(scale_card_num) | scale_card_num == "", NA_character_, scale_card_num)) %>%
    mutate(scale_position_num = if_else(is.na(scale_position_num) | scale_position_num == "", NA_character_, scale_position_num)) %>%
    mutate(snout_sample_num = if_else(is.na(snout_sample_num) | snout_sample_num == "", NA_character_, snout_sample_num)) %>%
    mutate(cwt_tag_code = if_else(is.na(cwt_tag_code) | cwt_tag_code == "", NA_character_, cwt_tag_code)) %>%
    mutate(genetic_sample_num = if_else(is.na(genetic_sample_num) | genetic_sample_num == "", NA_character_, genetic_sample_num)) %>%
    mutate(otolith_sample_num = if_else(is.na(otolith_sample_num) | otolith_sample_num == "", NA_character_, otolith_sample_num)) %>%
    mutate(fish_comment = if_else(is.na(fish_comment) | fish_comment == "", NA_character_, fish_comment))
  return(edit_individual_fish)
})

# Generate values to show in modal
output$individual_fish_modal_update_vals = renderDT({
  individual_fish_modal_up_vals = individual_fish_edit() %>%
    select(fish_condition, fish_trauma, gill_condition, spawn_condition, fish_sample_num, scale_card_num,
           scale_position_num, age_code, snout_sample_num, cwt_tag_code, cwt_result, genetic_sample_num,
           otolith_sample_num, pct_eggs, eggs_gram, eggs_number, fish_comment)
  # Generate table
  datatable(individual_fish_modal_up_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$ind_fish_edit, {
  old_individual_fish_vals = selected_individual_fish_data() %>%
    select(fish_condition, fish_trauma, gill_condition, spawn_condition, fish_sample_num, scale_card_num,
           scale_position_num, age_code, snout_sample_num, cwt_tag_code, cwt_result, genetic_sample_num,
           otolith_sample_num, pct_eggs, eggs_gram, eggs_number, fish_comment)
  old_individual_fish_vals[] = lapply(old_individual_fish_vals, remisc::set_empty)
  old_individual_fish_vals = old_individual_fish_vals %>%
    mutate(pct_eggs = as.integer(pct_eggs)) %>%
    mutate(eggs_gram = as.numeric(eggs_gram)) %>%
    mutate(eggs_number = as.integer(eggs_number))
  new_individual_fish_vals = individual_fish_edit() %>%
    select(fish_condition, fish_trauma, gill_condition, spawn_condition, fish_sample_num, scale_card_num,
           scale_position_num, age_code, snout_sample_num, cwt_tag_code, cwt_result, genetic_sample_num,
           otolith_sample_num, pct_eggs, eggs_gram, eggs_number, fish_comment)
  new_individual_fish_vals[] = lapply(new_individual_fish_vals, remisc::set_empty)
  new_individual_fish_vals = new_individual_fish_vals %>%
    mutate(pct_eggs = as.integer(pct_eggs)) %>%
    mutate(eggs_gram = as.numeric(eggs_gram)) %>%
    mutate(eggs_number = as.integer(eggs_number))
  showModal(
    tags$div(id = "individual_fish_update_modal",
             if ( !length(input$individual_fishes_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_individual_fish_vals, new_individual_fish_vals)) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please change at least one value!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Update individual fish data to these new values?",
                 fluidPage (
                   DT::DTOutput("individual_fish_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_ind_fish_edits","Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_ind_fish_edits, {
  tryCatch({
    individual_fish_update(individual_fish_edit())
    shinytoastr::toastr_success("Sample data was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_individual_fish_edit_vals = get_individual_fish(selected_fish_encounter_data()$fish_encounter_id) %>%
    select(fish_condition, fish_trauma, gill_condition, spawn_condition, fish_sample_num, scale_card_num,
           scale_position_num, age_code, snout_sample_num, cwt_tag_code, cwt_result, genetic_sample_num,
           otolith_sample_num, pct_eggs, eggs_gram, eggs_number, fish_comment, created_dt, created_by,
           modified_dt, modified_by)
  replaceData(individual_fish_dt_proxy, post_individual_fish_edit_vals)
})

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$individual_fish_modal_delete_vals = renderDT({
  individual_fish_modal_del_id = selected_individual_fish_data()$individual_fish_id
  individual_fish_modal_del_vals = get_individual_fish(selected_fish_encounter_data()$fish_encounter_id) %>%
    filter(individual_fish_id == individual_fish_modal_del_id) %>%
    select(fish_condition, fish_trauma, gill_condition, spawn_condition, fish_sample_num, scale_card_num,
           scale_position_num, age_code, snout_sample_num, cwt_tag_code, cwt_result, genetic_sample_num,
           otolith_sample_num, pct_eggs, eggs_gram, eggs_number, fish_comment)
  # Generate table
  datatable(individual_fish_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$ind_fish_delete, {
  individual_fish_id = selected_individual_fish_data()$individual_fish_id
  individual_fish_dependencies = get_individual_fish_dependencies(individual_fish_id)
  table_names = paste0(names(individual_fish_dependencies), collapse = ", ")
  showModal(
    tags$div(id = "individual_fish_delete_modal",
             if ( ncol(individual_fish_dependencies) > 0L ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("Please delete associated individual fish data from the following tables first: {table_names}"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Are you sure you want to delete this set of individual fish data from the database?",
                 fluidPage (
                   DT::DTOutput("individual_fish_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_individual_fish", "Delete data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_individual_fish, {
  tryCatch({
    individual_fish_delete(selected_individual_fish_data())
    shinytoastr::toastr_success("Sample data was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  individual_fish_after_delete = get_individual_fish(selected_fish_encounter_data()$fish_encounter_id) %>%
    select(fish_condition, fish_trauma, gill_condition, spawn_condition, fish_sample_num, scale_card_num,
           scale_position_num, age_code, snout_sample_num, cwt_tag_code, cwt_result, genetic_sample_num,
           otolith_sample_num, pct_eggs, eggs_gram, eggs_number, fish_comment, created_dt, created_by,
           modified_dt, modified_by)
  replaceData(individual_fish_dt_proxy, individual_fish_after_delete)
})
