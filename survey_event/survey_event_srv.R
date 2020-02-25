#========================================================
# Generate lut select ui's
#========================================================

output$event_species_select = renderUI({
  species_list = get_event_species()$species
  species_list = c("", species_list)
  selectizeInput("event_species_select", label = "species",
                 choices = species_list, selected = NULL,
                 width = "175px")
})

output$survey_design_select = renderUI({
  survey_design_list = get_survey_design()$survey_design
  survey_design_list = c("", survey_design_list)
  selectizeInput("survey_design_select", label = "survey_design",
                 choices = survey_design_list, selected = NULL,
                 width = "100px")
})

output$cwt_detect_method_select = renderUI({
  cwt_detect_method_list = get_cwt_detect_method()$cwt_detect_method
  cwt_detect_method_list = c("", cwt_detect_method_list)
  selectizeInput("cwt_detect_method_select", label = "cwt_detect_method",
                 choices = cwt_detect_method_list, selected = "Not applicable",
                 width = "125px")
})

output$run_select = renderUI({
  run_list = get_run()$run
  run_list = c("", run_list)
  selectizeInput("run_select", label = "run",
                 choices = run_list, selected = "Unknown",
                 width = "125px")
})

output$run_year_select = renderUI({
  run_year_list = seq(as.integer(year_vals()) - 4L, as.integer(year_vals()) + 1L)
  selectizeInput("run_year_select", label = "run_year",
                 choices = run_year_list, selected = as.integer(year_vals()),
                 width = "125px")
})

#========================================================
# Primary datatable for survey_events
#========================================================

# Primary DT datatable for survey_intent
output$survey_events = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  survey_event_title = glue("Species data for {input$stream_select} on ",
                             "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                             "to {selected_survey_data()$lo_rm}")
  survey_event_data = get_survey_event(selected_survey_data()$survey_id) %>%
    select(species, survey_design, cwt_detect_method, run, run_year, pct_fish_seen, species_comment,
           created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(survey_event_data,
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
              htmltools::em(htmltools::strong(survey_event_title))))
})

# Create surveys DT proxy object
survey_event_dt_proxy = dataTableProxy(outputId = "survey_events")

#========================================================
# Collect event values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_survey_event_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  survey_event_data = get_survey_event(selected_survey_data()$survey_id)
  survey_event_row = input$survey_events_rows_selected
  selected_survey_event = tibble(survey_event_id = survey_event_data$survey_event_id[survey_event_row],
                                 species_id = survey_event_data$species_id[survey_event_row],
                                 species = survey_event_data$species[survey_event_row],
                                 survey_design = survey_event_data$survey_design[survey_event_row],
                                 cwt_detect_method = survey_event_data$cwt_detect_method[survey_event_row],
                                 run = survey_event_data$run[survey_event_row],
                                 run_year = survey_event_data$run_year[survey_event_row],
                                 pct_fish_seen = survey_event_data$pct_fish_seen[survey_event_row],
                                 species_comment = survey_event_data$species_comment[survey_event_row],
                                 created_date = survey_event_data$created_date[survey_event_row],
                                 created_by = survey_event_data$created_by[survey_event_row],
                                 modified_date = survey_event_data$modified_date[survey_event_row],
                                 modified_by = survey_event_data$modified_by[survey_event_row])
  return(selected_survey_event)
})

#========================================================
# Update event select inputs to values in selected row
#========================================================

# Update all survey input values to values in selected row
observeEvent(input$survey_events_rows_selected, {
  ssedat = selected_survey_event_data()
  updateSelectizeInput(session, "event_species_select", selected = ssedat$species)
  updateSelectizeInput(session, "survey_design_select", selected = ssedat$survey_design)
  updateSelectizeInput(session, "cwt_detect_method_select", selected = ssedat$cwt_detect_method)
  updateSelectizeInput(session, "run_select", selected = ssedat$run)
  updateSelectizeInput(session, "run_year_select", selected = ssedat$run_year)
  updateNumericInput(session, "pct_fish_seen_input", value = ssedat$pct_fish_seen)
  updateTextAreaInput(session, "se_comment_input", value = ssedat$species_comment)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
survey_event_create = reactive({
  req(input$surveys_rows_selected)
  # Survey_id
  survey_id_input = selected_survey_data()$survey_id
  # Species
  event_species_input = input$event_species_select
  if (event_species_input == "" ) {
    species_id = NA_character_
  } else {
    event_species_vals = get_event_species()
    species_id = event_species_vals %>%
      filter(species == event_species_input) %>%
      pull(species_id)
  }
  # Survey design
  survey_design_input = input$survey_design_select
  if ( survey_design_input == "" ) {
    survey_design_type_id = NA
  } else {
    survey_design_vals = get_survey_design()
    survey_design_type_id = survey_design_vals %>%
      filter(survey_design == survey_design_input) %>%
      pull(survey_design_type_id)
  }
  # CWT detect method
  cwt_detect_method_input = input$cwt_detect_method_select
  if ( cwt_detect_method_input == "" ) {
    cwt_detection_method_id = NA
  } else {
    cwt_detect_method_vals = get_cwt_detect_method()
    cwt_detection_method_id = cwt_detect_method_vals %>%
      filter(cwt_detect_method == cwt_detect_method_input) %>%
      pull(cwt_detection_method_id)
  }
  # Run
  run_input = input$run_select
  if ( run_input == "" ) {
    run_id = NA
  } else {
    run_vals = get_run()
    run_id = run_vals %>%
      filter(run == run_input) %>%
      pull(run_id)
  }
  new_survey_event = tibble(survey_id = survey_id_input,
                            species = event_species_input,
                            species_id = species_id,
                            survey_design = survey_design_input,
                            survey_design_type_id = survey_design_type_id,
                            cwt_detect_method = cwt_detect_method_input,
                            cwt_detection_method_id = cwt_detection_method_id,
                            run = run_input,
                            run_id = run_id,
                            run_year = input$run_year_select,
                            pct_fish_seen = input$pct_fish_seen_input,
                            species_comment = input$se_comment_input,
                            created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                            created_by = Sys.getenv("USERNAME"))
  new_survey_event = new_survey_event %>%
    mutate(run_year = if_else(is.na(run_year) | run_year == "", NA_character_, run_year)) %>%
    mutate(run_year = as.integer(run_year)) %>%
    mutate(pct_fish_seen = as.integer(pct_fish_seen)) %>%
    mutate(species_comment = if_else(is.na(species_comment) | species_comment == "", NA_character_, species_comment))
  return(new_survey_event)
})

# Generate values to show in modal
output$survey_event_modal_insert_vals = renderDT({
  survey_event_modal_in_vals = survey_event_create() %>%
    select(species, survey_design, cwt_detect_method, run, run_year,
           pct_fish_seen, species_comment)
  # Generate table
  datatable(survey_event_modal_in_vals,
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
observeEvent(input$survey_event_add, {
  new_survey_event_vals = survey_event_create()
  existing_survey_event_vals = get_survey_event(selected_survey_data()$survey_id) %>%
    select(species, survey_design, run, run_year)
  dup_event_flag = dup_survey_event(new_survey_event_vals, existing_survey_event_vals)
  showModal(
    # Verify all fields have data...none can be blank
    tags$div(id = "survey_event_insert_modal",
             if ( is.na(new_survey_event_vals$species_id) |
                  is.na(new_survey_event_vals$survey_design_type_id) |
                  is.na(new_survey_event_vals$cwt_detection_method_id) |
                  is.na(new_survey_event_vals$run_id) |
                  is.na(new_survey_event_vals$run_year) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Values are required in all fields from species, through run_year"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( dup_event_flag == TRUE ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Species data already exists. Please edit either species, survey_design, run, or run_year before proceeding."),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert new species data to the database?"),
                 fluidPage (
                   DT::DTOutput("survey_event_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_survey_event", "Insert species data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
survey_event_insert_vals = reactive({
  new_event_values = survey_event_create() %>%
    select(survey_id, species_id, survey_design_type_id,
           cwt_detection_method_id, run_id, run_year,
           pct_fish_seen, species_comment, created_by)
  return(new_event_values)
})

# Update DB and reload DT
observeEvent(input$insert_survey_event, {
  tryCatch({
    survey_event_insert(survey_event_insert_vals())
    shinytoastr::toastr_success("New species data was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_event_insert_vals = get_survey_event(selected_survey_data()$survey_id) %>%
    select(species, survey_design, cwt_detect_method, run, run_year, pct_fish_seen,
           species_comment, created_dt, created_by, modified_dt, modified_by)
  replaceData(survey_event_dt_proxy, post_event_insert_vals)
})

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
survey_event_edit = reactive({
  req(input$survey_events_rows_selected)
  # Species
  event_species_input = input$event_species_select
  if (event_species_input == "" ) {
    species_id = NA_character_
  } else {
    event_species_vals = get_event_species()
    species_id = event_species_vals %>%
      filter(species == event_species_input) %>%
      pull(species_id)
  }
  # Survey design
  survey_design_input = input$survey_design_select
  if ( survey_design_input == "" ) {
    survey_design_type_id = NA
  } else {
    survey_design_vals = get_survey_design()
    survey_design_type_id = survey_design_vals %>%
      filter(survey_design == survey_design_input) %>%
      pull(survey_design_type_id)
  }
  # CWT detect method
  cwt_detect_method_input = input$cwt_detect_method_select
  if ( cwt_detect_method_input == "" ) {
    cwt_detection_method_id = NA
  } else {
    cwt_detect_method_vals = get_cwt_detect_method()
    cwt_detection_method_id = cwt_detect_method_vals %>%
      filter(cwt_detect_method == cwt_detect_method_input) %>%
      pull(cwt_detection_method_id)
  }
  # Run
  run_input = input$run_select
  if ( run_input == "" ) {
    run_id = NA
  } else {
    run_vals = get_run()
    run_id = run_vals %>%
      filter(run == run_input) %>%
      pull(run_id)
  }
  edit_survey_event = tibble(survey_event_id = selected_survey_event_data()$survey_event_id,
                             species = event_species_input,
                             species_id = species_id,
                             survey_design = survey_design_input,
                             survey_design_type_id = survey_design_type_id,
                             cwt_detect_method = cwt_detect_method_input,
                             cwt_detection_method_id = cwt_detection_method_id,
                             run = run_input,
                             run_id = run_id,
                             run_year = input$run_year_select,
                             pct_fish_seen = input$pct_fish_seen_input,
                             species_comment = input$se_comment_input,
                             modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                             modified_by = Sys.getenv("USERNAME"))
  edit_survey_event = edit_survey_event %>%
    mutate(run_year = if_else(is.na(run_year) | run_year == "", NA_character_, run_year)) %>%
    mutate(run_year = as.integer(run_year)) %>%
    mutate(pct_fish_seen = as.integer(pct_fish_seen)) %>%
    mutate(species_comment = if_else(is.na(species_comment) | species_comment == "", NA_character_, species_comment))
  return(edit_survey_event)
})

# Generate values to show in modal
output$survey_event_modal_update_vals = renderDT({
  survey_event_modal_up_vals = survey_event_edit() %>%
    select(species, survey_design, cwt_detect_method, run, run_year,
           pct_fish_seen, species_comment)
  # Generate table
  datatable(survey_event_modal_up_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$survey_event_edit, {
  new_survey_event_vals = survey_event_edit() %>%
    select(species, survey_design, cwt_detect_method, run, run_year,
           pct_fish_seen, species_comment)
  old_survey_event_vals = selected_survey_event_data() %>%
    select(species, survey_design, cwt_detect_method, run, run_year,
           pct_fish_seen, species_comment)
  showModal(
    tags$div(id = "survey_event_update_modal",
             if ( !length(input$survey_events_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_survey_event_vals, new_survey_event_vals)) ) {
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
                 title = "Update species data to these new values?",
                 fluidPage (
                   DT::DTOutput("survey_event_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_survey_event_edits","Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_survey_event_edits, {
  tryCatch({
    survey_event_update(survey_event_edit())
    shinytoastr::toastr_success("Species data was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_survey_event_edit_vals = get_survey_event(selected_survey_data()$survey_id) %>%
    select(species, survey_design, cwt_detect_method, run, run_year, pct_fish_seen,
           species_comment, created_dt, created_by, modified_dt, modified_by)
  replaceData(survey_event_dt_proxy, post_survey_event_edit_vals)
})

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$survey_event_modal_delete_vals = renderDT({
  survey_event_modal_del_id = selected_survey_event_data()$survey_event_id
  survey_event_modal_del_vals = get_survey_event(selected_survey_data()$survey_id) %>%
    filter(survey_event_id == survey_event_modal_del_id) %>%
    select(species, survey_design, cwt_detect_method, run, run_year,
           pct_fish_seen, species_comment)
  # Generate table
  datatable(survey_event_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$survey_event_delete, {
  survey_event_id = selected_survey_event_data()$survey_event_id
  survey_event_dependencies = get_survey_event_dependencies(survey_event_id)
  table_names = paste0(names(survey_event_dependencies), collapse = ", ")
  table_names = gsub("survey_event", "species_data", table_names)
  showModal(
    tags$div(id = "survey_event_delete_modal",
             if ( ncol(survey_event_dependencies) > 0L ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("Please delete associated species data from the following tables first: {table_names}"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Are you sure you want to delete this species data from the database?",
                 fluidPage (
                   DT::DTOutput("survey_event_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_survey_event", "Delete species")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_survey_event, {
  tryCatch({
    survey_event_delete(selected_survey_event_data())
    shinytoastr::toastr_success("Species data was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  survey_events_after_delete = get_survey_event(selected_survey_data()$survey_id) %>%
    select(species, survey_design, cwt_detect_method, run, run_year, pct_fish_seen,
           species_comment, created_dt, created_by, modified_dt, modified_by)
  replaceData(survey_event_dt_proxy, survey_events_after_delete)
})
