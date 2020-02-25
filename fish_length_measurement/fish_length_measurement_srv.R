#========================================================
# Generate lut select ui's
#========================================================

output$length_type_select = renderUI({
  length_type_list = get_length_type()$length_type
  length_type_list = c("", length_type_list)
  selectizeInput("length_type_select", label = "length_type",
                 choices = length_type_list, selected = "Fork length",
                 width = "250px")
})

#========================================================
# Primary datatable for length measurements
#========================================================

# Primary DT datatable for survey_intent
output$length_measurements = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_encounters_rows_selected)
  req(input$individual_fishes_rows_selected)
  req(!is.na(selected_individual_fish_data()$individual_fish_id))
  length_measurements_title = glue("{selected_survey_event_data()$species} data for {input$stream_select} on ",
                               "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                               "to {selected_survey_data()$lo_rm}")
  length_measurement_data = get_length_measurements(selected_individual_fish_data()$individual_fish_id) %>%
    select(length_type, length_cm, created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(length_measurement_data,
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
              htmltools::em(htmltools::strong(length_measurements_title))))
})

# Create surveys DT proxy object
length_measurement_dt_proxy = dataTableProxy(outputId = "length_measurements")

#========================================================
# Collect measurement values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_length_measurement_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_encounters_rows_selected)
  req(input$individual_fishes_rows_selected)
  req(input$length_measurements_rows_selected)
  req(!is.na(selected_individual_fish_data()$individual_fish_id))
  length_measurement_data = get_length_measurements(selected_individual_fish_data()$individual_fish_id)
  length_measurement_row = input$length_measurements_rows_selected
  selected_length_measurement = tibble(fish_length_measurement_id = length_measurement_data$fish_length_measurement_id[length_measurement_row],
                                       length_type = length_measurement_data$length_type[length_measurement_row],
                                       length_cm = length_measurement_data$length_cm[length_measurement_row],
                                       created_date = length_measurement_data$created_date[length_measurement_row],
                                       created_by = length_measurement_data$created_by[length_measurement_row],
                                       modified_date = length_measurement_data$modified_date[length_measurement_row],
                                       modified_by = length_measurement_data$modified_by[length_measurement_row])
  return(selected_length_measurement)
})

#========================================================
# Update event select inputs to values in selected row
#========================================================

# Update all survey input values to values in selected row
observeEvent(input$length_measurements_rows_selected, {
  slmdat = selected_length_measurement_data()
  updateSelectizeInput(session, "length_type_select", selected = slmdat$length_type)
  updateNumericInput(session, "length_cm_input", value = slmdat$length_cm)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
length_measurement_create = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_encounters_rows_selected)
  req(input$individual_fishes_rows_selected)
  req(!is.na(selected_individual_fish_data()$individual_fish_id))
  # individual_fish_id
  individual_fish_id_input = selected_individual_fish_data()$individual_fish_id
  # length_type
  length_type_input = input$length_type_select
  if ( length_type_input == "" ) {
    fish_length_measurement_type_id = NA
  } else {
    fish_length_vals = get_length_type()
    fish_length_measurement_type_id = fish_length_vals %>%
      filter(length_type == length_type_input) %>%
      pull(fish_length_measurement_type_id)
  }
  new_length_measurement = tibble(individual_fish_id = individual_fish_id_input,
                                  length_type = length_type_input,
                                  fish_length_measurement_type_id = fish_length_measurement_type_id,
                                  length_cm = input$length_cm_input,
                                  created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                                  created_by = Sys.getenv("USERNAME"))
  return(new_length_measurement)
})

# Generate values to show in modal
output$lengh_measurement_modal_insert_vals = renderDT({
  length_measurement_modal_in_vals = length_measurement_create() %>%
    select(length_type, length_cm)
  # Generate table
  datatable(length_measurement_modal_in_vals,
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
observeEvent(input$fish_meas_add, {
  req(!is.na(selected_individual_fish_data()$individual_fish_id))
  new_length_measurement_vals = length_measurement_create()
  existing_length_measurement_vals = get_length_measurements(selected_individual_fish_data()$individual_fish_id)
  dup_length_type_flag = dup_length_type(new_length_measurement_vals, existing_length_measurement_vals)
  showModal(
    # Verify required fields have data...none can be blank
    tags$div(id = "length_measurement_insert_modal",
             if ( is.na(new_length_measurement_vals$fish_length_measurement_type_id) |
                  is.na(new_length_measurement_vals$length_cm) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("No fields can be blank, please add both length_type and length_cm"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else if ( dup_length_type_flag == TRUE ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Length type already exists. Please edit the length_type before proceeding."),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'm',
                 title = glue("Insert new length data to the database?"),
                 fluidPage (
                   DT::DTOutput("lengh_measurement_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_length_measurements", "Insert length data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
length_measurement_insert_vals = reactive({
  new_length_meas_values = length_measurement_create() %>%
    select(individual_fish_id, fish_length_measurement_type_id, length_cm, created_by)
  return(new_length_meas_values)
})

# Update DB and reload DT
observeEvent(input$insert_length_measurements, {
  tryCatch({
    length_measurement_insert(length_measurement_insert_vals())
    shinytoastr::toastr_success("New measurement was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_length_measurement_insert_vals = get_length_measurements(selected_individual_fish_data()$individual_fish_id) %>%
    select(length_type, length_cm, created_dt, created_by, modified_dt, modified_by)
  replaceData(length_measurement_dt_proxy, post_length_measurement_insert_vals)
})

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
length_measurement_edit = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_encounters_rows_selected)
  req(input$individual_fishes_rows_selected)
  req(!is.na(selected_individual_fish_data()$individual_fish_id))
  # length_type
  length_type_input = input$length_type_select
  if ( length_type_input == "" ) {
    fish_length_measurement_type_id = NA
  } else {
    fish_length_vals = get_length_type()
    fish_length_measurement_type_id = fish_length_vals %>%
      filter(length_type == length_type_input) %>%
      pull(fish_length_measurement_type_id)
  }
  edit_length_measurement = tibble(fish_length_measurement_id = selected_length_measurement_data()$fish_length_measurement_id,
                                   length_type = length_type_input,
                                   fish_length_measurement_type_id = fish_length_measurement_type_id,
                                   length_cm = input$length_cm_input,
                                   created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                                   created_by = Sys.getenv("USERNAME"))
  return(edit_length_measurement)
})

# Generate values to show in modal
output$length_measurement_modal_update_vals = renderDT({
  length_measurement_modal_up_vals = length_measurement_edit() %>%
    select(length_type, length_cm)
  # Generate table
  datatable(length_measurement_modal_up_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$fish_meas_edit, {
  old_length_measurement_vals = selected_length_measurement_data() %>%
    select(length_type, length_cm) %>%
    mutate(length_cm = as.numeric(length_cm))
  new_length_measurement_vals = length_measurement_edit() %>%
    select(length_type, length_cm) %>%
    mutate(length_cm = as.numeric(length_cm))
  showModal(
    tags$div(id = "length_measurement_update_modal",
             if ( !length(input$length_measurements_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_length_measurement_vals, new_length_measurement_vals)) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please change at least one value!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'm',
                 title = "Update fish length data to these new values?",
                 fluidPage (
                   DT::DTOutput("length_measurement_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_length_meas_edits","Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_length_meas_edits, {
  tryCatch({
    length_measurement_update(length_measurement_edit())
    shinytoastr::toastr_success("Measurement was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_length_measurement_edit_vals = get_length_measurements(selected_individual_fish_data()$individual_fish_id) %>%
    select(length_type, length_cm, created_dt, created_by, modified_dt, modified_by)
  replaceData(length_measurement_dt_proxy, post_length_measurement_edit_vals)
})

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$length_measurement_modal_delete_vals = renderDT({
  length_measurement_modal_del_id = selected_length_measurement_data()$fish_length_measurement_id
  length_measurement_modal_del_vals = get_length_measurements(selected_individual_fish_data()$individual_fish_id) %>%
    filter(fish_length_measurement_id == length_measurement_modal_del_id) %>%
    select(length_type, length_cm)
  # Generate table
  datatable(length_measurement_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$fish_meas_delete, {
  fish_length_measurement_id = selected_length_measurement_data()$fish_length_measurement_id
  showModal(
    tags$div(id = "length_measurement_delete_modal",
             if ( length(fish_length_measurement_id) == 0 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("Please select a row to delete!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'm',
                 title = "Are you sure you want to delete this set of length data from the database?",
                 fluidPage (
                   DT::DTOutput("length_measurement_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_length_measurements", "Delete data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_length_measurements, {
  tryCatch({
    length_measurement_delete(selected_length_measurement_data())
    shinytoastr::toastr_success("Measurement was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  length_measurement_after_delete = get_length_measurements(selected_individual_fish_data()$individual_fish_id) %>%
    select(length_type, length_cm, created_dt, created_by, modified_dt, modified_by)
  replaceData(length_measurement_dt_proxy, length_measurement_after_delete)
})
