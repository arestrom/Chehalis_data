
output$clarity_type_select = renderUI({
  clarity_type_list = get_clarity_type()$clarity_type
  clarity_type_list = c("", clarity_type_list)
  selectizeInput("clarity_type_select", label = "clarity_type",
                 choices = clarity_type_list, selected = NULL,
                 width = "250px")
})

# Primary DT datatable for survey_intent
output$waterbody_measure = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(!is.na(selected_survey_data()$survey_id))
  waterbody_meas_title = glue("Water measurements for {input$stream_select} on ",
                              "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                              "to {selected_survey_data()$lo_rm}")
  waterbody_meas_data = get_waterbody_meas(selected_survey_data()$survey_id) %>%
    select(clarity_type, clarity_meter, flow_cfs, start_temperature, start_tmp_time = start_tmp_dt,
           end_temperature, end_tmp_time = end_tmp_dt, water_ph, created_dt, created_by, modified_dt,
           modified_by)

  # Generate table
  datatable(waterbody_meas_data,
            selection = list(mode = 'single'),
            options = list(dom = 'tp',
                           scrollX = T,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: black; width: auto;',
              htmltools::em(htmltools::strong(waterbody_meas_title))))
})

# Create surveys DT proxy object
waterbody_measure_dt_proxy = dataTableProxy(outputId = "waterbody_measure")

#======================================================================
# Collect waterbody measurement values from selected row for later use
#======================================================================

# Create reactive to collect input values for update and delete actions
selected_waterbody_meas_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$waterbody_measure_rows_selected)
  waterbody_meas_data = get_waterbody_meas(selected_survey_data()$survey_id)
  waterbody_meas_row = input$waterbody_measure_rows_selected
  selected_waterbody_meas = tibble(waterbody_measurement_id = waterbody_meas_data$waterbody_measurement_id[waterbody_meas_row],
                                   clarity_type = waterbody_meas_data$clarity_type[waterbody_meas_row],
                                   clarity_meter = waterbody_meas_data$clarity_meter[waterbody_meas_row],
                                   flow_cfs = waterbody_meas_data$flow_cfs[waterbody_meas_row],
                                   start_temperature = waterbody_meas_data$start_temperature[waterbody_meas_row],
                                   start_tmp_time = waterbody_meas_data$start_tmp_time[waterbody_meas_row],
                                   start_tmp_dt = waterbody_meas_data$start_tmp_dt[waterbody_meas_row],
                                   end_temperature = waterbody_meas_data$end_temperature[waterbody_meas_row],
                                   end_tmp_time = waterbody_meas_data$end_tmp_time[waterbody_meas_row],
                                   end_tmp_dt = waterbody_meas_data$end_tmp_dt[waterbody_meas_row],
                                   water_ph = waterbody_meas_data$water_ph[waterbody_meas_row],
                                   created_date = waterbody_meas_data$created_date[waterbody_meas_row],
                                   created_by = waterbody_meas_data$created_by[waterbody_meas_row],
                                   modified_date = waterbody_meas_data$modified_date[waterbody_meas_row],
                                   modified_by = waterbody_meas_data$modified_by[waterbody_meas_row])
  return(selected_waterbody_meas)
})

#========================================================
# Update intent select inputs to values in selected row
#========================================================

# Update all survey input values to values in selected row
observeEvent(input$waterbody_measure_rows_selected, {
  swbmdat = selected_waterbody_meas_data()
  updateSelectizeInput(session, "clarity_type_select", selected = swbmdat$clarity_type)
  updateNumericInput(session, "clarity_input", value = swbmdat$clarity_meter)
  updateNumericInput(session, "flow_cfs_input", value = swbmdat$flow_cfs)
  updateNumericInput(session, "start_temperature_input", value = swbmdat$start_temperature)
  updateTimeInput(session, "start_temperature_time_select", value = swbmdat$start_tmp_time)
  updateNumericInput(session, "end_temperature_input", value = swbmdat$end_temperature)
  updateTimeInput(session, "end_temperature_time_select", value = swbmdat$end_tmp_time)
  updateNumericInput(session, "water_ph_input", value = swbmdat$water_ph)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Disable "New" button if a row of measurements already exists
observe({
  input$insert_waterbody_meas
  waterbody_meas_data = get_waterbody_meas(selected_survey_data()$survey_id)
  if (nrow(waterbody_meas_data) >= 1L) {
    shinyjs::disable("wbm_add")
  } else {
    shinyjs::enable("wbm_add")
  }
})

# Create reactive to collect input values for insert actions
waterbody_meas_create = reactive({
  req(input$surveys_rows_selected)
  # Survey_id
  survey_id_input = selected_survey_data()$survey_id
  # Survey date
  survey_date = selected_survey_data()$survey_date
  # Clarity type
  clarity_type_input = input$clarity_type_select
  if ( clarity_type_input == "" ) {
    water_clarity_type_id = NA_character_
  } else {
    clarity_type_vals = get_clarity_type()
    water_clarity_type_id = clarity_type_vals %>%
      filter(clarity_type == clarity_type_input) %>%
      pull(water_clarity_type_id)
  }
  # Time values
  start_tmp_dt = format(input$start_temperature_time_select)
  if (nchar(start_tmp_dt) < 16) { start_tmp_dt = NA_character_ }
  start_tmp_dt = as.POSIXct(start_tmp_dt)
  end_tmp_dt = format(input$end_temperature_time_select)
  if (nchar(end_tmp_dt) < 16) { end_tmp_dt = NA_character_ }
  end_tmp_dt = as.POSIXct(end_tmp_dt)
  new_waterbody_meas = tibble(survey_id = survey_id_input,
                              clarity_type = clarity_type_input,
                              water_clarity_type_id = water_clarity_type_id,
                              clarity_meter = input$clarity_input,
                              flow_cfs = input$flow_cfs_input,
                              survey_date = survey_date,
                              start_temperature = input$start_temperature_input,
                              # Need to create full datetime values below modal
                              start_tmp_dt = start_tmp_dt,
                              end_temperature = input$end_temperature_input,
                              end_tmp_dt = end_tmp_dt,
                              water_ph = input$water_ph_input,
                              created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                              created_by = Sys.getenv("USERNAME"))
  new_waterbody_meas = new_waterbody_meas %>%
    mutate(clarity_type = if_else(is.na(clarity_type) | clarity_type == "", NA_character_, clarity_type))
  return(new_waterbody_meas)
})

# Generate values to show in modal
output$waterbody_meas_modal_insert_vals = renderDT({
  waterbody_meas_modal_in_vals = waterbody_meas_create() %>%
    mutate(start_tmp_dt = format(start_tmp_dt, "%H:%M")) %>%
    mutate(end_tmp_dt = format(end_tmp_dt, "%H:%M")) %>%
    select(clarity_type, clarity_meter, flow_cfs, start_temperature, start_tmp_dt,
           end_temperature, end_tmp_dt, water_ph)
  # Generate table
  datatable(waterbody_meas_modal_in_vals,
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
observeEvent(input$wbm_add, {
  new_waterbody_meas_vals = waterbody_meas_create()
  showModal(
    # Verify all fields have data...none can be blank
    tags$div(id = "waterbody_meas_insert_modal",
             if ( is.na(new_waterbody_meas_vals$clarity_meter) &
                  is.na(new_waterbody_meas_vals$flow_cfs) &
                  is.na(new_waterbody_meas_vals$start_temperature) &
                  is.na(new_waterbody_meas_vals$end_temperature) &
                  is.na(new_waterbody_meas_vals$water_ph) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("All fields can not be blank, please add at least one measurement"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert these new measurements to the database?"),
                 fluidPage (
                   DT::DTOutput("waterbody_meas_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_waterbody_meas", "Insert measurements")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values that will actually be inserted
waterbody_meas_insert_vals = reactive({
  new_waterbody_meas_values = waterbody_meas_create() %>%
    mutate(start_temperature_time = case_when(
      is.na(start_tmp_dt) ~ as.POSIXct(NA),
      !is.na(start_tmp_dt) ~ as.POSIXct(paste0(format(survey_date), " ", format(start_tmp_dt, "%H:%M")),
                                      tz = "America/Los_Angeles"))) %>%
    mutate(start_temperature_time = with_tz(start_temperature_time, tzone = "UTC")) %>%
    mutate(end_temperature_time = case_when(
      is.na(end_tmp_dt) ~ as.POSIXct(NA),
      !is.na(end_tmp_dt) ~ as.POSIXct(paste0(format(survey_date), " ", format(end_tmp_dt, "%H:%M")),
                                        tz = "America/Los_Angeles"))) %>%
    mutate(end_temperature_time = with_tz(end_temperature_time, tzone = "UTC")) %>%
    select(survey_id, water_clarity_type_id, clarity_meter,
           flow_cfs, start_temperature_time, start_temperature,
           end_temperature_time, end_temperature, water_ph,
           created_by)
  return(new_waterbody_meas_values)
})

# Update DB and reload DT
observeEvent(input$insert_waterbody_meas, {
  tryCatch({
    waterbody_meas_insert(waterbody_meas_insert_vals())
    shinytoastr::toastr_success("New measurements were added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_waterbody_meas_insert_vals = get_waterbody_meas(selected_survey_data()$survey_id) %>%
    select(clarity_type, clarity_meter, flow_cfs, start_temperature, start_tmp_dt,
           end_temperature, end_tmp_dt, water_ph, created_dt, created_by, modified_dt,
           modified_by)
  replaceData(waterbody_measure_dt_proxy, post_waterbody_meas_insert_vals)
})

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
waterbody_meas_edit = reactive({
  # Survey_id
  survey_id_input = selected_survey_data()$survey_id
  # Survey date
  survey_date = selected_survey_data()$survey_date
  # Clarity type
  clarity_type_input = input$clarity_type_select
  if ( clarity_type_input == "" ) {
    water_clarity_type_id = NA_character_
  } else {
    clarity_type_vals = get_clarity_type()
    water_clarity_type_id = clarity_type_vals %>%
      filter(clarity_type == clarity_type_input) %>%
      pull(water_clarity_type_id)
  }
  # Time values
  start_tmp_dt = format(input$start_temperature_time_select)
  if (nchar(start_tmp_dt) < 16) { start_tmp_dt = NA_character_ }
  start_tmp_dt = as.POSIXct(start_tmp_dt)
  end_tmp_dt = format(input$end_temperature_time_select)
  if (nchar(end_tmp_dt) < 16) { end_tmp_dt = NA_character_ }
  end_tmp_dt = as.POSIXct(end_tmp_dt)
  edit_waterbody_meas = tibble(waterbody_measurement_id = selected_waterbody_meas_data()$waterbody_measurement_id,
                               clarity_type = clarity_type_input,
                               water_clarity_type_id = water_clarity_type_id,
                               clarity_meter = input$clarity_input,
                               flow_cfs = input$flow_cfs_input,
                               survey_date = survey_date,
                               start_temperature = input$start_temperature_input,
                               start_tmp_dt = start_tmp_dt,
                               end_temperature = input$end_temperature_input,
                               end_tmp_dt = end_tmp_dt,
                               water_ph = input$water_ph_input,
                               modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                               modified_by = Sys.getenv("USERNAME"))
  edit_waterbody_meas = edit_waterbody_meas %>%
    mutate(clarity_type = if_else(is.na(clarity_type) | clarity_type == "", NA_character_, clarity_type)) %>%
    mutate(start_temperature_time = case_when(
      is.na(start_tmp_dt) ~ as.POSIXct(NA),
      !is.na(start_tmp_dt) ~ as.POSIXct(paste0(format(survey_date), " ", format(start_tmp_dt, "%H:%M")),
                                        tz = "America/Los_Angeles"))) %>%
    mutate(start_temperature_time = with_tz(start_temperature_time, tzone = "UTC")) %>%
    mutate(end_temperature_time = case_when(
      is.na(end_tmp_dt) ~ as.POSIXct(NA),
      !is.na(end_tmp_dt) ~ as.POSIXct(paste0(format(survey_date), " ", format(end_tmp_dt, "%H:%M")),
                                      tz = "America/Los_Angeles"))) %>%
    mutate(end_temperature_time = with_tz(end_temperature_time, tzone = "UTC"))
  return(edit_waterbody_meas)
})

# Generate values to show in modal
output$waterbody_meas_modal_update_vals = renderDT({
  waterbody_meas_modal_up_vals = waterbody_meas_edit() %>%
    mutate(start_tmp_dt = format(start_tmp_dt, "%H:%M")) %>%
    mutate(end_tmp_dt = format(end_tmp_dt, "%H:%M")) %>%
    select(clarity_type, clarity_meter, flow_cfs, start_temperature, start_tmp_dt,
           end_temperature, end_tmp_dt, water_ph)
  # Generate table
  datatable(waterbody_meas_modal_up_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$wbm_edit, {
  old_wbm_vals = selected_waterbody_meas_data() %>%
    select(clarity_type, clarity_meter, flow_cfs, start_temperature, start_tmp_dt,
           end_temperature, end_tmp_dt, water_ph)
  #old_wbm_vals[] = lapply(old_wbm_vals, remisc::set_na_type)
  new_wbm_vals = waterbody_meas_edit() %>%
    mutate(clarity_meter = as.numeric(clarity_meter)) %>%
    mutate(water_ph = as.numeric(water_ph)) %>%
    mutate(start_tmp_dt = format(start_tmp_dt, "%H:%M")) %>%
    mutate(start_temperature = as.numeric(start_temperature)) %>%
    mutate(end_temperature = as.numeric(end_temperature)) %>%
    mutate(end_tmp_dt = format(end_tmp_dt, "%H:%M")) %>%
    select(clarity_type, clarity_meter, flow_cfs, start_temperature, start_tmp_dt,
           end_temperature, end_tmp_dt, water_ph)
  #new_wbm_vals[] = lapply(new_wbm_vals, remisc::set_na_type)
  showModal(
    tags$div(id = "waterbody_meas_update_modal",
             if ( !length(input$waterbody_measure_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_wbm_vals, new_wbm_vals)) ) {
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
                 title = "Update waterbody measurements to these new values?",
                 fluidPage (
                   DT::DTOutput("waterbody_meas_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_wbm_edits","Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_wbm_edits, {
  tryCatch({
    waterbody_meas_update(waterbody_meas_edit())
    shinytoastr::toastr_success("Measurements were edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_wbm_edit_vals = get_waterbody_meas(selected_survey_data()$survey_id) %>%
    select(clarity_type, clarity_meter, flow_cfs, start_temperature, start_tmp_dt,
           end_temperature, end_tmp_dt, water_ph, created_dt, created_by, modified_dt,
           modified_by)
  replaceData(waterbody_measure_dt_proxy, post_wbm_edit_vals)
})

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$waterbody_meas_modal_delete_vals = renderDT({
  waterbody_meas_modal_del_id = selected_waterbody_meas_data()$waterbody_measurement_id
  waterbody_meas_modal_del_vals = get_waterbody_meas(selected_survey_data()$survey_id) %>%
    filter(waterbody_measurement_id == waterbody_meas_modal_del_id) %>%
    select(clarity_type, clarity_meter, flow_cfs, start_temperature, start_tmp_dt,
           end_temperature, end_tmp_dt, water_ph)
  # Generate table
  datatable(waterbody_meas_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$wbm_delete, {
  waterbody_measurement_id = selected_waterbody_meas_data()$waterbody_measurement_id
  showModal(
    tags$div(id = "waterbody_meas_delete_modal",
             if ( length(waterbody_measurement_id) == 0 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to delete!" ),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Are you sure you want to delete these measurements from the database?",
                 fluidPage (
                   DT::DTOutput("waterbody_meas_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_waterbody_meas", "Delete measurements")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_waterbody_meas, {
  tryCatch({
    waterbody_meas_delete(selected_waterbody_meas_data())
    shinytoastr::toastr_success("Measurements were deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_waterbody_meas_delete_vals = get_waterbody_meas(selected_survey_data()$survey_id) %>%
    select(clarity_type, clarity_meter, flow_cfs, start_temperature, start_tmp_dt,
           end_temperature, end_tmp_dt, water_ph, created_dt, created_by, modified_dt,
           modified_by)
  replaceData(waterbody_measure_dt_proxy, post_waterbody_meas_delete_vals)
})

