#========================================================
# Generate lut select ui's
#========================================================

output$redd_status_select = renderUI({
  redd_status_list = get_redd_status()$redd_status
  redd_status_list = c("", redd_status_list)
  selectizeInput("redd_status_select", label = "redd_status",
                 choices = redd_status_list, selected = NULL,
                 width = "250px")
})

# Reactive to hold list of redd locations for past four months for species and reach
current_redd_locations = reactive({
  input$insert_redd_location
  input$save_redd_loc_edits
  input$delete_redd_location
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  redd_locs = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id)
  return(redd_locs)
})

output$redd_name_select = renderUI({
  redd_name_list = current_redd_locations()$redd_name
  redd_name_list = c("no location data", redd_name_list)
  selectizeInput("redd_name_select", label = "redd_name",
                 choices = redd_name_list, selected = NULL,
                 width = "225px")
})

#========================================================
# Primary datatable for redd_encounters
#========================================================

# Primary DT datatable for survey_intent
output$redd_encounters = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(!is.na(selected_survey_event_data()$survey_event_id))
  redd_encounter_title = glue("{selected_survey_event_data()$species} redd data for {input$stream_select} on ",
                              "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                              "to {selected_survey_data()$lo_rm}")
  redd_encounter_data = get_redd_encounter(selected_survey_event_data()$survey_event_id) %>%
    select(redd_encounter_dt, redd_status, redd_count, redd_name, redd_comment,
           created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(redd_encounter_data,
            selection = list(mode = 'single'),
            options = list(dom = 'Blftp',
                           pageLength = 5,
                           lengthMenu = c(1, 5, 10, 20, 50, 200),
                           scrollX = T,
                           buttons = c('excel', 'print'),
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: black; width: auto;',
              htmltools::em(htmltools::strong(redd_encounter_title))))
})

# Create surveys DT proxy object
redd_encounter_dt_proxy = dataTableProxy(outputId = "redd_encounters")

# Set row selection in redd_encounter_dt to NULL if selection changes in survey_event dt
observe({
  input$survey_events_rows_selected
  selectRows(redd_encounter_dt_proxy, NULL)
})

#========================================================
# Collect encounter values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_redd_encounter_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_encounters_rows_selected)
  req(!is.na(selected_survey_event_data()$survey_event_id))
  redd_encounter_data = get_redd_encounter(selected_survey_event_data()$survey_event_id)
  redd_encounter_row = input$redd_encounters_rows_selected
  selected_redd_encounter = tibble(redd_encounter_id = redd_encounter_data$redd_encounter_id[redd_encounter_row],
                                   redd_encounter_time = redd_encounter_data$redd_encounter_time[redd_encounter_row],
                                   redd_status = redd_encounter_data$redd_status[redd_encounter_row],
                                   redd_count = redd_encounter_data$redd_count[redd_encounter_row],
                                   redd_location_id = redd_encounter_data$redd_location_id[redd_encounter_row],
                                   redd_name = redd_encounter_data$redd_name[redd_encounter_row],
                                   redd_comment = redd_encounter_data$redd_comment[redd_encounter_row],
                                   created_date = redd_encounter_data$created_date[redd_encounter_row],
                                   created_by = redd_encounter_data$created_by[redd_encounter_row],
                                   modified_date = redd_encounter_data$modified_date[redd_encounter_row],
                                   modified_by = redd_encounter_data$modified_by[redd_encounter_row])
  return(selected_redd_encounter)
})

#========================================================
# Update event select inputs to values in selected row
#========================================================

# Update all input values to values in selected row
observeEvent(input$redd_encounters_rows_selected, {
  sredat = selected_redd_encounter_data()
  updateTimeInput(session, "redd_encounter_time_select", value = sredat$redd_encounter_time)
  updateSelectizeInput(session, "redd_status_select", selected = sredat$redd_status)
  updateSelectizeInput(session, "redd_name_select", selected = sredat$redd_name)
  updateNumericInput(session, "redd_count_input", value = sredat$redd_count)
  updateTextAreaInput(session, "redd_comment_input", value = sredat$redd_comment)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
redd_encounter_create = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  #req(input$redd_locations_rows_selected)
  req(!is.na(selected_survey_event_data()$survey_event_id))
  # Survey date
  survey_date = selected_survey_data()$survey_date
  # Survey_event_id
  survey_event_id_input = selected_survey_event_data()$survey_event_id
  # Redd encounter time
  redd_encounter_dt = format(input$redd_encounter_time_select)
  if (nchar(redd_encounter_dt) < 16) { redd_encounter_dt = NA_character_ }
  redd_encounter_dt = as.POSIXct(redd_encounter_dt)
  # Redd status
  redd_status_input = input$redd_status_select
  if ( redd_status_input == "" ) {
    redd_status_id = NA
  } else {
    redd_status_vals = get_redd_status()
    redd_status_id = redd_status_vals %>%
      filter(redd_status == redd_status_input) %>%
      pull(redd_status_id)
  }
  # Redd name, location_id
  loc_select = input$redd_name_select
  if ( is.null(loc_select) | is.na(loc_select) | loc_select == "" | loc_select == "no location data" ) {
    redd_location_id = NA
    redd_name_input = NA
  } else {
    redd_location = current_redd_locations() %>%
      filter(redd_name == loc_select)
    redd_location_id = redd_location$redd_location_id
    redd_name_input = redd_location$redd_name
  }
  new_redd_encounter = tibble(survey_event_id = survey_event_id_input,
                              survey_date = survey_date,
                              # Need to create full datetime values below modal
                              redd_encounter_dt = redd_encounter_dt,
                              redd_status = redd_status_input,
                              redd_status_id = redd_status_id,
                              redd_count = input$redd_count_input,
                              redd_name = redd_name_input,
                              redd_location_id = redd_location_id,
                              redd_comment = input$redd_comment_input,
                              created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                              created_by = Sys.getenv("USERNAME"))
  return(new_redd_encounter)
})

# Generate values to show in modal
output$redd_encounter_modal_insert_vals = renderDT({
  redd_encounter_modal_in_vals = redd_encounter_create() %>%
    mutate(redd_encounter_dt = format(redd_encounter_dt, "%H:%M")) %>%
    select(redd_encounter_dt, redd_status, redd_count,
           redd_name, redd_comment)
  # Generate table
  datatable(redd_encounter_modal_in_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

# Modal for new redd encounters. No need for dup flag, multiple rows possible
observeEvent(input$redd_enc_add, {
  new_redd_encounter_vals = redd_encounter_create()
  showModal(
    # Verify required fields have data...none can be blank
    tags$div(id = "redd_encounter_insert_modal",
             if ( is.na(new_redd_encounter_vals$redd_count) |
                  is.na(new_redd_encounter_vals$redd_status_id) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Values are required for redd_status and redd_count"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert new redd data to the database?"),
                 fluidPage (
                   DT::DTOutput("redd_encounter_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_redd_encounter", "Insert redd data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
redd_encounter_insert_vals = reactive({
  new_redd_enc_values = redd_encounter_create() %>%
    mutate(redd_encounter_datetime = case_when(
      is.na(redd_encounter_dt) ~ as.POSIXct(NA),
      !is.na(redd_encounter_dt) ~ as.POSIXct(paste0(format(survey_date), " ",
                                                    format(redd_encounter_dt, "%H:%M")),
                                             tz = "America/Los_Angeles"))) %>%
    mutate(redd_encounter_datetime = with_tz(redd_encounter_datetime, tzone = "UTC")) %>%
    select(survey_event_id, redd_location_id, redd_status_id, redd_encounter_datetime,
           redd_count, comment_text = redd_comment, created_by)
  return(new_redd_enc_values)
})

# Update DB and reload DT
observeEvent(input$insert_redd_encounter, {
  tryCatch({
    redd_encounter_insert(redd_encounter_insert_vals())
    shinytoastr::toastr_success("New redd count data was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_redd_encounter_insert_vals = get_redd_encounter(selected_survey_event_data()$survey_event_id) %>%
    select(redd_encounter_dt, redd_status, redd_count, redd_name, redd_comment,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_encounter_dt_proxy, post_redd_encounter_insert_vals)
})

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
redd_encounter_edit = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(!is.na(selected_redd_encounter_data()$redd_encounter_id))
  # Survey date
  survey_date = selected_survey_data()$survey_date
  # Survey_event_id
  survey_event_id_input = selected_survey_event_data()$survey_event_id
  # Redd encounter time
  redd_encounter_dt = format(input$redd_encounter_time_select)
  if (nchar(redd_encounter_dt) < 16) { redd_encounter_dt = NA_character_ }
  redd_encounter_dt = as.POSIXct(redd_encounter_dt)
  # Redd status
  redd_status_input = input$redd_status_select
  if ( redd_status_input == "" ) {
    redd_status_id = NA
  } else {
    redd_status_vals = get_redd_status()
    redd_status_id = redd_status_vals %>%
      filter(redd_status == redd_status_input) %>%
      pull(redd_status_id)
  }
  # Redd name, location_id
  loc_select = input$redd_name_select
  if ( is.null(loc_select) | is.na(loc_select) | loc_select == "" | loc_select == "no location data" ) {
    redd_location_id = NA
    redd_name_input = NA
  } else {
    redd_location = current_redd_locations() %>%
      filter(redd_name == loc_select)
    redd_location_id = redd_location$redd_location_id
    redd_name_input = redd_location$redd_name
  }
  edit_redd_encounter = tibble(redd_encounter_id = selected_redd_encounter_data()$redd_encounter_id,
                               # Need to create full datetime values below modal
                               survey_date = survey_date,
                               redd_encounter_dt = redd_encounter_dt,
                               redd_status = redd_status_input,
                               redd_status_id = redd_status_id,
                               redd_count = input$redd_count_input,
                               redd_name = redd_name_input,
                               redd_location_id = redd_location_id,
                               redd_comment = input$redd_comment_input,
                               modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                               modified_by = Sys.getenv("USERNAME"))
  edit_redd_encounter = edit_redd_encounter %>%
    mutate(redd_encounter_time = case_when(
      is.na(redd_encounter_dt) ~ as.POSIXct(NA),
      !is.na(redd_encounter_dt) ~ as.POSIXct(paste0(format(survey_date), " ", format(redd_encounter_dt, "%H:%M")),
                                        tz = "America/Los_Angeles"))) %>%
    mutate(redd_encounter_time = with_tz(redd_encounter_time, tzone = "UTC"))
  return(edit_redd_encounter)
})

# Generate values to show in modal
output$redd_encounter_modal_update_vals = renderDT({
  redd_encounter_modal_up_vals = redd_encounter_edit() %>%
    mutate(redd_encounter_dt = format(redd_encounter_dt, "%H:%M")) %>%
    select(redd_encounter_dt, redd_status, redd_count,
           redd_name, redd_comment)
  # Generate table
  datatable(redd_encounter_modal_up_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

# Edit modal
observeEvent(input$redd_enc_edit, {
  old_redd_encounter_vals = selected_redd_encounter_data() %>%
    mutate(redd_encounter_dt = format(redd_encounter_time, "%H:%M")) %>%
    select(redd_encounter_dt, redd_status, redd_count,
           redd_name, redd_comment)
  old_redd_encounter_vals[] = lapply(old_redd_encounter_vals, remisc::set_na)
  new_redd_encounter_vals = redd_encounter_edit() %>%
    mutate(redd_count = as.integer(redd_count)) %>%
    mutate(redd_encounter_dt = format(redd_encounter_dt, "%H:%M")) %>%
    select(redd_encounter_dt, redd_status, redd_count,
           redd_name, redd_comment)
  new_redd_encounter_vals[] = lapply(new_redd_encounter_vals, remisc::set_na)
  showModal(
    tags$div(id = "redd_encounter_update_modal",
             if ( !length(input$redd_encounters_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_redd_encounter_vals, new_redd_encounter_vals)) ) {
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
                 title = "Update redd data to these new values?",
                 fluidPage (
                   DT::DTOutput("redd_encounter_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_redd_enc_edits", "Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_redd_enc_edits, {
  tryCatch({
    redd_encounter_update(redd_encounter_edit())
    shinytoastr::toastr_success("Redd count data was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_redd_encounter_edit_vals = get_redd_encounter(selected_survey_event_data()$survey_event_id) %>%
    select(redd_encounter_dt, redd_status, redd_count, redd_name, redd_comment,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_encounter_dt_proxy, post_redd_encounter_edit_vals)
}, priority = 9999)

# Reload location DT after deleting encounter
observeEvent(input$save_redd_loc_edits, {
  species_id = selected_survey_event_data()$species_id
  redd_counts_after_location_edit = get_redd_encounter(selected_survey_event_data()$survey_event_id) %>%
    select(redd_encounter_dt, redd_status, redd_count, redd_name, redd_comment,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_encounter_dt_proxy, redd_counts_after_location_edit)
}, priority = -1)

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$redd_encounter_modal_delete_vals = renderDT({
  redd_encounter_modal_del_id = selected_redd_encounter_data()$redd_encounter_id
  redd_encounter_modal_del_vals = get_redd_encounter(selected_survey_event_data()$survey_event_id) %>%
    filter(redd_encounter_id == redd_encounter_modal_del_id) %>%
    select(redd_encounter_dt, redd_status, redd_count, redd_name, redd_comment)
  # Generate table
  datatable(redd_encounter_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

# Reactive to hold dependencies
redd_encounter_dependencies = reactive({
  redd_encounter_id = selected_redd_encounter_data()$redd_encounter_id
  redd_enc_dep = get_redd_encounter_dependencies(redd_encounter_id)
  return(redd_enc_dep)
})

observeEvent(input$redd_enc_delete, {
  redd_encounter_id = selected_redd_encounter_data()$redd_encounter_id
  redd_enc_dependencies = redd_encounter_dependencies()
  table_names = paste0(paste0("'", names(redd_enc_dependencies), "'"), collapse = ", ")
  showModal(
    tags$div(id = "redd_encounter_delete_modal",
             if ( ncol(redd_enc_dependencies) > 0L ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("Please delete associated redd data from the following tables first: {table_names}"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Are you sure you want to delete this redd data from the database?",
                 fluidPage (
                   DT::DTOutput("redd_encounter_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_redd_encounter", "Delete redd data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_redd_encounter, {
  tryCatch({
    redd_encounter_delete(selected_redd_encounter_data())
    shinytoastr::toastr_success("Redd count data was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  redd_encounters_after_delete = get_redd_encounter(selected_survey_event_data()$survey_event_id) %>%
    select(redd_encounter_dt, redd_status, redd_count, redd_name, redd_comment,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_encounter_dt_proxy, redd_encounters_after_delete)
})
