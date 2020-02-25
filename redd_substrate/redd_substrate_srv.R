#========================================================
# Generate lut select ui's
#========================================================

# Substrate level
output$substrate_level_select = renderUI({
  substrate_level_list = get_substrate_level()$substrate_level
  substrate_level_list = c("", substrate_level_list)
  selectizeInput("substrate_level_select", label = "substrate_level",
                 choices = substrate_level_list, selected = NULL,
                 width = "150px")
})

# Substrate type
output$substrate_type_select = renderUI({
  substrate_type_list = get_substrate_type()$substrate_type
  substrate_type_list = c("", substrate_type_list)
  selectizeInput("substrate_type_select", label = "substrate_type",
                 choices = substrate_type_list, selected = NULL,
                 width = "150px")
})

#========================================================
# Primary datatable for redd_substrate
#========================================================

# Primary DT datatable for survey_intent
output$redd_substrates = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_encounters_rows_selected)
  req(!is.na(selected_redd_encounter_data()$redd_encounter_id))
  redd_substrate_title = glue("{selected_survey_event_data()$species} redd substrate data for {input$stream_select} on ",
                               "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                               "to {selected_survey_data()$lo_rm}")
  redd_substrate_data = get_redd_substrate(selected_redd_encounter_data()$redd_encounter_id) %>%
    select(substrate_level, substrate_type, substrate_pct,
           created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(redd_substrate_data,
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
              htmltools::em(htmltools::strong(redd_substrate_title))))
})

# Create surveys DT proxy object
redd_substrate_dt_proxy = dataTableProxy(outputId = "redd_substrates")

#========================================================
# Collect individual_redd values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_redd_substrate_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_encounters_rows_selected)
  req(input$redd_substrates_rows_selected)
  req(!is.na(selected_redd_encounter_data()$redd_encounter_id))
  redd_substrate_data = get_redd_substrate(selected_redd_encounter_data()$redd_encounter_id)
  redd_substrate_row = input$redd_substrates_rows_selected
  selected_redd_substrate = tibble(redd_substrate_id = redd_substrate_data$redd_substrate_id[redd_substrate_row],
                                   substrate_level = redd_substrate_data$substrate_level[redd_substrate_row],
                                   substrate_type = redd_substrate_data$substrate_type[redd_substrate_row],
                                   substrate_pct = redd_substrate_data$substrate_pct[redd_substrate_row],
                                   created_date = redd_substrate_data$created_date[redd_substrate_row],
                                   created_by = redd_substrate_data$created_by[redd_substrate_row],
                                   modified_date = redd_substrate_data$modified_date[redd_substrate_row],
                                   modified_by = redd_substrate_data$modified_by[redd_substrate_row])
  return(selected_redd_substrate)
})

#========================================================
# Update select inputs to values in selected row
#========================================================

# Update all input values to values in selected row
observeEvent(input$redd_substrates_rows_selected, {
  srsdat = selected_redd_substrate_data()
  updateSelectizeInput(session, "substrate_level_select", selected = srsdat$substrate_level)
  updateSelectizeInput(session, "substrate_type_select", selected = srsdat$substrate_type)
  updateNumericInput(session, "substrate_pct_input", value = srsdat$substrate_pct)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Disable "New" button if four rows of substrate already exists
# There are only four categories in the lut
observe({
  req(!is.na(selected_redd_encounter_data()$redd_encounter_id))
  input$insert_redd_substrate
  input$delete_redd_substrate
  redd_substrate_data = get_redd_substrate(selected_redd_encounter_data()$redd_encounter_id)
  if (nrow(redd_substrate_data) >= 4L) {
    shinyjs::disable("substrate_add")
  } else {
    shinyjs::enable("substrate_add")
  }
})

# Create reactive to collect input values for insert actions
redd_substrate_create = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_encounters_rows_selected)
  req(!is.na(selected_redd_encounter_data()$redd_encounter_id))
  # Redd_encounter_id
  redd_encounter_id_input = selected_redd_encounter_data()$redd_encounter_id
  # Substrate level
  substrate_level_input = input$substrate_level_select
  if ( substrate_level_input == "" ) {
    substrate_level_id = NA
  } else {
    substrate_level_vals = get_substrate_level()
    substrate_level_id = substrate_level_vals %>%
      filter(substrate_level == substrate_level_input) %>%
      pull(substrate_level_id)
  }
  # Substrate_type
  substrate_type_input = input$substrate_type_select
  if ( substrate_type_input == "" ) {
    substrate_type_id = NA
  } else {
    substrate_type_vals = get_substrate_type()
    substrate_type_id = substrate_type_vals %>%
      filter(substrate_type == substrate_type_input) %>%
      pull(substrate_type_id)
  }
  new_redd_substrate = tibble(redd_encounter_id = redd_encounter_id_input,
                              substrate_level = substrate_level_input,
                              substrate_level_id = substrate_level_id,
                              substrate_type = substrate_type_input,
                              substrate_type_id = substrate_type_id,
                              substrate_pct = input$substrate_pct_input,
                              created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                              created_by = Sys.getenv("USERNAME"))
  return(new_redd_substrate)
})

# Generate values to show in modal
output$redd_substrate_modal_insert_vals = renderDT({
  redd_substrate_modal_in_vals = redd_substrate_create() %>%
    select(substrate_level, substrate_type, substrate_pct)
  # Generate table
  datatable(redd_substrate_modal_in_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

# Modal for new individual_redds. Need dup flag
observeEvent(input$substrate_add, {
  new_redd_substrate_vals = redd_substrate_create()
  new_level = redd_substrate_create()$substrate_level
  existing_substrate = get_redd_substrate(selected_redd_encounter_data()$redd_encounter_id)
  old_levels = existing_substrate$substrate_level
  existing_pct = sum(existing_substrate$substrate_pct)
  new_substrate_pct = existing_pct + new_redd_substrate_vals$substrate_pct
  new_type = redd_substrate_create()$substrate_type
  old_types = existing_substrate$substrate_type
  showModal(
    tags$div(id = "redd_substrate_insert_modal",
             # Verify required fields have data...none can be blank
             if ( is.na(new_redd_substrate_vals$substrate_level) |
                  is.na(new_redd_substrate_vals$substrate_type) |
                  is.na(new_redd_substrate_vals$substrate_pct) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Values are required in all fields"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify no levels are repeated
             } else if ( new_level %in% old_levels ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("The '{new_level}' substrate level has already been entered. Please select a different substrate level"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify no substrate types are repeated
             } else if ( new_type %in% old_types ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("The '{new_type}' substrate type has already been entered. Please select a different substrate type"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify substrate pct does not exceed 100
             } else if ( new_substrate_pct > 100 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("The substrate_percent total exceeds 100%. Please select a different value or edit previous values"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert new redd substrate data to the database?"),
                 fluidPage (
                   DT::DTOutput("redd_substrate_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_redd_substrate", "Insert substrate data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
redd_substrate_insert_vals = reactive({
  new_redd_substrate_values = redd_substrate_create() %>%
    select(redd_encounter_id, substrate_level_id, substrate_type_id,
           substrate_pct, created_by)
  return(new_redd_substrate_values)
})

# Update DB and reload DT
observeEvent(input$insert_redd_substrate, {
  tryCatch({
    redd_substrate_insert(redd_substrate_insert_vals())
    shinytoastr::toastr_success("New substrate data was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_redd_substrate_insert_vals = get_redd_substrate(selected_redd_encounter_data()$redd_encounter_id) %>%
    select(substrate_level, substrate_type, substrate_pct,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_substrate_dt_proxy, post_redd_substrate_insert_vals)
}, priority = 99999)

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
redd_substrate_edit = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_encounters_rows_selected)
  req(!is.na(selected_redd_substrate_data()$redd_substrate_id))
  # Redd_substrate_id
  redd_substrate_id_input = selected_redd_substrate_data()$redd_substrate_id
  substrate_level_input = input$substrate_level_select
  if ( substrate_level_input == "" ) {
    substrate_level_id = NA
  } else {
    substrate_level_vals = get_substrate_level()
    substrate_level_id = substrate_level_vals %>%
      filter(substrate_level == substrate_level_input) %>%
      pull(substrate_level_id)
  }
  # Substrate_type
  substrate_type_input = input$substrate_type_select
  if ( substrate_type_input == "" ) {
    substrate_type_id = NA
  } else {
    substrate_type_vals = get_substrate_type()
    substrate_type_id = substrate_type_vals %>%
      filter(substrate_type == substrate_type_input) %>%
      pull(substrate_type_id)
  }
  edit_redd_substrate = tibble(redd_substrate_id = redd_substrate_id_input,
                               substrate_level = substrate_level_input,
                               substrate_level_id = substrate_level_id,
                               substrate_type = substrate_type_input,
                               substrate_type_id = substrate_type_id,
                               substrate_pct = input$substrate_pct_input,
                               modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                               modified_by = Sys.getenv("USERNAME"))
  return(edit_redd_substrate)
})

# Generate values to show in modal
output$redd_substrate_modal_update_vals = renderDT({
  redd_substrate_modal_up_vals = redd_substrate_edit() %>%
    select(substrate_level, substrate_type, substrate_pct)
  # Generate table
  datatable(redd_substrate_modal_up_vals,
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
observeEvent(input$substrate_edit, {
  old_redd_substrate_vals = selected_redd_substrate_data() %>%
    select(substrate_level, substrate_type, substrate_pct)
  current_type = old_redd_substrate_vals$substrate_type
  old_redd_substrate_vals[] = lapply(old_redd_substrate_vals, remisc::set_na)
  new_redd_substrate_vals = redd_substrate_edit() %>%
    mutate(substrate_pct = as.integer(substrate_pct)) %>%
    select(substrate_level, substrate_type, substrate_pct)
  new_redd_substrate_vals[] = lapply(new_redd_substrate_vals, remisc::set_na)
  existing_substrate = get_redd_substrate(selected_redd_encounter_data()$redd_encounter_id)
  existing_pct = sum(existing_substrate$substrate_pct) - old_redd_substrate_vals$substrate_pct
  new_substrate_pct = existing_pct + new_redd_substrate_vals$substrate_pct
  new_type = redd_substrate_edit()$substrate_type
  old_types = existing_substrate$substrate_type
  showModal(
    tags$div(id = "redd_substrate_update_modal",
             if ( !length(input$redd_substrates_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify at least one value has been edited
             } else if ( isTRUE(all_equal(old_redd_substrate_vals, new_redd_substrate_vals)) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please change at least one value!"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify no substrate types are repeated
             } else if ( !new_type == current_type & new_type %in% old_types ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("The '{new_type}' substrate type has already been entered. Please select a different substrate type"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify substrate pct does not exceed 100
             } else if ( new_substrate_pct > 100 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("The substrate_percent total exceeds 100%. Please select a different value or edit previous values"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Update individual redd data to these new values?",
                 fluidPage (
                   DT::DTOutput("redd_substrate_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_substrate_edits", "Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_substrate_edits, {
  tryCatch({
    redd_substrate_update(redd_substrate_edit())
    shinytoastr::toastr_success("Substrate data was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_redd_substrate_edit_vals = get_redd_substrate(selected_redd_encounter_data()$redd_encounter_id) %>%
    select(substrate_level, substrate_type, substrate_pct,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_substrate_dt_proxy, post_redd_substrate_edit_vals)
}, priority = 9999)

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$redd_substrate_modal_delete_vals = renderDT({
  redd_substrate_modal_del_id = selected_redd_substrate_data()$redd_substrate_id
  redd_substrate_modal_del_vals = get_redd_substrate(selected_redd_encounter_data()$redd_encounter_id) %>%
    filter(redd_substrate_id == redd_substrate_modal_del_id) %>%
    select(substrate_level, substrate_type, substrate_pct)
  # Generate table
  datatable(redd_substrate_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$substrate_delete, {
  redd_substrate_id = selected_redd_substrate_data()$redd_substrate_id
  showModal(
    tags$div(id = "redd_substrate_delete_modal",
             if ( length(redd_substrate_id) == 0L ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("Please select a row to delete"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Are you sure you want to delete this redd substrate data from the database?",
                 fluidPage (
                   DT::DTOutput("redd_substrate_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_redd_substrate", "Delete substrate data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_redd_substrate, {
  tryCatch({
    redd_substrate_delete(selected_redd_substrate_data())
    shinytoastr::toastr_success("Substrate data was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  redd_substrates_after_delete = get_redd_substrate(selected_redd_encounter_data()$redd_encounter_id) %>%
    select(substrate_level, substrate_type, substrate_pct,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_substrate_dt_proxy, redd_substrates_after_delete)
})

