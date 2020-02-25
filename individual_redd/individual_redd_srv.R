#========================================================
# Generate lut select ui's
#========================================================

# Redd shape
output$redd_shape_select = renderUI({
  redd_shape_list = get_redd_shape()$redd_shape
  redd_shape_list = c("", redd_shape_list)
  selectizeInput("redd_shape_select", label = "redd_shape",
                 choices = redd_shape_list, selected = NULL,
                 width = "125px")
})

# Dewatered type
output$dewatered_type_select = renderUI({
  dewatered_type_list = get_dewatered_type()$dewatered_type
  dewatered_type_list = c("", dewatered_type_list)
  selectizeInput("dewatered_type_select", label = "dewatered_type",
                 choices = dewatered_type_list, selected = NULL,
                 width = "200px")
})

#========================================================
# Primary datatable for individual_redds
#========================================================

# Primary DT datatable for survey_intent
output$individual_redds = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_encounters_rows_selected)
  req(!is.na(selected_redd_encounter_data()$redd_encounter_id))
  individual_redd_title = glue("{selected_survey_event_data()$species} individual redd data for {input$stream_select} on ",
                               "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                               "to {selected_survey_data()$lo_rm}")
  individual_redd_data = get_individual_redd(selected_redd_encounter_data()$redd_encounter_id) %>%
    select(redd_shape, dewatered_type, pct_visible, redd_length_m, redd_width_m,
           redd_depth_m, tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(individual_redd_data,
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
              htmltools::em(htmltools::strong(individual_redd_title))))
})

# Create surveys DT proxy object
individual_redd_dt_proxy = dataTableProxy(outputId = "individual_redds")

#========================================================
# Collect individual_redd values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_individual_redd_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_encounters_rows_selected)
  req(input$individual_redds_rows_selected)
  req(!is.na(selected_redd_encounter_data()$redd_encounter_id))
  individual_redd_data = get_individual_redd(selected_redd_encounter_data()$redd_encounter_id)
  individual_redd_row = input$individual_redds_rows_selected
  selected_individual_redd = tibble(individual_redd_id = individual_redd_data$individual_redd_id[individual_redd_row],
                                    redd_shape = individual_redd_data$redd_shape[individual_redd_row],
                                    dewatered_type = individual_redd_data$dewatered_type[individual_redd_row],
                                    pct_visible = individual_redd_data$pct_visible[individual_redd_row],
                                    redd_length_m = individual_redd_data$redd_length_m[individual_redd_row],
                                    redd_width_m = individual_redd_data$redd_width_m[individual_redd_row],
                                    redd_depth_m = individual_redd_data$redd_depth_m[individual_redd_row],
                                    tailspill_height_m = individual_redd_data$tailspill_height_m[individual_redd_row],
                                    pct_superimposed = individual_redd_data$pct_superimposed[individual_redd_row],
                                    pct_degraded = individual_redd_data$pct_degraded[individual_redd_row],
                                    superimposed_redd_name = individual_redd_data$superimposed_redd_name[individual_redd_row],
                                    individual_redd_comment = individual_redd_data$individual_redd_comment[individual_redd_row],
                                    created_date = individual_redd_data$created_date[individual_redd_row],
                                    created_by = individual_redd_data$created_by[individual_redd_row],
                                    modified_date = individual_redd_data$modified_date[individual_redd_row],
                                    modified_by = individual_redd_data$modified_by[individual_redd_row])
  return(selected_individual_redd)
})

#========================================================
# Update select inputs to values in selected row
#========================================================

# Update all input values to values in selected row
observeEvent(input$individual_redds_rows_selected, {
  sirdat = selected_individual_redd_data()
  updateSelectizeInput(session, "redd_shape_select", selected = sirdat$redd_shape)
  updateSelectizeInput(session, "dewatered_type_select", selected = sirdat$dewatered_type)
  updateNumericInput(session, "pct_visible_input", value = sirdat$pct_visible)
  updateNumericInput(session, "redd_length_input", value = sirdat$redd_length_m)
  updateNumericInput(session, "redd_width_input", value = sirdat$redd_width_m)
  updateNumericInput(session, "redd_depth_input", value = sirdat$redd_depth_m)
  updateNumericInput(session, "tailspill_height_input", value = sirdat$tailspill_height_m)
  updateNumericInput(session, "pct_superimposed_input", value = sirdat$pct_superimposed)
  updateNumericInput(session, "pct_degraded_input", value = sirdat$pct_degraded)
  updateTextInput(session, "superimposed_redd_name_input", value = sirdat$superimposed_redd_name)
  updateTextAreaInput(session, "ind_redd_comment_input", value = sirdat$individual_redd_comment)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Disable "New" button if a row of comments already exists
observe({
  req(!is.na(selected_redd_encounter_data()$redd_encounter_id))
  input$insert_individual_redd
  ind_redd_data = get_individual_redd(selected_redd_encounter_data()$redd_encounter_id)
  if (nrow(ind_redd_data) >= 1L) {
    shinyjs::disable("ind_redd_add")
  } else {
    shinyjs::enable("ind_redd_add")
  }
})

# Create reactive to collect input values for insert actions
individual_redd_create = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_encounters_rows_selected)
  req(!is.na(selected_redd_encounter_data()$redd_encounter_id))
  # Redd_encounter_id
  redd_encounter_id_input = selected_redd_encounter_data()$redd_encounter_id
  # Redd count
  redd_count_input = selected_redd_encounter_data()$redd_count
  # Redd shape
  redd_shape_input = input$redd_shape_select
  if ( redd_shape_input == "" ) {
    redd_shape_id = NA
  } else {
    redd_shape_vals = get_redd_shape()
    redd_shape_id = redd_shape_vals %>%
      filter(redd_shape == redd_shape_input) %>%
      pull(redd_shape_id)
  }
  # Dewatered_type
  dewatered_type_input = input$dewatered_type_select
  if ( dewatered_type_input == "" ) {
    redd_dewatered_type_id = NA
  } else {
    dewatered_type_vals = get_dewatered_type()
    redd_dewatered_type_id = dewatered_type_vals %>%
      filter(dewatered_type == dewatered_type_input) %>%
      pull(redd_dewatered_type_id)
  }
  new_individual_redd = tibble(redd_encounter_id = redd_encounter_id_input,
                               redd_count = redd_count_input,
                               redd_shape = redd_shape_input,
                               redd_shape_id = redd_shape_id,
                               dewatered_type = dewatered_type_input,
                               redd_dewatered_type_id = redd_dewatered_type_id,
                               pct_visible = input$pct_visible_input,
                               redd_length_m = input$redd_length_input,
                               redd_width_m = input$redd_width_input,
                               redd_depth_m = input$redd_depth_input,
                               tailspill_height_m = input$tailspill_height_input,
                               pct_superimposed = input$pct_superimposed_input,
                               pct_degraded = input$pct_degraded_input,
                               superimposed_redd_name = input$superimposed_redd_name_input,
                               individual_redd_comment = input$ind_redd_comment_input,
                               created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                               created_by = Sys.getenv("USERNAME"))
  return(new_individual_redd)
})

# Generate values to show in modal
output$individual_redd_modal_insert_vals = renderDT({
  individual_redd_modal_in_vals = individual_redd_create() %>%
    select(redd_shape, dewatered_type, pct_visible, redd_length_m, redd_width_m,
           redd_depth_m, tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, individual_redd_comment)
  # Generate table
  datatable(individual_redd_modal_in_vals,
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
observeEvent(input$ind_redd_add, {
  new_individual_redd_vals = individual_redd_create()
  showModal(
    tags$div(id = "individual_redd_insert_modal",
             # Verify required fields have data...none can be blank
             if ( is.na(new_individual_redd_vals$redd_shape) | new_individual_redd_vals$redd_shape == "" ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("A value is required for redd_shape"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify redd count is one
             } else if (!individual_redd_create()$redd_count == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Individual redd data can only be entered if the redd_count value equals 1"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert new individual redd data to the database?"),
                 fluidPage (
                   DT::DTOutput("individual_redd_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_individual_redd", "Insert individual redd")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
individual_redd_insert_vals = reactive({
  new_ind_redd_values = individual_redd_create() %>%
    select(redd_encounter_id, redd_shape_id, redd_dewatered_type_id,
           pct_visible, redd_length_m, redd_width_m, redd_depth_m,
           tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, individual_redd_comment, created_by)
  return(new_ind_redd_values)
})

# Update DB and reload DT
observeEvent(input$insert_individual_redd, {
  tryCatch({
    individual_redd_insert(individual_redd_insert_vals())
    shinytoastr::toastr_success("New individual redd data was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_individual_redd_insert_vals = get_individual_redd(selected_redd_encounter_data()$redd_encounter_id) %>%
    select(redd_shape, dewatered_type, pct_visible, redd_length_m, redd_width_m,
           redd_depth_m, tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, created_dt, created_by, modified_dt, modified_by)
  replaceData(individual_redd_dt_proxy, post_individual_redd_insert_vals)
}, priority = 99999)

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
individual_redd_edit = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_encounters_rows_selected)
  req(input$individual_redds_rows_selected)
  req(!is.na(selected_individual_redd_data()$individual_redd_id))
  # Redd_encounter_id
  individual_redd_id_input = selected_individual_redd_data()$individual_redd_id
  # Redd count
  redd_count_input = selected_redd_encounter_data()$redd_count
  # Redd shape
  redd_shape_input = input$redd_shape_select
  if ( redd_shape_input == "" ) {
    redd_shape_id = NA
  } else {
    redd_shape_vals = get_redd_shape()
    redd_shape_id = redd_shape_vals %>%
      filter(redd_shape == redd_shape_input) %>%
      pull(redd_shape_id)
  }
  # Dewatered_type
  dewatered_type_input = input$dewatered_type_select
  if ( dewatered_type_input == "" ) {
    redd_dewatered_type_id = NA
  } else {
    dewatered_type_vals = get_dewatered_type()
    redd_dewatered_type_id = dewatered_type_vals %>%
      filter(dewatered_type == dewatered_type_input) %>%
      pull(redd_dewatered_type_id)
  }
  edit_individual_redd = tibble(individual_redd_id = individual_redd_id_input,
                                redd_count = redd_count_input,
                                redd_shape = redd_shape_input,
                                redd_shape_id = redd_shape_id,
                                dewatered_type = dewatered_type_input,
                                redd_dewatered_type_id = redd_dewatered_type_id,
                                pct_visible = input$pct_visible_input,
                                redd_length_m = input$redd_length_input,
                                redd_width_m = input$redd_width_input,
                                redd_depth_m = input$redd_depth_input,
                                tailspill_height_m = input$tailspill_height_input,
                                pct_superimposed = input$pct_superimposed_input,
                                pct_degraded = input$pct_degraded_input,
                                superimposed_redd_name = input$superimposed_redd_name_input,
                                individual_redd_comment = input$ind_redd_comment_input,
                                modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                                modified_by = Sys.getenv("USERNAME"))
  return(edit_individual_redd)
})

# Generate values to show in modal
output$individual_redd_modal_update_vals = renderDT({
  individual_redd_modal_up_vals = individual_redd_edit() %>%
    select(redd_shape, dewatered_type, pct_visible, redd_length_m, redd_width_m,
           redd_depth_m, tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, individual_redd_comment)
  # Generate table
  datatable(individual_redd_modal_up_vals,
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
observeEvent(input$ind_redd_edit, {
  old_individual_redd_vals = selected_individual_redd_data() %>%
    select(redd_shape, dewatered_type, pct_visible, redd_length_m, redd_width_m,
           redd_depth_m, tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, individual_redd_comment)
  old_individual_redd_vals[] = lapply(old_individual_redd_vals, remisc::set_na)
  new_individual_redd_vals = individual_redd_edit() %>%
    mutate(redd_length_m = as.numeric(redd_length_m)) %>%
    mutate(redd_width_m = as.numeric(redd_width_m)) %>%
    mutate(redd_depth_m = as.numeric(redd_depth_m)) %>%
    mutate(tailspill_height_m = as.numeric(tailspill_height_m)) %>%
    select(redd_shape, dewatered_type, pct_visible, redd_length_m, redd_width_m,
           redd_depth_m, tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, individual_redd_comment)
  new_individual_redd_vals[] = lapply(new_individual_redd_vals, remisc::set_na)
  showModal(
    tags$div(id = "individual_redd_update_modal",
             if ( !length(input$individual_redds_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_individual_redd_vals, new_individual_redd_vals)) ) {
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
                 title = "Update individual redd data to these new values?",
                 fluidPage (
                   DT::DTOutput("individual_redd_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_ind_redd_edits", "Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_ind_redd_edits, {
  tryCatch({
    individual_redd_update(individual_redd_edit())
    shinytoastr::toastr_success("Individual redd data was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_individual_redd_edit_vals = get_individual_redd(selected_redd_encounter_data()$redd_encounter_id) %>%
    select(redd_shape, dewatered_type, pct_visible, redd_length_m, redd_width_m,
           redd_depth_m, tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, created_dt, created_by, modified_dt, modified_by)
  replaceData(individual_redd_dt_proxy, post_individual_redd_edit_vals)
})

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$individual_redd_modal_delete_vals = renderDT({
  individual_redd_modal_del_id = selected_individual_redd_data()$individual_redd_id
  individual_redd_modal_del_vals = get_individual_redd(selected_redd_encounter_data()$redd_encounter_id) %>%
    filter(individual_redd_id == individual_redd_modal_del_id) %>%
    select(redd_shape, dewatered_type, pct_visible, redd_length_m, redd_width_m,
           redd_depth_m, tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, individual_redd_comment)
  # Generate table
  datatable(individual_redd_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$ind_redd_delete, {
  individual_redd_id = selected_individual_redd_data()$individual_redd_id
  showModal(
    tags$div(id = "individual_redd_delete_modal",
             if ( length(individual_redd_id) == 0L ) {
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
                 title = "Are you sure you want to delete this individual redd data from the database?",
                 fluidPage (
                   DT::DTOutput("individual_redd_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_individual_redd", "Delete individual redd data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_individual_redd, {
  tryCatch({
    individual_redd_delete(selected_individual_redd_data())
    shinytoastr::toastr_success("Individual redd data was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  individual_redds_after_delete = get_individual_redd(selected_redd_encounter_data()$redd_encounter_id) %>%
    select(redd_shape, dewatered_type, pct_visible, redd_length_m, redd_width_m,
           redd_depth_m, tailspill_height_m, pct_superimposed, pct_degraded,
           superimposed_redd_name, individual_redd_comment,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(individual_redd_dt_proxy, individual_redds_after_delete)
})

