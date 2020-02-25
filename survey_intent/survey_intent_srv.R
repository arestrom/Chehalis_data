
output$intent_species_select = renderUI({
  species_list = get_intent_species()$species
  species_list = c("", species_list)
  selectizeInput("intent_species_select", label = "species",
                 choices = species_list, selected = NULL,
                 width = "175px")
})

output$intent_count_type_select = renderUI({
  count_type_list = get_count_type()$count_type
  count_type_list = c("", count_type_list)
  selectizeInput("intent_count_type_select", label = "count_type",
                 choices = count_type_list, selected = NULL,
                 width = "100px")
})

# Primary DT datatable for survey_intent
output$survey_intents = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  survey_intent_title = glue("Survey intent for {input$stream_select} on ",
                             "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                             "to {selected_survey_data()$lo_rm}")
  survey_intent_data = get_survey_intent(selected_survey_data()$survey_id) %>%
    select(species, count_type, created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(survey_intent_data,
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
              htmltools::em(htmltools::strong(survey_intent_title))))
})

# Create surveys DT proxy object
survey_intent_dt_proxy = dataTableProxy(outputId = "survey_intents")

#========================================================
# Collect intent values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_survey_intent_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_intents_rows_selected)
  req(!is.na(selected_survey_data()$survey_id))
  survey_intent_data = get_survey_intent(selected_survey_data()$survey_id)
  survey_intent_row = input$survey_intents_rows_selected
  selected_survey_intent = tibble(survey_intent_id = survey_intent_data$survey_intent_id[survey_intent_row],
                                  species = survey_intent_data$species[survey_intent_row],
                                  count_type = survey_intent_data$count_type[survey_intent_row],
                                  created_date = survey_intent_data$created_date[survey_intent_row],
                                  created_by = survey_intent_data$created_by[survey_intent_row],
                                  modified_date = survey_intent_data$modified_date[survey_intent_row],
                                  modified_by = survey_intent_data$modified_by[survey_intent_row])
  return(selected_survey_intent)
})

#========================================================
# Update intent select inputs to values in selected row
#========================================================

# Update all survey input values to values in selected row
observeEvent(input$survey_intents_rows_selected, {
  ssidat = selected_survey_intent_data()
  updateSelectizeInput(session, "intent_species_select", selected = ssidat$species)
  updateSelectizeInput(session, "intent_count_type_select", selected = ssidat$count_type)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
survey_intent_create = reactive({
  req(input$surveys_rows_selected)
  # Survey_id
  survey_id_input = selected_survey_data()$survey_id
  # Species
  intent_species_input = input$intent_species_select
  if (intent_species_input == "" ) {
    species_id = NA_character_
  } else {
    intent_species_vals = get_intent_species()
    species_id = intent_species_vals %>%
      filter(species == intent_species_input) %>%
      pull(species_id)
  }
  # Count type
  count_type_input = input$intent_count_type_select
  if ( count_type_input == "" ) {
    count_type_id = NA
  } else {
    count_type_vals = get_count_type()
    count_type_id = count_type_vals %>%
      filter(count_type == count_type_input) %>%
      pull(count_type_id)
  }
  new_survey_intent = tibble(survey_id = survey_id_input,
                             species = intent_species_input,
                             species_id = species_id,
                             count_type = count_type_input,
                             count_type_id = count_type_id,
                             created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                             created_by = Sys.getenv("USERNAME"))
  return(new_survey_intent)
})

# Generate values to show in modal
output$survey_intent_modal_insert_vals = renderDT({
  survey_intent_modal_in_vals = survey_intent_create() %>%
    select(species, count_type)
  # Generate table
  datatable(survey_intent_modal_in_vals,
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
observeEvent(input$intent_add, {
  new_survey_intent_vals = survey_intent_create()
  existing_survey_intent_vals = get_survey_intent(selected_survey_data()$survey_id) %>%
    select(species, count_type)
  dup_intent_flag = dup_survey_intent(new_survey_intent_vals, existing_survey_intent_vals)
  showModal(
    # Verify all fields have data...none can be blank
    tags$div(id = "survey_intent_insert_modal",
             if ( is.na(new_survey_intent_vals$species_id) |
                  is.na(new_survey_intent_vals$count_type_id) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("No fields can be blank, please add both species and intent"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( dup_intent_flag == TRUE ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Survey intent already exists. Please edit the species or count type before proceeding." ),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'm',
                 title = glue("Insert new survey intent to the database?"),
                 fluidPage (
                   DT::DTOutput("survey_intent_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_survey_intent", "Insert intent")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
survey_intent_insert_vals = reactive({
  new_intent_values = survey_intent_create() %>%
    select(survey_id, species_id, count_type_id, created_by)
  return(new_intent_values)
})

# Update DB and reload DT
observeEvent(input$insert_survey_intent, {
  tryCatch({
    survey_intent_insert(survey_intent_insert_vals())
    shinytoastr::toastr_success("New survey intent was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_intent_insert_vals = get_survey_intent(selected_survey_data()$survey_id) %>%
    select(species, count_type, created_dt, created_by, modified_dt, modified_by)
  replaceData(survey_intent_dt_proxy, post_intent_insert_vals)
})

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
survey_intent_edit = reactive({
  # Species
  intent_species_input = input$intent_species_select
  if (intent_species_input == "" ) {
    species_id = NA_character_
  } else {
    intent_species_vals = get_intent_species()
    species_id = intent_species_vals %>%
      filter(species == intent_species_input) %>%
      pull(species_id)
  }
  # Count type
  count_type_input = input$intent_count_type_select
  if ( count_type_input == "" ) {
    count_type_id = NA
  } else {
    count_type_vals = get_count_type()
    count_type_id = count_type_vals %>%
      filter(count_type == count_type_input) %>%
      pull(count_type_id)
  }
  edit_survey_intent = tibble(survey_intent_id = selected_survey_intent_data()$survey_intent_id,
                              species = intent_species_input,
                              species_id = species_id,
                              count_type = count_type_input,
                              count_type_id = count_type_id,
                              modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                              modified_by = Sys.getenv("USERNAME"))
  return(edit_survey_intent)
})

# Generate values to show in modal
output$survey_intent_modal_update_vals = renderDT({
  survey_intent_modal_up_vals = survey_intent_edit() %>%
    select(species, count_type)
  # Generate table
  datatable(survey_intent_modal_up_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$intent_edit, {
  new_intent_vals = survey_intent_edit() %>%
    select(species, count_type)
  old_intent_vals = selected_survey_intent_data() %>%
    select(species, count_type)
  all_intent_vals = get_survey_intent(selected_survey_data()$survey_id) %>%
    select(species, count_type)
  dup_intent_flag = dup_survey_intent(new_intent_vals, all_intent_vals)
  showModal(
    tags$div(id = "survey_intent_update_modal",
             if ( !length(input$survey_intents_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_intent_vals, new_intent_vals)) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please change at least one value!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( dup_intent_flag == TRUE ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Survey intent already exists. Please edit the species or count type before proceeding." ),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Update survey intent to these new values?",
                 fluidPage (
                   DT::DTOutput("survey_intent_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_intent_edits","Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_intent_edits, {
  tryCatch({
    survey_intent_update(survey_intent_edit())
    shinytoastr::toastr_success("Survey intent was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_intent_edit_vals = get_survey_intent(selected_survey_data()$survey_id) %>%
    select(species, count_type, created_dt, created_by, modified_dt, modified_by)
  replaceData(survey_intent_dt_proxy, post_intent_edit_vals)
})

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$survey_intent_modal_delete_vals = renderDT({
  survey_intent_modal_del_id = selected_survey_intent_data()$survey_intent_id
  survey_intent_modal_del_vals = get_survey_intent(selected_survey_data()$survey_id) %>%
    filter(survey_intent_id == survey_intent_modal_del_id) %>%
    select(species, count_type)
  # Generate table
  datatable(survey_intent_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$intent_delete, {
  survey_intent_id = selected_survey_intent_data()$survey_intent_id
  showModal(
    tags$div(id = "survey_intent_delete_modal",
             if ( length(survey_intent_id) == 0 ) {
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
                 title = "Are you sure you want to delete this survey intent from the database?",
                 fluidPage (
                   DT::DTOutput("survey_intent_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_survey_intent", "Delete intent")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_survey_intent, {
  tryCatch({
    survey_intent_delete(selected_survey_intent_data())
    shinytoastr::toastr_success("Survey intent was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  survey_intents_after_delete = get_survey_intent(selected_survey_data()$survey_id) %>%
    select(species, count_type, created_dt, created_by, modified_dt, modified_by)
  replaceData(survey_intent_dt_proxy, survey_intents_after_delete)
})











