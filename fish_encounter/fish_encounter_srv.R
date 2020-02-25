#========================================================
# Generate lut select ui's
#========================================================

# Reactive to hold list of redd locations for past four months for species and reach
current_fish_locations = reactive({
  input$insert_fish_location
  input$save_fish_loc_edits
  input$delete_fish_location
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  fish_locs = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id)
  return(fish_locs)
})

output$fish_name_select = renderUI({
  fish_name_list = current_fish_locations()$fish_name
  fish_name_list = c("no location data", fish_name_list)
  selectizeInput("fish_name_select", label = "fish_name",
                 choices = fish_name_list, selected = NULL,
                 width = "125px")
})

output$fish_status_select = renderUI({
  fish_status_list = get_fish_status()$fish_status
  fish_status_list = c("", fish_status_list)
  selectizeInput("fish_status_select", label = "fish_status",
                 choices = fish_status_list, selected = NULL,
                 width = "75px")
})

output$sex_select = renderUI({
  sex_list = get_sex()$sex
  sex_list = c("", sex_list)
  selectizeInput("sex_select", label = "fish_sex",
                 choices = sex_list, selected = "Not applicable",
                 width = "115px")
})

output$maturity_select = renderUI({
  maturity_list = get_maturity()$maturity
  maturity_list = c("", maturity_list)
  selectizeInput("maturity_select", label = "maturity",
                 choices = maturity_list, selected = "Not applicable",
                 width = "115px")
})

output$origin_select = renderUI({
  origin_list = get_origin()$origin
  origin_list = c("", origin_list)
  selectizeInput("origin_select", label = "origin",
                 choices = origin_list, selected = "Unknown",
                 width = "100px")
})

output$cwt_status_select = renderUI({
  cwt_status_list = get_cwt_status()$cwt_status
  cwt_status_list = c("", cwt_status_list)
  selectizeInput("cwt_status_select", label = "cwt_status",
                 choices = cwt_status_list, selected = "Not applicable",
                 width = "210px")
})

output$clip_status_select = renderUI({
  clip_status_list = get_clip_status()$clip_status
  clip_status_list = c("", clip_status_list)
  selectizeInput("clip_status_select", label = "clip_status",
                 choices = clip_status_list, selected = "Not applicable",
                 width = "210px")
})

output$fish_behavior_select = renderUI({
  fish_behavior_list = get_fish_behavior()$fish_behavior
  fish_behavior_list = c("", fish_behavior_list)
  selectizeInput("fish_behavior_select", label = "fish_behavior",
                 choices = fish_behavior_list, selected = NULL,
                 width = "115px")
})

output$prev_counted_select = renderUI({
  prev_counted_list = c("No", "Yes")
  selectizeInput("prev_counted_select", label = "prev_counted",
                 choices = prev_counted_list, selected = "Yes",
                 width = "90px")
})

#========================================================
# Primary datatable for fish_encounters
#========================================================

# Primary DT datatable for survey_intent
output$fish_encounters = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(!is.na(selected_survey_event_data()$survey_event_id))
  fish_encounter_title = glue("{selected_survey_event_data()$species} data for {input$stream_select} on ",
                              "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                              "to {selected_survey_data()$lo_rm}")
  fish_encounter_data = get_fish_encounter(selected_survey_event_data()$survey_event_id) %>%
    select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin, cwt_status,
           clip_status, fish_behavior, prev_counted, created_dt, created_by, modified_dt,
           modified_by)

  # Generate table
  datatable(fish_encounter_data,
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
              htmltools::em(htmltools::strong(fish_encounter_title))))
})

# Create surveys DT proxy object
fish_encounter_dt_proxy = dataTableProxy(outputId = "fish_encounters")

# Set row selection in fish_encounters_dt to NULL if selection changes in survey_events dt
observe({
  input$survey_events_rows_selected
  selectRows(fish_encounter_dt_proxy, NULL)
})

#========================================================
# Collect encounter values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_fish_encounter_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_encounters_rows_selected)
  req(!is.na(selected_survey_event_data()$survey_event_id))
  fish_encounter_data = get_fish_encounter(selected_survey_event_data()$survey_event_id)
  fish_encounter_row = input$fish_encounters_rows_selected
  selected_fish_encounter = tibble(fish_encounter_id = fish_encounter_data$fish_encounter_id[fish_encounter_row],
                                   fish_encounter_time = fish_encounter_data$fish_encounter_time[fish_encounter_row],
                                   fish_count = fish_encounter_data$fish_count[fish_encounter_row],
                                   fish_status = fish_encounter_data$fish_status[fish_encounter_row],
                                   fish_name = fish_encounter_data$fish_name[fish_encounter_row],
                                   sex = fish_encounter_data$sex[fish_encounter_row],
                                   maturity = fish_encounter_data$maturity[fish_encounter_row],
                                   origin = fish_encounter_data$origin[fish_encounter_row],
                                   cwt_status = fish_encounter_data$cwt_status[fish_encounter_row],
                                   clip_status = fish_encounter_data$clip_status[fish_encounter_row],
                                   fish_behavior = fish_encounter_data$fish_behavior[fish_encounter_row],
                                   prev_counted = fish_encounter_data$prev_counted[fish_encounter_row],
                                   created_date = fish_encounter_data$created_date[fish_encounter_row],
                                   created_by = fish_encounter_data$created_by[fish_encounter_row],
                                   modified_date = fish_encounter_data$modified_date[fish_encounter_row],
                                   modified_by = fish_encounter_data$modified_by[fish_encounter_row])
  return(selected_fish_encounter)
})

#========================================================
# Update event select inputs to values in selected row
#========================================================

# Update all input values to values in selected row
observeEvent(input$fish_encounters_rows_selected, {
  sfedat = selected_fish_encounter_data()
  updateTimeInput(session, "fish_encounter_time_select", value = sfedat$fish_encounter_time)
  updateNumericInput(session, "fish_count_input", value = sfedat$fish_count)
  updateSelectizeInput(session, "fish_status_select", selected = sfedat$fish_status)
  updateSelectizeInput(session, "fish_name_select", selected = sfedat$fish_name)
  updateSelectizeInput(session, "sex_select", selected = sfedat$sex)
  updateSelectizeInput(session, "maturity_select", selected = sfedat$maturity)
  updateSelectizeInput(session, "origin_select", selected = sfedat$origin)
  updateSelectizeInput(session, "cwt_status_select", selected = sfedat$cwt_status)
  updateSelectizeInput(session, "clip_status_select", selected = sfedat$clip_status)
  updateSelectizeInput(session, "fish_behavior_select", selected = sfedat$fish_behavior)
  updateSelectizeInput(session, "prev_counted_select", selected = sfedat$prev_counted)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
fish_encounter_create = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(!is.na(selected_survey_event_data()$survey_event_id))
  # Survey date
  survey_date = selected_survey_data()$survey_date
  # Survey_event_id
  survey_event_id_input = selected_survey_event_data()$survey_event_id
  # # Location....NA for now
  # fish_location_input = NA
  # Fish encounter time
  fish_encounter_dt = format(input$fish_encounter_time_select)
  if (nchar(fish_encounter_dt) < 16) { fish_encounter_dt = NA_character_ }
  fish_encounter_dt = as.POSIXct(fish_encounter_dt)
  # Fish status
  fish_status_input = input$fish_status_select
  if ( fish_status_input == "" ) {
    fish_status_id = NA
  } else {
    fish_status_vals = get_fish_status()
    fish_status_id = fish_status_vals %>%
      filter(fish_status == fish_status_input) %>%
      pull(fish_status_id)
  }
  # Fish name, location_id
  loc_select = input$fish_name_select
  if ( is.null(loc_select) | is.na(loc_select) | loc_select == "" | loc_select == "no location data" ) {
    fish_location_id = NA
    fish_name_input = NA
  } else {
    fish_location = current_fish_locations() %>%
      filter(fish_name == loc_select)
    fish_location_id = fish_location$fish_location_id
    fish_name_input = fish_location$fish_name
  }
  # Sex
  sex_input = input$sex_select
  if ( sex_input == "" ) {
    sex_id = NA
  } else {
    sex_vals = get_sex()
    sex_id = sex_vals %>%
      filter(sex == sex_input) %>%
      pull(sex_id)
  }
  # Maturity
  maturity_input = input$maturity_select
  if ( maturity_input == "" ) {
    maturity_id = NA
  } else {
    maturity_vals = get_maturity()
    maturity_id = maturity_vals %>%
      filter(maturity == maturity_input) %>%
      pull(maturity_id)
  }
  # Origin
  origin_input = input$origin_select
  if ( origin_input == "" ) {
    origin_id = NA
  } else {
    origin_vals = get_origin()
    origin_id = origin_vals %>%
      filter(origin == origin_input) %>%
      pull(origin_id)
  }
  # CWT status
  cwt_status_input = input$cwt_status_select
  if ( cwt_status_input == "" ) {
    cwt_status_id = NA
  } else {
    cwt_status_vals = get_cwt_status()
    cwt_detection_status_id = cwt_status_vals %>%
      filter(cwt_status == cwt_status_input) %>%
      pull(cwt_detection_status_id)
  }
  # Clip status
  clip_status_input = input$clip_status_select
  if ( clip_status_input == "" ) {
    clip_status_id = NA
  } else {
    clip_status_vals = get_clip_status()
    adipose_clip_status_id = clip_status_vals %>%
      filter(clip_status == clip_status_input) %>%
      pull(adipose_clip_status_id)
  }
  # Fish behavior
  fish_behavior_input = input$fish_behavior_select
  if ( fish_behavior_input == "" ) {
    fish_behavior_id = NA
  } else {
    fish_behavior_vals = get_fish_behavior()
    fish_behavior_type_id = fish_behavior_vals %>%
      filter(fish_behavior == fish_behavior_input) %>%
      pull(fish_behavior_type_id)
  }
  new_fish_encounter = tibble(survey_event_id = survey_event_id_input,
                              survey_date = survey_date,
                              #fish_location_id = fish_location_input,
                              # Need to create full datetime values below modal
                              fish_encounter_dt = fish_encounter_dt,
                              fish_count = input$fish_count_input,
                              fish_status = fish_status_input,
                              fish_status_id = fish_status_id,
                              fish_name = fish_name_input,
                              fish_location_id,
                              sex = sex_input,
                              sex_id = sex_id,
                              maturity = maturity_input,
                              maturity_id = maturity_id,
                              origin = origin_input,
                              origin_id = origin_id,
                              cwt_status = cwt_status_input,
                              cwt_detection_status_id = cwt_detection_status_id,
                              clip_status = clip_status_input,
                              adipose_clip_status_id = adipose_clip_status_id,
                              fish_behavior = fish_behavior_input,
                              fish_behavior_type_id = fish_behavior_type_id,
                              prev_counted = input$prev_counted_select,
                              created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                              created_by = Sys.getenv("USERNAME"))
  return(new_fish_encounter)
})

# Generate values to show in modal
output$fish_encounter_modal_insert_vals = renderDT({
  fish_encounter_modal_in_vals = fish_encounter_create() %>%
    mutate(fish_encounter_dt = format(fish_encounter_dt, "%H:%M")) %>%
    select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin, cwt_status,
           clip_status, fish_behavior, prev_counted)
  # Generate table
  datatable(fish_encounter_modal_in_vals,
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
observeEvent(input$fish_enc_add, {
  new_fish_encounter_vals = fish_encounter_create()
  showModal(
    # Verify required fields have data...none can be blank
    tags$div(id = "fish_encounter_insert_modal",
             if ( is.na(new_fish_encounter_vals$fish_count) |
                  is.na(new_fish_encounter_vals$fish_status_id) |
                  is.na(new_fish_encounter_vals$sex_id) |
                  is.na(new_fish_encounter_vals$maturity_id) |
                  is.na(new_fish_encounter_vals$origin_id) |
                  is.na(new_fish_encounter_vals$cwt_detection_status_id) |
                  is.na(new_fish_encounter_vals$adipose_clip_status_id) |
                  is.na(new_fish_encounter_vals$fish_behavior_type_id) |
                  is.na(new_fish_encounter_vals$prev_counted) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Values are required in all fields except fish_encounter_dt"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert new fish data to the database?"),
                 fluidPage (
                   DT::DTOutput("fish_encounter_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_fish_encounter", "Insert fish data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
fish_encounter_insert_vals = reactive({
  new_fish_enc_values = fish_encounter_create() %>%
    mutate(fish_encounter_datetime = case_when(
      is.na(fish_encounter_dt) ~ as.POSIXct(NA),
      !is.na(fish_encounter_dt) ~ as.POSIXct(paste0(format(survey_date), " ",
                                                    format(fish_encounter_dt, "%H:%M")),
                                             tz = "America/Los_Angeles"))) %>%
    mutate(fish_encounter_datetime = with_tz(fish_encounter_datetime, tzone = "UTC")) %>%
    mutate(previously_counted_indicator = if_else(prev_counted == "No", 1L, 0L)) %>%
    select(survey_event_id, fish_location_id, fish_status_id, sex_id, maturity_id, origin_id,
           cwt_detection_status_id, adipose_clip_status_id, fish_behavior_type_id,
           fish_encounter_datetime, fish_count, previously_counted_indicator, created_by)
  return(new_fish_enc_values)
})

# Update DB and reload DT
observeEvent(input$insert_fish_encounter, {
  tryCatch({
    fish_encounter_insert(fish_encounter_insert_vals())
    shinytoastr::toastr_success("Fish count data was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_fish_encounter_insert_vals = get_fish_encounter(selected_survey_event_data()$survey_event_id) %>%
    select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin,
           cwt_status, clip_status, fish_behavior, prev_counted, created_dt,
           created_by, modified_dt, modified_by)
  replaceData(fish_encounter_dt_proxy, post_fish_encounter_insert_vals)
})

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
fish_encounter_edit = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(!is.na(selected_fish_encounter_data()$fish_encounter_id))
  # Survey date
  survey_date = selected_survey_data()$survey_date
  # Fish encounter time
  fish_encounter_dt = format(input$fish_encounter_time_select)
  if (nchar(fish_encounter_dt) < 16) { fish_encounter_dt = NA_character_ }
  fish_encounter_dt = as.POSIXct(fish_encounter_dt)
  # Fish status
  fish_status_input = input$fish_status_select
  if ( fish_status_input == "" ) {
    fish_status_id = NA
  } else {
    fish_status_vals = get_fish_status()
    fish_status_id = fish_status_vals %>%
      filter(fish_status == fish_status_input) %>%
      pull(fish_status_id)
  }
  # Fish name, location_id
  loc_select = input$fish_name_select
  if ( is.null(loc_select) | is.na(loc_select) | loc_select == "" | loc_select == "no location data" ) {
    fish_location_id = NA
    fish_name_input = NA
  } else {
    fish_location = current_fish_locations() %>%
      filter(fish_name == loc_select)
    fish_location_id = fish_location$fish_location_id
    fish_name_input = fish_location$fish_name
  }
  # Sex
  sex_input = input$sex_select
  if ( sex_input == "" ) {
    sex_id = NA
  } else {
    sex_vals = get_sex()
    sex_id = sex_vals %>%
      filter(sex == sex_input) %>%
      pull(sex_id)
  }
  # Maturity
  maturity_input = input$maturity_select
  if ( maturity_input == "" ) {
    maturity_id = NA
  } else {
    maturity_vals = get_maturity()
    maturity_id = maturity_vals %>%
      filter(maturity == maturity_input) %>%
      pull(maturity_id)
  }
  # Origin
  origin_input = input$origin_select
  if ( origin_input == "" ) {
    origin_id = NA
  } else {
    origin_vals = get_origin()
    origin_id = origin_vals %>%
      filter(origin == origin_input) %>%
      pull(origin_id)
  }
  # CWT status
  cwt_status_input = input$cwt_status_select
  if ( cwt_status_input == "" ) {
    cwt_status_id = NA
  } else {
    cwt_status_vals = get_cwt_status()
    cwt_detection_status_id = cwt_status_vals %>%
      filter(cwt_status == cwt_status_input) %>%
      pull(cwt_detection_status_id)
  }
  # Clip status
  clip_status_input = input$clip_status_select
  if ( clip_status_input == "" ) {
    clip_status_id = NA
  } else {
    clip_status_vals = get_clip_status()
    adipose_clip_status_id = clip_status_vals %>%
      filter(clip_status == clip_status_input) %>%
      pull(adipose_clip_status_id)
  }
  # Fish behavior
  fish_behavior_input = input$fish_behavior_select
  if ( fish_behavior_input == "" ) {
    fish_behavior_id = NA
  } else {
    fish_behavior_vals = get_fish_behavior()
    fish_behavior_type_id = fish_behavior_vals %>%
      filter(fish_behavior == fish_behavior_input) %>%
      pull(fish_behavior_type_id)
  }
  edit_fish_encounter = tibble(fish_encounter_id = selected_fish_encounter_data()$fish_encounter_id,
                               # Need to create full datetime values below modal
                               fish_encounter_dt = fish_encounter_dt,
                               fish_count = input$fish_count_input,
                               fish_status = fish_status_input,
                               fish_status_id = fish_status_id,
                               fish_name = fish_name_input,
                               fish_location_id = fish_location_id,
                               sex = sex_input,
                               sex_id = sex_id,
                               maturity = maturity_input,
                               maturity_id = maturity_id,
                               origin = origin_input,
                               origin_id = origin_id,
                               cwt_status = cwt_status_input,
                               cwt_detection_status_id = cwt_detection_status_id,
                               clip_status = clip_status_input,
                               adipose_clip_status_id = adipose_clip_status_id,
                               fish_behavior = fish_behavior_input,
                               fish_behavior_type_id = fish_behavior_type_id,
                               # Need to create prev_counted boolean below
                               prev_counted = input$prev_counted_select,
                               modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                               modified_by = Sys.getenv("USERNAME"))
  edit_fish_encounter = edit_fish_encounter %>%
    mutate(fish_encounter_time = case_when(
      is.na(fish_encounter_dt) ~ as.POSIXct(NA),
      !is.na(fish_encounter_dt) ~ as.POSIXct(paste0(format(survey_date), " ", format(fish_encounter_dt, "%H:%M")),
                                        tz = "America/Los_Angeles"))) %>%
    mutate(fish_encounter_time = with_tz(fish_encounter_time, tzone = "UTC")) %>%
    mutate(previously_counted_indicator = if_else(prev_counted == "No", FALSE, TRUE))
  return(edit_fish_encounter)
})

# Generate values to show in modal
output$fish_encounter_modal_update_vals = renderDT({
  fish_encounter_modal_up_vals = fish_encounter_edit() %>%
    mutate(fish_encounter_dt = format(fish_encounter_dt, "%H:%M")) %>%
    select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin, cwt_status,
           clip_status, fish_behavior, prev_counted)
  # Generate table
  datatable(fish_encounter_modal_up_vals,
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
observeEvent(input$fish_enc_edit, {
  old_fish_encounter_vals = selected_fish_encounter_data() %>%
    mutate(fish_encounter_dt = format(fish_encounter_time, "%H:%M")) %>%
    select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin, cwt_status,
           clip_status, fish_behavior, prev_counted)
  new_fish_encounter_vals = fish_encounter_edit() %>%
    mutate(fish_count = as.integer(fish_count)) %>%
    mutate(fish_encounter_dt = format(fish_encounter_dt, "%H:%M")) %>%
    select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin, cwt_status,
           clip_status, fish_behavior, prev_counted)
  showModal(
    tags$div(id = "fish_encounter_update_modal",
             if ( !length(input$fish_encounters_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_fish_encounter_vals, new_fish_encounter_vals)) ) {
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
                 title = "Update fish data to these new values?",
                 fluidPage (
                   DT::DTOutput("fish_encounter_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_fish_enc_edits","Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_fish_enc_edits, {
  tryCatch({
    fish_encounter_update(fish_encounter_edit())
    shinytoastr::toastr_success("Fish count data was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_fish_encounter_edit_vals = get_fish_encounter(selected_survey_event_data()$survey_event_id) %>%
    select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin,
           cwt_status, clip_status, fish_behavior, prev_counted, created_dt,
           created_by, modified_dt, modified_by)
  replaceData(fish_encounter_dt_proxy, post_fish_encounter_edit_vals)
}, priority = 9999)

# Update fish encounter table if something in fish location changed
observeEvent(input$save_fish_loc_edits, {
fish_counts_after_location_edit = get_fish_encounter(selected_survey_event_data()$survey_event_id) %>%
  select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin,
         cwt_status, clip_status, fish_behavior, prev_counted, created_dt,
         created_by, modified_dt, modified_by)
replaceData(fish_encounter_dt_proxy, fish_counts_after_location_edit)
}, priority = -1)

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$fish_encounter_modal_delete_vals = renderDT({
  fish_encounter_modal_del_id = selected_fish_encounter_data()$fish_encounter_id
  fish_encounter_modal_del_vals = get_fish_encounter(selected_survey_event_data()$survey_event_id) %>%
    filter(fish_encounter_id == fish_encounter_modal_del_id) %>%
    select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin, cwt_status,
           clip_status, fish_behavior, prev_counted)
  # Generate table
  datatable(fish_encounter_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$fish_enc_delete, {
  fish_encounter_id = selected_fish_encounter_data()$fish_encounter_id
  fish_encounter_dependencies = get_fish_encounter_dependencies(fish_encounter_id)
  table_names = paste0(names(fish_encounter_dependencies), collapse = ", ")
  showModal(
    tags$div(id = "fish_encounter_delete_modal",
             if ( ncol(fish_encounter_dependencies) > 0L ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 glue("Please delete associated fish data from the following tables first: {table_names}"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Are you sure you want to delete this fish data from the database?",
                 fluidPage (
                   DT::DTOutput("fish_encounter_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_fish_encounter", "Delete fish data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_fish_encounter, {
  tryCatch({
    fish_encounter_delete(selected_fish_encounter_data())
    shinytoastr::toastr_success("Fish count data was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  fish_encounters_after_delete = get_fish_encounter(selected_survey_event_data()$survey_event_id) %>%
    select(fish_encounter_dt, fish_count, fish_status, fish_name, sex, maturity, origin,
           cwt_status, clip_status, fish_behavior, prev_counted, created_dt,
           created_by, modified_dt, modified_by)
  replaceData(fish_encounter_dt_proxy, fish_encounters_after_delete)
})
