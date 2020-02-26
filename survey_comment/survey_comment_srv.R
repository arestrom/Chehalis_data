
output$area_surveyed_select = renderUI({
  area_surveyed_list = get_area_surveyed()$area_surveyed
  area_surveyed_list = c("", area_surveyed_list)
  selectizeInput("area_surveyed_select", label = "area_surveyed_condition",
                 choices = area_surveyed_list, selected = NULL,
                 width = "175px")
})

output$abundance_condition_select = renderUI({
  abundance_condition_list = get_abundance_condition()$abundance_condition
  abundance_condition_list = c("", abundance_condition_list)
  selectizeInput("abundance_condition_select", label = "abundance_condition",
                 choices = abundance_condition_list, selected = NULL,
                 width = "125px")
})

output$stream_condition_select = renderUI({
  stream_condition_list = get_stream_condition()$stream_condition
  stream_condition_list = c("", stream_condition_list)
  selectizeInput("stream_condition_select", label = "stream_condition",
                 choices = stream_condition_list, selected = NULL,
                 width = "200px")
})

output$stream_flow_select = renderUI({
  stream_flow_list = get_stream_flow()$stream_flow
  stream_flow_list = c("", stream_flow_list)
  selectizeInput("stream_flow_select", label = "stream_flow",
                 choices = stream_flow_list, selected = NULL,
                 width = "125px")
})

output$count_condition_select = renderUI({
  count_condition_list = get_count_condition()$count_condition
  count_condition_list = c("", count_condition_list)
  selectizeInput("count_condition_select", label = "count_condition",
                 choices = count_condition_list, selected = NULL,
                 width = "250px")
})

output$survey_direction_select = renderUI({
  survey_direction_list = get_survey_direction()$survey_direction
  survey_direction_list = c("", survey_direction_list)
  selectizeInput("survey_direction_select", label = "survey_direction",
                 choices = survey_direction_list, selected = NULL,
                 width = "150px")
})

output$survey_timing_select = renderUI({
  survey_timing_list = get_survey_timing()$survey_timing
  survey_timing_list = c("", survey_timing_list)
  selectizeInput("survey_timing_select", label = "survey_timing_condition",
                 choices = survey_timing_list, selected = NULL,
                 width = "150px")
})

output$visibility_condition_select = renderUI({
  visibility_condition_list = get_visibility_condition()$visibility_condition
  visibility_condition_list = c("", visibility_condition_list)
  selectizeInput("visibility_condition_select", label = "visibility_condition",
                 choices = visibility_condition_list, selected = NULL,
                 width = "150px")
})

output$visibility_type_select = renderUI({
  visibility_type_list = get_visibility_type()$visibility_type
  visibility_type_list = c("", visibility_type_list)
  selectizeInput("visibility_type_select", label = "visibility_type",
                 choices = visibility_type_list, selected = NULL,
                 width = "150px")
})

output$weather_type_select = renderUI({
  weather_type_list = get_weather_type()$weather_type
  weather_type_list = c("", weather_type_list)
  selectInput("weather_type_select", label = "weather_type",
                 choices = weather_type_list, selected = NULL,
                 width = "150px")
})

# Primary DT datatable for comments
output$survey_comments = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  survey_comment_title = glue("Survey comments for {input$stream_select} on ",
                              "{selected_survey_data()$survey_date} from river mile {selected_survey_data()$up_rm} ",
                              "to {selected_survey_data()$lo_rm}")
  survey_comment_data = get_survey_comment(selected_survey_data()$survey_id) %>%
    select(area_surveyed, abundance_condition, stream_condition,
           stream_flow, count_condition, survey_direction, survey_timing,
           visibility_condition, visibility_type, weather_type, survey_comment,
           created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(survey_comment_data,
            selection = list(mode = 'single'),
            options = list(dom = 'tp',
                           scrollX = T,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: black; width: auto;',
              htmltools::em(htmltools::strong(survey_comment_title))))
})

# Create surveys DT proxy object
survey_comment_dt_proxy = dataTableProxy(outputId = "survey_comments")

#========================================================
# Collect comment values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_survey_comment_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_comments_rows_selected)
  survey_comment_data = get_survey_comment(selected_survey_data()$survey_id)
  survey_comment_row = input$survey_comments_rows_selected
  selected_survey_comment = tibble(survey_comment_id = survey_comment_data$survey_comment_id[survey_comment_row],
                                   area_surveyed = survey_comment_data$area_surveyed[survey_comment_row],
                                   abundance_condition = survey_comment_data$abundance_condition[survey_comment_row],
                                   stream_condition = survey_comment_data$stream_condition[survey_comment_row],
                                   stream_flow = survey_comment_data$stream_flow[survey_comment_row],
                                   count_condition = survey_comment_data$count_condition[survey_comment_row],
                                   survey_direction = survey_comment_data$survey_direction[survey_comment_row],
                                   survey_timing = survey_comment_data$survey_timing[survey_comment_row],
                                   visibility_condition = survey_comment_data$visibility_condition[survey_comment_row],
                                   visibility_type = survey_comment_data$visibility_type[survey_comment_row],
                                   weather_type = survey_comment_data$weather_type[survey_comment_row],
                                   survey_comment_text = survey_comment_data$survey_comment[survey_comment_row],
                                   created_date = survey_comment_data$created_date[survey_comment_row],
                                   created_by = survey_comment_data$created_by[survey_comment_row],
                                   modified_date = survey_comment_data$modified_date[survey_comment_row],
                                   modified_by = survey_comment_data$modified_by[survey_comment_row])
  return(selected_survey_comment)
})

#========================================================
# Update comment select inputs to values in selected row
#========================================================

# Update all survey input values to values in selected row
observeEvent(input$survey_comments_rows_selected, {
  sscdat = selected_survey_comment_data()
  updateSelectizeInput(session, "area_surveyed_select", selected = sscdat$area_surveyed)
  updateSelectizeInput(session, "abundance_condition_select", selected = sscdat$abundance_condition)
  updateSelectizeInput(session, "stream_condition_select", selected = sscdat$stream_condition)
  updateSelectizeInput(session, "stream_flow_select", selected = sscdat$stream_flow)
  updateSelectizeInput(session, "count_condition_select", selected = sscdat$count_condition)
  updateSelectizeInput(session, "survey_direction_select", selected = sscdat$survey_direction)
  updateSelectizeInput(session, "survey_timing_select", selected = sscdat$survey_timing)
  updateSelectizeInput(session, "visibility_condition_select", selected = sscdat$visibility_condition)
  updateSelectizeInput(session, "visibility_type_select", selected = sscdat$visibility_type)
  updateSelectizeInput(session, "weather_type_select", selected = sscdat$weather_type)
  updateTextAreaInput(session, "sc_comment_input", value = sscdat$survey_comment_text)
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Disable "New" button if a row of comments already exists
observe({
  input$insert_survey_comment
  survey_comment_data = get_survey_comment(selected_survey_data()$survey_id)
  if (nrow(survey_comment_data) >= 1L) {
    shinyjs::disable("comment_add")
  } else {
    shinyjs::enable("comment_add")
  }
})

# Create reactive to collect input values for insert actions
survey_comment_create = reactive({
  req(input$surveys_rows_selected)
  # Survey_id
  survey_id_input = selected_survey_data()$survey_id
  # Area surveyed
  area_surveyed_input = input$area_surveyed_select
  if (area_surveyed_input == "" ) {
    area_surveyed_id = NA_character_
  } else {
    area_surveyed_vals = get_area_surveyed()
    area_surveyed_id = area_surveyed_vals %>%
      filter(area_surveyed == area_surveyed_input) %>%
      pull(area_surveyed_id)
  }
  # Fish abundance condition
  abundance_condition_input = input$abundance_condition_select
  if ( abundance_condition_input == "" ) {
    fish_abundance_condition_id = NA
  } else {
    abundance_condition_vals = get_abundance_condition()
    fish_abundance_condition_id = abundance_condition_vals %>%
      filter(abundance_condition == abundance_condition_input) %>%
      pull(fish_abundance_condition_id)
  }
  # Stream condition
  stream_condition_input = input$stream_condition_select
  if ( stream_condition_input == "" ) {
    stream_condition_id = NA
  } else {
    stream_condition_vals = get_stream_condition()
    stream_condition_id = stream_condition_vals %>%
      filter(stream_condition == stream_condition_input) %>%
      pull(stream_condition_id)
  }
  # Stream flow type
  stream_flow_input = input$stream_flow_select
  if ( stream_flow_input == "" ) {
    stream_flow_type_id = NA
  } else {
    stream_flow_vals = get_stream_flow()
    stream_flow_type_id = stream_flow_vals %>%
      filter(stream_flow == stream_flow_input) %>%
      pull(stream_flow_type_id)
  }
  # Count condition
  count_condition_input = input$count_condition_select
  if ( count_condition_input == "" ) {
    survey_count_condition_id = NA
  } else {
    count_condition_vals = get_count_condition()
    survey_count_condition_id = count_condition_vals %>%
      filter(count_condition == count_condition_input) %>%
      pull(survey_count_condition_id)
  }
  # Survey direction
  survey_direction_input = input$survey_direction_select
  if ( survey_direction_input == "" ) {
    survey_direction_id = NA
  } else {
    survey_direction_vals = get_survey_direction()
    survey_direction_id = survey_direction_vals %>%
      filter(survey_direction == survey_direction_input) %>%
      pull(survey_direction_id)
  }
  # Survey timing
  survey_timing_input = input$survey_timing_select
  if ( survey_timing_input == "" ) {
    survey_timing_id = NA
  } else {
    survey_timing_vals = get_survey_timing()
    survey_timing_id = survey_timing_vals %>%
      filter(survey_timing == survey_timing_input) %>%
      pull(survey_timing_id)
  }
  # Visibility condition
  visibility_condition_input = input$visibility_condition_select
  if (visibility_condition_input == "" ) {
    visibility_condition_id = NA
  } else {
    visibility_condition_vals = get_visibility_condition()
    visibility_condition_id = visibility_condition_vals %>%
      filter(visibility_condition == visibility_condition_input) %>%
      pull(visibility_condition_id)
  }
  # Visibility type
  visibility_type_input = input$visibility_type_select
  if ( visibility_type_input == "" ) {
    visibility_type_id = NA
  } else {
    visibility_type_vals = get_visibility_type()
    visibility_type_id = visibility_type_vals %>%
      filter(visibility_type == visibility_type_input) %>%
      pull(visibility_type_id)
  }
  # Weather type
  weather_type_input = input$weather_type_select
  if ( weather_type_input == "" ) {
    weather_type_id = NA
  } else {
    weather_type_vals = get_weather_type()
    weather_type_id = weather_type_vals %>%
      filter(weather_type == weather_type_input) %>%
      pull(weather_type_id)
  }
  new_survey_comment = tibble(survey_id = survey_id_input,
                              area_surveyed = area_surveyed_input,
                              area_surveyed_id = area_surveyed_id,
                              abundance_condition = abundance_condition_input,
                              fish_abundance_condition_id = fish_abundance_condition_id,
                              stream_condition = stream_condition_input,
                              stream_condition_id = stream_condition_id,
                              stream_flow = stream_flow_input,
                              stream_flow_type_id = stream_flow_type_id,
                              count_condition = count_condition_input,
                              survey_count_condition_id = survey_count_condition_id,
                              survey_direction = survey_direction_input,
                              survey_direction_id = survey_direction_id,
                              survey_timing = survey_timing_input,
                              survey_timing_id = survey_timing_id,
                              visibility_condition = visibility_condition_input,
                              visibility_condition_id = visibility_condition_id,
                              visibility_type = visibility_type_input,
                              visibility_type_id = visibility_type_id,
                              weather_type = weather_type_input,
                              weather_type_id = weather_type_id,
                              comment_text = input$sc_comment_input,
                              created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                              created_by = Sys.getenv("USERNAME"))
  return(new_survey_comment)
})

# Generate values to show in modal
output$survey_comment_modal_insert_vals = renderDT({
  survey_comment_modal_in_vals = survey_comment_create() %>%
    select(area_surveyed, abundance_condition, stream_condition, stream_flow,
           count_condition, survey_direction, survey_timing, visibility_condition,
           visibility_type, weather_type, comment_text)
  # Generate table
  datatable(survey_comment_modal_in_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

# Modal for new comments. No need for dup_flag since New button is disabled if row already exists
observeEvent(input$comment_add, {
  new_survey_comment_vals = survey_comment_create()
  showModal(
    # Verify at least one field has data...all can not be blank
    tags$div(id = "survey_comment_insert_modal",
             if ( is.na(new_survey_comment_vals$area_surveyed_id) &
                  is.na(new_survey_comment_vals$fish_abundance_condition_id) &
                  is.na(new_survey_comment_vals$stream_condition_id) &
                  is.na(new_survey_comment_vals$stream_flow_type_id) &
                  is.na(new_survey_comment_vals$survey_count_condition_id) &
                  is.na(new_survey_comment_vals$survey_direction_id) &
                  is.na(new_survey_comment_vals$survey_timing_id) &
                  is.na(new_survey_comment_vals$visibility_condition_id) &
                  is.na(new_survey_comment_vals$visibility_type_id) &
                  is.na(new_survey_comment_vals$weather_type_id) &
                  (is.na(new_survey_comment_vals$comment_text) |
                  new_survey_comment_vals$comment_text == "" )) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("All fields are blank, please add at least one comment"),
                 easyClose = TRUE,
                 footer = NULL
               )
             # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert new survey comments to the database?"),
                 fluidPage (
                   DT::DTOutput("survey_comment_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_survey_comment", "Insert comments")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
survey_comment_insert_vals = reactive({
  new_comment_values = survey_comment_create() %>%
    select(survey_id, area_surveyed_id, fish_abundance_condition_id, stream_condition_id,
           stream_flow_type_id, survey_count_condition_id, survey_direction_id,
           survey_timing_id, visibility_condition_id, visibility_type_id,
           weather_type_id, comment_text, created_by)
  return(new_comment_values)
})

# Update DB and reload DT
observeEvent(input$insert_survey_comment, {
  tryCatch({
    survey_comment_insert(survey_comment_insert_vals())
    shinytoastr::toastr_success("New survey comments were added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_comment_insert_vals = get_survey_comment(selected_survey_data()$survey_id) %>%
    select(area_surveyed, abundance_condition, stream_condition,
           stream_flow, count_condition, survey_direction, survey_timing,
           visibility_condition, visibility_type, weather_type, survey_comment,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(survey_comment_dt_proxy, post_comment_insert_vals)
})

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
survey_comment_edit = reactive({
  # Area surveyed
  area_surveyed_input = input$area_surveyed_select
  if (area_surveyed_input == "" ) {
    area_surveyed_id = NA_character_
  } else {
    area_surveyed_vals = get_area_surveyed()
    area_surveyed_id = area_surveyed_vals %>%
      filter(area_surveyed == area_surveyed_input) %>%
      pull(area_surveyed_id)
  }
  # Fish abundance condition
  abundance_condition_input = input$abundance_condition_select
  if ( abundance_condition_input == "" ) {
    fish_abundance_condition_id = NA
  } else {
    abundance_condition_vals = get_abundance_condition()
    fish_abundance_condition_id = abundance_condition_vals %>%
      filter(abundance_condition == abundance_condition_input) %>%
      pull(fish_abundance_condition_id)
  }
  # Stream condition
  stream_condition_input = input$stream_condition_select
  if ( stream_condition_input == "" ) {
    stream_condition_id = NA
  } else {
    stream_condition_vals = get_stream_condition()
    stream_condition_id = stream_condition_vals %>%
      filter(stream_condition == stream_condition_input) %>%
      pull(stream_condition_id)
  }
  # Stream flow type
  stream_flow_input = input$stream_flow_select
  if ( stream_flow_input == "" ) {
    stream_flow_type_id = NA
  } else {
    stream_flow_vals = get_stream_flow()
    stream_flow_type_id = stream_flow_vals %>%
      filter(stream_flow == stream_flow_input) %>%
      pull(stream_flow_type_id)
  }
  # Count condition
  count_condition_input = input$count_condition_select
  if ( count_condition_input == "" ) {
    survey_count_condition_id = NA
  } else {
    count_condition_vals = get_count_condition()
    survey_count_condition_id = count_condition_vals %>%
      filter(count_condition == count_condition_input) %>%
      pull(survey_count_condition_id)
  }
  # Survey direction
  survey_direction_input = input$survey_direction_select
  if ( survey_direction_input == "" ) {
    survey_direction_id = NA
  } else {
    survey_direction_vals = get_survey_direction()
    survey_direction_id = survey_direction_vals %>%
      filter(survey_direction == survey_direction_input) %>%
      pull(survey_direction_id)
  }
  # Survey timing
  survey_timing_input = input$survey_timing_select
  if ( survey_timing_input == "" ) {
    survey_timing_id = NA
  } else {
    survey_timing_vals = get_survey_timing()
    survey_timing_id = survey_timing_vals %>%
      filter(survey_timing == survey_timing_input) %>%
      pull(survey_timing_id)
  }
  # Visibility condition
  visibility_condition_input = input$visibility_condition_select
  if (visibility_condition_input == "" ) {
    visibility_condition_id = NA
  } else {
    visibility_condition_vals = get_visibility_condition()
    visibility_condition_id = visibility_condition_vals %>%
      filter(visibility_condition == visibility_condition_input) %>%
      pull(visibility_condition_id)
  }
  # Visibility type
  visibility_type_input = input$visibility_type_select
  if ( visibility_type_input == "" ) {
    visibility_type_id = NA
  } else {
    visibility_type_vals = get_visibility_type()
    visibility_type_id = visibility_type_vals %>%
      filter(visibility_type == visibility_type_input) %>%
      pull(visibility_type_id)
  }
  # Weather type
  weather_type_input = input$weather_type_select
  if ( weather_type_input == "" ) {
    weather_type_id = NA
  } else {
    weather_type_vals = get_weather_type()
    weather_type_id = weather_type_vals %>%
      filter(weather_type == weather_type_input) %>%
      pull(weather_type_id)
  }
  edit_survey_comment = tibble(survey_comment_id = selected_survey_comment_data()$survey_comment_id,
                               area_surveyed = area_surveyed_input,
                               area_surveyed_id = area_surveyed_id,
                               abundance_condition = abundance_condition_input,
                               fish_abundance_condition_id = fish_abundance_condition_id,
                               stream_condition = stream_condition_input,
                               stream_condition_id = stream_condition_id,
                               stream_flow = stream_flow_input,
                               stream_flow_type_id = stream_flow_type_id,
                               count_condition = count_condition_input,
                               survey_count_condition_id = survey_count_condition_id,
                               survey_direction = survey_direction_input,
                               survey_direction_id = survey_direction_id,
                               survey_timing = survey_timing_input,
                               survey_timing_id = survey_timing_id,
                               visibility_condition = visibility_condition_input,
                               visibility_condition_id = visibility_condition_id,
                               visibility_type = visibility_type_input,
                               visibility_type_id = visibility_type_id,
                               weather_type = weather_type_input,
                               weather_type_id = weather_type_id,
                               comment_text = input$sc_comment_input,
                               modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                               modified_by = Sys.getenv("USERNAME"))
  edit_survey_comment = edit_survey_comment %>%
    mutate(comment_text = if_else(is.na(comment_text) | comment_text == "", NA_character_, comment_text))
  return(edit_survey_comment)
})

# Generate values to show in modal
output$survey_comment_modal_update_vals = renderDT({
  survey_comment_modal_up_vals = survey_comment_edit() %>%
    select(area_surveyed, abundance_condition, stream_condition, stream_flow,
           count_condition, survey_direction, survey_timing, visibility_condition,
           visibility_type, weather_type, comment_text)
  # Generate table
  datatable(survey_comment_modal_up_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$comment_edit, {
  old_comment_vals = selected_survey_comment_data() %>%
    select(area_surveyed, abundance_condition, stream_condition, stream_flow,
           count_condition, survey_direction, survey_timing, visibility_condition,
           visibility_type, weather_type, comment_text = survey_comment_text)
  old_comment_vals[] = lapply(old_comment_vals, remisc::set_empty)
  new_comment_vals = survey_comment_edit() %>%
    select(area_surveyed, abundance_condition, stream_condition, stream_flow,
           count_condition, survey_direction, survey_timing, visibility_condition,
           visibility_type, weather_type, comment_text)
  new_comment_vals[] = lapply(new_comment_vals, remisc::set_empty)
  showModal(
    tags$div(id = "survey_comment_update_modal",
             if ( !length(input$survey_comments_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_comment_vals, new_comment_vals)) ) {
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
                 title = "Update survey comments to these new values?",
                 fluidPage (
                   DT::DTOutput("survey_comment_modal_update_vals"),
                   br(),
                   br(),
                   actionButton("save_comment_edits","Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_comment_edits, {
  tryCatch({
    survey_comment_update(survey_comment_edit())
    shinytoastr::toastr_success("Survey comments were edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_comment_edit_vals = get_survey_comment(selected_survey_data()$survey_id) %>%
    select(area_surveyed, abundance_condition, stream_condition,
           stream_flow, count_condition, survey_direction, survey_timing,
           visibility_condition, visibility_type, weather_type, survey_comment,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(survey_comment_dt_proxy, post_comment_edit_vals)
})

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$survey_comment_modal_delete_vals = renderDT({
  survey_comment_modal_del_id = selected_survey_comment_data()$survey_comment_id
  survey_comment_modal_del_vals = get_survey_comment(selected_survey_data()$survey_id) %>%
    filter(survey_comment_id == survey_comment_modal_del_id) %>%
    select(area_surveyed, abundance_condition, stream_condition, stream_flow,
           count_condition, survey_direction, survey_timing, visibility_condition,
           visibility_type, weather_type, survey_comment)
  # Generate table
  datatable(survey_comment_modal_del_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$comment_delete, {
  survey_comment_id = selected_survey_comment_data()$survey_comment_id
  showModal(
    tags$div(id = "survey_comment_delete_modal",
             if ( length(survey_comment_id) == 0 ) {
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
                 title = "Are you sure you want to delete this set of comments from the database?",
                 fluidPage (
                   DT::DTOutput("survey_comment_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_survey_comment", "Delete comments")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_survey_comment, {
  tryCatch({
    survey_comment_delete(selected_survey_comment_data())
    shinytoastr::toastr_success("Survey comments were deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  survey_comments_after_delete = get_survey_comment(selected_survey_data()$survey_id) %>%
    select(area_surveyed, abundance_condition, stream_condition,
           stream_flow, count_condition, survey_direction, survey_timing,
           visibility_condition, visibility_type, weather_type, survey_comment,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(survey_comment_dt_proxy, survey_comments_after_delete)
})

# Set row selection to NULL if survey comment is deleted
observeEvent(input$delete_survey_comment, {
  selectRows(survey_dt_proxy, NULL)
})










