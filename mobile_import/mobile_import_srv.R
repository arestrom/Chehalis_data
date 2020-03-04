
#========================================================
# Generate select ui's
#========================================================

output$project_select = renderUI({
  project_list = c("417821: Chehalis SGS")
  selectizeInput("project_select", label = "Select your project or profile ID",
                 choices = project_list, selected = project_list[[1]],
                 width = "275px")
})

output$mobile_form_select = renderUI({
  mobile_form_list = c("3363255: Stream Survey Form")
  selectizeInput("mobile_form_select", label = "Select the mobile form",
                 choices = mobile_form_list, selected = mobile_form_list[[1]],
                 width = "275px")
})

#========================================================
# Get info on surveys that need to be uploaded
#========================================================

# Event reactive to get new Access token
new_access_token = eventReactive(input$check_for_new_surveys, {
  access_token = NULL
  while (is.null(access_token)) {
    access_token = iformr::get_iform_access_token(
      server_name = "wdfw",
      client_key_name = "r6production_key",
      client_secret_name = "r6production_secret")
  }
  return(access_token)
})

# Update DB and reload DT
new_survey_data = eventReactive(input$check_for_new_surveys, {
  req(input$project_select)
  req(input$mobile_form_select)
  #req(!is.null(new_access_token()))
  prof_id = as.integer(remisc::get_text_item(input$project_select, item = 1, sep = ":"))
  pf_page_id = as.integer(remisc::get_text_item(input$mobile_form_select, item = 1, sep = ":"))
  tryCatch({
    new_surveys = count_new_surveys(profile_id = prof_id,
                                    parent_form_page_id = pf_page_id,
                                    access_token = new_access_token())
    shinytoastr::toastr_success("Search complete")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Connection error", conditionMessage(e))
  })
  return(new_surveys)
})

#========================================================
# New surveys summary datatable
#========================================================

# Primary DT datatable for survey_intent
output$new_surveys = renderDT({
  req(input$tabs == "mobile_import")
  new_surveys_title = glue("New surveys for mobile form {input$mobile_form_select}")
  # Generate table
  new_survey_data = tibble(n_surveys = 0L,
                           first_id = 0L,
                           start_date = "Unknown",
                           last_id = 0L,
                           end_date = "Unknown")
  datatable(new_survey_data,
            colnames = c('Number of new surveys', 'First ID', 'Start date', 'Last ID', 'End date'),
            options = list(dom = 't',
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: black; width: auto;',
              htmltools::em(htmltools::strong(new_surveys_title))))
})

# Create surveys DT proxy object
new_surveys_dt_proxy = dataTableProxy(outputId = "new_surveys")

#========================================================
# Missing streams datatable
#========================================================

# Primary DT datatable for survey_intent
output$missing_streams = renderDT({
  req(input$tabs == "mobile_import")
  missing_streams_title = glue("If any streams are missing, please edit {input$mobile_form_select}")
  # Generate table
  missing_stream_data = tibble(parent_form_survey_id = "None",
                               survey_date = "",
                               stream_name = "",
                               stream_name_text = "")
  datatable(missing_stream_data,
            colnames = c('Parent form ID', 'Survey date', 'Stream uuid', 'Stream name'),
            options = list(dom = 't',
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: black; width: auto;',
              htmltools::em(htmltools::strong(missing_streams_title))))
})

# Create surveys DT proxy object
missing_streams_dt_proxy = dataTableProxy(outputId = "missing_streams")

#========================================================
# Missing reaches datatable
#========================================================

# Primary DT datatable for survey_intent
output$missing_reaches = renderDT({
  req(input$tabs == "mobile_import")
  missing_reaches_title = glue("If any reaches are missing, please edit {input$mobile_form_select}")
  # Generate table
  missing_reach_data = tibble(parent_form_survey_id = "None",
                              survey_date = "",
                              reach = "",
                              reach_text = "")
  datatable(missing_reach_data,
            colnames = c('Parent form ID', 'Survey date', 'Reach key value', 'Reach description'),
            options = list(dom = 't',
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: black; width: auto;',
              htmltools::em(htmltools::strong(missing_reaches_title))))
})

# Create surveys DT proxy object
missing_reaches_dt_proxy = dataTableProxy(outputId = "missing_reaches")

# Generate counts for new_surveys datatable
observeEvent(input$check_for_new_surveys, {
  # Set progress bar...estimate 3 seconds
  progress = Progress$new(session, min = 1, max = 3)
  on.exit(progress$close())
  # Just output message without bar for now
  progress$set(message = 'Contacting server',
               detail = 'One moment please...')
  for (i in 1:3) {
    progress$set(value = i)
    Sys.sleep(1)
  }
  new_survey_vals = new_survey_data() %>%
    # Count number of surveys and get dates and ids of first, last
    mutate(n_surveys = n()) %>%
    mutate(first_id = min(parent_form_survey_id)) %>%
    mutate(last_id = max(parent_form_survey_id)) %>%
    mutate(start_date = min(as.Date(survey_date))) %>%
    mutate(end_date = max(as.Date(survey_date))) %>%
    select(n_surveys, first_id, start_date, last_id, end_date) %>%
    distinct()
  replaceData(new_surveys_dt_proxy, new_survey_vals)
}, priority = 9999)

# Generate values for missing streams datatable
observeEvent(input$check_for_new_surveys, {
  missing_stream_vals = new_survey_data() %>%
    filter(nchar(stream_name) < 36L) %>%
    select(parent_form_survey_id, survey_date,
           stream_name, stream_name_text) %>%
    distinct()
  replaceData(missing_streams_dt_proxy, missing_stream_vals)
}, priority = 9999)

# #========================================================
# # Collect location values from selected row for later use
# #========================================================
#
# # Create reactive to collect input values for update and delete actions
# selected_reach_point_data = reactive({
#   req(input$tabs == "reach_point")
#   req(input$reach_points_rows_selected)
#   reach_point_data = get_reach_point(waterbody_id())
#   reach_point_row = input$reach_points_rows_selected
#   selected_reach_point = tibble(location_id = reach_point_data$location_id[reach_point_row],
#                                 location_coordinates_id = reach_point_data$location_coordinates_id[reach_point_row],
#                                 river_mile = reach_point_data$river_mile[reach_point_row],
#                                 reach_point_type = reach_point_data$reach_point_type[reach_point_row],
#                                 reach_point_code = reach_point_data$reach_point_code[reach_point_row],
#                                 reach_point_name = reach_point_data$reach_point_name[reach_point_row],
#                                 latitude = reach_point_data$latitude[reach_point_row],
#                                 longitude = reach_point_data$longitude[reach_point_row],
#                                 horiz_accuracy = reach_point_data$horiz_accuracy[reach_point_row],
#                                 reach_point_description = reach_point_data$reach_point_description[reach_point_row],
#                                 created_date = reach_point_data$created_date[reach_point_row],
#                                 created_by = reach_point_data$created_by[reach_point_row],
#                                 modified_date = reach_point_data$modified_date[reach_point_row],
#                                 modified_by = reach_point_data$modified_by[reach_point_row])
#   return(selected_reach_point)
# })
#
# #========================================================
# # Update inputs to values in selected row
# #========================================================
#
# # Update all input values to values in selected row
# observeEvent(input$reach_points_rows_selected, {
#   srpdat = selected_reach_point_data()
#   updateNumericInput(session, "river_mile_input", value = srpdat$river_mile)
#   updateSelectizeInput(session, "reach_point_type_select", selected = srpdat$reach_point_type)
#   updateTextInput(session, "reach_point_code_input", value = srpdat$reach_point_code)
#   updateTextInput(session, "reach_point_name_input", value = srpdat$reach_point_name)
#   updateNumericInput(session, "reach_point_latitude_input", value = srpdat$latitude)
#   updateNumericInput(session, "reach_point_longitude_input", value = srpdat$longitude)
#   updateNumericInput(session, "reach_point_horiz_accuracy_input", value = srpdat$horiz_accuracy)
#   updateTextAreaInput(session, "reach_point_description_input", value = srpdat$reach_point_description)
# })
#
# #================================================================
# # Get either selected reach coordinates or default stream centroid
# #================================================================
#
# # Get centroid of stream for setting view of redd_map
# selected_reach_point_coords = reactive({
#   req(input$tabs == "reach_point")
#   req(input$reach_points_rows_selected)
#   # Get centroid of stream....always available if stream is selected
#   center_lat = selected_stream_centroid()$center_lat
#   center_lon = selected_stream_centroid()$center_lon
#   # Get reach coordinates from inputs if present
#   if ( is.na(input$reach_point_latitude_input) | is.na(input$reach_point_longitude_input) ) {
#     reach_point_lat = center_lat
#     reach_point_lon = center_lon
#     reach_point_name = "need reach name"
#   } else {
#     reach_point_lat = input$reach_point_latitude_input
#     reach_point_lon = input$reach_point_longitude_input
#     reach_point_name = input$reach_point_name_input
#     if ( is.na(reach_point_name) | reach_point_name == "" ) {
#       reach_point_name = "need reach name"
#     }
#   }
#   reach_point_coords = tibble(reach_point_name = reach_point_name,
#                               reach_point_lat = reach_point_lat,
#                               reach_point_lon = reach_point_lon)
#   return(reach_point_coords)
# })
#
# # Output leaflet bidn map....could also use color to indicate species:
# # See: https://rstudio.github.io/leaflet/markers.html
# output$reach_point_map <- renderLeaflet({
#   reach_point_data = selected_reach_point_coords()
#   reach_point_lat = reach_point_data$reach_point_lat
#   reach_point_lon = reach_point_data$reach_point_lon
#   reach_point_name = reach_point_data$reach_point_name
#   m = leaflet() %>%
#     setView(
#       lng = selected_stream_centroid()$center_lon,
#       lat = selected_stream_centroid()$center_lat,
#       zoom = 14) %>%
#     # Needed to enable draggable circle-markers
#     addDrawToolbar(circleOptions = NA,
#                    circleMarkerOptions = NA,
#                    markerOptions = NA,
#                    polygonOptions = NA,
#                    rectangleOptions = NA,
#                    polylineOptions = NA) %>%
#     addPolylines(
#       data = wria_streams(),
#       group = "Streams",
#       weight = 3,
#       color = "#0000e6",
#       label = ~stream_label,
#       layerId = ~stream_label,
#       labelOptions = labelOptions(noHide = FALSE)) %>%
#     addCircleMarkers(
#       lng = reach_point_lon,
#       lat = reach_point_lat,
#       #layerId = fish_encounter_id,
#       popup = reach_point_name,
#       radius = 8,
#       color = "red",
#       fillOpacity = 0.5,
#       stroke = FALSE,
#       options = markerOptions(draggable = TRUE,
#                               riseOnHover = TRUE)) %>%
#     addProviderTiles("Esri.WorldImagery", group = "Esri World Imagery") %>%
#     addProviderTiles("OpenTopoMap", group = "Open Topo Map") %>%
#     addLayersControl(position = 'bottomright',
#                      baseGroups = c("Esri World Imagery", "Open Topo Map"),
#                      overlayGroups = c("Streams"),
#                      options = layersControlOptions(collapsed = TRUE))
#   m
# })
#
# # Create reactive to hold click data
# reach_point_marker_data = reactive({
#   req(input$reach_point_map_marker_click)
#   reach_point_click_data = input$reach_point_map_marker_click
#   reach_point_dat = tibble(latitude = round(as.numeric(reach_point_click_data$lat), digits = 7),
#                            longitude = round(as.numeric(reach_point_click_data$lng), digits = 7))
#   return(reach_point_dat)
# })
#
# # Get dataframe of updated locations
# output$reach_point_coordinates = renderUI({
#   if ( length(input$reach_point_map_marker_click) == 0L ) {
#     HTML("Drag marker to edit reach point. Click on marker to set coordinates")
#   } else {
#     HTML(glue("Reach point coordinates: ", { reach_point_marker_data()$latitude }, ": ", { reach_point_marker_data()$longitude }))
#   }
# })
#
# # Modal for new reach points...add or edit a point...write coordinates to lat, lon
# observeEvent(input$use_reach_point_map, {
#   showModal(
#     # Verify required fields have data...none can be blank
#     tags$div(id = "reach_point_map_modal",
#              modalDialog (
#                size = 'l',
#                title = glue("Add or edit a reach point"),
#                fluidPage (
#                  fluidRow(
#                    column(width = 12,
#                           leafletOutput("reach_point_map", height = 500),
#                           br()
#                    )
#                  ),
#                  fluidRow(
#                    column(width = 3,
#                           actionButton("capture_reach_point", "Capture reach point"),
#                           tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
#                                 tooltip = glue("You can zoom in on the map and drag the marker to the ",
#                                                "correct reach end-point location. Click on the marker ",
#                                                "to set the coordinates. Then click on the button to ",
#                                                "capture the location and send the coordinates to the ",
#                                                "data entry screen."))),
#                    column(width = 9,
#                           htmlOutput("reach_point_coordinates"))
#                  )
#                ),
#                easyClose = TRUE,
#                footer = NULL
#              )
#     )
#   )
# })
#
# #======================================================================
# # Update coordinate inputs to coordinates selected on map
# #======================================================================
#
# # Update all input values to values in selected row
# observeEvent(input$capture_reach_point, {
#   reach_point_coord_data = reach_point_marker_data()
#   updateNumericInput(session, "reach_point_latitude_input", value = reach_point_coord_data$latitude)
#   updateNumericInput(session, "reach_point_longitude_input", value = reach_point_coord_data$longitude)
#   removeModal()
# })
#
# #========================================================
# # Insert operations: reactives, observers and modals
# #========================================================
#
# # Create reactive to collect input values for insert actions
# reach_point_create = reactive({
#   # Location type
#   reach_point_type_input = input$reach_point_type_select
#   if ( reach_point_type_input == "" ) {
#     location_type_id = NA
#   } else {
#     reach_point_type_vals = get_location_type()
#     location_type_id = reach_point_type_vals %>%
#       filter(reach_point_type == reach_point_type_input) %>%
#       pull(location_type_id)
#   }
#   new_reach_point = tibble(river_mile = input$river_mile_input,
#                            reach_point_type = reach_point_type_input,
#                            location_type_id = location_type_id,
#                            reach_point_code = input$reach_point_code_input,
#                            reach_point_name = input$reach_point_name_input,
#                            latitude = input$reach_point_latitude_input,
#                            longitude = input$reach_point_longitude_input,
#                            horiz_accuracy = input$reach_point_horiz_accuracy_input,
#                            reach_point_description = input$reach_point_description_input,
#                            created_dt = lubridate::with_tz(Sys.time(), "UTC"),
#                            created_by = Sys.getenv("USERNAME"))
#   return(new_reach_point)
# })
#
# # Generate values to show in modal
# output$reach_point_modal_insert_vals = renderDT({
#   reach_point_modal_in_vals = reach_point_create() %>%
#     select(river_mile, reach_point_type, reach_point_code, reach_point_name, #channel_type, orientation_type,
#            latitude, longitude, horiz_accuracy, reach_point_description)
#   # Generate table
#   datatable(reach_point_modal_in_vals,
#             rownames = FALSE,
#             options = list(dom = 't',
#                            scrollX = T,
#                            ordering = FALSE,
#                            initComplete = JS(
#                              "function(settings, json) {",
#                              "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
#                              "}")))
# })
#
# # Modal for new redd locations
# observeEvent(input$reach_point_add, {
#   new_reach_point_vals = reach_point_create()
#   old_reach_point_vals = get_reach_point(waterbody_id())
#   new_coords = paste0(new_reach_point_vals$latitude, ":", new_reach_point_vals$longitude)
#   old_coords = paste0(old_reach_point_vals$latitude, ":", old_reach_point_vals$longitude)
#   showModal(
#     # Verify required fields have data...none can be blank
#     tags$div(id = "reach_point_insert_modal",
#              if ( is.na(new_reach_point_vals$river_mile) |
#                   is.na(new_reach_point_vals$location_type_id) |
#                   is.na(new_reach_point_vals$latitude) |
#                   is.na(new_reach_point_vals$longitude) ) {
#                modalDialog (
#                  size = "m",
#                  title = "Warning",
#                  paste0("Values are required for river_mile, location_type, and coordinates"),
#                  easyClose = TRUE,
#                  footer = NULL
#                )
#                # Verify no river_miles are duplicated
#              } else if ( new_reach_point_vals$river_mile %in% old_reach_point_vals$river_mile ) {
#                modalDialog (
#                  size = "m",
#                  title = "Warning",
#                  paste0("This river mile point has already been entered"),
#                  easyClose = TRUE,
#                  footer = NULL
#                )
#                # Verify no set of coordinates are duplicated
#              } else if ( new_coords %in% old_coords ) {
#                modalDialog (
#                  size = "m",
#                  title = "Warning",
#                  paste0("This set of coordinates has already been entered"),
#                  easyClose = TRUE,
#                  footer = NULL
#                )
#              } else {
#                # Write to DB
#                modalDialog (
#                  size = 'l',
#                  title = glue("Insert new reach points to the database?"),
#                  fluidPage (
#                    DT::DTOutput("reach_point_modal_insert_vals"),
#                    br(),
#                    br(),
#                    actionButton("insert_reach_point", "Insert reach point")
#                  ),
#                  easyClose = TRUE,
#                  footer = NULL
#                )
#              }
#     ))
# })
#
# # Reactive to hold values actually inserted
# reach_point_insert_vals = reactive({
#   new_reach_point_values = reach_point_create() %>%
#     mutate(waterbody_id = waterbody_id()) %>%
#     mutate(wria_id = wria_id()) %>%
#     mutate(stream_channel_type_id = "713a39a5-8e95-4069-b078-066699c321d8") %>%           # No data
#     mutate(location_orientation_type_id = "eb4652b7-5390-43d4-a98e-60ea54a1d518") %>%     # No data
#     select(waterbody_id, wria_id, location_type_id, stream_channel_type_id,
#            location_orientation_type_id, river_mile, reach_point_code,
#            reach_point_name, reach_point_description, latitude,
#            longitude, horiz_accuracy, created_by)
#   return(new_reach_point_values)
# })
#
# # Update DB and reload DT
# observeEvent(input$insert_reach_point, {
#   tryCatch({
#     reach_point_insert(reach_point_insert_vals())
#     shinytoastr::toastr_success("New reach end point was added")
#   }, error = function(e) {
#     shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
#   })
#   removeModal()
#   post_reach_point_insert_vals = get_reach_point(waterbody_id()) %>%
#     select(river_mile, reach_point_type, reach_point_code, reach_point_name,
#            latitude, longitude, horiz_accuracy, reach_point_description,
#            created_dt, created_by, modified_dt, modified_by)
#   replaceData(reach_point_dt_proxy, post_reach_point_insert_vals)
# }, priority = 9999)
#
# #========================================================
# # Edit operations: reactives, observers and modals
# #========================================================
#
# # Create reactive to collect input values for insert actions
# reach_point_edit = reactive({
#   # Location type
#   reach_point_type_input = input$reach_point_type_select
#   if ( reach_point_type_input == "" ) {
#     location_type_id = NA
#   } else {
#     reach_point_type_vals = get_location_type()
#     location_type_id = reach_point_type_vals %>%
#       filter(reach_point_type == reach_point_type_input) %>%
#       pull(location_type_id)
#   }
#   edit_reach_point = tibble(location_id = selected_reach_point_data()$location_id,
#                             river_mile = input$river_mile_input,
#                             reach_point_type = reach_point_type_input,
#                             location_type_id = location_type_id,
#                             reach_point_code = input$reach_point_code_input,
#                             reach_point_name = input$reach_point_name_input,
#                             latitude = input$reach_point_latitude_input,
#                             longitude = input$reach_point_longitude_input,
#                             horiz_accuracy = input$reach_point_horiz_accuracy_input,
#                             reach_point_description = input$reach_point_description_input,
#                             modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
#                             modified_by = Sys.getenv("USERNAME"))
#   return(edit_reach_point)
# })
#
# dependent_reach_point_surveys = reactive({
#   location_id = selected_reach_point_data()$location_id
#   reach_point_srv = get_reach_point_surveys(location_id)
#   return(reach_point_srv)
# })
#
# # Generate values to show check modal
# output$reach_point_edit_surveys = renderDT({
#   reach_pt_edit_srv = dependent_reach_point_surveys() %>%
#     select(survey_dt, upper_river_mile, lower_river_mile, observer)
#   reach_point_edit_dt_msg = glue("WARNING: All surveys linked to this reach point are shown below. ",
#                                  "Please verify that all surveys should be updated to the new values!")
#   # Generate table
#   datatable(reach_pt_edit_srv,
#             rownames = FALSE,
#             options = list(dom = 'ltp',
#                            pageLength = 10,
#                            lengthMenu = c(10, 25, 50),
#                            scrollX = T,
#                            ordering = FALSE,
#                            initComplete = JS(
#                              "function(settings, json) {",
#                              "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
#                              "}")),
#             caption = htmltools::tags$caption(
#               style = 'caption-side: top; text-align: left; color: blue; width: auto;',
#               htmltools::em(htmltools::strong(reach_point_edit_dt_msg))))
# })
#
# # Generate values to show in modal
# output$reach_point_modal_update_vals = renderDT({
#   reach_point_modal_edit_vals = reach_point_edit() %>%
#     select(river_mile, reach_point_type, reach_point_code, reach_point_name, #channel_type, orientation_type,
#            latitude, longitude, horiz_accuracy, reach_point_description)
#   # Generate table
#   datatable(reach_point_modal_edit_vals,
#             rownames = FALSE,
#             options = list(dom = 't',
#                            scrollX = T,
#                            ordering = FALSE,
#                            initComplete = JS(
#                              "function(settings, json) {",
#                              "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
#                              "}")))
# })
#
# # Generate flag to indicated if surveys affected by edits are too old and require data manager approval
# data_manager_flag = reactive({
#   chk_surveys = dependent_reach_point_surveys()
#   if ( nrow(chk_surveys) == 0L ) {
#     first_date = Sys.Date()
#   } else {
#     first_date = as.Date(min(chk_surveys$survey_date))
#   }
#   if (nrow(chk_surveys) == 0 ) {
#     dm_flag = FALSE
#   } else if ( nrow(chk_surveys) > 0 & (Sys.Date() - first_date) < 365 ) {
#     dm_flag = FALSE
#   } else if ( nrow(chk_surveys) > 0 & (Sys.Date() - first_date) > 365 ) {
#     dm_flag = TRUE
#   } else {
#     dm_flag = FALSE
#   }
#   return(dm_flag)
# })
#
# # Edit modal
# observeEvent(input$reach_point_edit, {
#   old_reach_point_vals = selected_reach_point_data() %>%
#     mutate(horiz_accuracy = as.numeric(horiz_accuracy)) %>%
#     mutate(river_mile = as.numeric(river_mile)) %>%
#     mutate(latitude = round(as.numeric(latitude), 6)) %>%
#     mutate(longitude = round(as.numeric(longitude), 6)) %>%
#     select(river_mile, reach_point_type, reach_point_code, reach_point_name,
#            latitude, longitude, horiz_accuracy, reach_point_description)
#   old_reach_point_vals[] = lapply(old_reach_point_vals, remisc::set_na)
#   new_reach_point_vals = reach_point_edit() %>%
#     mutate(horiz_accuracy = as.numeric(horiz_accuracy)) %>%
#     mutate(river_mile = as.numeric(river_mile)) %>%
#     mutate(latitude = round(as.numeric(latitude), 6)) %>%
#     mutate(longitude = round(as.numeric(longitude), 6)) %>%
#     select(river_mile, reach_point_type, reach_point_code, reach_point_name,
#            latitude, longitude, horiz_accuracy, reach_point_description)
#   new_reach_point_vals[] = lapply(new_reach_point_vals, remisc::set_na)
#   showModal(
#     tags$div(id = "reach_point_update_modal",
#              if ( data_manager_flag() == TRUE ) {
#                modalDialog (
#                  size = "m",
#                  title = "Warning",
#                  paste0("Edits would alter data for both historical and more recent surveys. ",
#                        "Please contact the data manager to request the updates. Another option ",
#                        "would be to enter a new reach end-point that differs by 0.01 Mile (52 ft) ",
#                        "from any existing end point on the stream."),
#                  fluidPage (
#                    br(),
#                    br(),
#                    DT::DTOutput("reach_point_edit_surveys")
#                  ),
#                  easyClose = TRUE,
#                  footer = NULL
#                )
#              } else if ( isTRUE(all_equal(old_reach_point_vals, new_reach_point_vals)) ) {
#                modalDialog (
#                  size = "m",
#                  title = "Warning",
#                  paste("Please change at least one value!"),
#                  easyClose = TRUE,
#                  footer = NULL
#                )
#              } else {
#                modalDialog (
#                  size = 'l',
#                  title = "Update reach point data to these new values?",
#                  fluidPage (
#                    DT::DTOutput("reach_point_modal_update_vals"),
#                    br(),
#                    br(),
#                    DT::DTOutput("reach_point_edit_surveys"),
#                    br(),
#                    br(),
#                    actionButton("save_reach_point_edits", "Save changes")
#                  ),
#                  easyClose = TRUE,
#                  footer = NULL
#                )
#              }
#     ))
# })
#
# # Update DB and reload DT
# observeEvent(input$save_reach_point_edits, {
#   tryCatch({
#     reach_point_update(reach_point_edit(), selected_reach_point_data())
#     shinytoastr::toastr_success("Reach end point was edited")
#   }, error = function(e) {
#     shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
#   })
#   removeModal()
#   post_reach_point_edit_vals = get_reach_point(waterbody_id()) %>%
#     select(river_mile, reach_point_type, reach_point_code, reach_point_name, #channel_type, orientation_type,
#            latitude, longitude, horiz_accuracy, reach_point_description,
#            created_dt, created_by, modified_dt, modified_by)
#   replaceData(reach_point_dt_proxy, post_reach_point_edit_vals)
# }, priority = 9999)
#
# #========================================================
# # Delete operations: reactives, observers and modals
# #========================================================
#
# # Generate values to show in modal
# output$reach_point_modal_delete_vals = renderDT({
#   reach_point_modal_del_id = selected_reach_point_data()$location_id
#   reach_point_modal_del_vals = get_reach_point(waterbody_id()) %>%
#     filter(location_id == reach_point_modal_del_id) %>%
#     select(river_mile, reach_point_type, reach_point_code, reach_point_name, #channel_type, orientation_type,
#            latitude, longitude, horiz_accuracy, reach_point_description)
#   # Generate table
#   datatable(reach_point_modal_del_vals,
#             rownames = FALSE,
#             options = list(dom = 't',
#                            scrollX = T,
#                            ordering = FALSE,
#                            initComplete = JS(
#                              "function(settings, json) {",
#                              "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
#                              "}")))
# })
#
# # Reactive to hold dependencies
# reach_point_dependencies = reactive({
#   location_id = selected_reach_point_data()$location_id
#   reach_point_dep = get_reach_point_dependencies(location_id)
#   return(reach_point_dep)
# })
#
# # Generate values to show check modal
# output$reach_point_delete_surveys = renderDT({
#   reach_pt_del_srv = dependent_reach_point_surveys()
#   reach_point_del_dt_msg = glue("All surveys below need to be reassigned to a different reach end point!")
#   # Generate table
#   datatable(reach_pt_del_srv,
#             rownames = FALSE,
#             options = list(dom = 'ltp',
#                            pageLength = 10,
#                            lengthMenu = c(10, 25, 50),
#                            scrollX = T,
#                            ordering = FALSE,
#                            initComplete = JS(
#                              "function(settings, json) {",
#                              "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
#                              "}")),
#             caption = htmltools::tags$caption(
#               style = 'caption-side: top; text-align: left; color: blue; width: auto;',
#               htmltools::em(htmltools::strong(reach_point_del_dt_msg))))
# })
#
# observeEvent(input$reach_point_delete, {
#   location_id = selected_reach_point_data()$location_id
#   reach_pt_dependencies = reach_point_dependencies()
#   table_names = paste0(paste0("'", names(reach_pt_dependencies), "'"), collapse = ", ")
#   # Customize the delete message depending on if other entries are linked to location
#   if ( ncol(reach_pt_dependencies) > 0L ) {
#     reach_point_delete_msg = glue("Other entries in {table_names} have been assigned to this reach point. ",
#                                   "All entries below must be reassigned to another river mile before the ",
#                                   "point can be deleted.")
#   } else {
#     reach_point_delete_msg = "Are you sure you want to delete this reach_point from the database?"
#   }
#   showModal(
#     tags$div(id = "reach_point_delete_modal",
#              if ( ncol(reach_pt_dependencies) > 0L ) {
#                modalDialog (
#                  size = "m",
#                  title = "Warning",
#                  reach_point_delete_msg,
#                  fluidPage(
#                    br(),
#                    DT::DTOutput(("reach_point_delete_surveys"))
#                  ),
#                  easyClose = TRUE,
#                  footer = NULL
#                )
#              } else {
#                modalDialog (
#                  size = 'l',
#                  title = reach_point_delete_msg,
#                  fluidPage (
#                    DT::DTOutput("reach_point_modal_delete_vals"),
#                    br(),
#                    br(),
#                    actionButton("delete_reach_point", "Delete reach point")
#                  ),
#                  easyClose = TRUE,
#                  footer = NULL
#                )
#              }
#     ))
# })
#
# # Update DB and reload DT
# observeEvent(input$delete_reach_point, {
#   tryCatch({
#     reach_point_delete(selected_reach_point_data())
#     shinytoastr::toastr_success("Reach end point was deleted")
#   }, error = function(e) {
#     shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
#   })
#   removeModal()
#   reach_points_after_delete = get_reach_point(waterbody_id()) %>%
#     select(river_mile, reach_point_type, reach_point_code, reach_point_name, #channel_type, orientation_type,
#            latitude, longitude, horiz_accuracy, reach_point_description,
#            created_dt, created_by, modified_dt, modified_by)
#   replaceData(reach_point_dt_proxy, reach_points_after_delete)
# }, priority = 9999)
#


