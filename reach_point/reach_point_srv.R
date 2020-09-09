
#========================================================
# Generate lut select ui's
#========================================================

output$reach_point_type_select = renderUI({
  reach_point_type_list = get_location_type()$reach_point_type
  reach_point_type_list = c("", reach_point_type_list)
  selectizeInput("reach_point_type_select", label = "reach_point_type",
                 choices = reach_point_type_list, selected = "Reach boundary point",
                 width = "175px")
})

#========================================================
# Primary datatable for reach points
#========================================================

# Primary DT datatable for survey_intent
output$reach_points = renderDT({
  req(input$tabs == "reach_point")
  req(input$stream_select)
  reach_point_title = glue("Reach points for {input$stream_select}")
  reach_point_data = get_reach_point(waterbody_id()) %>%
    select(river_mile, reach_point_type, reach_point_code, reach_point_name,
           latitude, longitude, horiz_accuracy, reach_point_description,
           created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(reach_point_data,
            selection = list(mode = 'single'),
            options = list(dom = 'ltp',
                           pageLength = 10,
                           lengthMenu = c(10, 25, 50),
                           scrollX = T,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: black; width: auto;',
              htmltools::em(htmltools::strong(reach_point_title))))
})

# Create surveys DT proxy object
reach_point_dt_proxy = dataTableProxy(outputId = "reach_points")

#========================================================
# Collect location values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_reach_point_data = reactive({
  req(input$tabs == "reach_point")
  req(input$reach_points_rows_selected)
  reach_point_data = get_reach_point(waterbody_id())
  reach_point_row = input$reach_points_rows_selected
  selected_reach_point = tibble(location_id = reach_point_data$location_id[reach_point_row],
                                location_coordinates_id = reach_point_data$location_coordinates_id[reach_point_row],
                                river_mile = reach_point_data$river_mile[reach_point_row],
                                reach_point_type = reach_point_data$reach_point_type[reach_point_row],
                                reach_point_code = reach_point_data$reach_point_code[reach_point_row],
                                reach_point_name = reach_point_data$reach_point_name[reach_point_row],
                                latitude = reach_point_data$latitude[reach_point_row],
                                longitude = reach_point_data$longitude[reach_point_row],
                                horiz_accuracy = reach_point_data$horiz_accuracy[reach_point_row],
                                reach_point_description = reach_point_data$reach_point_description[reach_point_row],
                                created_date = reach_point_data$created_date[reach_point_row],
                                created_by = reach_point_data$created_by[reach_point_row],
                                modified_date = reach_point_data$modified_date[reach_point_row],
                                modified_by = reach_point_data$modified_by[reach_point_row])
  return(selected_reach_point)
})

#========================================================
# Update inputs to values in selected row
#========================================================

# Update all input values to values in selected row
observeEvent(input$reach_points_rows_selected, {
  srpdat = selected_reach_point_data()
  updateNumericInput(session, "river_mile_input", value = srpdat$river_mile)
  updateSelectizeInput(session, "reach_point_type_select", selected = srpdat$reach_point_type)
  updateTextInput(session, "reach_point_code_input", value = srpdat$reach_point_code)
  updateTextInput(session, "reach_point_name_input", value = srpdat$reach_point_name)
  updateNumericInput(session, "reach_point_latitude_input", value = srpdat$latitude)
  updateNumericInput(session, "reach_point_longitude_input", value = srpdat$longitude)
  updateNumericInput(session, "reach_point_horiz_accuracy_input", value = srpdat$horiz_accuracy)
  updateTextAreaInput(session, "reach_point_description_input", value = srpdat$reach_point_description)
})

#================================================================
# Get either selected reach coordinates or default stream centroid
#================================================================

# Output leaflet bidn map....could also use color to indicate species:
# Notes:
# 1. input$reach_point_rows_selected is either row number or NULL
# 2. input$reach_point_latitude_input is either value or NA
# See: https://rstudio.github.io/leaflet/markers.html
output$reach_point_map <- renderLeaflet({
  req(input$tabs == "reach_point")
  # Force map to rerender every time use_reach_point_map button is clicked
  input$use_reach_point_map
  reach_coords = get_reach_point(waterbody_id()) %>%
    filter(!is.na(latitude) & !is.na(longitude)) %>%
    mutate(min_lat = min(latitude),
           min_lon = min(longitude),
           max_lat = max(latitude),
           max_lon = max(longitude)) %>%
    mutate(reach_descr = if_else(is.na(reach_point_description),
                                "No description",
                                reach_point_description)) %>%
    mutate(reach_label = paste0("River mile: ", river_mile, ", ", reach_descr)) %>%
    select(location_id, reach_label, latitude, longitude,
           min_lat, min_lon, max_lat, max_lon)
  # Get data for setting map bounds ========================
  if ( nrow(reach_coords) == 0L |
       is.na(input$reach_point_latitude_input) |
       is.na(input$reach_point_longitude_input) ) {
    bounds = tibble(lng1 = selected_stream_bounds()$min_lon,
                    lat1 = selected_stream_bounds()$min_lat,
                    lng2 = selected_stream_bounds()$max_lon,
                    lat2 = selected_stream_bounds()$max_lat)
  } else {
    # Add buffer, otherwise, if only a single point is available in reach_coords,
    # map zoom may be so high that the map does not render
    bounds = tibble(lng1 = (reach_coords$min_lon[1] - 0.0015),
                    lat1 = (reach_coords$min_lat[1] - 0.0015),
                    lng2 = (reach_coords$max_lon[1] + 0.0015),
                    lat2 = (reach_coords$max_lat[1] + 0.0015))
  }
  # Generate basemap ======================================
  edit_reach_loc = leaflet() %>%
    fitBounds(lng1 = bounds$lng1,
              lat1 = bounds$lat1,
              lng2 = bounds$lng2,
              lat2 = bounds$lat2) %>%
    addProviderTiles("Esri.WorldImagery", group = "Esri World Imagery") %>%
    addProviderTiles("OpenTopoMap", group = "Open Topo Map") %>%
    addLayersControl(position = 'bottomright',
                     baseGroups = c("Esri World Imagery", "Open Topo Map"),
                     overlayGroups = c("Streams"),
                     options = layersControlOptions(collapsed = TRUE)) %>%
    # Add edit features
    leaflet.extras::addDrawToolbar(.,
      targetGroup = "reach_edits",
      position = "topleft",
      polylineOptions = FALSE,
      polygonOptions = FALSE,
      circleOptions = FALSE,
      rectangleOptions = FALSE,
      markerOptions = FALSE,
      circleMarkerOptions = drawCircleMarkerOptions(
        color = "#ace600",
        stroke = TRUE,
        weight = 2,
        fillOpacity = 0.5),
      editOptions = editToolbarOptions(
        selectedPathOptions = selectedPathOptions()),
      singleFeature = TRUE) %>%
    addMeasure(primaryLengthUnit = "miles",
               activeColor = "#d91424",
               completedColor = "#d914c2") %>%
    addPolylines(
      data = wria_streams(),
      group = "Streams",
      weight = 3,
      color = "#0000e6",
      label = ~stream_label,
      layerId = ~stream_label,
      labelOptions = labelOptions(noHide = FALSE))
  # Test if reach_coords has data ==========================
  if ( !nrow(reach_coords) > 0L ) {
    return(edit_reach_loc)
  } else {
    edit_reach_loc_two = edit_reach_loc %>%
    # Add existing data if locations exist ===================
    addCircleMarkers(
      lng = reach_coords$longitude,
      lat = reach_coords$latitude,
      popup = reach_coords$reach_label,
      layerId = reach_coords$location_id,
      radius = 8,
      color = "red",
      fillOpacity = 0.5,
      stroke = FALSE,
      options = markerOptions(draggable = FALSE,
                              riseOnHover = TRUE))
    return(edit_reach_loc_two)
  }
})

# Create a reactive values object for drawn_features
reach_edit_rv = reactiveValues(lat = NULL, lon = NULL)

# Set rv to NULL on initiation of map
observeEvent(input$use_reach_point_map, {
  reach_edit_rv$lat = NULL
  reach_edit_rv$lon = NULL
}, priority = 9999)

# Assign coordinates from mapedit circle marker
observeEvent(c(input$reach_point_map_draw_all_features), {
  reach_edit_rv$lat = as.numeric(input$reach_point_map_draw_all_features$features[[1]]$geometry$coordinates[[2]])
  reach_edit_rv$lon = as.numeric(input$reach_point_map_draw_all_features$features[[1]]$geometry$coordinates[[1]])
})

# Get html output of updated locations
output$reach_point_coordinates = renderUI({
  coords_out = HTML(glue("Reach point location: ", {reach_edit_rv$lat}, ": ", {reach_edit_rv$lon}))
  return(coords_out)
})

# Modal for new reach points...add or edit a point...write coordinates to lat, lon
observeEvent(input$use_reach_point_map, {
  showModal(
    # Verify required fields have data...none can be blank
    tags$div(id = "reach_point_map_modal",
             modalDialog (
               size = 'l',
               title = glue("Add or edit a reach point"),
               fluidPage (
                 fluidRow(
                   column(width = 12,
                          leafletOutput("reach_point_map", height = 500),
                          br()
                   )
                 ),
                 fluidRow(
                   column(width = 3,
                          actionButton("capture_reach_point", "Save coordinates"),
                          tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
                                tooltip = glue("<span style='font-size:11px;'>",
                                               "You can zoom in on the map and use the circle marker ",
                                               "tool at the upper left to place a marker where the ",
                                               "reach end point should be located. You can use the ",
                                               "edit tool to fine-tune the position of the marker. ",
                                               "You can also use the measure tool at the upper right ",
                                               "to measure the distance in miles from the mouth. ",
                                               "When done, click on the 'Save coordinates' button.<span>"))),
                   column(width = 9,
                          htmlOutput("reach_point_coordinates"))
                 )
               ),
               easyClose = TRUE,
               footer = NULL
             )
    )
  )
}, priority = -1)

#======================================================================
# Update coordinate inputs to coordinates selected on map
#======================================================================

# Update all input values to values in selected row
observeEvent(input$capture_reach_point, {
  updateNumericInput(session, "reach_point_latitude_input", value = reach_edit_rv$lat)
  updateNumericInput(session, "reach_point_longitude_input", value = reach_edit_rv$lon)
  removeModal()
}, priority = 9999)

# Set rv to NULL
observeEvent(input$capture_reach_point, {
  reach_edit_rv$lat = NULL
  reach_edit_rv$lon = NULL
}, priority = -1)

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
reach_point_create = reactive({
  # Location type
  reach_point_type_input = input$reach_point_type_select
  if ( reach_point_type_input == "" ) {
    location_type_id = NA
  } else {
    reach_point_type_vals = get_location_type()
    location_type_id = reach_point_type_vals %>%
      filter(reach_point_type == reach_point_type_input) %>%
      pull(location_type_id)
  }
  new_reach_point = tibble(river_mile = input$river_mile_input,
                           reach_point_type = reach_point_type_input,
                           location_type_id = location_type_id,
                           reach_point_code = input$reach_point_code_input,
                           reach_point_name = input$reach_point_name_input,
                           latitude = input$reach_point_latitude_input,
                           longitude = input$reach_point_longitude_input,
                           horiz_accuracy = input$reach_point_horiz_accuracy_input,
                           reach_point_description = input$reach_point_description_input,
                           created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                           created_by = Sys.getenv("USERNAME"))
  return(new_reach_point)
})

# Generate values to show in modal
output$reach_point_modal_insert_vals = renderDT({
  reach_point_modal_in_vals = reach_point_create() %>%
    select(river_mile, reach_point_type, reach_point_code, reach_point_name,
           latitude, longitude, horiz_accuracy, reach_point_description)
  # Generate table
  datatable(reach_point_modal_in_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

# Modal for new redd locations
observeEvent(input$reach_point_add, {
  new_reach_point_vals = reach_point_create()
  old_reach_point_vals = get_reach_point(waterbody_id())
  new_coords = paste0(new_reach_point_vals$latitude, ":", new_reach_point_vals$longitude)
  old_coords = paste0(old_reach_point_vals$latitude, ":", old_reach_point_vals$longitude)
  showModal(
    # Verify required fields have data...none can be blank
    tags$div(id = "reach_point_insert_modal",
             if ( is.na(new_reach_point_vals$river_mile) |
                  is.na(new_reach_point_vals$location_type_id) |
                  is.na(new_reach_point_vals$latitude) |
                  is.na(new_reach_point_vals$longitude) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Values are required for river_mile, location_type, and coordinates"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify no river_miles are duplicated
             } else if ( new_reach_point_vals$river_mile %in% old_reach_point_vals$river_mile ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("This river mile point has already been entered"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify no set of coordinates are duplicated
             } else if ( new_coords %in% old_coords ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("This set of coordinates has already been entered"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               # Write to DB
               modalDialog (
                 size = 'l',
                 title = glue("Insert new reach points to the database?"),
                 fluidPage (
                   DT::DTOutput("reach_point_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_reach_point", "Insert reach point")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
reach_point_insert_vals = reactive({
  new_reach_point_values = reach_point_create() %>%
    mutate(waterbody_id = waterbody_id()) %>%
    mutate(wria_id = wria_id()) %>%
    mutate(stream_channel_type_id = "713a39a5-8e95-4069-b078-066699c321d8") %>%           # No data
    mutate(location_orientation_type_id = "eb4652b7-5390-43d4-a98e-60ea54a1d518") %>%     # No data
    select(waterbody_id, wria_id, location_type_id, stream_channel_type_id,
           location_orientation_type_id, river_mile, reach_point_code,
           reach_point_name, reach_point_description, latitude,
           longitude, horiz_accuracy, created_by)
  return(new_reach_point_values)
})

# Update DB and reload DT
observeEvent(input$insert_reach_point, {
  tryCatch({
    reach_point_insert(reach_point_insert_vals())
    shinytoastr::toastr_success("New reach end point was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_reach_point_insert_vals = get_reach_point(waterbody_id()) %>%
    select(river_mile, reach_point_type, reach_point_code, reach_point_name,
           latitude, longitude, horiz_accuracy, reach_point_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(reach_point_dt_proxy, post_reach_point_insert_vals)
}, priority = 9999)

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
reach_point_edit = reactive({
  # Location type
  reach_point_type_input = input$reach_point_type_select
  if ( reach_point_type_input == "" ) {
    location_type_id = NA
  } else {
    reach_point_type_vals = get_location_type()
    location_type_id = reach_point_type_vals %>%
      filter(reach_point_type == reach_point_type_input) %>%
      pull(location_type_id)
  }
  edit_reach_point = tibble(location_id = selected_reach_point_data()$location_id,
                            river_mile = input$river_mile_input,
                            reach_point_type = reach_point_type_input,
                            location_type_id = location_type_id,
                            reach_point_code = input$reach_point_code_input,
                            reach_point_name = input$reach_point_name_input,
                            latitude = input$reach_point_latitude_input,
                            longitude = input$reach_point_longitude_input,
                            horiz_accuracy = input$reach_point_horiz_accuracy_input,
                            reach_point_description = input$reach_point_description_input,
                            modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                            modified_by = Sys.getenv("USERNAME"))
  return(edit_reach_point)
})

dependent_reach_point_surveys = reactive({
  location_id = selected_reach_point_data()$location_id
  reach_point_srv = get_reach_point_surveys(location_id)
  return(reach_point_srv)
})

# Generate values to show check modal
output$reach_point_edit_surveys = renderDT({
  reach_pt_edit_srv = dependent_reach_point_surveys() %>%
    select(survey_dt, upper_river_mile, lower_river_mile, observer)
  reach_point_edit_dt_msg = glue("WARNING: All surveys linked to this reach point are shown below. ",
                                 "Please verify that all surveys should be updated to the new values!")
  # Generate table
  datatable(reach_pt_edit_srv,
            rownames = FALSE,
            options = list(dom = 'ltp',
                           pageLength = 10,
                           lengthMenu = c(10, 25, 50),
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: blue; width: auto;',
              htmltools::em(htmltools::strong(reach_point_edit_dt_msg))))
})

# Generate values to show in modal
output$reach_point_modal_update_vals = renderDT({
  reach_point_modal_edit_vals = reach_point_edit() %>%
    select(river_mile, reach_point_type, reach_point_code, reach_point_name,
           latitude, longitude, horiz_accuracy, reach_point_description)
  # Generate table
  datatable(reach_point_modal_edit_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

# Generate flag to indicated if surveys affected by edits are too old and require data manager approval
data_manager_flag = reactive({
  chk_surveys = dependent_reach_point_surveys()
  if ( nrow(chk_surveys) == 0L ) {
    first_date = Sys.Date()
  } else {
    first_date = as.Date(min(chk_surveys$survey_date))
  }
  if (nrow(chk_surveys) == 0 ) {
    dm_flag = FALSE
  } else if ( nrow(chk_surveys) > 0 & (Sys.Date() - first_date) < 365 ) {
    dm_flag = FALSE
  } else if ( nrow(chk_surveys) > 0 & (Sys.Date() - first_date) > 365 ) {
    dm_flag = TRUE
  } else {
    dm_flag = FALSE
  }
  return(dm_flag)
})

# Edit modal
observeEvent(input$reach_point_edit, {
  old_reach_point_vals = selected_reach_point_data() %>%
    mutate(horiz_accuracy = as.numeric(horiz_accuracy)) %>%
    mutate(river_mile = as.numeric(river_mile)) %>%
    mutate(latitude = round(as.numeric(latitude), 6)) %>%
    mutate(longitude = round(as.numeric(longitude), 6)) %>%
    select(river_mile, reach_point_type, reach_point_code, reach_point_name,
           latitude, longitude, horiz_accuracy, reach_point_description)
  old_reach_point_vals[] = lapply(old_reach_point_vals, remisc::set_na)
  new_reach_point_vals = reach_point_edit() %>%
    mutate(horiz_accuracy = as.numeric(horiz_accuracy)) %>%
    mutate(river_mile = as.numeric(river_mile)) %>%
    mutate(latitude = round(as.numeric(latitude), 6)) %>%
    mutate(longitude = round(as.numeric(longitude), 6)) %>%
    select(river_mile, reach_point_type, reach_point_code, reach_point_name,
           latitude, longitude, horiz_accuracy, reach_point_description)
  new_reach_point_vals[] = lapply(new_reach_point_vals, remisc::set_na)
  showModal(
    tags$div(id = "reach_point_update_modal",
             if ( data_manager_flag() == TRUE & !Sys.getenv("USERNAME") %in% c("stromas", "ronnelmr") ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Edits would alter data for both historical and more recent surveys. ",
                       "Please contact the data manager to request the updates. Another option ",
                       "would be to enter a new reach end-point that differs by 0.01 Mile (52 ft) ",
                       "from any existing end point on the stream."),
                 fluidPage (
                   br(),
                   br(),
                   DT::DTOutput("reach_point_edit_surveys")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_reach_point_vals, new_reach_point_vals)) ) {
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
                 title = "Update reach point data to these new values?",
                 fluidPage (
                   DT::DTOutput("reach_point_modal_update_vals"),
                   br(),
                   br(),
                   DT::DTOutput("reach_point_edit_surveys"),
                   br(),
                   br(),
                   actionButton("save_reach_point_edits", "Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_reach_point_edits, {
  tryCatch({
    reach_point_update(reach_point_edit(), selected_reach_point_data())
    shinytoastr::toastr_success("Reach end point was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  post_reach_point_edit_vals = get_reach_point(waterbody_id()) %>%
    select(river_mile, reach_point_type, reach_point_code, reach_point_name,
           latitude, longitude, horiz_accuracy, reach_point_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(reach_point_dt_proxy, post_reach_point_edit_vals)
}, priority = 9999)

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$reach_point_modal_delete_vals = renderDT({
  reach_point_modal_del_id = selected_reach_point_data()$location_id
  reach_point_modal_del_vals = get_reach_point(waterbody_id()) %>%
    filter(location_id == reach_point_modal_del_id) %>%
    select(river_mile, reach_point_type, reach_point_code, reach_point_name,
           latitude, longitude, horiz_accuracy, reach_point_description)
  # Generate table
  datatable(reach_point_modal_del_vals,
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
reach_point_dependencies = reactive({
  location_id = selected_reach_point_data()$location_id
  reach_point_dep = get_reach_point_dependencies(location_id)
  return(reach_point_dep)
})

# Generate values to show check modal
output$reach_point_delete_surveys = renderDT({
  reach_pt_del_srv = dependent_reach_point_surveys() %>%
    select(survey_dt, upper_river_mile, lower_river_mile, observer)
  reach_point_del_dt_msg = glue("All surveys below need to be reassigned to a different reach end point first!")
  # Generate table
  datatable(reach_pt_del_srv,
            rownames = FALSE,
            options = list(dom = 'ltp',
                           pageLength = 10,
                           lengthMenu = c(10, 25, 50, 100),
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: blue; width: auto;',
              htmltools::em(htmltools::strong(reach_point_del_dt_msg))))
})

observeEvent(input$reach_point_delete, {
  location_id = selected_reach_point_data()$location_id
  reach_pt_dependencies = reach_point_dependencies()
  table_names = paste0(paste0("'", names(reach_pt_dependencies), "'"), collapse = ", ")
  # Customize the delete message depending on if other entries are linked to location
  if ( ncol(reach_pt_dependencies) > 0L | nrow(dependent_reach_point_surveys()) > 0L ) {
    reach_point_delete_msg = glue("Other entries in {table_names} have been assigned to this reach point. ",
                                  "All entries below must be reassigned to another river mile before the ",
                                  "point can be deleted.")
  } else {
    reach_point_delete_msg = "Are you sure you want to delete this reach_point from the database?"
  }
  showModal(
    tags$div(id = "reach_point_delete_modal",
             if ( ncol(reach_pt_dependencies) > 0L | nrow(dependent_reach_point_surveys()) > 0L ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 reach_point_delete_msg,
                 fluidPage(
                   br(),
                   DT::DTOutput(("reach_point_delete_surveys"))
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = reach_point_delete_msg,
                 fluidPage (
                   DT::DTOutput("reach_point_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_reach_point", "Delete reach point")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$delete_reach_point, {
  tryCatch({
    reach_point_delete(selected_reach_point_data())
    shinytoastr::toastr_success("Reach end point was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  reach_points_after_delete = get_reach_point(waterbody_id()) %>%
    select(river_mile, reach_point_type, reach_point_code, reach_point_name,
           latitude, longitude, horiz_accuracy, reach_point_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(reach_point_dt_proxy, reach_points_after_delete)
}, priority = 9999)



