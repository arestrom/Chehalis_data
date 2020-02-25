
#========================================================
# Generate lut select ui's
#========================================================

output$fish_channel_type_select = renderUI({
  channel_type_list = get_fish_channel_type()$channel_type
  channel_type_list = c("", channel_type_list)
  selectizeInput("fish_channel_type_select", label = "channel_type",
                 choices = channel_type_list, selected = NULL,
                 width = "250px")
})

output$fish_orientation_type_select = renderUI({
  orientation_type_list = get_fish_orientation_type()$orientation_type
  orientation_type_list = c("", orientation_type_list)
  selectizeInput("fish_orientation_type_select", label = "orientation_type",
                 choices = orientation_type_list, selected = NULL,
                 width = "275px")
})

#========================================================
# Primary datatable for fish_locations
#========================================================

# Primary DT datatable for survey_intent
output$fish_locations = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  fish_location_title = glue("{selected_survey_event_data()$species} carcass locations for {input$stream_select} ",
                             "from river mile {selected_survey_data()$up_rm} to {selected_survey_data()$lo_rm}, ",
                             "for the period {format(as.Date(selected_survey_data()$survey_date) - months(3), '%m/%d/%Y')} ",
                             "to {format(as.Date(selected_survey_data()$survey_date), '%m/%d/%Y')}")
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  fish_location_data = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, fish_name, fish_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(fish_location_data,
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
              htmltools::em(htmltools::strong(fish_location_title))))
})

# Create surveys DT proxy object
fish_location_dt_proxy = dataTableProxy(outputId = "fish_locations")

#========================================================
# Collect location values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_fish_location_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_locations_rows_selected)
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  fish_location_data = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id)
  fish_location_row = input$fish_locations_rows_selected
  selected_fish_location = tibble(fish_location_id = fish_location_data$fish_location_id[fish_location_row],
                                  location_coordinates_id = fish_location_data$location_coordinates_id[fish_location_row],
                                  fish_name = fish_location_data$fish_name[fish_location_row],
                                  channel_type = fish_location_data$channel_type[fish_location_row],
                                  orientation_type = fish_location_data$orientation_type[fish_location_row],
                                  latitude = fish_location_data$latitude[fish_location_row],
                                  longitude = fish_location_data$longitude[fish_location_row],
                                  horiz_accuracy = fish_location_data$horiz_accuracy[fish_location_row],
                                  location_description = fish_location_data$location_description[fish_location_row],
                                  created_date = fish_location_data$created_date[fish_location_row],
                                  created_by = fish_location_data$created_by[fish_location_row],
                                  modified_date = fish_location_data$modified_date[fish_location_row],
                                  modified_by = fish_location_data$modified_by[fish_location_row])
  return(selected_fish_location)
})

#========================================================
# Update inputs to values in selected row
#========================================================

# Update all input values to values in selected row
observeEvent(input$fish_locations_rows_selected, {
  sfldat = selected_fish_location_data()
  updateTextInput(session, "fish_name_input", value = sfldat$fish_name)
  updateSelectizeInput(session, "fish_channel_type_select", selected = sfldat$channel_type)
  updateSelectizeInput(session, "fish_orientation_type_select", selected = sfldat$orientation_type)
  updateNumericInput(session, "fish_latitude_input", value = sfldat$latitude)
  updateNumericInput(session, "fish_longitude_input", value = sfldat$longitude)
  updateNumericInput(session, "fish_horiz_accuracy_input", value = sfldat$horiz_accuracy)
  updateTextAreaInput(session, "fish_location_description_input", value = sfldat$location_description)
})

#================================================================
# Get either selected fish coordinates or default stream centroid
#================================================================

# Get centroid of stream for setting view of redd_map
selected_fish_coords = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  # Get centroid of stream....always available if stream is selected
  center_lat = selected_stream_centroid()$center_lat
  center_lon = selected_stream_centroid()$center_lon
  # Get location_coordinates data should be nrow == 0 if no coordinates present
  if (!is.null(input$fish_locations_rows_selected) ) {
    fish_coords = get_fish_coordinates(selected_fish_location_data()$fish_location_id)
    fish_location_id = selected_fish_location_data()$fish_location_id
  } else {
    fish_coords = NULL
    fish_location_id = remisc::get_uuid(1L)
  }
  if ( is.null(fish_coords) | length(fish_coords$latitude) == 0 | length(fish_coords$longitude) == 0 ) {
    fish_lat = center_lat
    fish_lon = center_lon
    fish_name = "none"
  } else {
    fish_lat = fish_coords$latitude
    fish_lon = fish_coords$longitude
    fish_name = selected_fish_location_data()$fish_name
  }
  fish_coords = tibble(fish_location_id = fish_location_id,
                       fish_name = fish_name,
                       fish_lat = fish_lat,
                       fish_lon = fish_lon)
  return(fish_coords)
})

# Output leaflet bidn map....could also use color to indicate species:
# See: https://rstudio.github.io/leaflet/markers.html
output$fish_map <- renderLeaflet({
  fish_loc_data = selected_fish_coords()
  fish_lat = fish_loc_data$fish_lat
  fish_lon = fish_loc_data$fish_lon
  fish_name = fish_loc_data$fish_name
  fish_location_id = fish_loc_data$fish_location_id
  m = leaflet() %>%
    setView(
      lng = selected_stream_centroid()$center_lon,
      lat = selected_stream_centroid()$center_lat,
      zoom = 14) %>%
    # Needed to enable draggable circle-markers
    addDrawToolbar(circleOptions = NA,
                   circleMarkerOptions = NA,
                   markerOptions = NA,
                   polygonOptions = NA,
                   rectangleOptions = NA,
                   polylineOptions = NA) %>%
    addPolylines(
      data = wria_streams(),
      group = "Streams",
      weight = 3,
      color = "#0000e6",
      label = ~stream_label,
      layerId = ~stream_label,
      labelOptions = labelOptions(noHide = FALSE)) %>%
    addCircleMarkers(
      lng = fish_lon,
      lat = fish_lat,
      layerId = fish_location_id,
      popup = fish_name,
      radius = 8,
      color = "red",
      fillOpacity = 0.5,
      stroke = FALSE,
      options = markerOptions(draggable = TRUE,
                              riseOnHover = TRUE)) %>%
    addProviderTiles("Esri.WorldImagery", group = "Esri World Imagery") %>%
    addProviderTiles("OpenTopoMap", group = "Open Topo Map") %>%
    addLayersControl(position = 'bottomright',
                     baseGroups = c("Esri World Imagery", "Open Topo Map"),
                     overlayGroups = c("Streams"),
                     options = layersControlOptions(collapsed = TRUE))
  m
})

# Create reactive to hold click data
fish_marker_data = reactive({
  req(input$fish_map_marker_click)
  fish_click_data = input$fish_map_marker_click
  fish_mark_dat = tibble(latitude = round(as.numeric(fish_click_data$lat), digits = 6),
                         longitude = round(as.numeric(fish_click_data$lng), digits = 6))
  return(fish_mark_dat)
})

# Get dataframe of updated locations
output$fish_coordinates = renderUI({
  if ( length(input$fish_map_marker_click) == 0L ) {
    HTML("Drag marker to edit location. Click on marker to set coordinates")
  } else {
    HTML(glue("Fish location: ", {fish_marker_data()$latitude}, ": ", {fish_marker_data()$longitude}))
  }
})

# Modal for new redd locations...add or edit a point...write coordinates to lat, lon
observeEvent(input$fish_loc_map, {
  showModal(
    # Verify required fields have data...none can be blank
    tags$div(id = "fish_location_map_modal",
             modalDialog (
               size = 'l',
               title = glue("Add or edit fish location"),
               fluidPage (
                 fluidRow(
                   column(width = 12,
                          leafletOutput("fish_map", height = 500),
                          br()
                   )
                 ),
                 fluidRow(
                   column(width = 3,
                          actionButton("capture_fish_loc", "Capture carcass location"),
                          tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
                                tooltip = glue("You can zoom in on the map and drag the marker to the ",
                                               "correct carcass location. Click on the marker to set ",
                                               "the coordinates. Then click on the button to capture ",
                                               "the location and send the coordinates to the data ",
                                               "entry screen."))),
                   column(width = 9,
                          htmlOutput("fish_coordinates"))
                 )
               ),
               easyClose = TRUE,
               footer = NULL
             )
    )
  )
})

#======================================================================
# Update fish location coordinate inputs to coordinates selected on map
#======================================================================

# Update all input values to values in selected row
observeEvent(input$capture_fish_loc, {
  fish_coord_data = fish_marker_data()
  updateNumericInput(session, "fish_latitude_input", value = fish_coord_data$latitude)
  updateNumericInput(session, "fish_longitude_input", value = fish_coord_data$longitude)
  removeModal()
})

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
fish_location_create = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  # Channel type
  fish_channel_type_input = input$fish_channel_type_select
  if ( fish_channel_type_input == "" ) {
    stream_channel_type_id = NA
  } else {
    fish_channel_type_vals = get_fish_channel_type()
    stream_channel_type_id = fish_channel_type_vals %>%
      filter(channel_type == fish_channel_type_input) %>%
      pull(stream_channel_type_id)
  }
  # Orientation type
  fish_orientation_type_input = input$fish_orientation_type_select
  if ( fish_orientation_type_input == "" ) {
    location_orientation_type_id = NA
  } else {
    fish_orientation_type_vals = get_fish_orientation_type()
    location_orientation_type_id = fish_orientation_type_vals %>%
      filter(orientation_type == fish_orientation_type_input) %>%
      pull(location_orientation_type_id)
  }
  new_fish_location = tibble(fish_name = input$fish_name_input,
                             channel_type = fish_channel_type_input,
                             stream_channel_type_id = stream_channel_type_id,
                             orientation_type = fish_orientation_type_input,
                             location_orientation_type_id = location_orientation_type_id,
                             latitude = input$fish_latitude_input,
                             longitude = input$fish_longitude_input,
                             horiz_accuracy = input$fish_horiz_accuracy_input,
                             location_description = input$fish_location_description_input,
                             created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                             created_by = Sys.getenv("USERNAME"))
  return(new_fish_location)
})

# Generate values to show in modal
output$fish_location_modal_insert_vals = renderDT({
  fish_location_modal_in_vals = fish_location_create() %>%
    select(fish_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  # Generate table
  datatable(fish_location_modal_in_vals,
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
observeEvent(input$fish_loc_add, {
  new_fish_location_vals = fish_location_create()
  # Collect parameters for existing fish locations
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  old_fish_location_vals = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    filter(!fish_name %in% c("", "no location data")) %>%
    pull(fish_name)
  showModal(
    # Verify required fields have data...none can be blank
    tags$div(id = "fish_location_insert_modal",
             if ( is.na(new_fish_location_vals$stream_channel_type_id) |
                  is.na(new_fish_location_vals$location_orientation_type_id) |
                  is.na(new_fish_location_vals$latitude) |
                  is.na(new_fish_location_vals$longitude) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("Values are required for channel, orientation, and coordinates"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify fish name is unique for species, reach, and period
             } else if ( new_fish_location_vals$fish_name %in% old_fish_location_vals ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("To enter a new fish name (carcass code, or fish ID) it must be unique, for this reach and species, within the last three months"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert new fish location data to the database?"),
                 fluidPage (
                   DT::DTOutput("fish_location_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_fish_location", "Insert location")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
fish_location_insert_vals = reactive({
  new_fish_loc_values = fish_location_create() %>%
    mutate(waterbody_id = waterbody_id()) %>%
    mutate(wria_id = wria_id()) %>%
    mutate(location_type_id = "c8c4020f-36ac-46ec-b158-6d07a3812bc8") %>%     # fish encounter
    select(waterbody_id, wria_id, location_type_id,
           stream_channel_type_id, location_orientation_type_id,
           fish_name, location_description, latitude, longitude,
           horiz_accuracy, created_by)
  return(new_fish_loc_values)
})

# Update DB and reload DT
observeEvent(input$insert_fish_location, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  tryCatch({
    fish_location_insert(fish_location_insert_vals())
    shinytoastr::toastr_success("New carcass location was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  post_fish_location_insert_vals = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, fish_name, fish_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(fish_location_dt_proxy, post_fish_location_insert_vals)
}, priority = 9999)

# Update DB and reload DT
observeEvent(input$insert_fish_encounter, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  # Update fish location table
  fish_locs_after_fish_count_insert = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, fish_name, fish_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(fish_location_dt_proxy, fish_locs_after_fish_count_insert)
}, priority = -1)

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
fish_location_edit = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$fish_locations_rows_selected)
  req(!is.na(selected_fish_location_data()$fish_location_id))
  # Channel type
  fish_channel_type_input = input$fish_channel_type_select
  if ( fish_channel_type_input == "" ) {
    stream_channel_type_id = NA
  } else {
    fish_channel_type_vals = get_fish_channel_type()
    stream_channel_type_id = fish_channel_type_vals %>%
      filter(channel_type == fish_channel_type_input) %>%
      pull(stream_channel_type_id)
  }
  # Orientation type
  fish_orientation_type_input = input$fish_orientation_type_select
  if ( fish_orientation_type_input == "" ) {
    location_orientation_type_id = NA
  } else {
    fish_orientation_type_vals = get_fish_orientation_type()
    location_orientation_type_id = fish_orientation_type_vals %>%
      filter(orientation_type == fish_orientation_type_input) %>%
      pull(location_orientation_type_id)
  }
  edit_fish_location = tibble(fish_location_id = selected_fish_location_data()$fish_location_id,
                              fish_name = input$fish_name_input,
                              channel_type = fish_channel_type_input,
                              stream_channel_type_id = stream_channel_type_id,
                              orientation_type = fish_orientation_type_input,
                              location_orientation_type_id = location_orientation_type_id,
                              latitude = input$fish_latitude_input,
                              longitude = input$fish_longitude_input,
                              horiz_accuracy = input$fish_horiz_accuracy_input,
                              location_description = input$fish_location_description_input,
                              modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                              modified_by = Sys.getenv("USERNAME"))
  return(edit_fish_location)
})

dependent_fish_location_surveys = reactive({
  fish_loc_id = selected_fish_location_data()$fish_location_id
  fish_loc_srv = get_fish_location_surveys(fish_loc_id)
  return(fish_loc_srv)
})

# Generate values to show in check modal
output$fish_loc_surveys = renderDT({
  fish_loc_srv = dependent_fish_location_surveys()
  fish_location_warning = glue("WARNING: All  carcasses, photo's, or observations linked to this ",
                               "fish location are shown below. Please verify that all data below ",
                               "should be updated to the new values!")
  # Generate table
  datatable(fish_loc_srv,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: blue; width: auto;',
              htmltools::em(htmltools::strong(fish_location_warning))))
})

# Generate values to show in modal
output$fish_location_modal_update_vals = renderDT({
  fish_location_modal_up_vals = fish_location_edit() %>%
    select(fish_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  # Generate table
  datatable(fish_location_modal_up_vals,
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
observeEvent(input$fish_loc_edit, {
  old_fish_location_vals = selected_fish_location_data() %>%
    mutate(latitude = round(latitude, 6)) %>%
    mutate(longitude = round(longitude, 6)) %>%
    select(fish_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  old_fish_location_vals[] = lapply(old_fish_location_vals, remisc::set_na)
  new_fish_location_vals = fish_location_edit() %>%
    mutate(horiz_accuracy = as.numeric(horiz_accuracy)) %>%
    mutate(latitude = round(latitude, 6)) %>%
    mutate(longitude = round(longitude, 6)) %>%
    select(fish_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  new_fish_location_vals[] = lapply(new_fish_location_vals, remisc::set_na)
  showModal(
    tags$div(id = "fish_location_update_modal",
             if ( !length(input$fish_locations_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_fish_location_vals, new_fish_location_vals)) ) {
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
                 title = "Update fish location data to these new values?",
                 fluidPage (
                   DT::DTOutput("fish_location_modal_update_vals"),
                   br(),
                   DT::DTOutput("fish_loc_surveys"),
                   br(),
                   br(),
                   actionButton("save_fish_loc_edits", "Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_fish_loc_edits, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  tryCatch({
    fish_location_update(fish_location_edit(), selected_fish_location_data())
    shinytoastr::toastr_success("Carcass location was edited")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  # Update redd location table
  post_fish_location_edit_vals = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, fish_name, fish_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(fish_location_dt_proxy, post_fish_location_edit_vals)
}, priority = 9999)

# Update DB and reload DT
observeEvent(input$save_fish_enc_edits, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  # Update redd location table
  fish_locs_after_fish_count_edit = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, fish_name, fish_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(fish_location_dt_proxy, fish_locs_after_fish_count_edit)
}, priority = -1)

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$fish_location_modal_delete_vals = renderDT({
  fish_location_modal_del_id = selected_fish_location_data()$fish_location_id
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  fish_location_modal_del_vals = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    filter(fish_location_id == fish_location_modal_del_id) %>%
    select(fish_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  # Generate table
  datatable(fish_location_modal_del_vals,
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
fish_location_dependencies = reactive({
  fish_location_id = selected_fish_location_data()$fish_location_id
  fish_loc_dep = get_fish_location_dependencies(fish_location_id)
  return(fish_loc_dep)
})

# Generate values to show in modal
output$fish_location_modal_dependency_vals = renderDT({
  req(input$tabs == "data_entry")
  fish_location_modal_dep_vals = fish_location_dependencies() %>%
    select(fish_encounter_date, fish_encounter_time, fish_status,
           fish_count, fish_name)
  # Generate table
  datatable(fish_location_modal_dep_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$fish_loc_delete, {
  req(input$tabs == "data_entry")
  fish_location_id = selected_fish_location_data()$fish_location_id
  fish_loc_dependencies = fish_location_dependencies()
  showModal(
    tags$div(id = "fish_location_delete_modal",
             if ( !length(input$fish_locations_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( nrow(fish_loc_dependencies) > 0L ) {
               modalDialog (
                 size = "l",
                 title = paste("The fish count observation(s) listed below are linked to the carcass location data you selected. ",
                               "Please edit or delete the dependent fish count data in the 'Fish counts' data entry ",
                               "screen below before deleting the selected carcass location data."),
                 fluidPage (
                   DT::DTOutput("fish_location_modal_dependency_vals"),
                   br()
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Are you sure you want to delete this carcass location data from the database?",
                 fluidPage (
                   DT::DTOutput("fish_location_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_fish_location", "Delete location data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update redd_location DB and reload location DT....This works and is the model for others
observeEvent(input$delete_fish_location, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  tryCatch({
    fish_location_delete(selected_fish_location_data())
    shinytoastr::toastr_success("Carcass location was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  fish_locations_after_delete = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, fish_name, fish_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(fish_location_dt_proxy, fish_locations_after_delete)
}, priority = 9999)

# Reload location DT after deleting encounter
observeEvent(input$delete_fish_encounter, {
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  fish_locations_after_encounter_delete = get_fish_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, fish_name, fish_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(fish_location_dt_proxy, fish_locations_after_encounter_delete)
}, priority = -1)
