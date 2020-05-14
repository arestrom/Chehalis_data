

#========================================================
# Generate lut select ui's
#========================================================

output$channel_type_select = renderUI({
  channel_type_list = get_channel_type()$channel_type
  channel_type_list = c("", channel_type_list)
  selectizeInput("channel_type_select", label = "channel_type",
                 choices = channel_type_list, selected = NULL,
                 width = "250px")
})

output$orientation_type_select = renderUI({
  orientation_type_list = get_orientation_type()$orientation_type
  orientation_type_list = c("", orientation_type_list)
  selectizeInput("orientation_type_select", label = "orientation_type",
                 choices = orientation_type_list, selected = NULL,
                 width = "275px")
})

#========================================================
# Primary datatable for redd_locations
#========================================================

# Primary DT datatable for redd locations...pulling in redds by species and stream for 4 months past
output$redd_locations = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  redd_location_title = glue("{selected_survey_event_data()$species} redd locations for {input$stream_select} ",
                             "from river mile {selected_survey_data()$up_rm} to {selected_survey_data()$lo_rm}, ",
                             "for the period {format(as.Date(selected_survey_data()$survey_date) - months(4), '%m/%d/%Y')} ",
                             "to {format(as.Date(selected_survey_data()$survey_date), '%m/%d/%Y')}")
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  redd_location_data = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, redd_name, redd_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)

  # Generate table
  datatable(redd_location_data,
            selection = list(mode = 'single'),
            options = list(dom = 'lftp',
                           pageLength = 5,
                           lengthMenu = c(1, 5, 10, 20, 50),
                           scrollX = T,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")),
            caption = htmltools::tags$caption(
              style = 'caption-side: top; text-align: left; color: black; width: auto;',
              htmltools::em(htmltools::strong(redd_location_title))))
})

# Create surveys DT proxy object
redd_location_dt_proxy = dataTableProxy(outputId = "redd_locations")

#========================================================
# Collect location values from selected row for later use
#========================================================

# Create reactive to collect input values for update and delete actions
selected_redd_location_data = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_locations_rows_selected)
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  redd_location_data = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id)
  redd_location_row = input$redd_locations_rows_selected
  selected_redd_location = tibble(redd_location_id = redd_location_data$redd_location_id[redd_location_row],
                                  location_coordinates_id = redd_location_data$location_coordinates_id[redd_location_row],
                                  redd_name = redd_location_data$redd_name[redd_location_row],
                                  redd_status = redd_location_data$redd_status[redd_location_row],
                                  channel_type = redd_location_data$channel_type[redd_location_row],
                                  orientation_type = redd_location_data$orientation_type[redd_location_row],
                                  latitude = redd_location_data$latitude[redd_location_row],
                                  longitude = redd_location_data$longitude[redd_location_row],
                                  horiz_accuracy = redd_location_data$horiz_accuracy[redd_location_row],
                                  location_description = redd_location_data$location_description[redd_location_row],
                                  created_date = redd_location_data$created_date[redd_location_row],
                                  created_by = redd_location_data$created_by[redd_location_row],
                                  modified_date = redd_location_data$modified_date[redd_location_row],
                                  modified_by = redd_location_data$modified_by[redd_location_row])
  return(selected_redd_location)
})

#========================================================
# Update inputs to values in selected row
#========================================================

# Update all input values to values in selected row
observeEvent(input$redd_locations_rows_selected, {
  srldat = selected_redd_location_data()
  updateTextInput(session, "redd_name_input", value = srldat$redd_name)
  updateSelectizeInput(session, "channel_type_select", selected = srldat$channel_type)
  updateSelectizeInput(session, "orientation_type_select", selected = srldat$orientation_type)
  updateNumericInput(session, "redd_latitude_input", value = srldat$latitude)
  updateNumericInput(session, "redd_longitude_input", value = srldat$longitude)
  updateNumericInput(session, "redd_horiz_accuracy_input", value = srldat$horiz_accuracy)
  updateTextAreaInput(session, "location_description_input", value = srldat$location_description)
})

#================================================================
# Get either selected redd coordinates or default stream centroid
#================================================================

# Output leaflet bidn map....could also use color to indicate species:
# See: https://rstudio.github.io/leaflet/markers.html
output$redd_map <- renderLeaflet({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  # Force map to rerender every time redd_loc_map button is clicked
  input$redd_loc_map
  # Get data for possibly multiple carcass locations and fitting to bounds
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  redd_coords = get_redd_locations(waterbody_id(), up_rm, lo_rm,
                                   survey_date, species_id) %>%
    filter(!is.na(latitude) & !is.na(longitude)) %>%
    mutate(min_lat = min(latitude),
           min_lon = min(longitude),
           max_lat = max(latitude),
           max_lon = max(longitude)) %>%
    select(redd_location_id, redd_name, latitude, longitude,
           min_lat, min_lon, max_lat, max_lon)
  # Get data for setting map bounds ========================
  if ( nrow(redd_coords) == 0L |
       is.na(input$redd_latitude_input) |
       is.na(input$redd_longitude_input) ) {
    bounds = tibble(lng1 = selected_stream_bounds()$min_lon,
                    lat1 = selected_stream_bounds()$min_lat,
                    lng2 = selected_stream_bounds()$max_lon,
                    lat2 = selected_stream_bounds()$max_lat)
  } else {
    # Add buffer, otherwise, if only a single point is available in redd_coords,
    # map zoom may be so high that the map does not render
    bounds = tibble(lng1 = (redd_coords$min_lon[1] - 0.0015),
                    lat1 = (redd_coords$min_lat[1] - 0.0015),
                    lng2 = (redd_coords$max_lon[1] + 0.0015),
                    lat2 = (redd_coords$max_lat[1] + 0.0015))
  }
  # Generate basemap ======================================
  edit_redd_loc = leaflet() %>%
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
    leaflet.extras::addDrawToolbar(
      targetGroup = "redd_edits",
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
    addPolylines(
      data = wria_streams(),
      group = "Streams",
      weight = 3,
      color = "#0000e6",
      label = ~stream_label,
      layerId = ~stream_label,
      labelOptions = labelOptions(noHide = FALSE))
  # Test if carcass_coords has data ==========================
  if ( !nrow(redd_coords) > 0L ) {
    return(edit_redd_loc)
  } else {
    edit_redd_loc_two = edit_redd_loc %>%
      # Add existing data if locations exist ===================
    addCircleMarkers(
      lng = redd_coords$longitude,
      lat = redd_coords$latitude,
      popup = redd_coords$redd_name,
      layerId = redd_coords$redd_location_id,
      radius = 8,
      color = "red",
      fillOpacity = 0.5,
      stroke = FALSE,
      options = markerOptions(draggable = FALSE,
                              riseOnHover = TRUE))
    return(edit_redd_loc_two)
  }
})

# Create a reactive values object for drawn_features
redd_edit_rv = reactiveValues(lat = NULL, lon = NULL)

# Set rv to NULL on initiation of map
observeEvent(input$redd_loc_map, {
  redd_edit_rv$lat = NULL
  redd_edit_rv$lon = NULL
}, priority = 9999)

# Assign coordinates from mapedit circle marker
observeEvent(c(input$redd_map_draw_all_features), {
  redd_edit_rv$lat = as.numeric(input$redd_map_draw_all_features$features[[1]]$geometry$coordinates[[2]])
  redd_edit_rv$lon = as.numeric(input$redd_map_draw_all_features$features[[1]]$geometry$coordinates[[1]])
})

# Get html output of updated locations
output$redd_coordinates = renderUI({
  coords_out = HTML(glue("Redd location: ", {redd_edit_rv$lat}, ": ", {redd_edit_rv$lon}))
  return(coords_out)
})

# Modal for new redd locations...add or edit a point...write coordinates to lat, lon
observeEvent(input$redd_loc_map, {
  showModal(
    # Verify required fields have data...none can be blank
    tags$div(id = "redd_location_map_modal",
             modalDialog (
               size = 'l',
               title = glue("Add or edit redd location"),
               fluidPage (
                 fluidRow(
                   column(width = 12,
                          leafletOutput("redd_map", height = 500),
                          br()
                   )
                 ),
                 fluidRow(
                   column(width = 3,
                          actionButton("capture_redd_loc", "Save coordinates"),
                          tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
                                tooltip = glue("<span style='font-size:11px;'>",
                                               "You can zoom in on the map and use the circle tool at ",
                                               "the upper left to place a marker where you saw the redd. ",
                                               "You can also use the edit tool to move the marker you ",
                                               "just added if you need to fine-tune the position. When ",
                                               "satisfied, click on the 'Save coordinates' button. <span>"))),
                   column(width = 9,
                          htmlOutput("redd_coordinates"))
                 )
               ),
               easyClose = TRUE,
               footer = NULL
             )
    )
  )
}, priority = -1)

#======================================================================
# Update redd location coordinate inputs to coordinates selected on map
#======================================================================

# Update all input values to values in selected row
observeEvent(input$capture_redd_loc, {
  updateNumericInput(session, "redd_latitude_input", value = redd_edit_rv$lat)
  updateNumericInput(session, "redd_longitude_input", value = redd_edit_rv$lon)
  removeModal()
}, priority = 9999)

# Set rv to NULL
observeEvent(input$capture_redd_loc, {
  redd_edit_rv$lat = NULL
  redd_edit_rv$lon = NULL
}, priority = -1)

#========================================================
# Insert operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
redd_location_create = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  # Channel type
  channel_type_input = input$channel_type_select
  if ( channel_type_input == "" ) {
    stream_channel_type_id = NA
  } else {
    channel_type_vals = get_channel_type()
    stream_channel_type_id = channel_type_vals %>%
      filter(channel_type == channel_type_input) %>%
      pull(stream_channel_type_id)
  }
  # Orientation type
  orientation_type_input = input$orientation_type_select
  if ( orientation_type_input == "" ) {
    location_orientation_type_id = NA
  } else {
    orientation_type_vals = get_orientation_type()
    location_orientation_type_id = orientation_type_vals %>%
      filter(orientation_type == orientation_type_input) %>%
      pull(location_orientation_type_id)
  }
  new_redd_location = tibble(redd_name = input$redd_name_input,
                             channel_type = channel_type_input,
                             stream_channel_type_id = stream_channel_type_id,
                             orientation_type = orientation_type_input,
                             location_orientation_type_id = location_orientation_type_id,
                             latitude = input$redd_latitude_input,
                             longitude = input$redd_longitude_input,
                             horiz_accuracy = input$redd_horiz_accuracy_input,
                             location_description = input$location_description_input,
                             created_dt = lubridate::with_tz(Sys.time(), "UTC"),
                             created_by = Sys.getenv("USERNAME"))
  return(new_redd_location)
})

# Generate values to show in modal
output$redd_location_modal_insert_vals = renderDT({
  redd_location_modal_in_vals = redd_location_create() %>%
    select(redd_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  # Generate table
  datatable(redd_location_modal_in_vals,
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
observeEvent(input$redd_loc_add, {
  new_redd_location_vals = redd_location_create()
  # Collect parameters for existing redd locations
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  old_redd_location_vals = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    filter(!redd_name %in% c("", "no location data")) %>%
    pull(redd_name)
  showModal(
    # Verify required fields have data...none can be blank
    tags$div(id = "redd_location_insert_modal",
             if ( is.na(new_redd_location_vals$redd_name) |
                  new_redd_location_vals$redd_name == "" |
                  is.na(new_redd_location_vals$stream_channel_type_id) |
                  is.na(new_redd_location_vals$location_orientation_type_id) ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("At minimum, values are required for redd name (flag code or redd ID), channel type, and orientation type"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Verify redd name is unique for species, reach, and period
             } else if ( new_redd_location_vals$redd_name %in% old_redd_location_vals ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste0("To enter a new redd name (flag code, or redd ID) it must be unique, for this reach and species, within the last four months"),
                 easyClose = TRUE,
                 footer = NULL
               )
               # Write to DB
             } else {
               modalDialog (
                 size = 'l',
                 title = glue("Insert new redd location data to the database?"),
                 fluidPage (
                   DT::DTOutput("redd_location_modal_insert_vals"),
                   br(),
                   br(),
                   actionButton("insert_redd_location", "Insert location")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Reactive to hold values actually inserted
redd_location_insert_vals = reactive({
  new_redd_loc_values = redd_location_create() %>%
    mutate(waterbody_id = waterbody_id()) %>%
    mutate(wria_id = wria_id()) %>%
    mutate(location_type_id = "d5edb1c0-f645-4e82-92af-26f5637b2de0") %>%     # Redd encounter
    select(waterbody_id, wria_id, location_type_id,
           stream_channel_type_id, location_orientation_type_id,
           redd_name, location_description, latitude, longitude,
           horiz_accuracy, created_by)
  return(new_redd_loc_values)
})

# Update DB and reload DT
observeEvent(input$insert_redd_location, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  tryCatch({
    redd_location_insert(redd_location_insert_vals())
    shinytoastr::toastr_success("New redd location was added")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  post_redd_location_insert_vals = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, redd_name, redd_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_location_dt_proxy, post_redd_location_insert_vals)
}, priority = 9999)

# Update DB and reload DT
observeEvent(input$insert_redd_encounter, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  # Update redd location table
  redd_locs_after_redd_count_insert = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, redd_name, redd_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_location_dt_proxy, redd_locs_after_redd_count_insert)
}, priority = -1)

#========================================================
# Edit operations: reactives, observers and modals
#========================================================

# Create reactive to collect input values for insert actions
redd_location_edit = reactive({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_locations_rows_selected)
  req(!is.na(selected_redd_location_data()$redd_location_id))
  # Channel type
  channel_type_input = input$channel_type_select
  if ( channel_type_input == "" ) {
    stream_channel_type_id = NA
  } else {
    channel_type_vals = get_channel_type()
    stream_channel_type_id = channel_type_vals %>%
      filter(channel_type == channel_type_input) %>%
      pull(stream_channel_type_id)
  }
  # Orientation type
  orientation_type_input = input$orientation_type_select
  if ( orientation_type_input == "" ) {
    location_orientation_type_id = NA
  } else {
    orientation_type_vals = get_orientation_type()
    location_orientation_type_id = orientation_type_vals %>%
      filter(orientation_type == orientation_type_input) %>%
      pull(location_orientation_type_id)
  }
  edit_redd_location = tibble(redd_location_id = selected_redd_location_data()$redd_location_id,
                              redd_name = input$redd_name_input,
                              channel_type = channel_type_input,
                              stream_channel_type_id = stream_channel_type_id,
                              orientation_type = orientation_type_input,
                              location_orientation_type_id = location_orientation_type_id,
                              latitude = input$redd_latitude_input,
                              longitude = input$redd_longitude_input,
                              horiz_accuracy = input$redd_horiz_accuracy_input,
                              location_description = input$location_description_input,
                              modified_dt = lubridate::with_tz(Sys.time(), "UTC"),
                              modified_by = Sys.getenv("USERNAME"))
  return(edit_redd_location)
})

dependent_redd_location_surveys = reactive({
  redd_loc_id = selected_redd_location_data()$redd_location_id
  redd_loc_srv = get_redd_location_surveys(redd_loc_id)
  return(redd_loc_srv)
})

# Generate values to show check modal
output$redd_loc_surveys = renderDT({
  redd_loc_srv = dependent_redd_location_surveys()
  redd_location_warning = glue("WARNING: All previous redds, photo's, or observations linked to this ",
                               "redd location are shown below. Please verify that all data below ",
                               "should be updated to the new values!")
  # Generate table
  datatable(redd_loc_srv,
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
              htmltools::em(htmltools::strong(redd_location_warning))))
})

# Generate values to show in modal
output$redd_location_modal_update_vals = renderDT({
  redd_location_modal_up_vals = redd_location_edit() %>%
    select(redd_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  # Generate table
  datatable(redd_location_modal_up_vals,
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
observeEvent(input$redd_loc_edit, {
  old_redd_location_vals = selected_redd_location_data() %>%
    mutate(latitude = round(latitude, 6)) %>%
    mutate(longitude = round(longitude, 6)) %>%
    select(redd_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  old_redd_location_vals[] = lapply(old_redd_location_vals, remisc::set_na)
  new_redd_location_vals = redd_location_edit() %>%
    mutate(horiz_accuracy = as.numeric(horiz_accuracy)) %>%
    mutate(latitude = round(latitude, 6)) %>%
    mutate(longitude = round(longitude, 6)) %>%
    select(redd_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  new_redd_location_vals[] = lapply(new_redd_location_vals, remisc::set_na)
  showModal(
    tags$div(id = "redd_location_update_modal",
             if ( !length(input$redd_locations_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( isTRUE(all_equal(old_redd_location_vals, new_redd_location_vals)) ) {
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
                 title = "Update redd location data to these new values?",
                 fluidPage (
                   DT::DTOutput("redd_location_modal_update_vals"),
                   br(),
                   DT::DTOutput("redd_loc_surveys"),
                   br(),
                   br(),
                   actionButton("save_redd_loc_edits", "Save changes")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update DB and reload DT
observeEvent(input$save_redd_loc_edits, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  tryCatch({
    redd_location_update(redd_location_edit(), selected_redd_location_data())
    shinytoastr::toastr_success("Redd location was edited")
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
  post_redd_location_edit_vals = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, redd_name, redd_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_location_dt_proxy, post_redd_location_edit_vals)
}, priority = 9999)

# Update DB and reload DT
observeEvent(input$save_redd_enc_edits, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  # Update redd location table
  redd_locs_after_redd_count_edit = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, redd_name, redd_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_location_dt_proxy, redd_locs_after_redd_count_edit)
}, priority = -1)

#========================================================
# Delete operations: reactives, observers and modals
#========================================================

# Generate values to show in modal
output$redd_location_modal_delete_vals = renderDT({
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_locations_rows_selected)
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  redd_location_modal_del_id = selected_redd_location_data()$redd_location_id
  redd_location_modal_del_vals = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    filter(redd_location_id == redd_location_modal_del_id) %>%
    select(redd_name, channel_type, orientation_type, latitude,
           longitude, horiz_accuracy, location_description)
  # Generate table
  datatable(redd_location_modal_del_vals,
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
redd_location_dependencies = reactive({
  redd_location_id = selected_redd_location_data()$redd_location_id
  redd_loc_dep = get_redd_location_dependencies(redd_location_id)
  return(redd_loc_dep)
})

# Generate values to show in modal
output$redd_location_modal_dependency_vals = renderDT({
  req(input$tabs == "data_entry")
  redd_location_modal_dep_vals = redd_location_dependencies() %>%
    select(redd_encounter_date, redd_encounter_time, redd_status, redd_count,
           redd_name, redd_comment)
  # Generate table
  datatable(redd_location_modal_dep_vals,
            rownames = FALSE,
            options = list(dom = 't',
                           scrollX = T,
                           ordering = FALSE,
                           initComplete = JS(
                             "function(settings, json) {",
                             "$(this.api().table().header()).css({'background-color': '#9eb3d6'});",
                             "}")))
})

observeEvent(input$redd_loc_delete, {
  req(input$tabs == "data_entry")
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  req(input$redd_locations_rows_selected)
  redd_location_id = selected_redd_location_data()$redd_location_id
  redd_loc_dependencies = redd_location_dependencies()
  showModal(
    tags$div(id = "redd_location_delete_modal",
             if ( !length(input$redd_locations_rows_selected) == 1 ) {
               modalDialog (
                 size = "m",
                 title = "Warning",
                 paste("Please select a row to edit!"),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else if ( nrow(redd_loc_dependencies) > 0L ) {
               modalDialog (
                 size = "l",
                 title = paste("The redd count observation(s) listed below are linked to the redd location data you selected. ",
                               "Please edit or delete the dependent redd count data in the 'Redd counts' data entry ",
                               "screen below before deleting the selected redd location data."),
                 fluidPage (
                   DT::DTOutput("redd_location_modal_dependency_vals"),
                   br()
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             } else {
               modalDialog (
                 size = 'l',
                 title = "Are you sure you want to delete this redd location data from the database?",
                 fluidPage (
                   DT::DTOutput("redd_location_modal_delete_vals"),
                   br(),
                   br(),
                   actionButton("delete_redd_location", "Delete location data")
                 ),
                 easyClose = TRUE,
                 footer = NULL
               )
             }
    ))
})

# Update redd_location DB and reload location DT
observeEvent(input$delete_redd_location, {
  req(input$surveys_rows_selected)
  req(input$survey_events_rows_selected)
  tryCatch({
    redd_location_delete(selected_redd_location_data())
    shinytoastr::toastr_success("Redd location was deleted")
  }, error = function(e) {
    shinytoastr::toastr_error(title = "Database error", conditionMessage(e))
  })
  removeModal()
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  redd_locations_after_delete = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, redd_name, redd_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_location_dt_proxy, redd_locations_after_delete)
}, priority = 9999)

# Reload location DT after deleting encounter
observeEvent(input$delete_redd_encounter, {
  # Collect parameters
  up_rm = selected_survey_data()$up_rm
  lo_rm = selected_survey_data()$lo_rm
  survey_date = format(as.Date(selected_survey_data()$survey_date))
  species_id = selected_survey_event_data()$species_id
  redd_locations_after_encounter_delete = get_redd_locations(waterbody_id(), up_rm, lo_rm, survey_date, species_id) %>%
    select(survey_dt, species, redd_name, redd_status, channel_type, orientation_type,
           latitude, longitude, horiz_accuracy, location_description,
           created_dt, created_by, modified_dt, modified_by)
  replaceData(redd_location_dt_proxy, redd_locations_after_encounter_delete)
}, priority = -1)
