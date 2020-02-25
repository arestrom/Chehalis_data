#========================================================
# Get Initial WRIA
#========================================================

# wria_select
output$wria_select = renderUI({
  wria_list = get_wrias()
  selectizeInput("wria_select", label = NULL,
                 choices = wria_list,
                 selected = "23 Upper Chehalis",
                 width = "100%")
})

#========================================================
# Get initial set of streams for selected wria
#========================================================

# Get streams in wria
wria_streams = reactive({
  req(input$wria_select)
  streams = get_streams(chosen_wria = input$wria_select) %>%
    mutate(stream_label = if_else(is.na(stream_name) & !is.na(waterbody_name),
                                  waterbody_name, stream_name)) %>%
    mutate(stream_label = paste0(stream_name, ": ", llid)) %>%
    mutate(waterbody_id = tolower(waterbody_id)) %>%
    st_transform(4326) %>%
    select(waterbody_id, stream_id, stream_label, geometry)
  return(streams)
})

# Pull out stream_list from wria_streams
stream_list = reactive({
  stream_data = wria_streams() %>%
    st_drop_geometry() %>%
    select(stream_label) %>%
    arrange(stream_label) %>%
    distinct()
  return(stream_data$stream_label)
})

# stream_select
output$stream_select = renderUI({
  selectizeInput("stream_select", label = NULL,
                 choices = stream_list(), selected = stream_list()[1],
                 width = "100%")
})

#========================================================
# Get initial set of years for selected stream
#========================================================

# Filter to selected stream
waterbody_id = eventReactive(input$stream_select, {
  req(input$stream_select)
  stream_data = wria_streams() %>%
    st_drop_geometry() %>%
    filter(stream_label == input$stream_select) %>%
    select(waterbody_id) %>%
    distinct() %>%
    pull(waterbody_id)
  return(stream_data)
})

waterbody_survey_years = eventReactive(input$stream_select, {
  year_list = get_data_years(waterbody_id())
  if (length(year_list) == 0 ) {
    year_list = "No surveys"
  } else {
    year_list = year_list
  }
  return(year_list)
})

# year_select
output$year_select = renderUI({
  year_list = waterbody_survey_years()
  selectizeInput("year_select", label = NULL,
                 choices = year_list, selected = NULL,
                 width = "100%")
})

#========================================================
# Get data for initial map
#========================================================

selected_wria = reactive({
  req(input$wria_select)
  zoom_wria = wria_polys %>%
    filter(wria_name == input$wria_select) %>%
    st_transform(2927) %>%
    mutate(zoom_pt = st_transform(st_centroid(geometry), 4326)) %>%
    mutate(lat = st_coordinates(zoom_pt)[[2]]) %>%
    mutate(lon = st_coordinates(zoom_pt)[[1]]) %>%
    st_drop_geometry() %>%
    select(wria_name, lat, lon)
  return(zoom_wria)
})

# Output leaflet bidn map
output$stream_map <- renderLeaflet({
  req(input$wria_select)
  m = leaflet() %>%
    setView(lng = selected_wria()$lon[1],
            lat = selected_wria()$lat[1],
            zoom = 10) %>%
    addProviderTiles("Esri.WorldImagery", group = "Esri World Imagery") %>%
    addProviderTiles("OpenTopoMap", group = "Open Topo Map") %>%
    addLayersControl(position = 'bottomleft',
                     baseGroups = c("Esri World Imagery", "Open Topo Map"),
                     options = layersControlOptions(collapsed = TRUE))
  m
})

# Update leaflet proxy map
observe({
  stream_map_proxy = leafletProxy("stream_map")
  stream_map_proxy %>%
    clearShapes() %>%
    addPolylines(data = wria_streams(),
                 group = "Streams",
                 weight = 3,
                 color = "#0000e6",
                 label = ~stream_label,
                 layerId = ~stream_id,
                 labelOptions = labelOptions(noHide = FALSE)) %>%
    addLayersControl(position = 'bottomleft',
                     baseGroups = c("Esri World Imagery", "Open Topo Map"),
                     overlayGroups = c("Streams"),
                     options = layersControlOptions(collapsed = TRUE))
})

#========================================================
# Update stream select if map is clicked
#========================================================

# Reactive to record stream_id of stream_clicked
selected_map_stream_id = reactive({
  req(input$stream_map_shape_click$id)
  selected_map_stream_id = input$stream_map_shape_click$id
  return(selected_map_stream_id)
})

# Reactive to record stream_label of stream_clicked
selected_map_stream = reactive({
  req(input$stream_map_shape_click)
  stream_names = wria_streams() %>%
    st_drop_geometry() %>%
    select(stream_id, stream_label) %>%
    distinct()
  selected_map_stream_id = selected_map_stream_id()
  selected_map_stream_label = stream_names %>%
    filter(stream_id == selected_map_stream_id) %>%
    pull(stream_label)
  return(selected_map_stream_label)
})

# Update Stream input if stream on map is clicked
observeEvent(input$stream_map_shape_click, {
  clicked_stream = selected_map_stream()
  # Update
  updateSelectizeInput(session, "stream_select",
                       choices = stream_list(),
                       selected = clicked_stream)
})

# Get list of river mile end_points for waterbody_id
rm_list = reactive({
  wb_id = waterbody_id()
  stream_rms = get_end_points(waterbody_id())
  return(stream_rms)
})

# Generate year values as a reactive
year_vals = reactive({
  req(input$year_select)
  req(!input$year_select == "No surveys")
  year_vals = input$year_select
  return(year_vals)
})

#========================================================
# Get centroid of selected stream for fish_map & redd_map
#========================================================

# Get centroid of stream for setting view of fish_map
selected_stream_centroid = reactive({
  req(input$stream_select)
  stream_centroid_coords = get_stream_centroid(waterbody_id())
  return(stream_centroid_coords)
})

#========================================================
# Get wria for location insert, redds and fish
#========================================================

# Reactive to pull out wria_id
wria_id = reactive({
  req(input$wria_select)
  get_streams(chosen_wria = input$wria_select) %>%
    st_drop_geometry() %>%
    mutate(wria_id = tolower(wria_id)) %>%
    select(wria_id) %>%
    distinct() %>%
    pull(wria_id)
})

