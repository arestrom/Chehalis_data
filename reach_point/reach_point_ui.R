#=============================================================
# HTML content definition of redd_encounter accordion
#=============================================================

# Define the survey data content
reach_point_ui = tags$div(
  actionButton(inputId = "reach_point_add", label = "New", class = "new_button"),
  actionButton(inputId = "reach_point_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "reach_point_delete", label = "Delete", class = "delete_button"),
  actionButton(inputId = "use_reach_point_map", label = "Use map", class = "map_button"),
  tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
        tooltip = glue("<span style='font-size:11px;'>",
                       "Click on the 'Use map' button to interactively select the latitude, ",
                       "and longitude coordinates using a map interface. Select any row ",
                       "of data below to start.<span>")),
  br(),
  br(),
  numericInput(inputId = "river_mile_input", label = "rive_mile", value = NULL,
               min = 0, step = 0.1, width = "125px"),
  uiOutput("reach_point_type_select", inline = TRUE),
  textInput(inputId = "reach_point_code_input", label = "reach_point_code", width = "125px"),
  textInput(inputId = "reach_point_name_input", label = "reach_point_name", width = "175px"),
  numericInput(inputId = "reach_point_latitude_input", label = "latitude", value = NULL,
               min = 45, max = 49, step = 0.001, width = "125px"),
  numericInput(inputId = "reach_point_longitude_input", label = "longitude", value = NULL,
               min = -124, max = -116, step = 0.001, width = "125px"),
  numericInput(inputId = "reach_point_horiz_accuracy_input", label = "horiz_accuracy", value = NULL,
               min = 0, width = "100px"),
  textAreaInput(inputId = "reach_point_description_input", label = "reach_point_description", value = "",
                width = "300px", resize = "both"),
  br(),
  br(),
  br(),
  DT::DTOutput("reach_points")
)
