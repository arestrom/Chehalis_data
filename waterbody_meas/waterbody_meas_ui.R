#============================================================
# HTML content definition of waterbody_measurement accordian
#============================================================

# Define the survey data content
waterbody_meas_ui = tags$div(
  actionButton(inputId = "wbm_add", label = "New", class = "new_button"),
  actionButton(inputId = "wbm_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "wbm_delete", label = "Delete", class = "delete_button"),
  br(),
  br(),
  uiOutput("clarity_type_select", inline = TRUE),
  numericInput(inputId = "clarity_input", label = "clarity_meter", value = NA,
               min = 0, max = 50, step = 0.01, width = "100px"),
  numericInput(inputId = "flow_cfs_input", label = "flow_cfs", value = NA,
               min = 0, width = "100px"),
  numericInput(inputId = "start_temperature_input", label = "start_temperature", value = NA,
               min = 0, max = 45, step = 0.1, width = "100px"),
  timeInput(inputId = "start_temperature_time_select", "start_tmp_time", seconds = FALSE),
  numericInput(inputId = "end_temperature_input", label = "end_temperature", value = NA,
               min = 0, max = 45, step = 0.1, width = "100px"),
  timeInput(inputId = "end_temperature_time_select", "end_tmp_time", seconds = FALSE),
  numericInput(inputId = "water_ph_input", label = "water_ph", value = NA,
               min = 2, max = 12, step = 0.1, width = "100px"),
  br(),
  br(),
  br(),
  DT::DTOutput("waterbody_measure")
)
