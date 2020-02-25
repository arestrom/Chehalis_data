#=============================================================
# HTML content definition of fish_measurement accordion
#=============================================================

# Define the survey data content
fish_length_measurement_ui = tags$div(
  actionButton(inputId = "fish_meas_add", label = "New", class = "new_button"),
  actionButton(inputId = "fish_meas_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "fish_meas_delete", label = "Delete", class = "delete_button"),
  br(),
  br(),
  uiOutput("length_type_select", inline = TRUE),
  numericInput(inputId = "length_cm_input", label = "length_cm", value = NULL,
               min = 0, step = 1, width = "115px"),
  br(),
  br(),
  br(),
  DT::DTOutput("length_measurements"),
  verbatimTextOutput("chk_edit")
)
