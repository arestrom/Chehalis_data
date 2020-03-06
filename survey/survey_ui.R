#=========================================================
# HTML content definition of header accordian
#========================================================

# Define the survey data content
survey_ui = tags$div(
  actionButton(inputId = "survey_add", label = "New", class = "new_button"),
  actionButton(inputId = "survey_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "survey_delete", label = "Delete", class = "delete_button"),
  tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
        tooltip = glue("<span style='font-size:11px;'>",
                       "Shaded input boxes in all data entry screens below indicate ",
                       "required fields<span>")),
  br(),
  br(),
  dateInput(inputId = "survey_date_input", label = "survey_dt", format = "D M dd yyyy", value = Sys.Date()),
  uiOutput("survey_method_select", inline = TRUE),
  uiOutput("upper_rm_select", inline = TRUE),
  uiOutput("lower_rm_select", inline = TRUE),
  # selectizeInput(inputId = "upper_rm_select", label = "up_rm", choices = NULL, width = "150px"),
  # selectizeInput(inputId = "lower_rm_select", label = "lo_rm", choices = NULL, width = "150px"),
  timeInput(inputId = "start_time_select", "start_time", seconds = FALSE),
  timeInput(inputId = "end_time_select", "end_time", seconds = FALSE),
  textInput(inputId = "observer_input", label = "observer", value = NA, width = "100px"),
  textInput(inputId = "submitter_input", label = "submitter", value = NA, width = "100px"),
  uiOutput("data_source_select", inline = TRUE),
  uiOutput("data_source_unit_select", inline = TRUE),
  uiOutput("data_review_select", inline = TRUE),
  uiOutput("completion_select", inline = TRUE),
  uiOutput("incomplete_type_select", inline = TRUE),
  br(),
  br(),
  br(),
  DT::DTOutput("surveys")
)

