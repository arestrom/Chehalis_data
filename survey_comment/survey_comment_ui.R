#=========================================================
# HTML content definition of survey_comment accordian
#========================================================

# Define the survey data content
survey_comment_ui = tags$div(
  actionButton(inputId = "comment_add", label = "New", class = "new_button"),
  actionButton(inputId = "comment_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "comment_delete", label = "Delete", class = "delete_button"),
  br(),
  br(),
  uiOutput("area_surveyed_select", inline = TRUE),
  uiOutput("abundance_condition_select", inline = TRUE),
  uiOutput("stream_condition_select", inline = TRUE),
  uiOutput("stream_flow_select", inline = TRUE),
  uiOutput("count_condition_select", inline = TRUE),
  uiOutput("survey_direction_select", inline = TRUE),
  uiOutput("survey_timing_select", inline = TRUE),
  uiOutput("visibility_condition_select", inline = TRUE),
  uiOutput("visibility_type_select", inline = TRUE),
  uiOutput("weather_type_select", inline = TRUE),
  textAreaInput(inputId = "sc_comment_input", label = "survey_comment", value = "",
                width = "300px", resize = "both"),
  br(),
  br(),
  br(),
  DT::DTOutput("survey_comments")
)
