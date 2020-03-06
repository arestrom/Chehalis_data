#=========================================================
# HTML content definition of survey_comment accordian
#========================================================

# Define the survey data content
survey_comment_ui = tags$div(
  actionButton(inputId = "comment_add", label = "New", class = "new_button"),
  actionButton(inputId = "comment_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "comment_delete", label = "Delete", class = "delete_button"),
  tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
        tooltip = glue("<span style='font-size:11px;'>",
                       "Input fields with a '_condition' suffix are drawn from legacy ",
                       "condition codes. You can only enter one value from each select ",
                       "option. Any additional comments can be added to the 'survey_comment' ",
                       "input box. Drag the lower left corner of this box if you need ",
                       "additional space to add comments.<span>")),
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
