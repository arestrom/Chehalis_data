#=============================================================
# HTML content definition of species (survey_event) accordion
#=============================================================

# Define the survey data content
survey_event_ui = tags$div(
  actionButton(inputId = "survey_event_add", label = "New", class = "new_button"),
  actionButton(inputId = "survey_event_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "survey_event_delete", label = "Delete", class = "delete_button"),
  tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
        tooltip = glue("<span style='font-size:11px;'>",
                       "Please enter, at minimum, a survey design type, ",
                       "run type, and run_year for each species you intended ",
                       "to survey, even if no fish were encountered. You ",
                       "can enter 'Not applicable' for run type and cwt ",
                       "detection method if no fish were found. If no fish ",
                       "were encountered, please select the run year that ",
                       "you were primarily surveying for.<span>")),
  br(),
  br(),
  uiOutput("event_species_select", inline = TRUE),
  uiOutput("survey_design_select", inline = TRUE),
  uiOutput("cwt_detect_method_select", inline = TRUE),
  uiOutput("run_select", inline = TRUE),
  uiOutput("run_year_select", inline = TRUE),
  numericInput(inputId = "pct_fish_seen_input", label = "pct_fish_seen", value = NA,
               min = 0, max = 100, step = 1, width = "100px"),
  textAreaInput(inputId = "se_comment_input", label = "species_comment", value = "",
                width = "300px", resize = "both"),
  br(),
  br(),
  br(),
  DT::DTOutput("survey_events")
)
