#=============================================================
# HTML content definition of species (survey_event) accordion
#=============================================================

# Define the survey data content
survey_event_ui = tags$div(
  actionButton(inputId = "survey_event_add", label = "New", class = "new_button"),
  actionButton(inputId = "survey_event_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "survey_event_delete", label = "Delete", class = "delete_button"),
  br(),
  br(),
  uiOutput("event_species_select", inline = TRUE),
  uiOutput("survey_design_select", inline = TRUE),
  uiOutput("cwt_detect_method_select", inline = TRUE),
  uiOutput("run_select", inline = TRUE),
  uiOutput("run_year_select", inline = TRUE),
  # selectizeInput(inputId = "run_year_select", label = "Run year",
  #                choices = seq(as.integer(format(Sys.Date(), "%Y")) + 1, 1930L),
  #                #selected = as.integer(format(Sys.Date(), "%Y")),
  #                selected = 2017, width = "100px"),
  numericInput(inputId = "pct_fish_seen_input", label = "pct_fish_seen", value = NA,
               min = 0, max = 100, step = 1, width = "100px"),
  textAreaInput(inputId = "se_comment_input", label = "species_comment", value = "",
                width = "300px", resize = "both"),
  br(),
  br(),
  br(),
  DT::DTOutput("survey_events")
)
