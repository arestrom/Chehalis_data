#=========================================================
# HTML content definition of header page
#========================================================

# Define the mobile_import ui
mobile_import_ui = tags$div(
  fluidRow(
    column(
      width = 6,
      br(),
      br(),
      uiOutput("project_select"),
      br(),
      uiOutput("mobile_form_select"),
      br(),
      br(),
      actionButton(inputId = "check_for_new_surveys",
                   label = "Check for new surveys",
                   class = "check_for_surveys_buttqn"),
      actionButton(inputId = "import_mobile",
                   label = "Sync mobile data to SG local",
                   class = "import_button"),
      tippy("<i style='color:#1a5e86;padding-left:8px;', class='fas fa-info-circle'></i>",
            tooltip = glue("<span style='font-size:11px;'>",
                           "You will not be able to sync mobile data to the SG database ",
                           "until you have checked for new surveys and have verified that ",
                           "all streams and reaches are present. In other words, the ",
                           "'Missing streams', 'Missing reaches', and 'New reach points' ",
                           "datasets below must all be empty after clicking on the ",
                           "'Check for new surveys' button.<span>")),
      br(),
      br(),
      DT::DTOutput("new_surveys"),
      br(),
      br()
    ),
    column(
      width = 6,
      div(id = "sportswomen_image", img(src = "sportswomen.jpg", width = "80%")),
      br(),
      br(),
      br()
    )
  ),
  fluidRow(
    column(
      width = 12,
      br(),
      DT::DTOutput("missing_streams"),
      br(),
      br(),
      DT::DTOutput("missing_reaches"),
      br(),
      br(),
      DT::DTOutput("add_endpoints"),
      br(),
      br()
    )
  )
)
