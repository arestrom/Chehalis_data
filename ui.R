ui = dashboardPagePlus(
  shinyjs::useShinyjs(),
  shinytoastr::useToastr(),
  enable_preloader = TRUE,
  header = dash_header,
  sidebar = dash_leftsidebar,
  body = dashboardBody(
    includeCSS("www/salmon_data.css"),
    tabItems(
      tabItem(tabName = "wria_stream",
              fluidRow(
                br(),
                br(),
                boxPlus(
                  title = "Select Stream and survey years",
                  closable = FALSE,
                  solidHeader = FALSE,
                  collapsible = TRUE,
                  collapsed = FALSE,
                  width = 12,
                  enable_sidebar = TRUE,
                  sidebar_width = 25,
                  sidebar_start_open = TRUE,
                  sidebar_content = wria_stream_ui,
                  leafletOutput("stream_map", height = "800px")
                )
              )
      ),
      tabItem(tabName = "data_entry",
              fluidRow(
                br(),
                br(),
                boxPlus(
                  title = "Survey",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  survey_ui,
                  width = 12,
                  accordion(
                    accordionItem(
                      id = 1,
                      title = "Survey comments",
                      color = "purple",
                      collapsed = TRUE,
                      survey_comment_ui
                    ),
                    accordionItem(
                      id = 2,
                      title = "Survey intent",
                      color = "purple",
                      collapsed = TRUE,
                      survey_intent_ui
                    ),
                    accordionItem(
                      id = 3,
                      title = "Waterbody measurements",
                      color = "purple",
                      collapsed = TRUE,
                      waterbody_meas_ui
                    )
                  )
                ),
                boxPlus(
                  title = "Species data",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  survey_event_ui,
                  width = 12
                ),
                boxPlus(
                  title = "Fish encounters",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  width = 12,
                  accordion(
                    accordionItem(
                      id = 4,
                      title = "Fish location",
                      color = "purple",
                      collapsed = TRUE,
                      #fish_location_ui
                    ),
                    accordionItem(
                      id = 5,
                      title = "Fish counts",
                      color = "purple",
                      collapsed = FALSE,
                      #fish_encounter_ui
                    ),
                    accordionItem(
                      id = 6,
                      title = "Individual fish",
                      color = "purple",
                      collapsed = TRUE,
                      #individual_fish_ui
                    ),
                    accordionItem(
                      id = 7,
                      title = "Fish length measurement",
                      color = "purple",
                      collapsed = TRUE,
                      #fish_length_measurement_ui
                    )
                  )
                ),
                boxPlus(
                  title = "Redd encounters",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  width = 12,
                  accordion(
                    accordionItem(
                      id = 8,
                      title = "Redd location",
                      color = "purple",
                      collapsed = TRUE,
                      #redd_location_ui
                    ),
                    accordionItem(
                      id = 9,
                      title = "Redd counts",
                      color = "purple",
                      collapsed = FALSE,
                      #redd_encounter_ui
                    ),
                    accordionItem(
                      id = 10,
                      title = "Individual redd",
                      color = "purple",
                      collapsed = TRUE,
                      #individual_redd_ui
                    ),
                    accordionItem(
                      id = 11,
                      title = "Redd substrate",
                      color = "purple",
                      collapsed = TRUE,
                      #redd_substrate_ui
                    )
                  )
                )
              )
      ),
      tabItem(tabName = "reach_point",
              fluidRow(
                br(),
                br(),
                boxPlus(
                  title = "Reach points",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  #reach_point_ui,
                  width = 12
                )
              )
      ),
      tabItem(tabName = "mobile_import",
              fluidRow(
                br(),
                br(),
                boxPlus(
                  title = "Import from Mobile API (ToDo)",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  width = 12
                )
              )
      ),
      tabItem(tabName = "file_import",
              fluidRow(
                br(),
                br(),
                boxPlus(
                  title = "Import from external file or database (ToDo)",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  width = 12
                )
              )
      ),
      tabItem(tabName = "data_query",
              fluidRow(
                br(),
                br(),
                boxPlus(
                  title = "Export data using interactive query generator (ToDo)",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  width = 12
                )
              )
      ),
      tabItem(tabName = "reports",
              fluidRow(
                br(),
                br(),
                boxPlus(
                  title = "Generate automated reports (ToDo)",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  width = 12
                )
              )
      ),
      tabItem(tabName = "waterbody_edit",
              fluidRow(
                br(),
                br(),
                boxPlus(
                  title = "Edit waterbody geometries (ToDo)",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  width = 12
                )
              )
      ),
      tabItem(tabName = "about",
              fluidRow(
                br(),
                br(),
                boxPlus(
                  title = "About (ToDo)",
                  closable = FALSE,
                  collapsible = TRUE,
                  solidHeader = FALSE,
                  collapsed = FALSE,
                  width = 12
                )
              )
      )
    )
  ),
  title = "Salmon data"
)
