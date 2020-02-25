#=========================================================
# HTML content definition of header page
#========================================================

# Define the survey data content
wria_stream_ui = tags$div(
  div(id = "sthd_image", img(src = "steelhead.png", width = "60%")),
  br(),
  br(),
  br(),
  div(id = "wria_text", p("Select WRIA:")),
  uiOutput("wria_select"),
  br(),
  div(id = "stream_text", p("Select stream (or click on map):")),
  uiOutput("stream_select"),
  br(),
  br(),
  div(id = "year_text", p("Select survey year:")),
  uiOutput("year_select")
)
