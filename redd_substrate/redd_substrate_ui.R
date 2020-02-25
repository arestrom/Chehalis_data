#=============================================================
# HTML content definition of individual_redd accordion
#=============================================================

# Define the survey data content
redd_substrate_ui = tags$div(
  actionButton(inputId = "substrate_add", label = "New", class = "new_button"),
  actionButton(inputId = "substrate_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "substrate_delete", label = "Delete", class = "delete_button"),
  br(),
  br(),
  uiOutput("substrate_level_select", inline = TRUE),
  uiOutput("substrate_type_select", inline = TRUE),
  numericInput(inputId = "substrate_pct_input", label = "substrate_pct", value = NA,
               min = 0, max = 100, step = 5, width = "90px"),
  br(),
  br(),
  br(),
  DT::DTOutput("redd_substrates"),
  verbatimTextOutput("chk_substrate_edit")
)
