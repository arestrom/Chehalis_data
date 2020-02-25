#=========================================================
# HTML content definition of survey_intent accordian
#========================================================

# Define the survey data content
survey_intent_ui = tags$div(
  actionButton(inputId = "intent_add", label = "New", class = "new_button"),
  actionButton(inputId = "intent_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "intent_delete", label = "Delete", class = "delete_button"),
  tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
        tooltip = glue("Optional: Survey intent can be used to explicitly record ",
                       "the species and count types targeted during the survey. ",
                       "If survey intent is entered, then you do not need to enter ",
                       "counts of zero for fish or redds if none were encountered, ",
                       "because those zeros can be inferred. On the other hand, if ",
                       "survey intent is not specified and no fish or redds are ",
                       "encountered, then it is very important to enter counts of ",
                       "zero for each species and count type (carcass, live, redd) ",
                       "that the survey was intended to enumerate.")),
  br(),
  br(),
  uiOutput("intent_species_select", inline = TRUE),
  uiOutput("intent_count_type_select", inline = TRUE),
  br(),
  br(),
  br(),
  DT::DTOutput("survey_intents")
)
