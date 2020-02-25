#=============================================================
# HTML content definition of fish_encounter accordion
#=============================================================

# Define the survey data content
fish_encounter_ui = tags$div(
  actionButton(inputId = "fish_enc_add", label = "New", class = "new_button"),
  actionButton(inputId = "fish_enc_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "fish_enc_delete", label = "Delete", class = "delete_button"),
  tippy("<i style='color:#1a5e86;padding-left:8px', class='fas fa-info-circle'></i>",
        tooltip = glue("To enable tracking carcasses over time, please enter the fish ",
                       "location data first. Then to associate carcass data with ",
                       "a fish location, select the 'fish_name' in the drop-down menu.",
                       "To enter counts only (no location) select 'no location data' ",
                       "in the 'fish_name' drop-down. You can also edit existing ",
                       "fish count data and remove the location association ",
                       "by selecting 'no location data' in the drop-down.")),
  br(),
  br(),
  timeInput(inputId = "fish_encounter_time_select", "encounter_time", seconds = FALSE),
  numericInput(inputId = "fish_count_input", label = "fish_count", value = 0,
               min = 0, step = 1, width = "75px"),
  uiOutput("fish_status_select", inline = TRUE),
  uiOutput("fish_name_select", inline = TRUE),
  uiOutput("sex_select", inline = TRUE),
  uiOutput("maturity_select", inline = TRUE),
  uiOutput("origin_select", inline = TRUE),
  uiOutput("cwt_status_select", inline = TRUE),
  uiOutput("clip_status_select", inline = TRUE),
  uiOutput("fish_behavior_select", inline = TRUE),
  uiOutput("prev_counted_select", inline = TRUE),
  br(),
  br(),
  br(),
  DT::DTOutput("fish_encounters")
)
