#=============================================================
# HTML content definition of individual fish accordion
#=============================================================

# Define the survey data content
individual_fish_ui = tags$div(
  actionButton(inputId = "ind_fish_add", label = "New", class = "new_button"),
  actionButton(inputId = "ind_fish_edit", label = "Edit", class = "edit_button"),
  actionButton(inputId = "ind_fish_delete", label = "Delete", class = "delete_button"),
  br(),
  br(),
  uiOutput("fish_condition_select", inline = TRUE),
  uiOutput("fish_trauma_select", inline = TRUE),
  uiOutput("gill_condition_select", inline = TRUE),
  uiOutput("spawn_condition_select", inline = TRUE),
  textInput(inputId = "fish_sample_num_input", label = "fish_sample #", width = "125px"),
  textInput(inputId = "scale_card_num_input", label = "scale_card #", width = "125px"),
  textInput(inputId = "scale_position_num_input", label = "scale_pos #", width = "125px"),
  uiOutput("age_code_select", inline = TRUE),
  textInput(inputId = "snout_sample_num_input", label = "snout_sample #", width = "125px"),
  textInput(inputId = "cwt_tag_code_input", label = "cwt_tag_code", width = "100px"),
  uiOutput("cwt_result_select", inline = TRUE),
  textInput(inputId = "genetic_sample_num_input", label = "genetic_sample #", width = "125px"),
  textInput(inputId = "otolith_sample_num_input", label = "otolith_sample #", width = "125px"),
  numericInput(inputId = "pct_eggs_input", label = "pct_eggs_retained", value = NULL,
               min = 0, max = 100, step = 1, width = "115px"),
  numericInput(inputId = "eggs_gram_input", label = "eggs_gram", value = NULL,
               min = 0, max = 100, step = 1, width = "100px"),
  numericInput(inputId = "eggs_number_input", label = "eggs_number", value = NULL,
               min = 0, max = 100, step = 1, width = "100px"),
  textAreaInput(inputId = "ind_fish_comment_input", label = "fish_comment", value = "",
                width = "300px", resize = "both"),
  br(),
  br(),
  br(),
  DT::DTOutput("individual_fishes")
)
