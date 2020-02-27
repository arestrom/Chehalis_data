#=========================================================
# dashboardplus ui elements
#=========================================================

# Create the Shiny server
server = function(input, output, session) {
  source("wria_stream/wria_stream_srv.R", local = TRUE)
  source("survey/survey_srv.R", local = TRUE)
  source("survey_comment/survey_comment_srv.R", local = TRUE)
  source("survey_intent/survey_intent_srv.R", local = TRUE)
  source("waterbody_meas/waterbody_meas_srv.R", local = TRUE)
  source("survey_event/survey_event_srv.R", local = TRUE)
  source("fish_location/fish_location_srv.R", local = TRUE)
  source("fish_encounter/fish_encounter_srv.R", local = TRUE)
  source("individual_fish/individual_fish_srv.R", local = TRUE)
  source("fish_length_measurement/fish_length_measurement_srv.R", local = TRUE)
  # source("redd_encounter/redd_encounter_srv.R", local = TRUE)
  # source("redd_location/redd_location_srv.R", local = TRUE)
  # source("individual_redd/individual_redd_srv.R", local = TRUE)
  # source("redd_substrate/redd_substrate_srv.R", local = TRUE)
  # source("reach_point/reach_point_srv.R", local = TRUE)

  # # close the R session when Chrome closes...for standalone
  # session$onSessionEnded(function() {
  #   stopApp()
  #   q("no")
  # })

}
