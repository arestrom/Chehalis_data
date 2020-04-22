#=============================================================
# ShinyDashboardPlus header function
#=============================================================

dash_header = dashboardHeaderPlus(
  fixed = TRUE,
  title = tagList(
    span(class = "logo-lg", "Chehalis Basin data"),
    img(src = "ShinyDashboardPlus.svg")),
  enable_rightsidebar = FALSE,
  rightSidebarIcon = "bars"
)

