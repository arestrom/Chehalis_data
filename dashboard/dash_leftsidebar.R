#==========================================================================
# ShinyDashboardPlus left sidebar
#==========================================================================

dash_leftsidebar = dashboardSidebar(
  width = 175,
  sidebarMenu(
    id = "tabs",
    menuItem("Where and when", tabName = "wria_stream", icon = icon("globe")),
    menuItem("Data entry", tabName = "data_entry", icon = icon("database")),
    menuItem("Add reach point", tabName = "reach_point", icon = icon("map-pin")),
    menuItem("Import from mobile", tabName = "mobile_import", icon = icon("sync-alt")),
    menuItem("Import from file", tabName = "file_import", icon = icon("file-upload")),
    menuItem("Data query", tabName = "data_query", icon = icon("share-square")),
    menuItem("Sync to cloud DB", tabName = "cloud_sync", icon = icon("cloud-upload-alt")),
    menuItem("Reports", tabName = "reports", icon = icon("pencil-alt")),
    menuItem("Waterbody edit", tabName = "waterbody_edit", icon = icon("map-marked-alt")),
    menuItem("About", tabName = "about", icon = icon("info-circle"))
  )
)
