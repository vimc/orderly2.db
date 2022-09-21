.onLoad <- function(...) {
  ## TODO: we _really_ need the package name to be discoverable here
  ## or we get weird errors if the user gets it wrong. It's gettable
  ## here as:
  ##
  ## > packageName(environment(orderly_db_plugin))
  ##
  ## which is ok, and we could do this within the registration
  orderly2:::orderly_register_plugin("orderly2.db", orderly_db_plugin())
}
