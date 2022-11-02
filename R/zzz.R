.onLoad <- function(...) {
  orderly2::orderly_plugin_register(orderly_db_plugin(), "orderly2.db") # nocov
}
