test_prepare_example <- function(examples, data) {
  tmp <- tempfile()
  withr::defer_parent(unlink(tmp, recursive = TRUE))
  orderly2:::orderly_init(tmp)

  writeLines(c(
    "plugins:",
    "  orderly2.db: ~",
    "",
    "orderly2.db:",
    "  source:",
    "    driver: RSQLite::SQLite",
    "    args:",
    "      dbname: source.sqlite"),
    file.path(tmp, "orderly_config.yml"))

  con <- DBI::dbConnect(RSQLite::SQLite(),
                        dbname = file.path(tmp, "source.sqlite"))
  for (nm in names(data)) {
    DBI::dbWriteTable(con, nm, data[[nm]])
  }
  DBI::dbDisconnect(con)

  fs::dir_create(file.path(tmp, "src"))
  for (i in examples) {
    fs::dir_copy(file.path("examples", i), file.path(tmp, "src"))
  }

  tmp
}
