test_that("basic plugin use works", {
  root <- test_prepare_example("minimal", list(mtcars = mtcars))
  env <- new.env()
  id <- orderly2::orderly_run("minimal", root = root, envir = env)
  expect_type(id, "character")

  expect_true(file.exists(
    file.path(root, "archive", "minimal", id, "mygraph.png")))

  meta <- outpack::outpack_root_open(root)$metadata(id, TRUE)
  meta_db <- meta$custom$orderly$plugins$orderly2.db
  expect_setequal(names(meta_db), "data")
  expect_setequal(names(meta_db$data), "dat1")
  expect_setequal(names(meta_db$data$dat1), c("rows", "cols"))
  expect_equal(meta_db$data$dat1$rows, nrow(mtcars))
  expect_equal(meta_db$data$dat1$cols, as.list(names(mtcars)))
})


test_that("validate plugin configuration", {
  expect_error(
    orderly_db_config(list(), "orderly_config.yml"),
    "'orderly_config.yml:orderly2.db' must be named")
  expect_error(
    orderly_db_config(list(db = list()), "orderly_config.yml"),
    "Fields missing from orderly_config.yml:orderly2.db:db: driver, args")
  expect_error(
    orderly_db_config(list(db = list(driver = NULL, args = NULL)),
                      "orderly_config.yml"),
    "'orderly_config.yml:orderly2.db:db:driver' must be a scalar")
  expect_error(
    orderly_db_config(list(db = list(driver = "db", args = NULL)),
                      "orderly_config.yml"),
    paste("Expected fully qualified name for",
          "orderly_config.yml:orderly2.db:db:driver"))
  expect_error(
    orderly_db_config(list(db = list(driver = "pkg::db", args = NULL)),
                      "orderly_config.yml"),
    "'orderly_config.yml:orderly2.db:db:args' must be named")

  ## Success:
  expect_equal(
    orderly_db_config(
      list(db = list(driver = "pkg::db", args = list(a = 1))),
      "orderly_config.yml"),
    list(db = list(driver = c("pkg", "db"), args = list(a = 1))))
})


test_that("validate db for sqlite", {
  expect_error(
    orderly_db_config(
      list(db = list(driver = "RSQLite::SQLite",
                     args = list(dbname = ":memory:"))),
      "orderly_config.yml"),
    "Can't use an in-memory database with orderly2.db")

  db <- tempfile(tmpdir = normalizePath(tempdir(), mustWork = TRUE))
  expected <- list(db = list(driver = c("RSQLite", "SQLite"),
                             args = list(dbname = db)))

  expect_equal(
    orderly_db_config(
      list(db = list(driver = "RSQLite::SQLite",
                     args = list(dbname = db))),
      "orderly_config.yml"),
    expected)
  withr::with_dir(
    dirname(db),
    expect_equal(
      orderly_db_config(
        list(db = list(driver = "RSQLite::SQLite",
                       args = list(dbname = basename(db)))),
        "orderly_config.yml"),
      expected))
})
