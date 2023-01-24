test_that("basic plugin use works", {
  root <- test_prepare_example("minimal", list(mtcars = mtcars))
  env <- new.env()
  id <- orderly2::orderly_run("minimal", root = root, envir = env)
  expect_type(id, "character")

  expect_true(file.exists(
    file.path(root, "archive", "minimal", id, "mygraph.png")))

  meta <- outpack::outpack_root_open(root)$metadata(id, TRUE)
  meta_db <- meta$custom$orderly$plugins$orderly2.db
  expect_setequal(names(meta_db), c("data", "connection"))
  expect_setequal(names(meta_db$data), "dat1")
  expect_setequal(names(meta_db$data$dat1), c("rows", "cols"))
  expect_equal(meta_db$data$dat1$rows, nrow(mtcars))
  expect_equal(meta_db$data$dat1$cols, as.list(names(mtcars)))

  expect_length(meta_db$connection, 0)
})


test_that("allow connection", {
  root <- test_prepare_example("connection", list(mtcars = mtcars))
  env <- new.env()
  id <- orderly2::orderly_run("connection", root = root, envir = env)

  expect_type(id, "character")

  expect_true(file.exists(
    file.path(root, "archive", "connection", id, "mygraph.png")))

  ## Save a copy of the connection out:
  con <- env$con1
  expect_s4_class(con, "SQLiteConnection")
  expect_true(DBI::dbIsValid(con))
  expect_equal(DBI::dbListTables(con), "mtcars") # still works

  ## Force cleanup, check it closes connection:
  rm(env)
  gc()
  expect_false(DBI::dbIsValid(con))

  meta <- outpack::outpack_root_open(root)$metadata(id, TRUE)
  meta_db <- meta$custom$orderly$plugins$orderly2.db
  expect_setequal(names(meta_db), c("data", "connection"))
  expect_setequal(names(meta_db$data), "dat1")
  expect_setequal(names(meta_db$data$dat1), c("rows", "cols"))
  expect_equal(meta_db$data$dat1$rows, nrow(mtcars))
  expect_equal(meta_db$data$dat1$cols, as.list(names(mtcars)))

  expect_equal(meta_db$connection, list(con1 = "source"))
})


test_that("allow connection without data", {
  root <- test_prepare_example("connectiononly", list(mtcars = mtcars))
  env <- new.env()
  id <- orderly2::orderly_run("connectiononly", root = root, envir = env)

  expect_type(id, "character")

  expect_true(file.exists(
    file.path(root, "archive", "connectiononly", id, "mygraph.png")))

  meta <- outpack::outpack_root_open(root)$metadata(id, TRUE)
  meta_db <- meta$custom$orderly$plugins$orderly2.db
  expect_setequal(names(meta_db), c("data", "connection"))
  expect_equal(meta_db$data, list())
  expect_equal(meta_db$connection, list(con1 = "source"))
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
  ## Tweak so that things behave sensibly on windows:
  db <- gsub("\\", "/", db, fixed = TRUE)

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


test_that("validate orderly.yml read", {
  mock_root <- list(config = list(orderly2.db = list(db = list())))
  expect_error(
    orderly_db_read(list(1), "orderly.yml", mock_root),
    "'orderly.yml:orderly2.db' must be named")
  expect_error(
    orderly_db_read(list(data = list(a = TRUE)), "orderly.yml", mock_root),
    "Fields missing from orderly.yml:orderly2.db:data:a: query, database")
  expect_error(
    orderly_db_read(
      list(data = list(a = list(query = TRUE, database = "other"))),
      "orderly.yml", mock_root),
    "orderly.yml:orderly2.db:data:a:database must be one of 'db'")
  expect_error(
    orderly_db_read(
      list(data = list(a = list(query = TRUE, database = "db"))),
      "orderly.yml", mock_root),
    "'orderly.yml:orderly2.db:data:a:query' must be character")

  expect_equal(
    orderly_db_read(
      list(data = list(a = list(query = "SELECT *", database = "db"))),
      "orderly.yml", mock_root),
    list(data = list(a = list(query = "SELECT *", database = "db"))))
  expect_equal(
    orderly_db_read(
      list(data = list(a = list(query = c("SELECT", "*"), database = "db"))),
      "orderly.yml", mock_root),
    list(data = list(a = list(query = "SELECT\n*", database = "db"))))

  tmp <- tempfile()
  on.exit(unlink(tmp))
  writeLines(c("SELECT", "*"), tmp)
  expect_equal(
    orderly_db_read(
      list(data = list(a = list(query = tmp, database = "db"))),
      "orderly.yml", mock_root),
    list(data = list(a = list(query = "SELECT\n*", database = "db"))))
})


test_that("validate read connection", {
  mock_root <- list(config = list(orderly2.db = list(db = list())))
  expect_equal(
    orderly_db_read(
      list(connection = list(a = "db")),
      "orderly.yml", mock_root),
    list(connection = list(a = "db")))
  expect_error(
    orderly_db_read(
      list(connection = list(a = "missing")),
      "orderly.yml", mock_root),
    "orderly.yml:orderly2.db:connection:a must be one of 'db'")
  expect_error(
    orderly_db_read(
      list(connection = list(a = TRUE)),
      "orderly.yml", mock_root),
    "'orderly.yml:orderly2.db:connection:a' must be character")
})


test_that("require either connection or data", {
  mock_root <- list(config = list(orderly2.db = list(db = list())))
  expect_error(
    orderly_db_read(list(), "orderly.yml", mock_root),
    "At least one of 'data' or 'views' or 'connection' must be given")
})


test_that("pull data from run function", {
  cars <- cbind(name = rownames(mtcars), mtcars)
  rownames(cars) <- NULL
  path <- test_prepare_example("minimal", list(cars = cars))

  root <- orderly2:::orderly_root(path, FALSE)
  data <- list(data = list(a = list(query = "SELECT * from cars",
                                    database = "source")))
  tmp <- tempfile()
  env <- new.env(parent = topenv())
  meta <- orderly_db_run(data, root, list(), env, tmp)
  expect_equal(ls(env), "a")
  expect_equal(env$a, cars)

  expect_s3_class(meta, "json")
  expect_mapequal(
    jsonlite::fromJSON(meta),
    list(data = list(a = list(rows = nrow(cars), cols = names(cars))),
         connection = list()))
})


test_that("run function cleans up connections", {
  skip_if_not_installed("mockery")

  cars <- cbind(name = rownames(mtcars), mtcars)
  rownames(cars) <- NULL
  path <- test_prepare_example("minimal", list(cars = cars))

  con <- new.env() # just gives us a singleton
  mock_connect <- mockery::mock(con)
  mock_query <- mockery::mock(cars)
  mock_disconnect <- mockery::mock()
  mockery::stub(orderly_db_run, "orderly_db_connect", mock_connect)
  mockery::stub(orderly_db_run, "DBI::dbGetQuery", mock_query)
  mockery::stub(orderly_db_run, "orderly_db_disconnect", mock_disconnect)

  root <- orderly2:::orderly_root(path, FALSE)
  data <- list(data = list(a = list(query = "SELECT * from cars",
                                    database = "source")))
  tmp <- tempfile()
  env <- new.env(parent = topenv())
  meta <- orderly_db_run(data, root, list(), env, tmp)
  expect_equal(ls(env), "a")
  expect_equal(env$a, cars)

  mockery::expect_called(mock_connect, 1)
  expect_equal(mockery::mock_args(mock_connect)[[1]],
               list("source", root$config$orderly2.db))

  mockery::expect_called(mock_query, 1)
  expect_identical(mockery::mock_args(mock_query)[[1]][[1]], con)
  expect_equal(mockery::mock_args(mock_query)[[1]][[2]], "SELECT * from cars")

  mockery::expect_called(mock_disconnect, 1)
  expect_equal(mockery::mock_args(mock_disconnect)[[1]],
               list(list(source = con)))
})


test_that("can construct plugin", {
  expect_identical(orderly_db_plugin(),
                   orderly2:::.plugins$orderly2.db)
})


test_that("can construct a view, then read from it", {
  root <- test_prepare_example("view", list(mtcars = mtcars))
  env <- new.env()
  id <- orderly2::orderly_run("view", root = root, envir = env)
  expect_type(id, "character")
  expect_true(file.exists(
    file.path(root, "archive", "view", id, "mygraph.png")))

  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(root, "source.sqlite"))
  ## View not present here, it was only available to the client that
  ## created it.
  expect_equal(DBI::dbListTables(con), "mtcars")
})
