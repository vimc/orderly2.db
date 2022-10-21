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
