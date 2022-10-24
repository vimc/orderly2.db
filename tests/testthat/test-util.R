test_that("null-or-value works", {
  expect_equal(1 %||% NULL, 1)
  expect_equal(1 %||% 2, 1)
  expect_equal(NULL %||% NULL, NULL)
  expect_equal(NULL %||% 2, 2)
})


test_that("validate symbol", {
  expect_equal(check_symbol_from_str("a::b", "thing"),
               c("a", "b"))
  expect_error(check_symbol_from_str("a:b", "thing"),
               "Expected fully qualified name for thing")
})
