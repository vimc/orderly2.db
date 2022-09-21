## Functions copied over from orderly:

check_symbol_from_str <- function(str, name) {
  assert_scalar_character(str)
  dat <- strsplit(str, "::", fixed = TRUE)[[1L]]
  if (length(dat) != 2) {
    stop(sprintf("Expected fully qualified name for %s", name))
  }
  dat
}


## TODO: - in orderly we should check for paths starting with ~, or
## just use fs here
is_absolute_path <- function(path) {
  grepl("^(~|/|[A-Z]:)", path, ignore.case = TRUE)
}


check_fields <- orderly2:::check_fields
assert_named <- orderly2:::assert_named
assert_scalar_character <- orderly2:::assert_scalar_character
assert_character <- orderly2:::assert_character
match_value <- orderly2:::match_value
