orderly_db_plugin <- function() {
  schema <- system.file("orderly.db.json", package = "orderly2.db",
                        mustWork = TRUE)
  orderly2::orderly_plugin(orderly_db_config,
                           orderly_db_read,
                           orderly_db_run,
                           schema)
}


## Reads orderly_config.yml, configs plugin configuration. Evaluated
## from the root directory.
orderly_db_config <- function(data, filename) {
  fieldname <- function(x) sprintf("%s:orderly2.db:%s", filename, x)
  assert_named(data, unique = TRUE, name = sprintf("%s:orderly2.db", filename))
  for (nm in names(data)) {
    db <- data[[nm]]
    prefix <- sprintf("%s:orderly2.db:%s", filename, nm)
    check_fields(db, prefix, c("driver", "args"), NULL)
    driver <- check_symbol_from_str(db$driver, paste0(prefix, ":driver"))
    data[[nm]]$driver <- driver

    assert_named(db$args, TRUE, paste0(prefix, ":args"))
    ## TODO: instances here, once we support them, both instances and
    ## default_instance field

    ## There are two things to check with SQLite - first that we don't
    ## use in-memory databases as we'll never reconnect to them. This
    ## is probably not that useful a check but orderly does do it so
    ## we may have added it for some reason.
    ##
    ## The other is more important though, which is is that database
    ## files *should* be presented as relative paths, but these paths
    ## will be interpreted relative to the root. At the point where we
    ## process this configuration we are guaranteed to have the
    ## working directory be the orderly root so we can build an
    ## absolute path at this point.
    ##
    ## Similar checks would be required for other database backends
    ## that use absolute paths, there might be some way to make this
    ## more general.
    if (identical(driver, c("RSQLite", "SQLite"))) {
      assert_scalar_character(db$args$dbname, paste0(prefix, ":args:dbname"))
      if (db$args$dbname == ":memory:") {
        stop("Can't use an in-memory database with orderly2.db")
      }
      if (!fs::is_absolute_path(db$args$dbname)) {
        data[[nm]]$args$dbname <- file.path(getwd(), db$args$dbname)
      }
    }
  }
  data
}


## Reads orderly.yml, checks plugin use. Evaluated from the
## packet/report source directory
orderly_db_read <- function(data, filename, root) {
  prefix <- sprintf("%s:orderly2.db", filename)
  assert_named(data, name = prefix)
  check_fields(data, prefix, "data", NULL)

  assert_named(data$data, unique = TRUE, paste0(prefix, ":data"))
  for (nm in names(data$data)) {
    check_fields(data$data[[nm]], sprintf("%s:data:%s", prefix, nm),
                 c("query", "database"), NULL)
    match_value(data$data[[nm]]$database, names(root$config$orderly2.db),
                sprintf("%s:data:%s:database", prefix, nm))

    query <- data$data[[nm]]$query
    assert_character(query, sprintf("%s:data:%s:query", prefix, nm))
    if (length(query) == 1 && file.exists(query)) {
      query <- readLines(query)
    }
    if (length(query) > 1) {
      query <- paste(query, collapse = "\n")
    }
    data$data[[nm]]$query <- query
  }

  data
}


orderly_db_run <- function(data, root, parameters, environment, path) {
  config <- root$config$orderly2.db
  res <- list(data = list())

  connections <- list()

  for (nm in names(data$data)) {
    database <- data$data[[nm]]$database
    if (is.null(connections[[database]])) {
      connections[[database]] <- orderly_db_connect(database, config)
    }
    res$data[[nm]] <- DBI::dbGetQuery(
      connections[[database]], data$data[[nm]]$query)
    environment[[nm]] <- res$data[[nm]]
  }

  ## TODO: copy out connections to the environment (don't close them)

  for (con in connections) {
    DBI::dbDisconnect(con)
  }

  orderly_db_build_metadata(res)
}


## We then need to serialise something about the downloaded data,
## but for now we just return a list. Saving a hash would be ideal
## really, along side columns (+ types?) and number of rows.

## We might also save the final queries too, along with the instance
## information (i.e., all the information that might end up not end
## up in the final accounting?
orderly_db_build_metadata <- function(data) {
  dat <- list(
    data = lapply(data$data, function(d) {
      list(rows = jsonlite::unbox(nrow(d)),
           cols = names(d))
    }))
  jsonlite::toJSON(dat, auto_unbox = FALSE, pretty = FALSE, na = "null",
                   null = "null")
}


orderly_db_connect <- function(name, config) {
  x <- config[[name]]
  driver <- getExportedValue(x$driver[[1L]], x$driver[[2L]])
  do.call(DBI::dbConnect, c(list(driver()), x$args))
}
