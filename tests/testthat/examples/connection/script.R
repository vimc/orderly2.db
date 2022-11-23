dat_cmp <- DBI::dbReadTable(con1, "mtcars")

stopifnot(isTRUE(all.equal(dat1, dat_cmp)))

png("mygraph.png")
plot(dat1)
dev.off()
