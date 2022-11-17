dat1 <- DBI::dbReadTable(con1, "mtcars")

png("mygraph.png")
plot(dat1)
dev.off()
