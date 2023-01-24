png("mygraph.png")
par(mar = c(15, 4, .5, .5))
plot(mpg ~ cyl, dat, las = 2)
dev.off()
