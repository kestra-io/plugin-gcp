library(SparkR)

sparkR.session(appName = "SparkR-DataFrame-example")

localDF <- data.frame(name=c("John", "Smith", "Sarah"), age=c(19, 23, 18))

df <- createDataFrame(localDF)

printSchema(df)