import org.apache.spark.sql.functions._
import spark.implicits._


val inputPath = "hdfs://nyu-dataproc-m/user/jw8191_nyu_edu/steamspy_top10000_raw.jsonl"
val outputPath = "hdfs://nyu-dataproc-m/user/jw8191_nyu_edu/steamspy_cleaned"
val rawDf = spark.read.json(inputPath)


val analyticCols = Seq(
  "appid",
  "name",
  "developer",
  "publisher",
  "price",
  "initialprice",
  "discount",
  "owners",
  "average_forever",
  "average_2weeks",
  "median_forever",
  "median_2weeks",
  "ccu",
  "positive",
  "negative"
)
val dfSelected = rawDf.select(analyticCols.map(col): _*)


println("===== SELECTED SCHEMA =====")
dfSelected.printSchema()


println("===== SELECTED RECORD COUNT =====")
println(dfSelected.count())

println("===== RECORD COUNT AFTER DROPPING NULLS =====")
val dfNoNull = dfSelected.na.drop()
println(dfNoNull.count())


// If I break a `.withColumn` call across lines, but the beginning of the new line lacks an operator (a dot `.`),
// the Scala interpreter may sometimes interpret the first line as a complete statement.

// convert the price unit from cents to dollar
val dfPrice = dfNoNull.withColumn("price_usd", col("price").cast("double") / 100.0).withColumn("initialprice_usd", col("initialprice").cast("double") / 100.0)

println("===== PRICE CONVERSION SAMPLE =====")
dfPrice.select("appid", "name", "price", "price_usd", "initialprice", "initialprice_usd").show(5, false)

// parse owners range, example format: "20,000,000 .. 50,000,000"
val dfOwners = dfPrice.withColumn("owners_clean", regexp_replace(col("owners"), ",", "")).withColumn("owners_low", regexp_extract(col("owners_clean"), "^(\\d+)\\s*\\.\\.", 1).cast("double")).withColumn("owners_high", regexp_extract(col("owners_clean"), "\\.\\.\\s*(\\d+)$", 1).cast("double")).withColumn("owners_mid", (col("owners_low") + col("owners_high")) / 2.0)

println("===== OWNERS PARSING SAMPLE =====")
dfOwners.select("appid", "name", "owners", "owners_low", "owners_high", "owners_mid").show(5, false)

// calculate rating
val dfRating = dfOwners.withColumn("total_reviews", col("positive") + col("negative")).withColumn(
    "rating",
    when(col("total_reviews") > 0,
      col("positive").cast("double") / col("total_reviews").cast("double")
    ).otherwise(lit(null))
  )

println("===== RATING SAMPLE =====")
dfRating.select("appid", "name", "positive", "negative", "total_reviews", "rating").show(5, false)

// calculate estimated revenue
// including free-to-play games with unpredictable revenue, so price=0 => estimated_revenue=0
val dfRevenue = dfRating.withColumn("estimated_revenue", col("price_usd") * col("owners_mid"))

println("===== ESTIMATED REVENUE SAMPLE =====")
dfRevenue.select("appid", "name", "price_usd", "owners_mid", "estimated_revenue").show(5, false)


val cleanedDf = dfRevenue.select(
  col("appid"),
  col("name"),
  col("developer"),
  col("publisher"),
  col("discount"),
  col("owners"),
  col("average_forever"),
  col("average_2weeks"),
  col("median_forever"),
  col("median_2weeks"),
  col("ccu"),
  col("positive"),
  col("negative"),
  col("price_usd"),
  col("initialprice_usd"),
  col("owners_low"),
  col("owners_high"),
  col("owners_mid"),
  col("total_reviews"),
  col("rating"),
  col("estimated_revenue")
)

println("===== FINAL CLEANED SAMPLE =====")
cleanedDf.show(10, false)

println("===== FINAL CLEANED SCHEMA =====")
cleanedDf.printSchema()

println("===== FINAL CLEANED RECORD COUNT =====")
println(cleanedDf.count())


cleanedDf.write.mode("overwrite").parquet(outputPath)
println("===== DATA WRITTEN TO HDFS =====")
