import org.apache.spark.sql.functions._
import spark.implicits._

// PART 1: SteamSpy Data (Jingjing Wang - jw8191)
// =====================================
val inputPath = "hdfs://nyu-dataproc-m/user/jw8191_nyu_edu/steamspy_cleaned"
val df = spark.read.parquet(inputPath)

val numericCols = Seq(
  "discount",
  "average_forever",
  "average_2weeks",
  "median_forever",
  "median_2weeks",
  "ccu",
  "positive",
  "negative",
  "price_usd",
  "initialprice_usd",
  "owners_low",
  "owners_high",
  "owners_mid",
  "total_reviews",
  "rating",
  "estimated_revenue"
)


// Initial Code Analysis 
// =====================================
println("===== NUMERIC COLUMNS =====")
numericCols.foreach(println)

println("===== MEAN OF NUMERICAL COLUMNS =====")
val meanExprs = numericCols.map(c => avg(col(c)).alias(c + "_mean"))
df.select(meanExprs: _*).show(false)

println("===== MEDIAN OF NUMERICAL COLUMNS =====")
val medianExprs = numericCols.map(c =>
  expr(s"percentile_approx($c, 0.5)").alias(c + "_median")
)
df.select(medianExprs: _*).show(false)

println("===== MODE OF NUMERICAL COLUMNS =====")
numericCols.foreach { c =>
  println(s"--- Mode for column: $c ---")
  df.groupBy(col(c)).count().orderBy(desc("count"), asc(c)).show(5, false)
}

println("===== STANDARD DEVIATION OF RATING =====")
df.select(stddev(col("rating")).alias("rating_stddev")).show(false)


println("===== MIN OF NUMERICAL COLUMNS =====")
val minExprs = numericCols.map(c => min(col(c)).alias(c + "_min"))
df.select(minExprs: _*).show(false)

println("===== MAX OF NUMERICAL COLUMNS =====")
val maxExprs = numericCols.map(c => max(col(c)).alias(c + "_max"))
df.select(maxExprs: _*).show(false)


// Code Cleaning, Text formatting & Creating binary column for "rating"
// =====================================
val dfTextCleaned = df.withColumn("name_clean", lower(col("name"))).withColumn("developer_clean", lower(col("developer"))).withColumn("publisher_clean", lower(col("publisher")))

println("===== TEXT CLEANING SAMPLE =====")
dfTextCleaned.select("name", "name_clean", "developer", "developer_clean", "publisher", "publisher_clean").show(10, false)

val dfFinal = dfTextCleaned.withColumn("is_top_rated", when(col("rating") > 0.8, 1).otherwise(0))

println("===== BINARY COLUMN SAMPLE =====")
dfFinal.select("name", "rating", "is_top_rated")
  .show(10, false)

println("===== DISTRIBUTION OF is_top_rated =====")
dfFinal.groupBy("is_top_rated").count().show()




// PART 2: AO3 Data (Celia Liang - cl7093)
// =====================================
val ao3Path = "hdfs://nyu-dataproc-m/user/cl7093_nyu_edu/course_project/output/works_cleaned"
val ao3Df = spark.read.option("header", "true").csv(ao3Path)
val ao3Numeric = ao3Df.withColumn("word_count", col("word_count").cast("double"))

println("===== AO3 WORD COUNT MEAN =====")
ao3Numeric.select(avg("word_count").alias("mean_word_count")).show()

println("===== AO3 WORD COUNT MEDIAN =====")
ao3Numeric.select(expr("percentile_approx(word_count, 0.5)").alias("median_word_count")).show()

println("===== AO3 WORD COUNT MODE =====")
ao3Numeric.groupBy("word_count").count().orderBy(desc("count")).show(5)

println("===== AO3 WORD COUNT STANDARD DEVIATION =====")
ao3Numeric.select(stddev("word_count").alias("stddev_word_count")).show()

println("===== AO3 WORD COUNT MIN =====")
ao3Numeric.select(min("word_count").alias("min_word_count")).show()

println("===== AO3 WORD COUNT MAX =====")
ao3Numeric.select(max("word_count").alias("max_word_count")).show()

// Cleaning 1 - Text formatting, normalize language column to lowercase
val ao3Cleaned = ao3Numeric.withColumn("language_clean", lower(col("language")))

println("===== AO3 TEXT FORMATTING SAMPLE =====")
ao3Cleaned.select("language", "language_clean").show(10)

// Cleaning 2 - Binary column, is_long_fic based on word_count > 10000
val ao3Final = ao3Cleaned.withColumn("is_long_fic",
  when(col("word_count") > 10000, 1).otherwise(0))

println("===== AO3 BINARY COLUMN SAMPLE =====")
ao3Final.select("word_count", "is_long_fic").show(10)

println("===== AO3 DISTRIBUTION OF is_long_fic =====")
ao3Final.groupBy("is_long_fic").count().show()
