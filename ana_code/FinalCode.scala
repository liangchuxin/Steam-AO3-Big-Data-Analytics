import org.apache.spark.sql.functions._
import spark.implicits._


val steamPath = "hdfs://nyu-dataproc-m/user/jw8191_nyu_edu/steamspy_cleaned"
val df = spark.read.parquet(steamPath)

val ao3tagPath = "hdfs://nyu-dataproc-m/user/cl7093_nyu_edu/course_project/output/tags_cleaned"
val ao3tagDf = spark.read.option("header", "true").csv(ao3tagPath)

val ao3workPath = "hdfs://nyu-dataproc-m/user/cl7093_nyu_edu/course_project/output/works_cleaned"
val ao3workDf = spark.read.option("header", "true").csv(ao3workPath)

// Merge two ao3 tables
val worksExploded = ao3workDf.withColumn("tag_id", explode(split(col("tags"), "\\+"))).withColumn("word_count_num", col("word_count").cast("double"))

val worksWithFandom = worksExploded.join(
  ao3tagDf.select(col("id"), col("name").alias("fandom_name"), col("cached_count")),
  worksExploded("tag_id") === ao3tagDf("id"),
  "inner"
)

// Aggregated by fandom_name(tag names): fanfic count and average word count
val ao3Agg_raw = worksWithFandom.groupBy("fandom_name").agg(
  count("*").alias("fic_count"),
  avg("word_count_num").alias("avg_word_count"),
  sum(col("cached_count").cast("double")).alias("cached_count")
)

println("===== AO3 FANDOM AGG SAMPLE =====")
ao3Agg_raw.orderBy(desc("fic_count")).show(20, false)


// Normalize game names，create the binary column for rating
val dfFinal = df.withColumn("is_top_rated", when(col("rating") > 0.8, 1).otherwise(0))
val steamNorm = dfFinal.withColumn(
    "name_norm",
    trim(
      regexp_replace(
        regexp_replace(
          regexp_replace(lower(col("name")), "\\(.*?\\)", ""),   // remove parentheses
          "[^a-z0-9 ]", " "                                      // remove punctuation
        ),
        "\\s+", " "                                              // collapse spaces
      )
    )
  ).withColumn(
    "name_main", trim(split(col("name_norm"), ":").getItem(0))
  )


// Normalize ao3 tag names
val ao3Norm = ao3Agg_raw.withColumn(
    "tag_norm",
    trim(
      regexp_replace(
        regexp_replace(
          regexp_replace(lower(col("fandom_name")), "\\(.*?\\)", ""),
          "[^a-z0-9 ]", " "
        ),
        "\\s+", " "
        )
    )
  ).withColumn(
    "tag_main", trim(split(col("tag_norm"), ":").getItem(0))
  )


// Merge the tables of ao3 and steamspy by comparing the normalized game/tag names
val exactJoinDf = steamNorm.join(ao3Norm, steamNorm("name_norm") === ao3Norm("tag_norm"), "inner")
println("===== EXACT NORMALIZED JOIN COUNT =====")
println(exactJoinDf.count())

val mainJoinDf = steamNorm.join(ao3Norm, steamNorm("name_main") === ao3Norm("tag_main"), "inner")
println("===== MAIN TITLE JOIN COUNT =====")
println(mainJoinDf.count())

val combinedJoinDf = exactJoinDf.unionByName(mainJoinDf).dropDuplicates("appid", "tag_norm")

println("===== COMBINED JOIN COUNT =====")
println(combinedJoinDf.count())
combinedJoinDf.select("appid", "name", "name_norm", "name_main", "tag_norm", "tag_main", "cached_count").show(20, false)


// Calculate the women's community engagement for every game, (active player number) / (number of derivative works per game)
val withMetrics = combinedJoinDf.withColumn("avg_per_tag", when(col("fic_count") > 0, 
   col("average_forever").cast("double") / col("fic_count")).otherwise(lit(null).cast("double"))).withColumn("med_per_tag", 
   when(col("fic_count") > 0, col("median_forever").cast("double") / col("fic_count")).otherwise(lit(null).cast("double"))).withColumn("ccu_per_tag", 
   when(col("fic_count") > 0, col("ccu").cast("double") / col("fic_count")).otherwise(lit(null).cast("double")))

val metricsDf = withMetrics.na.drop(Seq("avg_per_tag", "med_per_tag", "ccu_per_tag"))
println("===== METRICS RECORD COUNT =====")
println(metricsDf.count())


// Find the mediams of the three engagement index
val medianRow = metricsDf.select(
  expr("percentile_approx(avg_per_tag, 0.5)").alias("avg_per_tag_median"),
  expr("percentile_approx(med_per_tag, 0.5)").alias("med_per_tag_median"),
  expr("percentile_approx(ccu_per_tag, 0.5)").alias("ccu_per_tag_median")
).first()

val avgMedian = medianRow.getAs[Double]("avg_per_tag_median")
val medMedian = medianRow.getAs[Double]("med_per_tag_median")
val ccuMedian = medianRow.getAs[Double]("ccu_per_tag_median")

println("===== MEDIANS =====")
println(s"avg_per_tag median = $avgMedian")
println(s"med_per_tag median = $medMedian")
println(s"ccu_per_tag median = $ccuMedian")


// Create a binary column for "Female Community Engagement" 
// Assigning a value of 1 to games where two out of the three data columns exceed the median, and 0 to the rest
val comparedDf = metricsDf.withColumn("avg_above_med", when(col("avg_per_tag") > lit(avgMedian), 1).otherwise(0)).withColumn("med_above_med", 
   when(col("med_per_tag") > lit(medMedian), 1).otherwise(0)).withColumn("ccu_above_med", 
   when(col("ccu_per_tag") > lit(ccuMedian), 1).otherwise(0)).withColumn("female_engagement_score",
    col("avg_above_med") + col("med_above_med") + col("ccu_above_med")
  ).withColumn("is_high_female_engagement",
    when(col("female_engagement_score") >= 2, 1).otherwise(0)
  )

println("===== BINARY ENGAGEMENT SAMPLE =====")
comparedDf.select(
  "name",
  "avg_per_tag", "med_per_tag", "ccu_per_tag",
  "avg_above_med", "med_above_med", "ccu_above_med",
  "female_engagement_score", "is_high_female_engagement",
  "rating", "is_top_rated"
).show(20, false)


// Compare the binary columns 'is_top_rated' against 'is_high_female_engagement'
// Tallying the respective counts for the four combinations: 11, 10, 01, and 00
// This result represents a comparison between "game reception in mixed-gender communities" and "game reception in female-centric communities."
val comboDf = comparedDf.withColumn(
    "rating_engagement_combo",
    concat(col("is_top_rated").cast("string"), col("is_high_female_engagement").cast("string"))
  )

println("===== COMBO COUNTS =====")
comboDf.groupBy("rating_engagement_combo").count().orderBy("rating_engagement_combo").show(false)


// Identify the `estimated_revenue` corresponding to each of the four combinations listed above
// This result serves as the relationship between "Capital Market Performance" and "Female Market Sentiment"
// Note: This section does not cover the analysis of free-to-play games.
val paidComboDf = comboDf.filter(col("price_usd") > 0)

println("===== REVENUE STATS BY COMBO (PAID GAMES ONLY) =====")
paidComboDf.groupBy("rating_engagement_combo").agg(
    min("estimated_revenue").alias("min_estimated_revenue"),
    max("estimated_revenue").alias("max_estimated_revenue"),
    avg("estimated_revenue").alias("avg_estimated_revenue"),
    expr("percentile_approx(estimated_revenue, 0.5)").alias("median_estimated_revenue"),
    count("*").alias("group_count")
  ).orderBy("rating_engagement_combo").show(false)


// Analysis about the average word count of the derivative works in the four combinations
println("===== WORD COUNT STATS BY COMBO =====")
comboDf.groupBy("rating_engagement_combo").agg(
    min("avg_word_count").alias("min_word_count_per_game"),
    max("avg_word_count").alias("max_word_count_per_game"),
    avg("avg_word_count").alias("avg_word_count_per_game"),
    expr("percentile_approx(avg_word_count, 0.5)").alias("mediam_word_count_per_game"),
    count("*").alias("group_count")
  ).orderBy("rating_engagement_combo").show(false)



// Save the result
val outputPath = "hdfs://nyu-dataproc-m/user/jw8191_nyu_edu/final_joined_analysis"
comboDf.write.mode("overwrite").parquet(outputPath)

println("===== FINAL JOINED ANALYSIS WRITTEN TO HDFS =====")
println(outputPath)
