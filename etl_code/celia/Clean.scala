val works = spark.read.option("header", "true").csv("hdfs:///user/cl7093_nyu_edu/course_project/input/works-20210226.csv")

val tags = spark.read.option("header", "true").csv("hdfs:///user/cl7093_nyu_edu/course_project/input/tags-20210226.csv")

val cleanedWorks = works
  .filter(works("creation date").isNotNull)
  .filter(works("word_count").isNotNull)
  .filter(works("tags").isNotNull)
  .select("creation date", "language", "complete", "word_count", "tags")

val cleanedTags = tags
  .filter(tags("type") === "Fandom")
  .filter(tags("name").isNotNull)
  .select("id", "name", "canonical", "cached_count", "merger_id")

println("=== Cleaned works count ===")
println(cleanedWorks.count())

println("=== Cleaned tags count ===")
println(cleanedTags.count())

cleanedWorks.write.option("header", "true").csv("hdfs:///user/cl7093_nyu_edu/course_project/output/works_cleaned")

cleanedTags.write.option("header", "true").csv("hdfs:///user/cl7093_nyu_edu/course_project/output/tags_cleaned")

println("=== Done ===")
