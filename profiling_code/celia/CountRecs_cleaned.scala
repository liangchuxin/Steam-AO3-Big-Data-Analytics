val works = spark.read.option("header", "true").csv("hdfs:///user/cl7093_nyu_edu/course_project/output/works_cleaned")

val tags = spark.read.option("header", "true").csv("hdfs:///user/cl7093_nyu_edu/course_project/output/tags_cleaned")

println("=== Cleaned works total record count ===")
println(works.count())

println("=== Cleaned tags total record count ===")
println(tags.count())

println("=== Unique values in 'language' ===")
works.groupBy("language").count().orderBy(org.apache.spark.sql.functions.desc("count")).show()

println("=== Unique values in 'complete' ===")
works.groupBy("complete").count().show()

println("=== Unique values in tag 'canonical' ===")
tags.groupBy("canonical").count().show()
