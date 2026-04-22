val works = spark.read.option("header", "true").csv("hdfs:///user/cl7093_nyu_edu/course_project/input/works-20210226.csv")

val tags = spark.read.option("header", "true").csv("hdfs:///user/cl7093_nyu_edu/course_project/input/tags-20210226.csv")

println("=== Works total record count ===")
println(works.count())

println("=== Tags total record count ===")
println(tags.count())

println("=== Unique values in 'language' ===")
works.groupBy("language").count().orderBy(org.apache.spark.sql.functions.desc("count")).show()

println("=== Unique values in 'complete' ===")
works.groupBy("complete").count().show()

println("=== Unique values in 'restricted' ===")
works.groupBy("restricted").count().show()

println("=== Unique values in tag 'type' ===")
tags.groupBy("type").count().orderBy(org.apache.spark.sql.functions.desc("count")).show()

println("=== Unique values in tag 'canonical' ===")
tags.groupBy("canonical").count().show()
