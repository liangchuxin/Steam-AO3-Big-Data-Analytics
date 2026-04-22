import org.apache.spark.sql.functions._
import spark.implicits._

val inputPath = "steamspy_top10000_raw.jsonl"
val df = spark.read.json(inputPath)


println("===== PRINT SCHEMA =====")
df.printSchema()


println("===== TOTAL RECORD COUNT =====")
val totalCount = df.count()
println(s"Total number of records: $totalCount")


println("===== MAP TO KEY-VALUE AND COUNT =====")
val mappedCount = df.rdd.map(row => ("record", 1)).count()
println(s"Mapped record count: $mappedCount")


println("===== DISTINCT COUNTS FOR ANALYTIC COLUMNS =====")

val analyticCols = Seq(
  "appid",
  "name",
  "price",
  "initialprice",
  "discount",
  "owners",
  "average_forever",
  "average_2weeks",
  "ccu",
  "positive",
  "negative"
)

analyticCols.foreach { c =>
  val distinctCount = df.select(col(c)).distinct().count()
  println(s"Distinct count for column '$c': $distinctCount")
}
