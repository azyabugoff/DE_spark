import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sys.process._

object filter{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val dir = spark.conf.get("spark.filter.output_dir_prefix")
    val topic = spark.conf.get("spark.filter.topic_name")
    var offset = spark.conf.get("spark.filter.offset")
    if (offset != "earliest") {
      offset = s"""{"$topic":{"0":$offset}}"""
    }

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "lab04_input_data",
      "startingOffsets" -> offset
    )

    val df = spark.read.format("kafka").options(kafkaParams).load

    val jsonString = df
      .select(col("value").cast("string"))
      .as[String]

    val parsed = spark
      .read
      .json(jsonString)
      .withColumn("date", to_date(from_unixtime(col("timestamp") / 1000)))
      .withColumn("date", from_unixtime(unix_timestamp(col("date"), "yyyy-MM-dd"), "yyyyMMdd"))
      .withColumn("p_date", col("date"))

    val views = parsed.filter(col("event_type") === "view")
    val purchases = parsed.filter(col("event_type") === "buy")

    val PARTITION_KEY = "p_date"

    s"hdfs dfs -rm -r -f ${dir}/view/*".!!
    s"hdfs dfs -rm -r -f ${dir}/buy/*".!!

    views
      .write
      .format("json")
      .mode("overwrite")
      .partitionBy(PARTITION_KEY)
      .save(dir + "/view")

    purchases
      .write
      .format("json")
      .mode("overwrite")
      .partitionBy(PARTITION_KEY)
      .save(dir + "/buy")

    s"hdfs dfs -rm -r -f ${dir}/view/_SUCCESS".!!
    s"hdfs dfs -rm -r -f ${dir}/buy/_SUCCESS".!!
  }
}

