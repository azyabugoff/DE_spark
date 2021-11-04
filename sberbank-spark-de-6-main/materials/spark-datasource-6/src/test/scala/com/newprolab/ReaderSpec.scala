package com.newprolab

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ReaderSpec extends AnyFlatSpec with should.Matchers with Spark {

  "Spark" should "work" in {
    val df = spark.range(10)
//    df.show()
    df.count shouldBe 10
  }

  it should "create new stream" in {
    val mySchema = StructType(StructField("bar", StringType) :: StructField("foo", IntegerType) :: Nil)
    val df =
      spark.readStream
        .schema(mySchema)
        .option("foo", "bar")
        .format("org.apache.spark.sql.npl.MyDatasourceProvider").load()

    df.printSchema()

    val sq = df.writeStream
      .format("console").trigger(Trigger.ProcessingTime("10 seconds")).start()

    Thread.sleep(20000)
    sq.stop()

//      .option("checkpointLocation", "/tmp/chk/test01")
  }
}
