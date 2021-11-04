package com.newprolab

import org.apache.spark.sql.streaming.Trigger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class WriterSpec extends AnyFlatSpec with should.Matchers with Spark {
  "Spark" should "work" in {
    val df = spark.range(10)
    //    df.show()
    df.count shouldBe 10
  }

  it should "use custom sink" in {
    val df = spark.readStream.format("rate").load
    val sq = df.writeStream
      .option("checkpointLocation", "/tmp/chk/test0")
      .format("org.apache.spark.sql.npl.MyDatasinkProvider").trigger(Trigger.ProcessingTime("10 seconds")).start()
    Thread.sleep(20000)
    sq.stop()
  }
}
