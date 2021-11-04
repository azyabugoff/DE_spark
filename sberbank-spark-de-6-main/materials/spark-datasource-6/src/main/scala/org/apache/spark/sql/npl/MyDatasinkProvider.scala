package org.apache.spark.sql.npl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode

class MyDatasinkProvider extends StreamSinkProvider {
  def createSink(sqlContext: SQLContext,
                 parameters: Map[String, String],
                 partitionColumns: Seq[String],
                 outputMode: OutputMode): Sink = {
    new MyDatasink()
  }
}

class MyDatasink() extends Sink {
  def addBatch(batchId: Long, data: DataFrame): Unit = {
    val schema = data.schema
    val rdd: RDD[InternalRow] = data.queryExecution.toRdd
    rdd.foreachPartition { p =>
      while(p.hasNext) {
        val next: InternalRow = p.next()
        val fields = next.toSeq(schema)
        println(fields.mkString(","))
      }
    }
  }
}