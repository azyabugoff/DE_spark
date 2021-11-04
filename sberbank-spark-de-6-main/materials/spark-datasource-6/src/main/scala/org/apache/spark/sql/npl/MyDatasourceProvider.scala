package org.apache.spark.sql.npl

import com.newprolab.Spark
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

// Datasource v1 API
class MyDatasourceProvider extends StreamSourceProvider with Logging {

  val defaultSchema: StructType = StructType(StructField("id", IntegerType) :: Nil)

  def sourceSchema(sqlContext: SQLContext, // ex SparkSession in Spark 1.x
                   schema: Option[StructType],
                   providerName: String,
                   parameters: Map[String, String]): (String, StructType) = {

    log.info(s"sourceSchema call, providerName=$providerName")
    log.debug(s"params: ${parameters.mkString(",")}")

    ("MyDatasource", schema.getOrElse(defaultSchema))
  }

  def createSource(sqlContext: SQLContext,
                   metadataPath: String,
                   schema: Option[StructType],
                   providerName: String,
                   parameters: Map[String, String]): Source = {

    log.info(s"createSource call, providerName=$providerName")
    log.debug(s"params: ${parameters.mkString(",")}")
    log.debug(s"metadataPath: $metadataPath")
    log.debug(s"schema=${schema.map(_.simpleString)}")
    new MyDatasource(dSchema = schema.getOrElse(throw new IllegalArgumentException("No schema provided!")))
  }
}

class MyDatasource(dSchema: StructType) extends Source with Logging with Spark {

  var i = 0

  def schema: StructType = dSchema

  def getOffset: Option[Offset] = {
//    Option.empty[Offset]
    val thisOffset = Option(new MyOffset(i))
    log.debug(s"getOffset call returns $thisOffset")
    thisOffset
  }

  def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    log.debug(s"getBatch call with offset range: $start >> $end")
//    val rdd: RDD[InternalRow] = sc.parallelize(rowList)
    val rdd: RDD[InternalRow] = new RDD[InternalRow](sc, Nil) {
      def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
        val ir = InternalRow.fromSeq(UTF8String.fromString(0.toString) :: 0 :: Nil)
        (0 to 9).map(_ => ir).toIterator
      }

      protected def getPartitions: Array[Partition] = Array(MyPartition(0))
    }
    val schema: StructType = dSchema
    val IsStreaming: Boolean = true
    i += 1
    spark.internalCreateDataFrame(rdd, schema, IsStreaming)
  }

  def stop(): Unit = ???
}

class MyOffset(i: Int) extends Offset {
  def json(): String = i.toString
}

case class MyPartition(index: Int) extends Partition