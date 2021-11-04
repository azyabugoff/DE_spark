package com.newprolab

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait Spark {

  lazy val spark: SparkSession =
    SparkSession.getActiveSession
      .getOrElse(
        SparkSession
          .builder().appName("test").master("local[*]").getOrCreate()
      )

  lazy val sc: SparkContext = spark.sparkContext

}
