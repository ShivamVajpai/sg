package com.sg.interview.BooleanChange

import org.apache.hadoop.mapred.Master
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession.builder

object scala {
  def main(args: Array[String]): Unit
  =
  {
    val conf = new SparkConf().setMaster("local").setAppName("BooleanChange")
    val spark = org.apache.spark.sql.SparkSession.builder.config(conf).getOrCreate
    val sqlC = spark.sqlContext
    val sc = spark.sparkContext

    val sampleDF = sqlC.read.option("HEADER","True").option("INFERSCHEMA","True").csv("src/main/resources/sample.csv")
    //sampleDF.printSchema()
    //sampleDF.collect.foreach(println)

    sampleDF.createOrReplaceTempView("SampleTable")
    val outputDF = spark.sql(
      """select sensor, Mnemonic, data, next_data, timestamp, next_timeStamp
        |from
        |( select *,
        |LEAD(data) OVER( PARTITION BY sensor, Mnemonic ORDER BY timestamp) AS next_data,
        |LEAD(timestamp) OVER( PARTITION BY sensor, Mnemonic ORDER BY timestamp) AS next_timeStamp
        |from SampleTable
        |) a
        | where data <> next_data""".stripMargin)
    outputDF.collect.foreach(println)

  }
}
