package com.sg.interview.BooleanChange.processor

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



object SparkDSLMnemonicDataProcessor {
  def apply(spark: SparkSession): SparkDSLMnemonicDataProcessor = {
    val sparkDSLMnemonicDataProcessor = new SparkDSLMnemonicDataProcessor()
    sparkDSLMnemonicDataProcessor.spark = spark
    sparkDSLMnemonicDataProcessor
  }
}

class SparkDSLMnemonicDataProcessor extends MnemonicDataProcessor {

  var spark: SparkSession = _

  def processData(inputDf: Dataset[Row]): Dataset[Row] = {

    inputDf.printSchema()
    val stagingDf=inputDf
      .withColumn("new_data",lead(col("data"),1).over(Window.partitionBy("sensor","mnemonic").orderBy("timestamp")))
      .withColumn("new_timestamp",lead(col("timestamp"),1).over(Window.partitionBy("sensor","mnemonic").orderBy("timestamp")))
    stagingDf.select("sensor","mnemonic","data","new_data","timestamp","new_timestamp").filter(col("data") =!= col("new_data"))
    //stagingDf
  }
}
