package com.sg.interview.BooleanChange.processor

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkSQLMnemonicDataProcessor {
  def apply(spark: SparkSession): SparkSQLMnemonicDataProcessor = {
    val sparkSQLMnemonicDataProcessor = new SparkSQLMnemonicDataProcessor()
    sparkSQLMnemonicDataProcessor.spark = spark
    sparkSQLMnemonicDataProcessor
  }
}

class SparkSQLMnemonicDataProcessor extends MnemonicDataProcessor {

  var spark: SparkSession = _

  def processData(inputDf: Dataset[Row]): Dataset[Row] = {
    inputDf.createOrReplaceTempView("SampleTable")
    spark.sql(
      """select sensor, Mnemonic, data, next_data, timestamp, next_timeStamp
        |from
        |( select *,
        |LEAD(data) OVER( PARTITION BY sensor, Mnemonic ORDER BY timestamp) AS next_data,
        |LEAD(timestamp) OVER( PARTITION BY sensor, Mnemonic ORDER BY timestamp) AS next_timeStamp
        |from SampleTable
        |) a
        | where data <> next_data""".stripMargin)
  }
}
