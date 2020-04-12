package com.sg.interview.BooleanChange.reader

import com.sg.interview.BooleanChange.context.ContextFactory
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CSVMnemonicDataReader {
  def apply(spark: SparkSession): CSVMnemonicDataReader = {
    val reader = new CSVMnemonicDataReader()
    reader.spark = spark
    reader
  }
}


class CSVMnemonicDataReader extends MnemonicDataReader {
  private var spark: SparkSession = _

  override def readInputData(): Dataset[Row] = {
    val path = ContextFactory.getCurrentContext.getConfigManager.
      getInputProperties("file-path")
    spark.sqlContext.read.option("HEADER","True").option("INFERSCHEMA","True").csv(path)
  }
}
