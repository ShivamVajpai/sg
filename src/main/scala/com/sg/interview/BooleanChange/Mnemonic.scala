package com.sg.interview.BooleanChange

import com.sg.interview.BooleanChange.context.ContextFactory
import com.sg.interview.BooleanChange.processor.MnemonicDataProcessor
import com.sg.interview.BooleanChange.reader.MnemonicDataReader
import com.sg.interview.BooleanChange.writer.MnemonicOutputDataWriter
import org.apache.spark.sql.SparkSession

object Mnemonic {
  def main(args: Array[String]): Unit = {

    val env = System.getenv("mnemonic.lifecycle")

    val contextManager = ContextFactory.initForProdDSLWithLocalSparkSession()
    //val contextManager = ContextFactory.initForProdSQLWithLocalSparkSession()

    val inputDf = contextManager.readInputDataframes
    val outputDf = contextManager.processInputDataframes(inputDf)
    val dataWritten = contextManager.writeDataframes(outputDf)

    println(s"The data has been processed : ${dataWritten}")
  }
}
