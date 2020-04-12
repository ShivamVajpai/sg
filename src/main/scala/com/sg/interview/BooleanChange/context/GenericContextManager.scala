package com.sg.interview.BooleanChange.context

import com.sg.interview.BooleanChange.config.ConfigManager
import com.sg.interview.BooleanChange.processor.MnemonicDataProcessor
import com.sg.interview.BooleanChange.reader.MnemonicDataReader
import com.sg.interview.BooleanChange.writer.MnemonicOutputDataWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}


abstract class GenericContextManager extends ContextManager {

  var spark:SparkSession = _
  var mnemonicDataReader: MnemonicDataReader = _
  var mnemonicDataProcessor: MnemonicDataProcessor = _
  var mnemonicOutputDataWriter: MnemonicOutputDataWriter = _
  var configManager: ConfigManager = _

  override def readInputDataframes: Dataset[Row]
    = mnemonicDataReader.readInputData

  override def processInputDataframes(inputDF: Dataset[Row]): Dataset[Row]
    = mnemonicDataProcessor.processData(inputDF)

  override def writeDataframes(outputDf: Dataset[Row]): Boolean
    = mnemonicOutputDataWriter.writeProcessedData(outputDf)

  override def getConfigManager: ConfigManager = configManager
}
