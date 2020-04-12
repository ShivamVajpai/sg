package com.sg.interview.BooleanChange.context

import com.sg.interview.BooleanChange.config.ConfigManager
import org.apache.spark.sql.{Dataset, Row}

trait ContextManager {

  def readInputDataframes: Dataset[Row]

  def processInputDataframes(inputDF: Dataset[Row]): Dataset[Row]

  def writeDataframes(outputDf: Dataset[Row]): Boolean

  def getCurrentContext: ContextManager

  def getConfigManager: ConfigManager

}
