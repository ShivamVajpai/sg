package com.sg.interview.BooleanChange.config

import com.sg.interview.BooleanChange.config.model.SG
import pureconfig._
import pureconfig.generic.auto._

object ConfigManager {

  def apply(configPath:String): ConfigManager = {
    var sg:SG = null
    if( configPath!=null && !configPath.isEmpty) {
      sg = ConfigSource.file(configPath).load[SG].right.get
    } else {
      sg = ConfigSource.default.load[SG].right.get
    }
    val cfgMgr = new ConfigManager()
    cfgMgr.sg= sg
    cfgMgr
  }
  /*
  def main(args: Array[String]): Unit = {
    val cm = apply(null)
    println(cm.sg.sg.input.get("file-path").orNull)
  }
}
*/

class ConfigManager {
  var sg:SG = _

  def getInputProperties(keyName:String):String = {
    sg.sg.input.get(keyName).orNull
  }
  def getSparkProperties(keyName:String): String = {
    sg.sg.spark.get(keyName).orNull
  }
}
