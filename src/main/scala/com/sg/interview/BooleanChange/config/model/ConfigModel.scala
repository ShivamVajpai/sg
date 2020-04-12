package com.sg.interview.BooleanChange.config.model
import pureconfig._
import pureconfig.generic.auto._


case class SG(sg:SGProperties)

case class SGProperties(
                         input:Map[String,String],
                         spark:Map[String,String]
                       )
class ConfigModel {

}
