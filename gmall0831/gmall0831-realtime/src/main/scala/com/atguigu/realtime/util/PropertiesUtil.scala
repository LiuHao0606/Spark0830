package com.atguigu.realtime.util

import java.io.InputStream
import java.util.Properties

import scala.collection.mutable

object PropertiesUtil {
  private val map: mutable.Map[String, Properties] = mutable.Map[String, Properties]()
  def getProperty(propFile:String,propName:String) ={

    val props: Properties = map.getOrElseUpdate(propFile, {
      val is: InputStream = ClassLoader.getSystemResourceAsStream(propFile)
      val props = new Properties()
      props.load(is)
      props
    })
    props.getProperty(propName)
  }

  def main(args: Array[String]): Unit = {
    //println(getProperty("config.properties", "kafka.broker.list"))
  }
}
