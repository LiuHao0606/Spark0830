package com.atguigu.test02.project

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

object RemarkUDAF extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = StructType( StructField("cityname",StringType) :: Nil)

  //设置缓冲区类型
  override def bufferSchema: StructType =
    StructType(StructField("map",MapType(StringType,LongType)) :: StructField("total",LongType)  ::  Nil )

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=Map[StringType,LongType]()
    buffer(1)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      //总数加1
      buffer(1)=buffer.getLong(1)+1L
      //每个城市加
      val cityName=input.getString(0)
      var map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
      buffer(0)= map+(cityName -> (map.getOrElse(cityName,0L)+1L))
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //更新总数
    buffer1(1)=buffer1.getLong(1) + buffer2.getLong(1)
    //更新每个城市记录
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)
    buffer1(0)=map1.foldLeft(map2){
      case (map,(cityname,count))=>
        map + (cityname->(map.getOrElse(cityname,0L)+count))
    }
  }

  override def evaluate(buffer: Row): Any = {
    var cityCountMap: collection.Map[String, Long] = buffer.getMap[String, Long](0)
    val total=buffer.getLong(1)

    //按照点击数排序取前2名
    val top2List: List[(String, Long)] = cityCountMap.toList.sortBy(-_._2).take(2)
    val top2Remark: List[CityRemark] = top2List.map {
      case (cityname, count) => CityRemark(cityname, count.toDouble / total)
    }
    val result: List[CityRemark] = top2Remark :+ CityRemark("其他",top2Remark.foldLeft(1D)(_-_.rate))
    result.mkString(",")
   // CityRemark("其他",(1D :/ top2Remark)(_-_.rate))

  }
}
case class CityRemark(cityName:String,rate:Double){
  private val df = new DecimalFormat(".00%")
  override def toString: String = s"$cityName:${df.format(rate)}"
}