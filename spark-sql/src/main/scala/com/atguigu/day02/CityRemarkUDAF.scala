package com.atguigu.day02

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, MapType, StringType, StructField, StructType}

class CityRemarkUDAF extends UserDefinedAggregateFunction{

  //传入值为： 城市名
  override def inputSchema: StructType = StructType(StructField("cityneme",StringType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("citycount",MapType(StringType,LongType)) :: StructField("total",LongType) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  //单个城市-->点击量  如北京-->1000
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //每个地区商品点击量
    buffer(0)=Map[String,Long]()
    //某地区单个商品点击总量
    buffer(1)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(input.isNullAt(0)!=null){
      val cityname: String = input.getString(0)
      var map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
      map += cityname -> (map.getOrElse(cityname,0L)+1L)
      buffer(0)=map

      buffer(1)=buffer.getLong(1)+1L

    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val map2: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val resultMap: collection.Map[String, Long] = map2.foldLeft(map1) {
      case (map, (cityname, count)) =>
        val m: collection.Map[String, Long] = map + (cityname -> (map.getOrElse(cityname, 0L) + count))
        m
    }
    buffer1(0)=resultMap
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)

  }

  override def evaluate(buffer: Row): Any = {
    val cmap: collection.Map[String, Long] = buffer.getMap[String, Long](0)
    val total: Long = buffer.getLong(1)
    var list: List[CityRemark] = cmap.toList.sortBy(-_._2).take(2).map {
      case (cityname, count) => CityRemark(cityname, count.toDouble / total)
    }

    val otherrate: Double = list.foldLeft(1D)((r, cr) => r - cr.rate)
    val otherCityRemark: CityRemark = CityRemark("其他", otherrate)

    list :+= otherCityRemark
    //val list2: List[CityRemark] = list.:+(otherCityRemark)



    list.mkString(",")

  }
}

case class CityRemark(cityname:String,rate:Double){
  override def toString: String = s"$cityname:$rate"
}