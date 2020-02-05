package com.atguigu.day02

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

class CityRemarkUDAF1 extends UserDefinedAggregateFunction {

  // StringType ... 北京
  override def inputSchema: StructType = StructType(StructField("city_name", StringType) :: Nil)

  // 缓冲数据的数据类型
  override def bufferSchema: StructType =
    StructType(StructField("city_count", MapType(StringType, LongType)) :: StructField("total", LongType) :: Nil)


  // 最终结果的类型 北京21.2%，天津13.2%，其他65.6%
  override def dataType: DataType = StringType

  // 确定性
  override def deterministic: Boolean = true

  // 缓存初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 缓存map对象  北京->1000  天津->200  ...
    buffer(0) = Map[String, Long]()
    // 某地区, 某个商品总的点击量
    buffer(1) = 0L
  }

  // 分区内的聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val cityName: String = input.getString(0)
      var cityCountMap: collection.Map[String, Long] = buffer.getMap[String, Long](0)
      cityCountMap += cityName -> (cityCountMap.getOrElse(cityName, 0L) + 1L)
      buffer(0) = cityCountMap
      buffer(1) = buffer.getLong(1) + 1L // 更新点击的总数
    }
  }

  // 分区间的聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)

    // 把map2 的数据合并到map1中, 然后再重新赋值到 buffer1(0)
    val resultMap: collection.Map[String, Long] = map2.foldLeft(map1) {
      case (map, (cityName, count)) => map + (cityName -> (map.getOrElse(cityName, 0L) + count))
    }
    buffer1(0) = resultMap
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  // 最后的返回值
  // 北京21.2%，天津13.2%，其他65.6%
  override def evaluate(buffer: Row): String = {
    val cityCountMap: collection.Map[String, Long] = buffer.getMap[String, Long](0)
    val total: Long = buffer.getLong(1)
    var top2List: List[CityRemark] = cityCountMap.toList.sortBy(-_._2).take(2).map{
      case (cityName, count) => CityRemark(cityName, count.toDouble / total)
    }
    top2List :+= CityRemark("其他", top2List.foldLeft(1D)((rate, cr) => rate - cr.rate))

    top2List.mkString(", ")

  }
}
