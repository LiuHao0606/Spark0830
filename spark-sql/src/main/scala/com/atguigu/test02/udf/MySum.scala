package com.atguigu.test02.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

class MySum extends UserDefinedAggregateFunction{

  //输入数据类型
  override def inputSchema: StructType = StructType(StructField("col",DoubleType)::Nil)

  //缓冲区的类型
  override def bufferSchema: StructType = StructType(StructField("sum",DoubleType)::Nil)

  //聚合后的数据类型
  override def dataType: DataType = DoubleType

  //确定性判断：相同的输入是否具有相同的是输出
  override def deterministic: Boolean = true

  //对缓冲区进行初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0D
  }

  //分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //对传入数据进行非空判断
    if (!input.isNullAt(0)){
      //传入的值
      val value: Double = input.getDouble(0)
      //更新缓冲区
      buffer(0)=buffer.getDouble(0)+value
    }
  }

  //分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //传入的值
      val value: Double = buffer2.getDouble(0)
      //更新缓冲区
      buffer1(0)=buffer1.getDouble(0)+value
  }

  //返回聚合后的值
  override def evaluate(buffer: Row): Any = buffer(0)
}
