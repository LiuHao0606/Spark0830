package com.atguigu.day01.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

class MySumUDF extends UserDefinedAggregateFunction{

  //定义聚合函数输入数据的类型
  override def inputSchema: StructType = StructType(StructField("col",DoubleType) :: Nil)
  //缓冲区的数据类型
  override def bufferSchema: StructType = StructType(StructField("sum",DoubleType) :: Nil)
  //最终返回值数据类型
  override def dataType: DataType = DoubleType
  //确定性：相同的输入是否应该返回相同的输出
  override def deterministic: Boolean = true
  //初始化：定制缓冲区的零值
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0)=0D
  //分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //buffer(0)=buffer.getDouble(0)+input.getDouble(0)
    buffer(0)=buffer.getDouble(0)+input.getAs[Double](0)
  }
  //分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getDouble(0)+buffer2.getDouble(0)
  }
  //返回最终的值
  override def evaluate(buffer: Row): Any = buffer(0)
}
