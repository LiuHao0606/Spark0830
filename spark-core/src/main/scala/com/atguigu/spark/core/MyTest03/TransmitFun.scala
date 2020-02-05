package com.atguigu.spark.core.MyTest03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransmitFun {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Searcher]))

    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    val searcher = new Searcher("hello")
    val result: RDD[String] = searcher.getMatchedRDD1(rdd)
    result.collect.foreach(println)
  }
}
//需求: 在 RDD 中查找出来包含 query 子字符串的元素

// query 为需要查找的子字符串
class Searcher(val query: String) extends Serializable  {
  // 判断 s 中是否包括子字符串 query
  def isMatch(s : String) = {
    s.contains(query)
  }
  // 过滤出包含 query字符串的字符串组成的新的 RDD
  def getMatchedRDD1(rdd: RDD[String]) ={
    rdd.filter(isMatch)  //
  }
  // 过滤出包含 query字符串的字符串组成的新的 RDD
  def getMatchedRDD2(rdd: RDD[String]) ={
    val q=query
    rdd.filter(_.contains(q))
  }

}
/*
1.方法或函数的传递
  对象必须序列化
2.属性值的传递
  a.对象序列化
  b.传递局部变量
 */
