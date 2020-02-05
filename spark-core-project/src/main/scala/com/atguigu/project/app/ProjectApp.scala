package com.atguigu.project.app

import com.atguigu.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyAcc").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lineRDD: RDD[String] =
      sc.textFile("C:\\Users\\LH\\Desktop\\2019_08_30\\01_spark\\02_资料\\spark-core数据\\user_visit_action.txt")
    val userActionRDD: RDD[UserVisitAction] = lineRDD.map(x => {
      val splits: Array[String] = x.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong
      )
    })


    //1.Top10热门品类:按照点击，下单，支付数量进行排序，取前十
    val top10ActiveList: List[CategoryCountInfo] = CategoryTop10App.stateCategory(sc, userActionRDD)
    println(top10ActiveList)
    //2.Top10热门品类中每个品类的 Top10 活跃 Session 统计
//    val top10SessionList: RDD[(Long, List[(String, Int)])] = CategorySessionTop10App.stateCategory(sc, userActionRDD, top10ActiveList)
//    top10SessionList.collect().foreach(println)
   // CategorySessionTop10App.stateCategory2(sc, userActionRDD, top10ActiveList)

    //需求 3: 页面单跳转化率统计

  


    sc.stop()
  }
}
