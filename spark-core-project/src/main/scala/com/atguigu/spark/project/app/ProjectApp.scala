package com.atguigu.spark.project.app


import com.atguigu.spark.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CartesianRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sourceRDD: RDD[String] = sc.textFile("C:\\Users\\LH\\Desktop\\2019_08_30\\01_spark\\02_资料\\spark-core数据\\user_visit_action.txt")

    val userActionRDD: RDD[UserVisitAction] = sourceRDD.map(x => {
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

    //1.需求 1: Top10 热门品类
    //val top10RDD: Array[CategoryCountInfo] = CatagoryTop10App.statCatagoryTop10App(sc, userActionRDD)

    //2.需求 2:Top10热门品类中每个品类的 Top10 活跃 Session 统计
    //CateforyTop10Session.calcCateforyTop10Session2(sc,top10RDD,userActionRDD)

    //3.需求 3: 页面单跳转化率统计
    PageConvertApp.statPageConvertRate(sc,userActionRDD,"1,2,3,4,5,6,7")
    sc.stop()
  }
}
