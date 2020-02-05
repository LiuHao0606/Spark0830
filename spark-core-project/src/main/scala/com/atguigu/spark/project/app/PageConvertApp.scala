package com.atguigu.spark.project.app

import java.text.DecimalFormat

import com.atguigu.spark.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageConvertApp {
  def statPageConvertRate(sc:SparkContext,userActionRDD: RDD[UserVisitAction],pages:String) ={
    val splits: Array[String] = pages.split(",")

    val prepages: Array[String]=splits.slice(0,splits.length-1)
    val postpages: Array[String] = splits.slice(1, splits.length)
    val needPageFlow: Array[String] = prepages.zip(postpages).map(t => t._1 + "->" + t._2)

    //计算每个页面的点击量
    val everyPageCount: collection.Map[Long, Long] = userActionRDD
      .filter(u => splits.contains(u.page_id.toString))
      .map(u => (u.page_id, 1))
      .countByKey()

    //计算每页的跳转流
    val totalPageFlows: collection.Map[String, Long] = userActionRDD.filter(u => splits.contains(u.page_id.toString))
      .groupBy(u => u.session_id)
      .flatMap {
        case (_, it) =>
          val list: List[UserVisitAction] = it.toList.sortBy(u => u.action_time)
          val preList: List[UserVisitAction] = list.slice(0, list.length - 1)
          val postList: List[UserVisitAction] = list.slice(1, list.length)
          val pageCountList: List[String] = preList.zip(postList).map {
            case (pre, post) => pre.page_id + "->" + post.page_id
          }
          pageCountList.filter(flow => needPageFlow.contains(flow)).map((_, 1))
      }.countByKey()

    //计算跳转率
    val result: collection.Map[String, String] = totalPageFlows.map {
      case (flow, count) =>
        var page = flow.split("->")(0)
        var rate = count.toDouble / everyPageCount.getOrElse(page.toLong, Long.MaxValue)
        val formatter = new DecimalFormat(".00%")
        (page, formatter.format(rate))
    }

    println(result)

  }
}
