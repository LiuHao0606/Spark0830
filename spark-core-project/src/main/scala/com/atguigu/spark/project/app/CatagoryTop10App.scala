package com.atguigu.spark.project.app

import com.atguigu.spark.project.acc.MyCatagoryTop10Acc
import com.atguigu.spark.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CatagoryTop10App {
    def statCatagoryTop10App(sc:SparkContext,userActionRDD: RDD[UserVisitAction])  ={

        val acc = new MyCatagoryTop10Acc
        sc.register(acc,"test1-top10累加器")
        userActionRDD.foreach(u=>acc.add(u))

        val rdd2: Map[String, Map[(String, String), Long]] = acc.value.groupBy(_._1._1)
        val rdd3: Array[CategoryCountInfo] = rdd2.map {
            case (cid, m) =>
                CategoryCountInfo(
                    cid,
                    m.getOrElse((cid, "click"), 0L),
                    m.getOrElse((cid, "order"), 0L),
                    m.getOrElse((cid, "pay"), 0L)
                )
        }.toArray
          .sortBy(t => (-t.clickCount, -t.orderCount, -t.payCount))
          .take(10)
        //rdd3.foreach(println)
        rdd3
    }
}
