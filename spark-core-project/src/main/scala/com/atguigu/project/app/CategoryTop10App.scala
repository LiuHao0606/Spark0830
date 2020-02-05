package com.atguigu.project.app

import com.atguigu.project.acc.CategoryAcc
import com.atguigu.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CategoryTop10App {
    def stateCategory(sc:SparkContext,userAisitActionRDD:RDD[UserVisitAction]) ={

        val acc = new CategoryAcc
        sc.register(acc)

        userAisitActionRDD.foreach(acc.add)

        val map1: Map[String, Map[(String, String), Long]] = acc.value.groupBy(t => t._1._1)

        val list2: List[CategoryCountInfo] = map1.map {
            case (id, tupleToLong) => CategoryCountInfo(id,
                tupleToLong.getOrElse((id, "click"), 0),
                tupleToLong.getOrElse((id, "order"), 0),
                tupleToLong.getOrElse((id, "pay"), 0))
        }.toList

        val sortList: List[CategoryCountInfo] = list2.sortBy(x => (x.clickCount, x.orderCount, x.payCount))(Ordering.Tuple3(Ordering.Long.reverse,
            Ordering.Long.reverse, Ordering.Long.reverse)).take(10)
        sortList
    }
}

