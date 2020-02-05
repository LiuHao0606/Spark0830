package com.atguigu.spark.project.app

import com.atguigu.spark.project.bean.{CategoryCountInfo, CategorySession, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object CateforyTop10Session {
    def calcCateforyTop10Session(sc:SparkContext,catagoryTop10Arr:Array[CategoryCountInfo],userActionRDD: RDD[UserVisitAction]) ={
        val cidTop10Arr: Array[String] = catagoryTop10Arr.map(_.categoryId)
        val rdd1: RDD[UserVisitAction] = userActionRDD.filter(u => cidTop10Arr.contains(u.click_category_id.toString))
        val rdd2: RDD[((Long, String), Int)] = rdd1.map(u => ((u.click_category_id, u.session_id), 1))
        val rdd3: RDD[(Long, (String, Int))] = rdd2.reduceByKey(_ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        val rdd4: RDD[(Long, Iterable[(String, Int)])] = rdd3.groupByKey()
        val rdd5: RDD[(Long, List[(String, Int)])] = rdd4.map {
            case (cid, it) => {
                (cid, it.toList.sortBy(-_._2).take(10))
            }
        }
        rdd5.collect.foreach(println)
    }

    def calcCateforyTop10Session2(sc:SparkContext,catagoryTop10Arr:Array[CategoryCountInfo],userActionRDD: RDD[UserVisitAction]) ={
        val cidTop10Arr: Array[Long] = catagoryTop10Arr.map(_.categoryId.toLong)
        val rdd1: RDD[UserVisitAction] = userActionRDD.filter(u => cidTop10Arr.contains(u.click_category_id))
        val rdd2: RDD[((Long, String), Int)] = rdd1.map(u => ((u.click_category_id, u.session_id), 1))
        val rdd3: RDD[(Long, CategorySession)] = rdd2.reduceByKey(new MyPartitoner(cidTop10Arr), _ + _).map {
            case ((cid, sid), count) => (cid, CategorySession(cid, sid, count))
        }
        val result: RDD[CategorySession] = rdd3.mapPartitions(it => {
            var set: mutable.TreeSet[CategorySession] = mutable.TreeSet[CategorySession]()
            var caid = 0L
            it.foreach {
                case (cid, u) =>
                    caid = cid
                    set += u
                    if (set.size > 10) {
                        set = set.take(10)
                    }
            }
            set.toIterator
        })
        result.collect.foreach(println)

    }
}

class MyPartitoner(cidTop10Arr: Array[Long]) extends Partitioner{

    private val map: Map[Long, Int] = cidTop10Arr.zipWithIndex.toMap

    override def numPartitions: Int = cidTop10Arr.size

    override def getPartition(key: Any): Int = {
        key match {
            case (cid:Long,_)=>map(cid)
            case _=>0
        }
    }
}