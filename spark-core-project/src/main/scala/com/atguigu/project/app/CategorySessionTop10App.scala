package com.atguigu.project.app

import com.atguigu.project.bean.{CategoryCountInfo, CategorySession, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object CategorySessionTop10App {
  def stateCategory(sc:SparkContext,userAisitActionRDD:RDD[UserVisitAction],top10List:List[CategoryCountInfo])={
    //需求 2: Top10热门品类中每个品类的 Top10 活跃 Session 统计
    val top10CidList: List[String] = top10List.map(_.categoryId)
    val rdd1: RDD[UserVisitAction] = userAisitActionRDD.filter(
      u => top10CidList.contains(u.click_category_id.toString))
    val rdd2: RDD[((Long, String), Int)] = rdd1.map(u => ((u.click_category_id, u.session_id), 1))
    val rdd3: RDD[(Long, (String, Int))] = rdd2.reduceByKey(_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count)) }
    val rdd4: RDD[(Long, Iterable[(String, Int)])] = rdd3.groupByKey()

    val rdd5: RDD[(Long, List[(String, Int)])] = rdd4.map {
      case (cid, it) => (cid, it.toList.sortBy(-_._2).take(10))}
    rdd5
  }

  def stateCategory1(sc:SparkContext,userAisitActionRDD:RDD[UserVisitAction],top10List:List[CategoryCountInfo])={
    //需求 2: Top10热门品类中每个品类的 Top10 活跃 Session 统计
    val top10CidList: List[String] = top10List.map(_.categoryId)
    val rdd1: RDD[UserVisitAction] = userAisitActionRDD.filter(
      u => top10CidList.contains(u.click_category_id.toString))
    val rdd2: RDD[((Long, String), Int)] = rdd1.map(u => ((u.click_category_id, u.session_id), 1))
    val rdd3: RDD[(Long, (String, Int))] = rdd2.reduceByKey(_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count)) }
    val rdd4: RDD[(Long, Iterable[(String, Int)])] = rdd3.groupByKey()

    top10CidList.foreach(id=>{
      val countRDD: RDD[(Long, Iterable[(String, Int)])] = rdd4.filter(t => t._1 == id.toLong)
      val sidCountRDD: RDD[(String, Int)] = countRDD.flatMap(t => t._2)
      val oneTop10List: Array[CategorySession] = sidCountRDD.sortBy(t => t._2, false).take(10).map {
        case (sid, count) => CategorySession(id, sid, count)
      }
      oneTop10List.foreach(println)
      println("-----------------")
    })
  }

  def stateCategory2(sc:SparkContext,userAisitActionRDD:RDD[UserVisitAction],top10List:List[CategoryCountInfo])={
    //需求 2: Top10热门品类中每个品类的 Top10 活跃 Session 统计
    val top10CidList: List[String] = top10List.map(_.categoryId)
    val rdd1: RDD[UserVisitAction] = userAisitActionRDD.filter(
      u => top10CidList.contains(u.click_category_id.toString))
    val rdd2: RDD[((Long, String), Int)] = rdd1.map(u => ((u.click_category_id, u.session_id), 1))

    val rdd3: RDD[CategorySession] = rdd2.reduceByKey(new MyPartitioner(top10CidList),_ + _).map {
      case ((cid, sid), count) => CategorySession(cid.toString, sid, count)
    }

    val resRDD: RDD[CategorySession] = rdd3.mapPartitions(it => {
      var treeSet: mutable.TreeSet[CategorySession] = mutable.TreeSet[CategorySession]()
      it.foreach(u => {
        treeSet += u
        if (treeSet.size > 10) {
          treeSet=treeSet.take(10)
        }
      })

      treeSet.toIterator
    })

    resRDD.collect().foreach(println)

  }
}
class MyPartitioner(top10CidList:List[String]) extends Partitioner{
  private val indexList: Map[String, Int] = top10CidList.zipWithIndex.toMap

  override def numPartitions: Int = top10CidList.size

  override def getPartition(key: Any): Int = {
     key match {
       case (cid, s)=>indexList(cid.toString)
     }
  }
}
