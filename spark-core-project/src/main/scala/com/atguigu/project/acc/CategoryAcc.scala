package com.atguigu.project.acc

import com.atguigu.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

class CategoryAcc extends AccumulatorV2[UserVisitAction,Map[(String,String),Long]]{

  private var map: Map[(String, String), Long] = Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
    val acc = new CategoryAcc
    acc.map=map
    acc
  }

  override def reset(): Unit = map= Map[(String, String), Long]()

  override def add(v: UserVisitAction): Unit = {
    if(v.click_category_id != -1){
        map += (v.click_category_id.toString,"click")-> (map.getOrElse((v.click_category_id.toString,"click"),0L)+1L)
    }else if(v.order_category_ids!="null"){
      val strings: Array[String] = v.order_category_ids.split(",")
      strings.foreach(s=>{
        map += (s,"order")-> (map.getOrElse((s,"order"),0L)+1L)
      })
    }else if(v.pay_category_ids!="null"){
      val strings: Array[String] = v.pay_category_ids.split(",")
      strings.foreach(s=>{
        map += (s,"pay")-> (map.getOrElse((s,"pay"),0L)+1L)
      })
    }

  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = {
    val o: CategoryAcc = other.asInstanceOf[CategoryAcc]
    o.map.foreach{
      case (k,v)=>map +=k->(map.getOrElse(k,0L)+v)
    }
  }

  override def value: Map[(String, String), Long] = map
}
