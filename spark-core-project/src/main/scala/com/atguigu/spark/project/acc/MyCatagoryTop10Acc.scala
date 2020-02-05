package com.atguigu.spark.project.acc

import com.atguigu.spark.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

class MyCatagoryTop10Acc extends AccumulatorV2[UserVisitAction,Map[(String,String),Long]]{
  private var map: Map[(String, String), Long] = Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
    val acc = new MyCatagoryTop10Acc
    acc.map ++= map
    acc
  }

  override def reset(): Unit = map=Map[(String, String), Long]()

  override def add(v: UserVisitAction): Unit = {
    if(v.click_category_id != -1){
     map += (v.click_category_id.toString,"click")->(map.getOrElse((v.click_category_id.toString,"click"),0L)+1)
    }else if(v.order_category_ids != "null"){
      val splits: Array[String] = v.order_category_ids.split(",")
      splits.foreach(cid=>{
        map += (cid,"order")->(map.getOrElse((cid,"order"),0L)+1)
      })
    }else if(v.pay_category_ids != "null"){
      val splits: Array[String] = v.pay_category_ids.split(",")
      splits.foreach(cid=>{
        map += (cid,"pay")->(map.getOrElse((cid,"pay"),0L)+1)
      })
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = {
    other match {
      case o:MyCatagoryTop10Acc=>
            o.map.foreach{
              case (k, c) => map += k -> (o.map.getOrElse(k,0L)+map.getOrElse(k,0L))
            }
      case _=>throw new UnsupportedOperationException
    }
  }

  override def value: Map[(String, String), Long] = map
}
