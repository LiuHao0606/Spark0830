package com.atguigu.canal.utils

import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType

object CanalHandler {
  def handler(tableName: String, rowDatasList: util.List[CanalEntry.RowData], eventType: CanalEntry.EventType) = {
    import  scala.collection.JavaConversions._
    if (tableName=="order_info" && eventType==EventType.INSERT && !rowDatasList.isEmpty){
        for(rowDate <- rowDatasList){
          val jsonObject = new JSONObject()
          val columnsList: util.List[CanalEntry.Column] = rowDate.getAfterColumnsList
          for(column <- columnsList){
            val key: String = column.getName
            val value: String = column.getValue
            jsonObject.put(key,value)
          }
          println(jsonObject.toJSONString)
        }

    }
  }

}
