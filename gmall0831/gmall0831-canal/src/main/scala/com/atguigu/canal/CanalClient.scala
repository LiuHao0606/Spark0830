package com.atguigu.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.atguigu.canal.utils.CanalHandler
import com.google.protobuf.ByteString

object CanalClient {
  def main(args: Array[String]): Unit = {
    //1.建立canal连接
    val conn: CanalConnector = CanalConnectors.newSingleConnector(
      new InetSocketAddress("hadoop103",11111),"example","","")
    conn.connect()//连接到canal
    conn.subscribe("gmall.*")//监听gmall下所有的表
    //2.读数据
    while(true){
      //get一次获取一个Message，一个Message表示一批数据，看成多条sql语句执行的结果
      //一个Message封装多个Entry，一个Entry可以看成一条sql语句执行的多条结果
      //一个Entry封装一个storeValue，可以看到是数据的序列化形式
      //storeValue里面可以解析出RowChange
      //一个RowChange有多个RowData，一个RowData封装了一行数据
      //一个RowData有多列column
      val msg: Message = conn.get(100)
      val entries: util.List[CanalEntry.Entry] = msg.getEntries
      import scala.collection.JavaConversions._
      if(entries != null && !entries.isEmpty){
        for(entry <- entries){
          val entryType: CanalEntry.EntryType = entry.getEntryType
          //如果EntryType涉及到行变化才处理
          if(entryType == EntryType.ROWDATA){
            val storeValue: ByteString = entry.getStoreValue
            val rowChange: RowChange = RowChange.parseFrom(storeValue)
            val rowDatasList: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
            CanalHandler.handler(entry.getHeader.getTableName,rowDatasList,rowChange.getEventType)
          }
        }
      }else{
        println("暂无数据，2s之后重新获取")
        Thread.sleep(2000)
      }

    }

  }
}
