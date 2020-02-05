package com.atguigu.realtime.test

import java.time.LocalDate

object Demo5 {

  implicit def intToRichDate(day:Int)=new RichDate(day)

  def main(args: Array[String]): Unit = {
      val ago:String="ago"
      val dt1: String = 2 days ago
      val dt2: String = 2 days "later"
    println(dt1)
    println(dt2)

  }
}
class RichDate(day: Int) {
    def days(dt:String)={
      val today: LocalDate = LocalDate.now()
      if("ago"==dt){
        today.plusDays(-day).toString
      }  else{
        today.plusDays(day).toString
      }
    }
}
