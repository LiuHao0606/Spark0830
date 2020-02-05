package com.atguigu.project.bean

case class CategorySession(categoryId: String,
                           sessionId: String,
                           clickCount: Long) extends Ordered[CategorySession] {
  //从大倒小排序
  override def compare(that: CategorySession): Int ={
    if (that.clickCount-this.clickCount==0){
        1
    }else{
      (that.clickCount-this.clickCount).toInt
    }

  }

}

