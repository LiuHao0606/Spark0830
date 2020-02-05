package com.atguigu.gmall0831logger.controller

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0831.common.util.ConstantUtil
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{PostMapping, RequestParam, ResponseBody, RestController}

@RestController //@Controller+@ResponseBody
class LoggerController {
   @PostMapping(Array("/log"))
    def doLogger(@RequestParam("log") log :String)={
     //1.添加时间戳ts
     val logts: String = addTs(log)
     //2.落盘
     saveLog2File(logts)

     //3.写入kafka
    send2Kafka(logts)

      "ok"
    }

  @Autowired
  var templete : KafkaTemplate[String, String] = _

  def send2Kafka(logts:String)={
    var topic:String=ConstantUtil.STARTUP_TOPIC
    if(JSON.parseObject(logts).getString("logType")=="event"){
      topic=ConstantUtil.EVENT_TOPIC
    }

    templete.send(topic,logts)

  }

  private val logger: Logger = LoggerFactory.getLogger(classOf[LoggerController])
  def saveLog2File(logts:String)={
    logger.info(logts)
  }

  def addTs(log:String)={
    val jsonObj: JSONObject = JSON.parseObject(log)
    jsonObj.put("ts",System.currentTimeMillis())
    jsonObj.toJSONString
  }

}
