package com.atguigu.gmall083001.gmall083001logger.controller

import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{PostMapping, RequestParam}

@Controller
class LoggerController {
  @PostMapping(Array("/log"))
    def logger(@RequestParam("log") log :String)={
          "ok"
    }
}
