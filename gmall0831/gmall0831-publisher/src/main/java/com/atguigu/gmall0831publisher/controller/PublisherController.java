package com.atguigu.gmall0831publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0831publisher.service.PublisherService;
import org.apache.commons.collections.map.HashedMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    public PublisherService service;

    @GetMapping("/realtime-total")
    public String getDauTotal(@RequestParam("date") String date)
    {
        List<Map<String,Object>> resultList=new ArrayList<>();
        //return service.getDauTotal(date);

        Map<String,Object> map1=new HashMap<>();
        map1.put("id","dau");
        map1.put("name","新增日活");
        map1.put("value",service.getDauTotal(date));
        resultList.add(map1);
        Map<String,Object> map2=new HashMap<>();
        map2.put("id","new_mid");
        map2.put("name","新增设备");
        map2.put("value",service.getDauTotal(date));
        resultList.add(map2);
        return JSON.toJSONString(resultList);
    }

    @GetMapping("/realtime-hour")
    public String getHourDau(@RequestParam("id") String id,@RequestParam("date") String date)
    {
        if ("dau".equals(id)){
            Map today = service.getDauHour(date);
            Map yesterday = service.getDauHour(getYesterday(date));

            Map<String,Map<String,Long>> resultMap=new HashMap<>();
            resultMap.put("yesterday",yesterday);
            resultMap.put("today",today);
            return JSON.toJSONString(resultMap);
        }else {
            return "";
        }


    }

    private String getYesterday(String date) {
        LocalDate ld = LocalDate.parse(date);
        return ld.minusDays(-1).toString();
    }

}
