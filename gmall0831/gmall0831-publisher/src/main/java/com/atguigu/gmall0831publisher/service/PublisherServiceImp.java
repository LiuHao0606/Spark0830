package com.atguigu.gmall0831publisher.service;

import com.atguigu.gmall0831publisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImp implements PublisherService {

    @Autowired
    public DauMapper dauMapper;


    @Override
    public long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public long getMidTotal(String date) {
        return dauMapper.getMidTotal(date);
    }

    @Override
    public Map getDauHour(String date)
    {
        List<Map> list=dauMapper.getDauHour(date);
       Map<String,Long> resultMap=new HashMap<>();
        for (Map map : list) {
            String hour = (String) map.get("LOGHOUR");
            Long count = (Long)map.get("COUNT");
            resultMap.put(hour,count);
        }
        return resultMap;
    }
}
