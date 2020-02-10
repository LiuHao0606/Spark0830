package com.atguigu.gmall0831publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    // 查询日活总数
    long getDauTotal(String date);
    // 查询小时明细
    List<Map> getDauHour(String date);
}
