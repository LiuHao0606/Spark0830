package com.atguigu.gmall0831publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;

public interface PublisherService {

    long getDauTotal(String date);

    long getMidTotal(String date);

    Map getDauHour(String date);

}
