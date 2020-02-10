package com.atguigu.gmall0831publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages ="com.atguigu.gmall0831publisher.mapper" )
public class Gmall0831PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0831PublisherApplication.class, args);
    }

}
