package com.atguigu.gmall.publisher.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/4/30 11:56
 * @Description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ESServiceTest {

    @Autowired
    private ESService esService;

    @Test
    public void getDauTotal() {
        Long dauTotal = esService.getDauTotal("2021-04-29");
        System.out.println(dauTotal);
    }

    @Test
    public void getDauHour() {
        Map<String, Long> hourDauMap = esService.getDauHour("2021-04-29");
        System.out.println("分时统计某天日活数：" + hourDauMap);
    }
}