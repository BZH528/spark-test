package com.atguigu.gmall.publisher.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/12 17:25
 * @Description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MySQLServiceTest {

    @Autowired
    MySQLService mySQLService;

    @Test
    public void getTrademardStat() {
        List<Map> trademardStat = mySQLService.getTrademardStat("2021-05-12", "2021-05-13", 5);
        trademardStat.forEach(System.out::println);
    }
}