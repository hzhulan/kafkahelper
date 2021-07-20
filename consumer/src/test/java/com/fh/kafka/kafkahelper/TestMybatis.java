package com.fh.kafka.kafkahelper;

import com.fh.kafka.kafkahelper.offset.dao.OffsetMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TestMybatis {

    @Resource
    private OffsetMapper mapper;

    @Test
    public void test01() {
        Map test = mapper.test();
        System.out.println(test);
    }

    @Test
    public void test02() {
        Integer test = mapper.queryOffset("163");
        System.out.println(test);
    }

    @Test
    public void test03() {
        int num = mapper.insertOffset("163", 20);
        System.out.println(num);
    }


}

