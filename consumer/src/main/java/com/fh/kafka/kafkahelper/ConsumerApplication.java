package com.fh.kafka.kafkahelper;

import com.fh.kafka.kafkahelper.consumer.ConcurrentConsumer;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.fh.kafka.kafkahelper.offset.dao")
public class ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class);

        // 启动消费者
        ConcurrentConsumer consumer = new ConcurrentConsumer("consumer-1");
        consumer.consume();
    }
}
