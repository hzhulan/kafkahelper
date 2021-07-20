package com.fh.kafka.kafkahelper;

import com.fh.kafka.kafkahelper.consumer.ConcurrentConsumer;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@MapperScan("com.fh.kafka.kafkahelper.offset.dao")
public class ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class);

        AtomicInteger no = new AtomicInteger(1);

        ThreadPoolExecutor pool = new ThreadPoolExecutor(3, 3, 3000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        for (int i = 0; i < 3; i++) {
            pool.execute(() -> {
                // 启动消费者
                ConcurrentConsumer consumer = new ConcurrentConsumer("consumer-" + no.getAndIncrement());
                consumer.consume();
            });
        }
    }
}
