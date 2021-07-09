package com.fh.kafka.kafkahelper.consumer;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiDemo {
    public static void main(String[] args) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(3, 3, 30, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
        final AtomicInteger count = new AtomicInteger(1);
        for (int i = 0; i < 3; i++) {

            pool.execute(() -> {
                GbdConsumerFactory factory = new GbdConsumerFactory(String.format("%d号工厂", count.getAndIncrement()));
                factory.consume(factory.buildConsumer());
            });
        }
    }
}
