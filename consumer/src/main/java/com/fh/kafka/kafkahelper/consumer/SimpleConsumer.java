package com.fh.kafka.kafkahelper.consumer;

import com.fh.kafka.kafkahelper.common.bean.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);

    private KafkaConfig kafkaConfig;

    public SimpleConsumer() {
        this.kafkaConfig = new KafkaConfig();
    }

    public KafkaConsumer buildConsumer() {
        //1.创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.kafkaConfig.buildConsumerProps());

        //2.订阅Topic
        //创建一个只包含单个元素的列表，Topic的名字叫作customerCountries
        consumer.subscribe(Collections.singletonList("msg"));

        return consumer;
    }

    public void consume() {
        KafkaConsumer consumer = this.buildConsumer();
        LOGGER.info("【普通消费者】上线");
        AtomicInteger count = new AtomicInteger(0);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                if (!records.isEmpty()) {
                    LOGGER.info("\n===============time: {}===============\n", System.currentTimeMillis()/1000L);
                }
                for (ConsumerRecord<String, String> record : records) {
                    int number = count.incrementAndGet();
                    LOGGER.info("【普通消费者】No.{}: {}", number, record.value());
                }
            }
        } catch (Exception e) {
            LOGGER.error("消费异常", e);
        } finally {
            LOGGER.info("【普通消费者】下线");
        }
    }

    public static void main(String[] args) {
        new SimpleConsumer().consume();
    }
}
