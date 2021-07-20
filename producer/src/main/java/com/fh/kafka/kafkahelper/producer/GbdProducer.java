package com.fh.kafka.kafkahelper.producer;

import com.fh.kafka.kafkahelper.common.bean.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 生产者
 */
public class GbdProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GbdProducer.class);

    private static KafkaProducer producer = null;

    private static KafkaConfig kafkaConfig;

    static {
        kafkaConfig = new KafkaConfig();
    }

    private static void init() {
        if (producer == null) {
            producer = new KafkaProducer<String, String>(kafkaConfig.buildProducerProps());
        }
    }


    public static void send(String topic, String value) {

        init();

        try {
            int v = Integer.parseInt(value);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, v % 3, "test", value);
            producer.send(record);
            LOGGER.info("发送消息: {}, partition: {}.", v, v % 3);

        } catch (Exception e) {
            LOGGER.error("发送消息异常", e);
        } finally {
            producer.close();
            producer = null;
        }

    }

    public static void send(String topic, List<String> values) {

        init();

        try {
            // 使用轮询方式

            for (int i = 0; i < values.size(); i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, i % 3, "test", values.get(i));
                producer.send(record);
                LOGGER.info("发送消息: {}, partition: {}.", values.get(i), i % 3);
            }
        } catch (Exception e) {
            LOGGER.error("发送消息异常", e);
        } finally {
            producer.close();
            producer = null;
        }

    }

    public static void main(String[] args) {
        List<String> msgList = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            msgList.add(String.format("%d", i));
        }
        GbdProducer.send(kafkaConfig.getTopic(), msgList);
    }
}
