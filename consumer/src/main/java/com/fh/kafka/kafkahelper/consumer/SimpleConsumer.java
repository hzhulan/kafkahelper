package com.fh.kafka.kafkahelper.consumer;

import com.fh.kafka.kafkahelper.common.bean.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 简单消费者不作并发限制， 和ConsumerFactory中的消费者一起消费
 */
public class SimpleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);

    private KafkaConfig kafkaConfig;

    private KafkaConsumer<String, String> consumer;

    /**
     * 测试使用暂时用，可以持久化到数据库中
     */
    private Map<TopicPartition, OffsetAndMetadata> currentOffset = new ConcurrentHashMap<>();

    public SimpleConsumer() {
        this.kafkaConfig = new KafkaConfig();
        buildConsumer();
    }

    private KafkaConsumer buildConsumer() {
        //1.创建消费者
        this.consumer = new KafkaConsumer<>(this.kafkaConfig.buildConsumerProps());

        //2.订阅Topic
        //创建一个只包含单个元素的列表，Topic的名字叫作customerCountries
        this.consumer.subscribe(Collections.singletonList(kafkaConfig.getTopic()), new ConsumerRebalanceListener() {
            /**
             * rebalance之前调用
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                LOGGER.info("=============== 重平衡开始 ==============");
                commitOffset();
            }

            /**
             * rebalance之后调用， 对变化的分区使用保存的offset，进行seek
             * @param partitions
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                try {
                    for (TopicPartition partition : partitions) {
                        if (getOffset(partition) != null) {
                            consumer.seek(partition, getOffset(partition));
                            LOGGER.info("【重平衡seek】partition: {}, offset: {}.", partition.partition(), getOffset(partition));
                        } else {
                            LOGGER.info("【offset为空】partition: {}", partition.partition());
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("重平衡异常", e);
                }
                LOGGER.info("=============== 重平衡结束 ==============");
            }
        });

        return consumer;
    }

    public void consume() {
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
                    LOGGER.info("【普通消费者】No.{} parition: {}, offset: {}, value: {}", number, record.partition(), record.offset(),record.value());
                }
                commitOffset();
            }
        } catch (Exception e) {
            LOGGER.error("消费异常", e);
        } finally {
            LOGGER.info("【普通消费者】下线");
        }
    }

    /**
     * ============================ 自定义offset， 可以放到数据库中进行维护 =================================
     */
    private Long getOffset(TopicPartition partition) {
        if (currentOffset.get(partition) == null) {
            return null;
        }
        return currentOffset.get(partition).offset();
    }

    private void commitOffset() {

        this.consumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                LOGGER.info("commit失败！！！！！！！！！！！！！！！！！");
            }
        });
    }

    public static void main(String[] args) {
        new SimpleConsumer().consume();
    }
}
