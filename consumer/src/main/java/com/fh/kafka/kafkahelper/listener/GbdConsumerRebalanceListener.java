package com.fh.kafka.kafkahelper.listener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 消费者重平衡监听
 */
public class GbdConsumerRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(GbdConsumerRebalanceListener.class);

    private KafkaConsumer consumer;

    private Map<TopicPartition, OffsetAndMetadata> offsetsMap;

    public GbdConsumerRebalanceListener(KafkaConsumer consumer, Map<TopicPartition, OffsetAndMetadata> offsetsMap) {
        this.consumer = consumer;
        this.offsetsMap = offsetsMap;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.info("【GbdConsumerRebalanceListener-offset】{}", offsetsMap);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {

        }
    }

    /** ============================ 自定义offset， 可以放到数据库中进行维护 ================================= */
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}
