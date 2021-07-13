package com.fh.kafka.kafkahelper.consumer;

import com.fh.kafka.kafkahelper.common.bean.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class GbdConsumerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(GbdConsumerFactory.class);

    private String name;

    private AtomicInteger count = new AtomicInteger(2);


    private List<String> data = new ArrayList<>();

    private KafkaConfig kafkaConfig;

    private KafkaConsumer<String, String> consumer;

    /**
     * 测试使用暂时用静态map存放，如果分布式，需要持久化到数据库中
     */
    private static Map<TopicPartition, OffsetAndMetadata> currentOffset = new ConcurrentHashMap<>();

    public GbdConsumerFactory(String name) {
        this.name = name;
        this.kafkaConfig = new KafkaConfig();
        buildConsumer();
    }

    public void buildConsumer() {

        //1.创建消费者
        this.consumer = new KafkaConsumer<>(this.kafkaConfig.buildConsumerProps());

        //2.订阅Topic
        this.consumer.subscribe(Collections.singletonList(this.kafkaConfig.getTopic()));

    }

    public void consume(boolean isFirst) {

        LOGGER.info("【{}】上线", this.name);

        while (count.get() <= 0) {
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                LOGGER.error("【等待中断】", e);
            }

            LOGGER.info("【{}排队等待获取执行权限】", name);
        }

        if (!isFirst) {
            consumer.subscribe(Collections.singletonList(this.kafkaConfig.getTopic()), new ConsumerRebalanceListener() {

                /**
                 * rebalance之前调用
                 * @param partitions
                 */
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    LOGGER.info("=============== 重平衡开始 ==============");
                    commitOffset(currentOffset);
                }

                /**
                 * rebalance之后调用
                 * @param partitions
                 */
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    try {
                        for (TopicPartition partition : partitions) {
                            if (getOffset(partition) != null) {
                                consumer.seek(partition, getOffset(partition));
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("重平衡异常", e);
                    }
                    LOGGER.info("=============== 重平衡结束 ==============");
                }
            });
            consumer.resume(consumer.assignment());
        }


        try {
            outWhile:
            while (count.get() > 0) {

                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {

                    while (count.get() <= 0) {
                        break outWhile;
                    }

                    data.add(record.value());

                    LOGGER.info("【{}消费消息】partition: {}\toffset: {}\t value: {}.\ndata: {}.", name,
                            record.partition(), record.offset(), record.value(), data);

                    count.decrementAndGet();


                    // 设置需要提交的偏移量
                    currentOffset.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                    // 提交偏移量
                    commitOffset(currentOffset);

                    new Thread(() -> {
                        try {
                            TimeUnit.SECONDS.sleep(5);
                            count.incrementAndGet();
                        } catch (InterruptedException e) {
                            LOGGER.error("等待中断", e);
                        }
                    }).start();

                }
            }

        } catch (Exception e) {
            LOGGER.error("执行异常", e);
        } finally {

            consumer.unsubscribe();//此处不取消订阅暂停太久会出现订阅超时的错误
            consumer.pause(consumer.assignment());
            LOGGER.info("【{}下线，暂停工作】time: {}", name, System.currentTimeMillis() / 1000L);
        }

        LOGGER.info("【等待上线】");
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            LOGGER.error("中断", e);
        }

        this.consume(false);
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

    private void commitOffset(Map<TopicPartition, OffsetAndMetadata> currentOffset) {

        this.consumer.commitAsync(currentOffset, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    LOGGER.info("commit失败！！！！！！！！！！！！！！！！！");
                }
            }
        });
    }

}
