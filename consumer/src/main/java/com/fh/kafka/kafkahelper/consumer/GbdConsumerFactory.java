package com.fh.kafka.kafkahelper.consumer;

import com.fh.kafka.kafkahelper.common.bean.KafkaConfig;
import com.fh.kafka.kafkahelper.listener.GbdConsumerRebalanceListener;
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

    private Map<TopicPartition, OffsetAndMetadata> offsetsMap = new ConcurrentHashMap<>();

    private Map<TopicPartition, Long> currentOffset = new ConcurrentHashMap<>();

    public GbdConsumerFactory(String name) {
        this.name = name;
        this.kafkaConfig = new KafkaConfig();
    }

    public KafkaConsumer buildConsumer() {

        String topic = this.kafkaConfig.getTopic();

        //1.创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.kafkaConfig.buildConsumerProps());

        //2.订阅Topic
        //创建一个只包含单个元素的列表，Topic的名字叫作customerCountries
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }

    public void consume(KafkaConsumer consumer) {

        while (count.get() <= 0) {
            try {
                TimeUnit.SECONDS.sleep(15);
            } catch (InterruptedException e) {
                LOGGER.error("【等待中断】", e);
            }

//            LOGGER.info("【{}排队等待获取执行权限】", name);
        }

        if (consumer == null) {

            final KafkaConsumer newConsumer = buildConsumer();

//            consumer.subscribe(Arrays.asList(this.kafkaConfig.getTopic()), new GbdConsumerRebalanceListener(consumer, offsetsMap));
            consumer.subscribe(Arrays.asList(this.kafkaConfig.getTopic()), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    commitOffset(currentOffset);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    for (TopicPartition partition : partitions) {
                        newConsumer.seek(partition, getOffset(partition));
                    }
                }
            });


            consumer.resume(consumer.assignment());
//            LOGGER.info("【{}重新上线】", name);
        } else {
//            LOGGER.info("【{}初次上线】", name);
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

//                    Collections.sort(data);

                    LOGGER.info("【{}消费消息】partition: {}\toffset: {}\t value: {}.\ndata: {}.", name,
                            record.partition(), record.offset(), record.value(), data);

                    count.decrementAndGet();


                    // 设置需要提交的偏移量

                    offsetsMap.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));

                    currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);

//                    LOGGER.info("【offset】{}", offsetsMap);

//                    consumer.commitAsync(offsetsMap, new OffsetCommitCallback() {
//                        @Override
//                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                            System.out.println("提交位移异常");
//                        }
//                    });

                    consumer.commitSync();

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
//            LOGGER.info("【{}下线，暂停工作】time: {}", name, System.currentTimeMillis() / 1000L);
        }

        this.consume(null);
    }


    /** ============================ 自定义offset， 可以放到数据库中进行维护 ================================= */
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }

}
