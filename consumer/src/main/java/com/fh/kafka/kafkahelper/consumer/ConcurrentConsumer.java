package com.fh.kafka.kafkahelper.consumer;

import com.fh.kafka.kafkahelper.common.bean.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 需求：kafka的消费者中消费消息后，调用第三方工具发送异步任务，但是第三方工具的并发有一定的限制，需要我们进行控制
 * 实现: 定义一个线程安全的计数器TaskCount, 根据并发限制消费者的上线、下线。
 * 并发足够：消费者消费；并发不够，消费者下线，防止该分区阻塞
 * 例如： 3个分区对应最多3个消费者同时消费，一个消费者对应一个分区。但是消费者A的并发用完了，一直卡着，他对应分区后面的消息无法被其他消费者消费到，
 * 形成了消息积压，这个时候会将该消费者“下线”， 让其消费者B或消费者C“兼职”消费该分区。
 */
public class ConcurrentConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentConsumer.class);

    private String name;

    private TaskCount count = new TaskCount(3);


    private List<String> data = new ArrayList<>();

    private KafkaConfig kafkaConfig;

    private KafkaConsumer<String, String> consumer;

    /**
     * 抢救时间，单位：秒
     */
    private static final int RESCUE_SECOND = 3;

    /**
     * 模拟任务执行时间，单位：秒
     */
    private static final int TASK_EXECUTE_TIME = 5;

    /**
     * 重新订阅时间间隔，单位：秒
     */
    private static final int RESUBSCRIBE_PERIOD = 60;

    /**
     * 测试使用暂时用，可以持久化到数据库中
     */
    private Map<TopicPartition, OffsetAndMetadata> currentOffset = new ConcurrentHashMap<>();

    public ConcurrentConsumer(String name) {
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

    public void consume() {
        this.consume(true);
    }

    /**
     * 消费主体
     * @param isFirst
     */
    private void consume(boolean isFirst) {

        // 阻塞等待
        waitForExecute();

        if (!isFirst) {
            reSubscribe();
        }

        LOGGER.info("【{}】上线", this.name);


        try {
            outWhile:
            while (count.hasAuth()) {

                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {

                    // 无执行权限，退出
                    if (!count.hasAuth()) {
                        break outWhile;
                    }

                    // 日志打印，可忽略
                    data.add(record.value());
                    LOGGER.info("【{}消费消息】size: {}, partition: {}, offset: {}, value: {}\ndata: {}.", name,
                            data.size(),record.partition(), record.offset(), record.value(), data);

                    // 获取执行资格
                    count.acquire();

                    // 设置需要提交的偏移量
                    currentOffset.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                    // 提交偏移量
                    commitOffset();

                    // 执行内容
                    new Thread(() -> {
                        try {
                            TimeUnit.SECONDS.sleep(TASK_EXECUTE_TIME);
                        } catch (InterruptedException e) {
                            LOGGER.error("等待中断", e);
                        } finally {
                            count.release();
                        }
                    }).start();

                }
            }

        } catch (Exception e) {
            LOGGER.error("执行异常", e);
        }

        // 等待下线
        if (rescue()) {
            this.consume(true);
        } else {
            this.consume(false);
        }
    }

    /**
     * 阻塞等待执行权限
     */
    private void waitForExecute() {
        while (!count.hasAuth()) {
            try {
                TimeUnit.SECONDS.sleep(RESUBSCRIBE_PERIOD);
            } catch (InterruptedException e) {
                LOGGER.error("【等待中断】", e);
            }

            LOGGER.info("【{}排队等待获取执行权限】", name);
        }
    }

    /**
     * 重新"上线"
     */
    private void reSubscribe() {
        this.consumer = new KafkaConsumer<>(this.kafkaConfig.buildConsumerProps());
        this.consumer.subscribe(Collections.singletonList(this.kafkaConfig.getTopic()), new ConsumerRebalanceListener() {

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
        this.consumer.resume(this.consumer.assignment());
    }

    /**
     * 客户端抢救
     */
    private boolean rescue() {
        try {
            TimeUnit.SECONDS.sleep(RESCUE_SECOND);
        } catch (InterruptedException e) {
            LOGGER.error("【抢救中断】", e);
        }

        if (count.hasAuth()) {
            LOGGER.info("【抢救成功，继续消费】");
            return true;
        }

        this.consumer.unsubscribe();//此处不取消订阅暂停太久会出现订阅超时的错误
        this.consumer.pause(consumer.assignment());
        LOGGER.info("【{}下线，暂停工作】", name);

        return false;
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

        this.consumer.commitAsync(currentOffset, (offsets, exception) -> {
            if (exception != null) {
                LOGGER.info("commit失败！！！！！！！！！！！！！！！！！");
            }
        });
    }


    public static void main(String[] args) {
        ConcurrentConsumer factory = new ConcurrentConsumer(String.format("消费者"));
        factory.consume();
    }
}
