package com.fh.kafka.kafkahelper.common.bean;

import com.sun.istack.internal.NotNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConfig {

    private String bootStrapServer;

    private String groupId;

    private String topic;

    public KafkaConfig() {
        this.bootStrapServer = "localhost:9092";
        this.groupId = "jsGroup";
        this.topic = "msg";
    }

    public String getTopic() {
        return topic;
    }

    public void buildBootStrapServer(Properties props) {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServer);
    }

    public void buildGroupId(Properties props) {
        props.put("group.id", this.groupId);
    }

    public Properties buildConsumerProps() {
        Properties props = new Properties();

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        buildBootStrapServer(props);
        buildGroupId(props);

        return props;
    }


    public Properties buildProducerProps() {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        buildBootStrapServer(props);
        return props;
    }



}
