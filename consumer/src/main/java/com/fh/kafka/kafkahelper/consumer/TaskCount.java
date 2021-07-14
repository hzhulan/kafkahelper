package com.fh.kafka.kafkahelper.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 并发计数器
 */
public class TaskCount {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskCount.class);

    private AtomicInteger count;

    public TaskCount(int count) {
        this.count = new AtomicInteger(count);
    }

    public int get() {
        return this.count.get();
    }

    /**
     * 查询是否具有访问权限
     * @return
     */
    public boolean hasAuth() {
        return this.get() > 0;
    }

    /**
     * 占用执行权限
     */
    public boolean acquire() {
        if (this.get() > 0) {
            return this.count.getAndDecrement() > 0;
        }
        return false;
    }

    /**
     * 释放
     */
    public void release() {
        this.count.getAndIncrement();
    }
}
