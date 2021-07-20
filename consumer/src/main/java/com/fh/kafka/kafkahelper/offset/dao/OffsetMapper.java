package com.fh.kafka.kafkahelper.offset.dao;

import org.apache.ibatis.annotations.Select;

public interface OffsetMapper {

    @Select("select offset from consumer_offset where key=#{key}")
    int queryOffset(String key);
}
