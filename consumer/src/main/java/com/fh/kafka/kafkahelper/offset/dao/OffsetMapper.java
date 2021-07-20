package com.fh.kafka.kafkahelper.offset.dao;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.Map;

public interface OffsetMapper {

    @Select("select offset_value from consumer_offset where key=#{key}")
    Integer queryOffset(String key);

    @Insert("INSERT INTO consumer_offset VALUES (#{key}, #{offset}) on conflict(key) do update set offset_value=EXCLUDED.offset_value")
    int insertOffset(@Param("key") String key, @Param("offset") int offset);

    @Select("select version()")
    Map test();
}
