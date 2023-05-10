package com.jw.flink.sink;

import com.jw.flink.transformation.Access;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import scala.util.parsing.json.JSON;

/**
 * @author wangjie
 * @date 2021/11/23 下午11:10
 */
public class AccessMapper implements RedisMapper<Access> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "access");
    }

    @Override
    public String getKeyFromData(Access access) {
        return access.getDomain();
    }

    @Override
    public String getValueFromData(Access access) {
        return access.toString();
    }


}
