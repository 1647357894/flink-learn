package com.imooc.flink.app;

import com.alibaba.fastjson.JSON;
import com.imooc.flink.domain.Access;
import com.imooc.flink.udf.GaodeLocationMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Objects;

/**
 * @author wangjie
 * @date 2021/11/25 下午10:36
 */
public class ProvinceUserCntAppV1 {

    public static void main(String[] args) throws Exception {

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("114.132.198.147").
                setPort(19901)
                .setPassword("24045429941ccfjnz41")

                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Access> cleanStream = env.readTextFile("data/access.json").map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {

                try {
                    return JSON.parseObject(value, Access.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        }).filter(Objects::nonNull)
                //.filter(new FilterFunction<Access>() {
                //    @Override
                //    public boolean filter(Access value) throws Exception {
                //        return "startUp".equals(value.event);
                //    }
                //})
                ;

        cleanStream.map(new GaodeLocationMapFunction())
                .map(new MapFunction<Access, Tuple3<String,Integer,Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(Access value) throws Exception {
                        return Tuple3.of(value.province+"-"+value.city,value.nu,1);
                    }
                }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                        return value.f0+value.f1;
                    }
                })
                .sum(2)
                //.print().setParallelism(1)
                .addSink(new RedisSink<Tuple3<String, Integer, Integer>>(conf, new  RedisExampleMapper()))
        ;

        env.execute();
    }

    static class RedisExampleMapper implements RedisMapper<Tuple3<String, Integer, Integer>> {


        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "ProvinceUserCntAppV1");
        }

        @Override
        public String getKeyFromData(Tuple3<String, Integer, Integer> data) {
            return data.f0 +"-"+data.f1 ;
        }

        @Override
        public String getValueFromData(Tuple3<String, Integer, Integer> data) {
            return data.f2 + "";
        }
    }
}
