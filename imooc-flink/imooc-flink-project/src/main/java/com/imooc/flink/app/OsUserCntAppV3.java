package com.imooc.flink.app;

import com.alibaba.fastjson.JSON;
import com.imooc.flink.domain.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava30.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * 根据系统维度统计新老用户
 *
 * @author wangjie
 * @date 2021/11/24 上午6:41
 */
public class OsUserCntAppV3 {
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
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return "startUp".equals(value.event);
                    }
                })
                ;

        cleanStream
                .keyBy(x -> x.device)
                .process(new KeyedProcessFunction<String, Access, Access>() {
                    ValueState<BloomFilter<String>> state ;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<BloomFilter<String>> valueStateDescriptor = new ValueStateDescriptor<>("state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
                        }));
                      state = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(Access value, KeyedProcessFunction<String, Access, Access>.Context ctx, Collector<Access> out) throws Exception {
                        BloomFilter<String> bloomFilter = state.value();
                        if (bloomFilter==null) {
                            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(),10000);
                        }
                        if (!bloomFilter.mightContain(value.device)) {
                            bloomFilter.put(value.device);
                            state.update(bloomFilter);
                            value.nu2 = 1;
                        }
                        out.collect(value);

                    }
                })

                .print().setParallelism(1)
        ;

        env.execute();
    }


}
