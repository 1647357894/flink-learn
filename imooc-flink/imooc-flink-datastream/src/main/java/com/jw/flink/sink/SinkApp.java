package com.jw.flink.sink;

import com.jw.flink.transformation.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

/**
 * @author wangjie
 * @date 2021/11/23 上午5:54
 */
public class SinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        test01(env);

        env.execute("SinkApp");
    }

    private static void test01(StreamExecutionEnvironment env) {

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("114.132.198.147").
                setPort(19901)
                        .setPassword("24045429941ccfjnz41")

                .build();



        DataStreamSource<String> source = env.readTextFile("data/access.log");
        source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] values = value.split(",");
                return new Access(Long.parseLong(values[0]), values[1], Double.parseDouble(values[2]));
            }
        }) .addSink(new RedisSink<>(conf,new AccessMapper()));
    }
}
