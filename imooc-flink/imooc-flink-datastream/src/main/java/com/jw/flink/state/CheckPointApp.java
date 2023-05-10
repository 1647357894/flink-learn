package com.jw.flink.state;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

import java.util.concurrent.TimeUnit;

/**
 * @author wangjie
 * @date 2021/12/12 下午3:14
 */
public class CheckPointApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(5000);

// 高级选项：

// 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

//// 确认 checkpoints 之间的时间会进行 500 ms
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//
//// Checkpoint 必须在一分钟内完成，否则就会被抛弃
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//
//// 允许两个连续的 checkpoint 错误
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
//
//// 同一时间只允许一个 checkpoint 进行
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// 开启实验性的 unaligned checkpoints
//        env.getCheckpointConfig().enableUnalignedCheckpoints();


        //自定义设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));

        //env.setStateBackend(new FsStateBackend("file:///data/tmp/ck"));

        env.socketTextStream("localhost",9999).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if(value.contains("pk")) {
                    throw new RuntimeException("PK哥来了，快跑..");
                } else {
                    return value.toLowerCase();
                }
            }
        }).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(f -> f.f0).sum(1).print();

        env.execute();

    }
}
