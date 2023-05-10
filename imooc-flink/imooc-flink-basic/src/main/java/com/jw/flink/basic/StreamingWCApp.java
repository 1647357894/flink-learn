package com.jw.flink.basic;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 词频统计 流处理
 * @author wangjie
 * @date 2021/11/19 上午7:06
 */
public class StreamingWCApp {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

    //    业务处理
        source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] words = s.split(",");
                        for (String word : words) {
                            collector.collect(word.toLowerCase().trim());

                        }
                    }
                }).filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return StringUtils.isNotBlank(s);
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                })
                //.keyBy(0)
                        .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                            @Override
                            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                                return stringIntegerTuple2.getField(0);
                            }
                        })
                .sum(1).
        //    sink
                        print();
                env.execute("StreamingWCApp");
    }
}
