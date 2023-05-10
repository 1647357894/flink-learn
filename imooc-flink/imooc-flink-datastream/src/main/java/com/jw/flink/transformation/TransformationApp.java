package com.jw.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


/**
 * @author wangjie
 * @date 2021/11/21 下午4:06
 */
public class TransformationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //filter(env);
        //map(env);
        //flatMap(env);
        //keyBy(env);
        //reduce(env);

        //pkMap(env);
        //union(env);
        //connect(env);
        //coFlatMap(env);
        async(env);
        env.execute();
    }

    private static void async(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");

        //AsyncDataStream.unorderedWait(...) 无序模式
        //AsyncDataStream.orderedWait(...) 有序
        AsyncDataStream.unorderedWait(source, new AsyncFunction<String, String>() {
            @Override
            public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
                // 发送异步请求，接收 future 结果
                final Future<String> result = null;

                // 设置客户端完成请求后要执行的回调函数
                // 回调函数只是简单地把结果发给 future
                CompletableFuture.supplyAsync(new Supplier<String>() {

                    @Override
                    public String get() {
                        try {
                            return result.get();
                        } catch (InterruptedException | ExecutionException e) {
                            // 显示地处理异常。
                            return null;
                        }
                    }
                }).thenAccept( (String dbResult) -> {
                    resultFuture.complete(Collections.singleton(dbResult));
                });
            }
        },10, TimeUnit.SECONDS).print();
    }

    private static void coFlatMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        DataStreamSource<Access> accessDataStreamSource = env.fromElements(new Access(System.currentTimeMillis(),"abc.com",111D));
        ConnectedStreams<String, Access> connectedStreams = source.connect(accessDataStreamSource);
        connectedStreams.flatMap(new CoFlatMapFunction<String, Access, String>() {
            @Override
            public void flatMap1(String s, Collector<String> collector) throws Exception {
                collector.collect(s);
            }

            @Override
            public void flatMap2(Access access, Collector<String> collector) throws Exception {
                collector.collect(access.toString());
            }
        }).print();
    }

    /**
     * 双流 数据结构可以不同
     * @param env
     */
    private static void connect(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        DataStreamSource<Access> accessDataStreamSource = env.fromElements(new Access(System.currentTimeMillis(),"abc.com",111D));
        ConnectedStreams<String, Access> connectedStreams = source.connect(accessDataStreamSource);
        connectedStreams.map(new CoMapFunction<String, Access, String>() {
            //first stream logic
            @Override
            public String map1(String s) throws Exception {
                return s+":a";
            }

            //second stream logic
            @Override
            public String map2(Access access) throws Exception {
                return access+":b";
            }
        }).print();
    }

    /**
     * 多流合并，数据类型需一致
     * @param env
     */
    private static void union(StreamExecutionEnvironment env) {

        DataStreamSource<String> source = env.readTextFile("data/access.log");
        source.union(source ).print();
    }

    private static void pkMap(StreamExecutionEnvironment env) {
        env.setParallelism(2);
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        source.map(new PKMapFunction()).print();
    }

    private static void reduce(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        source.flatMap(new FlatMapFunction<String, Tuple2<String,Double>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Double>> out) throws Exception {
                String[] values = value.split(",");
                out.collect(Tuple2.of(values[1],Double.parseDouble(values[2])));
            }
        }).keyBy(x -> x.f0).reduce(new ReduceFunction<Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1+value2.f1);
            }
        }).print();
    }

    private static void keyBy(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] values = value.split(",");
                return new Access(Long.parseLong(values[0]), values[1], Double.parseDouble(values[2]));
            }
        }).keyBy((KeySelector<Access, String>) Access::getDomain).sum("traffic").print();

    }


    private static void flatMap(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String key = value.split(",")[1];
                out.collect(key);
            }
        }).print();

    }

    private static void map(StreamExecutionEnvironment env) {

    }

    private static void filter(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] values = value.split(",");
                return new Access(Long.parseLong(values[0]), values[1], Double.parseDouble(values[2]));
            }
        }).filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access value) throws Exception {
                return value.getTraffic() > 30;
            }
        }).print();

    }

}
