package com.jw.flink.window;

import com.jw.flink.source.Student;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author wangjie
 * @date 2021/11/27 下午10:43
 */
public class WindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test01(env);
        //test02(env);
        //test03(env);
        //test04(env);
        //test05(env);
        //testAgg(env);


        env.execute("WindowApp");
    }

    private static void test01(StreamExecutionEnvironment env) {

        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //env.socketTextStream("localhost",9999).map(new MapFunction<String, Integer>() {
        //    @Override
        //    public Integer map(String value) throws Exception {
        //        return Integer.parseInt(value);
        //    }
        //}).timeWindowAll(Time.seconds(5)).sum(0).print();

        //env.socketTextStream("localhost",9999).map(new MapFunction<String, Integer>() {
        //    @Override
        //    public Integer map(String value) throws Exception {
        //        if (StringUtils.isBlank(value)) {
        //            return 0;
        //        }
        //        return Integer.parseInt(value);
        //    }
        //}).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(0).print();



        //求和
        //java,1 c,2 js,3
        env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0].trim(),Integer.parseInt(split[1].trim()));
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                .sum(1).print();
    }

    private static void test02(StreamExecutionEnvironment env) {


        //求和
        //java,1 c,2 js,3
        env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0].trim(),Integer.parseInt(split[1].trim()));
            }
        }).keyBy(x -> x.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                System.out.println("value1 = " + value1 + ", value2 = " + value2);
                return Tuple2.of(value1.f0,value1.f1+value2.f1);
            }
        }).print();
    }

    private static void test03(StreamExecutionEnvironment env) {

        //滚动窗口求最大值

        //java,1 c,2 js,3
        env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of("maxValue",Integer.parseInt(split[1].trim()));
            }
        }).keyBy(x -> x.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

                System.out.println("key = " + key);
                Integer maxVal  = Integer.MIN_VALUE;
                for (Tuple2<String, Integer> element : elements) {
                    maxVal = Math.max(maxVal,element.f1);
                }
                out.collect("maxVal="+maxVal);
            }
        }).print();
    }


    private static void test04(StreamExecutionEnvironment env) {

        //滑动窗口求和
        //java,1 c,2 js,3
        env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0].trim(),Integer.parseInt(split[1].trim()));
            }
        }).keyBy(x -> x.f0).window(SlidingProcessingTimeWindows.of(Time.seconds(4),Time.seconds(2))).sum(1).print().setParallelism(1);
    }

    private static void test05(StreamExecutionEnvironment env) {

        //会话窗口求和
        //java,1 c,2 js,3
        env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0].trim(),Integer.parseInt(split[1].trim()));
            }
        }).keyBy(x -> x.f0)
                //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .window(ProcessingTimeSessionWindows.withDynamicGap(element -> element.f0.equals("a") ? 2000:5000))

                .sum(1).print().setParallelism(1);
    }

    private static void testAgg(StreamExecutionEnvironment env) {

        //avg
        //java,1 c,2 js,3
        env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0].trim(),Integer.parseInt(split[1].trim()));
            }
        }).keyBy(x -> x.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //.allowedLateness(Time.seconds(2))
                .aggregate(new AverageAggregate())
         .print();


    }


    /**
     * The accumulator is used to keep a running sum and a count. The {@code getResult} method
     * computes the average.
     */
    private static class AverageAggregate
            implements AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Double> {
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> accumulator1, Tuple2<Integer, Integer> accumulator2) {
            return new Tuple2<>(accumulator1.f0 + accumulator2.f0, accumulator1.f1 + accumulator2.f1);
        }
    }
}
