package com.jw.flink.wm;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * EventTime 结合WM的使用
 *
 * @author wangjie
 * @date 2021/11/28 下午4:41
 */
public class EventTimeWMApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        test01(env);
        env.execute("EventTimeWMApp");
    }

    private static void test01(StreamExecutionEnvironment env) {

        //旁路输出
        //接收延迟，迟到的数据
        OutputTag<Tuple2<String, Integer> > outputTag = new OutputTag<Tuple2<String, Integer> >("side-output") {};

        SingleOutputStreamOperator<String> lines = env.socketTextStream("localhost", 9999)
                //wm 0秒
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String element) {
                        String[] split = element.split(",");
                        return Long.parseLong(split[0].trim());
                    }
                });


        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[1].trim(), Integer.parseInt(split[2].trim()));
            }
        });
        SingleOutputStreamOperator<String> streamOperator = mapStream.keyBy(x -> x.f0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(outputTag)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                System.out.println("value1 = " + value1 + ", value2 = " + value2);
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }, new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
            FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

            @Override
            public void process(String key, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                out.collect("key = " + key + ", start = " + format.format(context.window().getStart())
                        + ", end = " + format.format(context.window().getEnd()));

                // 发送数据到旁路输出
                //context.output(outputTag, "sideout-" + String.valueOf(value));
            }
        });
        streamOperator.print();

        DataStream<Tuple2<String, Integer> > sideOutputStream = streamOperator.getSideOutput(outputTag);
        sideOutputStream.printToErr();
    }

}
