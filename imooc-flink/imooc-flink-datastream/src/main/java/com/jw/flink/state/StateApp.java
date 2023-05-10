package com.jw.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author wangjie
 * @date 2021/12/12 下午2:08
 */
public class StateApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        test01(env);

        env.execute("StateApp");
    }


    /**
     * 使用ValueState实现平均数
     */
    public static void test01(StreamExecutionEnvironment env) throws Exception {
        List<Tuple2<Long, Long>> list = new ArrayList<>();
        list.add(Tuple2.of(1L, 3L));
        list.add(Tuple2.of(1L, 7L));
        list.add(Tuple2.of(1L, 5L));
        list.add(Tuple2.of(2L, 4L));
        list.add(Tuple2.of(2L, 2L));
        list.add(Tuple2.of(2L, 0L));

        env.fromCollection(list)
                .keyBy(x -> x.f0)
                //.flatMap(new AvgWithValueState())
                .flatMap(new AvgWithMapState())
                .print();

    }

    public static class AvgWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {


        /**
         * The ValueState handle. The first field is the count, the second field a running sum.
         */
        private transient ValueState<Tuple2<Long, Double>> sum;

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Double>> out) throws Exception {

            // access the state value
            Tuple2<Long, Double> currentSum = sum.value();

            // update the count
            currentSum.f0 += 1;

            // add the second field of the input value
            currentSum.f1 += input.f1;

            // update the state
            sum.update(currentSum);

            // if the count reaches 2, emit the average and clear the state
            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<Long, Double>> descriptor =
                    new ValueStateDescriptor<>(
                            "average", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
                            }), // type information
                            Tuple2.of(0L, 0D)); // default value of the state, if nothing was set
            sum = getRuntimeContext().getState(descriptor);
        }
    }

    public static class AvgWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
        private transient MapState<String, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("avg", String.class, Long.class);
            mapState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
            mapState.put(UUID.randomUUID().toString(), value.f1);

            ArrayList<Long> elements = Lists.newArrayList(mapState.values());

            if (elements.size() == 2) {
                Long count = 0L;
                Long sum = 0L;

                for (Long element : elements) {
                    count++;
                    sum += element;
                }
                double avg = sum / count.doubleValue();
                out.collect(Tuple2.of(value.f0, avg));
                mapState.clear();
            }
        }
    }
}
