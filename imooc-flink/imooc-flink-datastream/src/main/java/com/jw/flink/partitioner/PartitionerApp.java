package com.jw.flink.partitioner;

import com.jw.flink.transformation.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author wangjie
 * @date 2021/11/23 上午7:08
 */
public class PartitionerApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> source = env.readTextFile("data/access.log");
        source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] values = value.split(",");
                return new Access(Long.parseLong(values[0]), values[1], Double.parseDouble(values[2]));
            }
        }).partitionCustom(new PKPartitioner(), new KeySelector<Access, String>() {
            @Override
            public String getKey(Access value) throws Exception {
                return value.getDomain();
            }
        }).
               map(new MapFunction<Access, Access>() {
                   @Override
                   public Access map(Access value) throws Exception {
                       System.out.println("current thread id is:" + Thread.currentThread().getId()+ ", value is:" + value.getDomain());

                       return value;
                   }
               }).print();
        env.execute();
    }
}
