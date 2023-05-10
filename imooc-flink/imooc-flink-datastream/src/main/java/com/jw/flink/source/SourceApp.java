package com.jw.flink.source;

import com.jw.flink.transformation.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author wangjie
 * @date 2021/11/21 下午2:58
 */
public class SourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //test01(env);
        //test02(env);
        //test03(env);
        //test04(env);
        test06(env);
        //test05(env);

        env.execute("SourceApp");
    }

    private static void test06(StreamExecutionEnvironment env) {
        env
                .fromElements(new Student(4,"ty",16))
        .addSink(JdbcSink.sink(
                "insert into student (id, name,age) values (?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.getId());
                    ps.setString(2, t.getName());
                    ps.setInt(3, t.getAge());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://114.132.198.147:19902/pk_flink_imooc?useSSL=false")
                        .withDriverName("com.mysql.cj.jdbc.Driver").withUsername("root").withPassword("mysql@19902")
                        .build()));
    }

    private static void test04(StreamExecutionEnvironment env) {
        DataStreamSource<Student> source = env.addSource(new StudentSource());
        System.out.println("source.getParallelism() = " + source.getParallelism());

        source.print();

    }

    private static void test05(StreamExecutionEnvironment env) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "114.132.198.147:19092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), properties));
        System.out.println("stream.getParallelism() = " + stream.getParallelism());
        stream.print();

    }

    private static void test03(StreamExecutionEnvironment env) throws Exception {
        //DataStreamSource<Access> source = env.addSource(new AccessSource());
        DataStreamSource<Access> source = env.addSource(new AccessSourceV2());
        source.setParallelism(2);
        System.out.println("source.getParallelism() = " + source.getParallelism());
        source.print();
    }

    private static void test02(StreamExecutionEnvironment env) throws Exception {
        env.setParallelism(3);
        DataStreamSource<Long> source = env.fromSequence(0, 10)
                .setParallelism(4)
                ;
        System.out.println("source.getParallelism() = " + source.getParallelism());

        SingleOutputStreamOperator<Long> filterStream = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 5;
            }
        }).setParallelism(1);

        System.out.println("filterStream.getParallelism() = " + filterStream.getParallelism());

        filterStream.print();

    }

    private static void test01(StreamExecutionEnvironment env) throws Exception {
        //设置全局并行度
        env.setParallelism( 2);

//带webui
//StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //StreamExecutionEnvironment.createRemoteEnvironment()

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        System.out.println("source parallelism : " + source.getParallelism());

        SingleOutputStreamOperator<String> filterStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !"fk".equals(value);
            }
        }).setParallelism(3);
        System.out.println("filter stream : " + filterStream.getParallelism());
        filterStream.print();

    }

}
