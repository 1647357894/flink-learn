package com.jw.flink.source;

import com.jw.flink.utils.MySQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author wangjie
 * @date 2021/11/21 下午4:59
 */
public class StudentSource extends RichSourceFunction<Student> {

    Connection connection;
    PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
         connection = MySQLUtils.getConnection();
         preparedStatement =  connection.prepareStatement("select * from student");
    }

    @Override
    public void close() throws Exception {
        super.close();

        MySQLUtils.close(connection,preparedStatement);
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");
            ctx.collect(new Student(id,name,age));
        }
    }

    @Override
    public void cancel() {

    }
}
