package com.jw.flink.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * @author wangjie
 * @date 2021/11/21 下午9:33
 */
public class PKMapFunction extends RichMapFunction<String,Access> {

    @Override
    public Access map(String value) throws Exception {
        System.out.println("-------map----------");
        String[] values = value.split(",");
        return new Access(Long.parseLong(values[0]), values[1], Double.parseDouble(values[2]));
    }

    /**
     * pre
     * 获取连接等操作
     * 根据并行度决定调用次数
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        System.out.println("-------open-----------");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }
}
