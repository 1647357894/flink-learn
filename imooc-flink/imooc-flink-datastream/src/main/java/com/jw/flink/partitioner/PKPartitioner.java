package com.jw.flink.partitioner;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author wangjie
 * @date 2021/11/23 上午7:09
 */
public class PKPartitioner implements Partitioner<String> {
    @Override
    public int partition(String s, int i) {
        System.out.println("i = " + i);
        if ("qq.com".equals(s)) {
            return 1;
        }
        if ("tx.com".equals(s)) {
            return 2;
        }
        if ("byte.com".equals(s)) {
            return 2;
        }
        return 0;
    }
}
