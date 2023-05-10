package com.jw.flink.source;

import com.jw.flink.transformation.Access;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class AccessSourceV2 implements ParallelSourceFunction<Access> {
    boolean running = true;

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {
        String[] domains = new String[]{"qq.com", "tx.com", "byte.com"};
        Random random = new Random();
        while (running) {
            for (int i = 0; i < 10; i++) {
                Access access = new Access(System.currentTimeMillis(), domains[random.nextInt(domains.length)], random.nextDouble());
                ctx.collect(access);

            }
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
