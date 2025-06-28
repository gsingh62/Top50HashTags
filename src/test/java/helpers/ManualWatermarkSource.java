package helpers;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class ManualWatermarkSource implements SourceFunction<Top50HashTags.HashtagWithTimestamp> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Top50HashTags.HashtagWithTimestamp> ctx) throws Exception {
        long T0 = System.currentTimeMillis();

        synchronized (ctx.getCheckpointLock()) {
            // On-time data (event time = T0 + 4min)
            ctx.collectWithTimestamp(new Top50HashTags.HashtagWithTimestamp("#on_time", T0 + 4 * 60 * 1000), T0 + 4 * 60 * 1000);

            // Late data (event time = T0)
            ctx.collectWithTimestamp(new Top50HashTags.HashtagWithTimestamp("#late", T0), T0);

            // Advance watermark to T0 + 11min (past 10min window + watermark slack)
            ctx.emitWatermark(new Watermark(T0 + 11 * 60 * 1000));

            // Truly late data (will go to side output if allowedLateness is 0)
            ctx.collectWithTimestamp(new Top50HashTags.HashtagWithTimestamp("#too_late", T0), T0);

            // Final watermark to close everything
            ctx.emitWatermark(new Watermark(T0 + 20 * 60 * 1000));
        }

        Thread.sleep(1000); // Give time for processing
    }

    @Override
    public void cancel() {
        running = false;
    }
}
