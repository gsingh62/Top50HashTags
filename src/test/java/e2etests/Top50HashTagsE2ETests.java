package e2etests;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.*;

public class Top50HashTagsE2ETests {
    @Test
    public void test_LateEventIsDropped() throws Exception {
        CollectSink.clear();  // clear previous test data
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<Tuple2<String, Long>> lateOutputTag =
                new OutputTag<>("late-data", Types.TUPLE(Types.STRING, Types.LONG));

        DataStream<Top50HashTags.HashtagWithTimestamp> source = env.addSource(new ManualWatermarkSource());

        SingleOutputStreamOperator<Tuple2<String, Long>> counts = source
                .map(e -> Tuple2.of(e.hashtag, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
                .sideOutputLateData(lateOutputTag)
                .aggregate(new Top50HashTags.CountAggregator(), new Top50HashTags.WindowedHashtagCountFunction());

        // Use your existing TopN window
        Top50HashTags.computeTopN(counts, 5).addSink(new CollectSink());

        env.execute("Top 50 Hashtags E2E Test");

        // --- Assertions happen after env.execute() blocks ---

        // Example assertion: did #on_time appear at least once?
        assertTrue(
                CollectSink.values.stream().anyMatch(s -> s.contains("#on_time"))
        );

        // Example assertion: did #too_late NOT appear?
        assertFalse(
                CollectSink.values.stream().anyMatch(s -> s.contains("#too_late"))
        );
    }
}
