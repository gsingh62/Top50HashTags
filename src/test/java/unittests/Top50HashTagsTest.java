package unittests;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class Top50HashTagsTest {

    private static final List<String> sinkOutput = new ArrayList<>();

    public static class CollectSink implements SinkFunction<String> {
        @Override
        public synchronized void invoke(String value, Context context) {
            sinkOutput.add(value);
        }
    }

    @BeforeEach
    public void setup() {
        sinkOutput.clear();
    }

    @AfterEach
    public void tearDown() {
        sinkOutput.clear();
    }

    @Test
    public void testTopNHashtagsPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long now = System.currentTimeMillis();

        List<Top50HashTags.HashtagWithTimestamp> input = List.of(
                new Top50HashTags.HashtagWithTimestamp("#flink", now - 1000),
                new Top50HashTags.HashtagWithTimestamp("#java", now - 2000),
                new Top50HashTags.HashtagWithTimestamp("#flink", now - 1500),
                new Top50HashTags.HashtagWithTimestamp("#streaming", now - 1000),
                new Top50HashTags.HashtagWithTimestamp("#java", now - 1000),
                new Top50HashTags.HashtagWithTimestamp("#kafka", now - 1000),
                new Top50HashTags.HashtagWithTimestamp("#kafka", now - 1000),
                new Top50HashTags.HashtagWithTimestamp("#elasticsearch", now - 1000)
        );

        DataStream<Top50HashTags.HashtagWithTimestamp> source = env
                .fromCollection(input)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Top50HashTags.HashtagWithTimestamp>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.timestamp)
                );

        DataStream<Tuple2<String, Long>> counts = Top50HashTags.computeHashtagCounts(source);
        DataStream<String> topN = Top50HashTags.computeTopN(counts, 3);
        topN.addSink(new CollectSink());

        env.execute("Top N Hashtag Test");

        assertFalse(sinkOutput.isEmpty(), "Output should not be empty");

        // Top 3 expected: #flink (2), #java (2), #kafka (2)
        assertTrue(sinkOutput.stream().anyMatch(s -> s.contains("#flink")));
        assertTrue(sinkOutput.stream().anyMatch(s -> s.contains("#java")));
        assertTrue(sinkOutput.stream().anyMatch(s -> s.contains("#kafka")));

        // Should NOT contain #streaming or #elasticsearch in top 3
        assertFalse(sinkOutput.stream().anyMatch(s -> s.contains("#streaming")));
        assertFalse(sinkOutput.stream().anyMatch(s -> s.contains("#elasticsearch")));
    }

    @Test
    public void testExtractHashtags() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> tweets = List.of(
                "{\"text\": \"I love #Flink and #AI\", \"timestamp\": 1729968800000}",
                "{\"text\": \"#Java is powerful\", \"timestamp\": 1729968801000}"
        );

        DataStream<String> tweetStream = env.fromCollection(tweets);
        DataStream<Top50HashTags.HashtagWithTimestamp> hashtags = Top50HashTags.extractHashtags(tweetStream);

        List<Top50HashTags.HashtagWithTimestamp> output = new ArrayList<>();
        hashtags.executeAndCollect().forEachRemaining(output::add);

        assertEquals(3, output.size());
        assertTrue(output.stream().anyMatch(h -> h.hashtag.equals("#flink")));
        assertTrue(output.stream().anyMatch(h -> h.hashtag.equals("#ai")));
        assertTrue(output.stream().anyMatch(h -> h.hashtag.equals("#java")));
    }
}
