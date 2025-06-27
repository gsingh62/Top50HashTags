import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Flink job that finds the top 50 hashtags in Twitter posts over the last 2 days.
 *
 * All tweets arrive in real-time and are processed using event time semantics,
 * with a 5-minute delay tolerance for late-arriving events.
 *
 * Input format: JSON lines like:
 *   {"text": "I love #Flink and #Streaming", "timestamp": 1729968800000}
 */
public class Top50HashTags {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Step 1: Ingest the stream of tweet JSON strings from a socket
        DataStream<String> tweetStream = env.socketTextStream("localhost", 9999);

        // Step 2: Parse each tweet JSON and extract hashtags with their event timestamp
        DataStream<HashtagWithTimestamp> hashTags = tweetStream
                .flatMap((String line, Collector<HashtagWithTimestamp> out) -> {
                    ObjectMapper mapper = new ObjectMapper();
                    TweetEvent tweet = mapper.readValue(line, TweetEvent.class);

                    // Extract all hashtags using regex
                    Pattern pattern = Pattern.compile("#\\w+");
                    Matcher matcher = pattern.matcher(tweet.text);
                    while (matcher.find()) {
                        out.collect(new HashtagWithTimestamp(matcher.group().toLowerCase(), tweet.timestamp));
                    }
                })
                .returns(HashtagWithTimestamp.class)

                // Step 3: Assign timestamps and generate watermarks (5-minute max out-of-orderness)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<HashtagWithTimestamp>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        // Step 4–6: Count hashtags in a 2-day sliding event-time window (sliding every 1 min)
        SingleOutputStreamOperator<Tuple2<String, Long>> hashTagCounts = hashTags
                // Map to (hashtag, 1) tuples for counting
                .map(event -> Tuple2.of(event.hashtag, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))

                // Key by hashtag to allow distributed counting
                .keyBy(tuple -> tuple.f0)

                // Use a sliding window of 2 days, sliding every 1 minute
                .window(SlidingEventTimeWindows.of(Time.days(2), Time.minutes(1)))

                // Allow lateness of up to 5 minutes for late events
                .allowedLateness(Time.minutes(5))

                // Aggregate counts per hashtag in each window
                .aggregate(new CountAggregator(), new WindowedHashtagCountFunction());

        // Step 7: Compute the Top 50 hashtags per window from the aggregated counts
        hashTagCounts
                // Group all hashtag counts to a single key for Top-N computation
                .keyBy(tuple -> "all")  // Instead of tuple.f0, we use a constant key

                // Same sliding window for Top-N as before
                .window(SlidingEventTimeWindows.of(Time.days(2), Time.minutes(1)))

                // Sort and emit the Top 50
                .process(new TopNWindowFunction(50))
                .print();

        // Start the Flink job
        env.execute("Top 50 Hashtags in Last 2 Days");
    }

    /**
     * Aggregates the counts per hashtag per window.
     */
    public static class CountAggregator implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        public Long createAccumulator() { return 0L; }
        public Long add(Tuple2<String, Long> value, Long acc) { return acc + value.f1; }
        public Long getResult(Long acc) { return acc; }
        public Long merge(Long a, Long b) { return a + b; }
    }

    /**
     * Enriches the aggregation result with the hashtag key.
     */
    public static class WindowedHashtagCountFunction
            extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Long> counts,
                            Collector<Tuple2<String, Long>> out) {
            out.collect(Tuple2.of(key, counts.iterator().next()));
        }
    }

    /**
     * Computes and emits the top N (50) hashtags per window.
     */
    public static class TopNWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        private final int topSize;

        public TopNWindowFunction(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void process(String key, Context ctx, Iterable<Tuple2<String, Long>> elements,
                            Collector<String> out) {
            List<Tuple2<String, Long>> list = new ArrayList<>();
            for (Tuple2<String, Long> entry : elements) {
                list.add(entry);
            }

            // Sort by descending count
            list.sort((a, b) -> Long.compare(b.f1, a.f1));

            // Emit Top N
            for (int i = 0; i < Math.min(topSize, list.size()); i++) {
                Tuple2<String, Long> e = list.get(i);
                out.collect((i + 1) + ". " + e.f0 + " -> " + e.f1);
            }
        }
    }

    /**
     * Extracted hashtag with associated event timestamp.
     */
    public static class HashtagWithTimestamp {
        public String hashtag;
        public long timestamp;

        public HashtagWithTimestamp() {}  // Required for Flink POJO serialization

        public HashtagWithTimestamp(String hashtag, long timestamp) {
            this.hashtag = hashtag;
            this.timestamp = timestamp;
        }
    }

    /**
     * Input tweet event parsed from JSON.
     */
    public static class TweetEvent {
        public String text;
        public long timestamp;
    }
}
