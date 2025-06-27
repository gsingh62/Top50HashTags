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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A flink job that finds the top 50 hash tags in twitter posts over last 2 days only.
 * There could be billions of posts in the last 2 days and they're all coming in realtime - a 5 minute delay in results
 * is allowed.
 * Example input from tweet database ->
 * {"text": "I love #Flink and #Streaming", "timestamp": 1729968800000}
 */

public class Top50HashTags {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> tweetStream = env.socketTextStream("localhost", 9999);
       DataStream<HashtagWithTimestamp> hashTags = tweetStream
               .flatMap((String line, Collector<HashtagWithTimestamp> out) -> {
                   ObjectMapper mapper = new ObjectMapper();
                   TweetEvent tweet = mapper.readValue(line, TweetEvent.class);
                   Pattern pattern = Pattern.compile("#\\w+");
                   Matcher matcher = pattern.matcher(tweet.text);
                   while (matcher.find()) {
                       out.collect(new HashtagWithTimestamp(matcher.group().toLowerCase(), tweet.timestamp));
                   }

               })
               .returns(HashtagWithTimestamp.class)
               .assignTimestampsAndWatermarks(WatermarkStrategy
                                        .<HashtagWithTimestamp>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                        .withTimestampAssigner((event, ts) -> event.timestamp)
               );

       // Step 1: Key by hashtag and count in parallel
       SingleOutputStreamOperator<Tuple2<String, Long>> hashTagCounts = hashTags
               .map(event -> Tuple2.of(event.hashtag, 1L))
               .returns(Types.TUPLE(Types.STRING, Types.LONG))
               .keyBy(tuple -> tuple.f0)
               .window(SlidingEventTimeWindows.of(Time.days(2), Time.minutes(1)))
               .allowedLateness(Time.minutes(5))
               .aggregate(new CountAggregator(), new WindowedHashtagCountFunction());

       env.execute("Top 50 Hashtags in Last 2 days");

       // Step 2: Send to one task for Top N
       hashTagCounts
               .keyBy(tuple -> tuple.f0) // Optional: Or use a constant key to group all
               .window(SlidingEventTimeWindows.of(Time.days(2), Time.minutes(1)))
               .process(new TopNWindowFunction(50))
               .print();
    }

    public static class WindowedHashtagCountFunction extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Long> counts, Collector<Tuple2<String, Long>> out) throws Exception {
            out.collect(Tuple2.of(key, counts.iterator().next()));
        }
    }
    public static class CountAggregator implements AggregateFunction<Tuple2<String, Long>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long acc) {
            return acc + value.f1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class TopNWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {
        private final int topSize;
        private final Map<String, Long> hashTagCounts = new HashMap<>();

        public TopNWindowFunction(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void process(String key, Context ctx, Iterable<Tuple2<String, Long>> elements, Collector<String> out) {
            List<Tuple2<String, Long>> list = new ArrayList<>();
            for (Tuple2<String, Long> entry: elements) {
                list.add(entry);
            }
            list.sort((a, b) -> Long.compare(b.f1, a.f1));
            for (int i = 0; i < Math.min(topSize, list.size()); i++) {
                Tuple2<String, Long> e = list.get(i);
                out.collect((i + 1) + ". " + e.f0 + " -> " + e.f1);
            }
        }
    }
    public static class HashtagWithTimestamp {
        public String hashtag;
        public long timestamp;

        public HashtagWithTimestamp(String hashtag, long timestamp) {
            this.hashtag = hashtag;
            this.timestamp = timestamp;
        }
    }

    public static class TweetEvent {
        public String text;
        public long timestamp;
    }
}