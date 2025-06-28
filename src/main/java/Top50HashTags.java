import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
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

public class Top50HashTags {

    public static DataStream<HashtagWithTimestamp> extractHashtags(DataStream<String> tweetStream) {
        return tweetStream
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
                        .withTimestampAssigner((event, ts) -> event.timestamp));
    }

    public static DataStream<Tuple2<String, Long>> computeHashtagCounts(DataStream<HashtagWithTimestamp> hashtags) {
        return hashtags
                .map(event -> Tuple2.of(event.hashtag, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tuple -> tuple.f0)
                .window(SlidingEventTimeWindows.of(Time.days(2), Time.minutes(1)))
                //.allowedLateness(Time.minutes(5))
                .aggregate(new CountAggregator(), new WindowedHashtagCountFunction());
    }

    public static DataStream<String> computeTopN(DataStream<Tuple2<String, Long>> hashtagCounts, int topN) {
        return hashtagCounts
                .keyBy(tuple -> "global")
                .window(SlidingEventTimeWindows.of(Time.days(2), Time.minutes(1)))
                .process(new TopNWindowFunction(topN));
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> tweetStream = env.socketTextStream("localhost", 9999);

        DataStream<HashtagWithTimestamp> hashtags = extractHashtags(tweetStream);
        DataStream<Tuple2<String, Long>> counts = computeHashtagCounts(hashtags);
        DataStream<String> topN = computeTopN(counts, 50);

        topN.print();
        env.execute("Top 50 Hashtags in Last 2 Days");
    }

    public static class WindowedHashtagCountFunction extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> counts, Collector<Tuple2<String, Long>> out) {
            out.collect(Tuple2.of(key, counts.iterator().next()));
        }
    }

    public static class CountAggregator implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        public Long createAccumulator() { return 0L; }
        public Long add(Tuple2<String, Long> value, Long acc) { return acc + value.f1; }
        public Long getResult(Long acc) { return acc; }
        public Long merge(Long a, Long b) { return a + b; }
    }

    public static class TopNWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {
        private final int topSize;

        public TopNWindowFunction(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void process(String key, Context ctx, Iterable<Tuple2<String, Long>> elements, Collector<String> out) {
            List<Tuple2<String, Long>> list = new ArrayList<>();
            for (Tuple2<String, Long> entry : elements) {
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

        public HashtagWithTimestamp() {}

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
