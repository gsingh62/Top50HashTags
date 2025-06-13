import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.dynamodb.source.DynamoDbStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.flink.streaming.api.windowing.time.Time;
import java.util.Date;

// Example post: This is the best day of my life #BestDayOfMyLife
public class Top50HashTags {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<PostRecord> dataStream = env
                .socketTextStream("localhost", 9999)
                .map(line -> parseLine(line))   // convert line to MyRecord
                .keyBy(value -> value.hashtag)
                .window(SlidingEventTimeWindows.of(Time.days(2), Time.minutes(5)))
                .sum(1);


        // Create a new DynamoDbStreamsSource to read from the specified DynamoDB Stream.
        //DynamoDbStreamsSource<String> dynamoDbStreamsSource = DynamoDbStreamsSource.<String>builder()
               // .setStreamArn("arn:aws:dynamodb:us-east-1:012345678901:table/ your_table_name/ stream/ stream-label")
                //.setDeserializationSchema(new SimpleStringSchema())
                //.build();

        // Specify watermarking strategy and the name of the DynamoDB Streams Source operator.
        // Specify return type using TypeInformation.
        // Specify UID of operator in line with Flink best practice.
        //DataStream<Tuple2<String, String>> cdcEvents = env.fromSource(dynamoDbStreamsSource, WatermarkStrategy.noWatermarks(), "PostsDB")
                //.flatMap(new Splitter());

        // interpret the insert-only DataStream as a Table
        //Table inputTable = tEnv.fromDataStream(cdcEvents, schema).as("timestamp", "hashtag");

        // register the Table object as a view and query it
        // the query contains an aggregation that produces updates
        //tEnv.createTemporaryView("InputTable", inputTable);
        //Table resultTable = tEnv.sqlQuery(
               // "SELECT hashtag, COUNT(*) FROM InputTable GROUP BY hashtag");

        // interpret the insert-only Table as a DataStream again
        //DataStream<Row> resultStream = tEnv.toDataStream(resultTable);

        // add a printing sink and execute in DataStream API
        //resultStream.print();
        dataStream.print();

        env.execute("Window WordCount");
    }

    public PostRecord parseLine(String line) {
        int firstComma = line.indexOf(',');

        long eventTime = Long.parseLong(line.substring(0, firstComma));
        String post = line.substring(firstComma + 1);
        return new PostRecord(eventTime, post);
    }

    public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    public class PostRecord {
        String post;
        long eventTime;

        public PostRecord(long eventTime, String post) {
            this.eventTime = eventTime;
            this.post = post;
        }
    }
}

/**
for each record - get timestamp, if in last 2 days, for all hash-tags
 */