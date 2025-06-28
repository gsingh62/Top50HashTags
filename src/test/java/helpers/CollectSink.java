package helpers;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class CollectSink implements SinkFunction<String> {
    public static final List<String> values = Collections.synchronizedList(new LinkedList<>());

    @Override
    public void invoke(String value, Context context) {
        values.add(value);
    }

    public static void clear() {
        values.clear();
    }
}