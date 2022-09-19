package hpi.des.flink_tutorial.util;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class StreamingFileSinkFactory {

    static public <IN> StreamingFileSink<IN> newSink(String pathToDir){
        return StreamingFileSink.forRowFormat(
                new Path(pathToDir),
                new SimpleStringEncoder<IN>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 100)
                                .build())
                .build();
    }
}
