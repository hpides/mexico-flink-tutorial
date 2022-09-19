package hpi.des.flink_tutorial.session1;

import hpi.des.flink_tutorial.util.InputFile;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TipInvestigation {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sourceStream = env.readTextFile(InputFile.getInputFilePath());

        sourceStream.flatMap(new TransformSourceStreamOperator())
                // exercise 1
                // exercise 2
                // exercise 3
                // exercise 4
                // exercise 5
                .print();



        env.execute("Exercise Session 1");
    }
}
