package hpi.des.flink_tutorial.session1;

import hpi.des.flink_tutorial.util.InputFile;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TipInvestigation {

/*
Session 1: tip investigation

In this session, our objective is to analyze the tips given to taxi drivers in New York City in April 2020.
o The code relative to this session is located at hpi.des.flink_tutorial.session1.TipInvestigation
o Use the data dictionary located at hpi.des.flink_tutorial.resources.data_dict.txt to have an overview of the input
  data
o Also use the tuples that we prepared for you in hpi.des.flink_tutorial.util.datatypes
o You can use the method print() to debug your application
  (https://ci.apache.org/projects/flink/flink-docs-stable/dev/datastream_api.html#data-sinks)
o In this session, we are using processing time
  (https://ci.apache.org/projects/flink/flink-docs-release-1.12/learn-flink/streaming_analytics.html#event-time-and-watermarks).
*/

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sourceStream = env.readTextFile(InputFile.getInputFilePath());

        sourceStream.flatMap(new TransformSourceStreamOperator())//.map(...).filter(...) etc.
                // exercise 1
                // exercise 2
                // exercise 3
                // exercise 4
                // exercise 5
                .print();



        env.execute("Exercise Session 1");
    }
}
