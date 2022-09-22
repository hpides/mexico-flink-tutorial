package hpi.des.flink_tutorial.session0;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/*
Session 0: Hello World!
Install IntelliJ https://www.jetbrains.com/de-de/idea/download/#section=linux and download the tutorial repository with
git clone git@github.com:hpides/mexico-flink-tutorial.git After that start IntelliJ and open the repository folder.
Build the project, open the file src/main/java/hpi/des/flink_tutorial/session0/HelloWorld.java and run the main
function.
 */

public class HelloWorld {
    public static void main(String[] args) throws Exception {

        // The StreamExecutionEnvironment defines a context in which your stream processing job is executed
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // We limit the parallelism of our application to 1, meaning that data will not be partitioned and
        // that there will be only one instance of each operator running one after the other.
        env.setParallelism(1);

        // We create an ArrayList of words that will be used as a source for our stream
        ArrayList<String> words = new ArrayList<>();
        words.add("hello");
        words.add("world");
        words.add("from");
        words.add("Flink");

        // we create a stream using the ArrayList "words" as the source. A stream processing job must have at least one source
        DataStream<String> stream = env.fromCollection(words);

        // operators such as filter, map, flatMap, etc., would be used here:
        // e.g. DataStream<String> transformedStream = stream.filter(***);

        // print() uses the stdout as a sink for our job. A stream processing job must have at least one sink.
        stream.print();

        // "compiles" and run the job
        env.execute("Exercise Session 0");
    }
}
