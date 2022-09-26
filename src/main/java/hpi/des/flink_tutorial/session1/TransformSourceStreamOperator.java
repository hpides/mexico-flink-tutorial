package hpi.des.flink_tutorial.session1;

import hpi.des.flink_tutorial.util.datatypes.TaxiRideTuple;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

// This is an operator example.

public class TransformSourceStreamOperator implements FlatMapFunction<String, TaxiRideTuple> {
    public void flatMap(String value, Collector<TaxiRideTuple> out) throws Exception {
        String[] fields = value.split(",");
        try {
            out.collect(new TaxiRideTuple(fields));
        } catch (Exception ignored) {}
    }
}
