package hpi.des.flink_tutorial.session1;

import hpi.des.flink_tutorial.util.TaxiRideTuple;
import hpi.des.flink_tutorial.util.TupleExercise1;
import org.apache.flink.api.common.functions.MapFunction;

/*
Exercise 1) Analyze the class TaxiRideTuple which defines the data type parsed from the input data. As we do not need
all the 18 fields defined in TaxiRideTuple, write an operator that will transform a TaxiRide to a Tuple7 containing only
the fields “tpep_pickup_date_time”, “tpep_dropoff_datetime”, “passenger_count”, “ratecodeID”, “payment_type”,
“Tip_amount” and “Total_amount”.

Tip 1) The map operator is one of the Flink operators capable of transforming a stream
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#map). Use the data dictionary located
at hpi.des.flink_tutorial.resources.data_dict.txt to have an overview of the input data.
 */

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise1Operator {
}

