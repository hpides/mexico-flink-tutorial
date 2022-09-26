package hpi.des.flink_tutorial.session1;

/*
Exercise 2) The value of the field “Tip_amount” applies only to fares paid with a credit card. Furthermore, we are only
interested in standard fares (you need to check the “ratecodeID”). Write an operator capable of filtering fares that
were not paid (you need to check the “payment_type”) with a credit card (not 1) or which are not standard fares (not 1)
and remove tuples with null as “ratecodeID”.

Tip 2) The filter operator is capable of filtering events from a stream
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#filter).
 */

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise2Operator {
}
