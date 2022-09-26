package hpi.des.flink_tutorial.session1;

/*
Exercise 4) We would like to improve our tip ratio metric by also calculating the tip ratio per passenger. Moreover, we
want to discard events having an invalid value (null or <= 0) for the number of passengers or containing a tip ratio per
passenger of <= 0.01. Add the ratio at the last position of the tuple.

Tip 4) The flatMap operator is capable of transforming a stream and outputting zero or many results from a single event
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#flatmap).
 */

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise4Operator {
}
