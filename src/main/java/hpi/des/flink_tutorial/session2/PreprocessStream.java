package hpi.des.flink_tutorial.session2;

import hpi.des.flink_tutorial.util.TaxiRideTuple;
import hpi.des.flink_tutorial.util.TupleExercise7;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class PreprocessStream implements FlatMapFunction<TaxiRideTuple, TupleExercise7>{

    @Override
    public void flatMap(TaxiRideTuple event, Collector<TupleExercise7> out) throws Exception {
        double tipRatio;
        double tipRatioPerPassenger;

        // keep only standard fares payed with credit card
        if(event.ratecodeID() != null && event.ratecodeID() == 1 &&
                event.payment_type() != null && event.payment_type() == 1){

            // check whether tip and total fare are valid and calculate ratio tip/fare
            if(event.Tip_amount() != null && event.Tip_amount() >= 0 &&
                    event.Total_amount() != null && event.Total_amount() > 0){
                tipRatio = event.Tip_amount()/event.Total_amount();

                // check whether number of passengers is valid and calculate tip ratio per passenger
                if(event.passenger_count() != null && event.passenger_count() > 0){
                    tipRatioPerPassenger = tipRatio/event.passenger_count();

                    // filter out ratios <= 1%
                    if(tipRatioPerPassenger > 0.01){
                        out.collect(new TupleExercise7(event.PULocationID(), event.tpep_pickup_datetime(),
                                event.DOLocationID(), event.tpep_dropoff_datetime(), tipRatioPerPassenger));
                    }
                }
            }
        }
    }
}
