package hpi.des.flink_tutorial.session2;

import hpi.des.flink_tutorial.util.TaxiRideTuple;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

public class PreprocessStream implements FlatMapFunction<TaxiRideTuple, Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>>{

    @Override
    public void flatMap(TaxiRideTuple event, Collector<Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>> out) throws Exception {
        double tipRatio;
        double tipRatioPerPassenger;

        // keep only standard fares payed with credit card
        if(event.f5 != null && event.f5 == 1 && event.f9 != null && event.f9 == 1){

            // check whether tip and total fare are valid and calculate ratio tip/fare
            if(event.f13 != null && event.f13 >= 0 && event.f16 != null && event.f16 > 0){
                tipRatio = event.f13/event.f16;

                // check whether number of passengers is valid and calculate tip ratio per passenger
                if(event.f3 != null && event.f3 > 0){
                    tipRatioPerPassenger = tipRatio/event.f3;

                    // filter out ratios <= 1%
                    if(tipRatioPerPassenger > 0.01){
                        out.collect(new Tuple5<>(event.f7, event.f1, event.f8, event.f2, tipRatioPerPassenger));
                    }
                }
            }
        }
    }
}
