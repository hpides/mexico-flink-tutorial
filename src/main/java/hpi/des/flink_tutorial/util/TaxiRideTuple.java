package hpi.des.flink_tutorial.util;

import org.apache.flink.api.java.tuple.Tuple18;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class TaxiRideTuple extends Tuple18<String, LocalDateTime, LocalDateTime, Integer, Double, Integer, String, Integer, Integer,
        Integer, Double, Double, Double, Double, Double, Double, Double, Double> {

    public TaxiRideTuple(){
        super();
    }

    public TaxiRideTuple(String VendorID, LocalDateTime tpep_pickup_datetime, LocalDateTime tpep_dropoff_datetime,
                         Integer passenger_count, Double trip_distance, Integer ratecodeID, String store_and_fwd_flag,
                         Integer PULocationID, Integer DOLocationID, Integer payment_type, Double Fare_amount,
                         Double Extra, Double Mta_tax, Double Tip_amount, Double Tolls_amount,
                         Double Improvement_surcharge, Double Total_amount, Double Congestion_surcharge){
        super(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, ratecodeID,
                store_and_fwd_flag, PULocationID, DOLocationID, payment_type, Fare_amount, Extra, Mta_tax, Tip_amount,
                Tolls_amount, Improvement_surcharge, Total_amount, Congestion_surcharge);
    }

    public TaxiRideTuple(String[] fields) throws Exception {
        this();

        if(fields.length != 18){
            throw new Exception();
        }

        for(int i = 0; i < this.getArity(); i++){
            if(i == 1 || i == 2){
                this.setField(this.parseDate(fields[i]), i);
            }
            else if(i == 3 || i == 5 || i == 7 || i == 8 || i == 9){
                this.setField(this.parseInt(fields[i]), i);
            }
            else if(i == 4 || i >= 10){
                this.setField(this.parseDouble(fields[i]), i);
            }
            else{
                this.setField(fields[i], i);
            }
        }
    }

    private LocalDateTime parseDate(String date){
        try{
            String timePattern = "yyyy-MM-dd HH:mm:ss";
            DateTimeFormatter parser = DateTimeFormatter.ofPattern(timePattern, Locale.US);
            return LocalDateTime.parse(date, parser);
        }
        catch (Exception e){
            return null;
        }
    }

    private Integer parseInt(String value){
        try {
            return Integer.parseInt(value);
        }
        catch (Exception e){
            return null;
        }
    }

    private Double parseDouble(String value){
        try {
            return Double.parseDouble(value);
        }
        catch (Exception e){
            return null;
        }
    }

    public String VendorID() {return this.f0;}
    public LocalDateTime tpep_pickup_datetime() {return this.f1;}
    public LocalDateTime tpep_dropoff_datetime() {return this.f2;}
    public Integer passenger_count() {return this.f3;}
    public Double trip_distance() {return this.f4;}
    public Integer ratecodeID() {return this.f5;}
    public String store_and_fwd_flag() {return this.f6;}
    public Integer PULocationID() {return this.f7;}
    public Integer DOLocationID() {return this.f8;}
    public Integer payment_type() {return this.f9;}
    public Double Fare_amount() {return this.f10;}
    public Double Extra() {return this.f11;}
    public Double Mta_tax() {return this.f12;}
    public Double Tip_amount() {return this.f13;}
    public Double Tolls_amount() {return this.f14;}
    public Double Improvement_surcharge() {return this.f15;}
    public Double Total_amount() {return this.f16;}
    public Double Congestion_surcharge() {return this.f17;}
}