package hpi.des.flink_tutorial.util;

import org.apache.flink.api.java.tuple.Tuple18;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

public class TaxiRideTuple extends Tuple18<String, LocalDateTime, LocalDateTime, Integer, Double, Integer, String, Integer, Integer,
        Integer, Double, Double, Double, Double, Double, Double, Double, Double> {

    public TaxiRideTuple(){
        super();
    }

    public TaxiRideTuple(String f0, LocalDateTime f1, LocalDateTime f2, Integer f3, Double f4, Integer f5,
                         String f6, Integer f7, Integer f8, Integer f9, Double f10, Double f11,
                         Double f12, Double f13, Double f14, Double f15, Double f16,
                         Double f17){
        super(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17);
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


}