/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hpi.des.flink_tutorial.util.datatypes;


import hpi.des.flink_tutorial.session3.generator.utils.DataGenerator;
import hpi.des.flink_tutorial.session3.generator.utils.GeoUtils;
import org.apache.flink.api.java.tuple.Tuple18;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * <p>A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the type of the event (start or end)
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the taxiId
 * - the driverId
 */
public class TaxiRideTuple extends Tuple18<String, LocalDateTime, LocalDateTime, Integer, Double, Integer, String, Integer, Integer,
		Integer, Double, Double, Double, Double, Double, Double, Double, Double> {

	/**
	 * Creates a new TaxiRide with now as start and end time.
	 */
	public TaxiRideTuple() {
		this.f1 = LocalDateTime.now();
		this.f2 = LocalDateTime.now();
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
	public long rideId() {return this.rideId;}
	public boolean isStart() {return this.isStart;}
	public float startLon() {return this.startLon;}
	public float startLat() {return this.startLat;}
	public float endLon() {return this.endLon;}
	public float endLat() {return this.endLat;}
	public long taxiId() {return this.taxiId;}
	public long driverId() {return this.driverId;}

	/**
	 * Invents a TaxiRide.
	 */
	public TaxiRideTuple(long rideId, boolean isStart) {
		DataGenerator g = new DataGenerator(rideId);

		this.rideId = rideId;
		this.isStart = isStart;
		this.f1 = g.startTime();
		this.f2 = isStart ? LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC) : g.endTime();
		this.startLon = g.startLon();
		this.startLat = g.startLat();
		this.endLon = g.endLon();
		this.endLat = g.endLat();
		this.f3 = g.passengerCnt();
		this.taxiId = g.taxiId();
		this.driverId = g.driverId();
	}

	/**
	 * Creates a TaxiRide with the given parameters.
	 */
	public TaxiRideTuple(long rideId, boolean isStart, LocalDateTime startTime, LocalDateTime endTime,
						 float startLon, float startLat, float endLon, float endLat,
						 int passengerCnt, long taxiId, long driverId) {
		this.rideId = rideId;
		this.isStart = isStart;
		this.f1 = startTime;
		this.f2 = endTime;
		this.startLon = startLon;
		this.startLat = startLat;
		this.endLon = endLon;
		this.endLat = endLat;
		this.f3 = passengerCnt;
		this.taxiId = taxiId;
		this.driverId = driverId;
	}

	public long rideId;
	public boolean isStart;
	public float startLon;
	public float startLat;
	public float endLon;
	public float endLat;
	public long taxiId;
	public long driverId;

	@Override
	public String toString() {

		return rideId + "," +
				(isStart ? "START" : "END") + "," +
				f1.toString() + "," +
				f2.toString() + "," +
				startLon + "," +
				startLat + "," +
				endLon + "," +
				endLat + "," +
				f3 + "," +
				taxiId + "," +
				driverId;
	}

	/**
	 * Compares this TaxiRide with the given one.
	 *
	 * <ul>
	 *     <li>sort by timestamp,</li>
	 *     <li>putting START events before END events if they have the same timestamp</li>
	 * </ul>
	 */
	public int compareTo(@Nullable TaxiRideTuple other) {
		if (other == null) {
			return 1;
		}
		int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
		if (compareTimes == 0) {
			if (this.isStart == other.isStart) {
				return 0;
			}
			else {
				if (this.isStart) {
					return -1;
				}
				else {
					return 1;
				}
			}
		}
		else {
			return compareTimes;
		}
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof TaxiRideTuple &&
				this.rideId == ((TaxiRideTuple) other).rideId;
	}

	@Override
	public int hashCode() {
		return (int) this.rideId;
	}

	/**
	 * Gets the ride's time stamp (start or end time depending on {@link #isStart}).
	 */
	public long getEventTime() {
		if (isStart) {
			return f1.toEpochSecond(ZoneOffset.UTC);
		}
		else {
			return f2.toEpochSecond(ZoneOffset.UTC);
		}
	}

	/**
	 * Gets the distance from the ride location to the given one.
	 */
	public double getEuclideanDistance(double longitude, double latitude) {
		if (this.isStart) {
			return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.startLon, this.startLat);
		} else {
			return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.endLon, this.endLat);
		}
	}
}
