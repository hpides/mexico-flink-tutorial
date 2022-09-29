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

import java.io.Serializable;

/**
 * A TaxiFare has payment information about a taxi ride.
 *
 * <p>It has these fields in common with the TaxiRides
 * - the rideId
 * - the taxiId
 * - the driverId
 * - the startTime
 *
 * <p>It also includes
 * - the paymentType
 * - the tip
 * - the tolls
 * - the totalFare
 */
public class TaxiFareTuple implements Serializable {

	/**
	 * Invents a TaxiFare.
	 */
	public TaxiFareTuple(long rideId) {
		DataGenerator g = new DataGenerator(rideId);

		this.rideId = rideId;
		this.taxiId = g.taxiId();
		this.driverId = g.driverId();
		this.startTime = g.startTime();
		this.paymentType = g.paymentType();
		this.tip = g.tip();
		this.tolls = g.tolls();
		this.totalFare = g.totalFare();
	}

	public long rideId() {return rideId;}
	public long taxiId() {return taxiId;}
	public long driverId() {return driverId;}
	public long startTime() {return startTime;}
	public String paymentType() {return paymentType;}
	public float tip() {return tip;}
	public float tolls() {return tolls;}
	public float totalFare() {return totalFare;}

	public long rideId;
	public long taxiId;
	public long driverId;
	public Long startTime;
	public String paymentType;
	public float tip;
	public float tolls;
	public float totalFare;

	@Override
	public String toString() {

		return rideId + "," +
				taxiId + "," +
				driverId + "," +
				startTime.toString() + "," +
				paymentType + "," +
				tip + "," +
				tolls + "," +
				totalFare;
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof TaxiFareTuple &&
				this.rideId == ((TaxiFareTuple) other).rideId;
	}

	@Override
	public int hashCode() {
		return (int) this.rideId;
	}

	/**
	 * Gets the fare's start time.
	 */
	public long getEventTime() {
		return startTime;
	}

}
