package com.revature.map;

import java.util.ArrayList;

public class Temp {
	static volatile private String[] delays = {"CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay"};
	public static void main(String[] args) {
		ArrayList<String> headers = new ArrayList<String>();
		String input = ",Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay";
		String[] lineOne = input.split(",");
		
		for(String header: lineOne) {
			headers.add(header);
		}
		
		System.out.println(headers.indexOf(delays[4]));
		System.out.println("0,2008,1,3,4,2003.0,1955,2211.0,2225,WN,335,N712SW,128.0,150.0,116.0,-14.0,8.0,IAD,TPA,810,4.0,8.0,0,N,0,,,,,".split(","));
	}
}
