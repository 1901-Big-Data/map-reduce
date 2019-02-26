package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DelayCount extends Mapper<LongWritable, Text, Text, IntWritable>{
	public enum BAD_INPUT {
		NO_DELAY_FOUND,
		NOT_A_NUMBER
	};
	static volatile private ArrayList<String> headers = new ArrayList<String>();
	// delays must be in the order of occurracne of the csv
	static volatile private String[] delays = {"CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay"};
	
	{
		String input = ",Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay";
		String[] lineOne = input.split(",");
		
		for(String header: lineOne) {
			headers.add(header);
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();	
		
		// Handle the header line
		if (line.charAt(0) == ',') {
			return;
		}
		
		String[] data = line.split(",");
		for (String delay: delays) {
			try {				
				int delayLocationInCSV = headers.indexOf(delay);
				
				if (delayLocationInCSV >= data.length) {
					//context.getCounter(BAD_INPUT.NO_DELAY_FOUND).increment(1L);
					return;
				}
				
				int time = Integer.parseInt(data[delayLocationInCSV]);
				if (time > 0) {
					context.write(new Text(delay), new IntWritable(time));
				}
			} catch (NumberFormatException e) {
				//context.getCounter(BAD_INPUT.NOT_A_NUMBER).increment(1L);
			}
		}
	}
}
