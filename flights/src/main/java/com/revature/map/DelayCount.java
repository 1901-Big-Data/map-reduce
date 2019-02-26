package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DelayCount extends Mapper<LongWritable, Text, Text, FloatWritable>{
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
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
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
				
				if (delayLocationInCSV >= data.length || delayLocationInCSV < 0) {
					// Need to upgrade Hadoop to be able to use custom counters
					//context.getCounter(Bad_Input.NO_DELAY_FOUND).increment(1L);
					return;
				}
				
				float time = Float.parseFloat(data[delayLocationInCSV]);
				if (time > 0) {
					context.write(new Text(delay), new FloatWritable(time));
				}
			} catch (NumberFormatException e) {
				//context.getCounter(BAD_INPUT.NOT_A_NUMBER).increment(1L);
			}
		}
	}
}
