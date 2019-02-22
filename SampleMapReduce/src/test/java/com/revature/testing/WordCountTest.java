package com.revature.testing;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.WordMapper;
import com.revature.reduce.SumReducer;

public class WordCountTest {


	/*
	 * Declare harnesses that let you test a mapper, a reducer, and
	 * a mapper and a reducer working together.
	 */
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		WordMapper mapper = new WordMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		SumReducer reducer = new SumReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testGenericMapper() {
		String input = "I hope this works";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		
		mapDriver.withOutput(new Text("I"), new IntWritable(1));
		mapDriver.withOutput(new Text("hope"), new IntWritable(1));
		mapDriver.withOutput(new Text("this"), new IntWritable(1));
		mapDriver.withOutput(new Text("works"), new IntWritable(1));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testPuncMapper() {
		String input = "Let's get this bread.";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		
		mapDriver.withOutput(new Text("Let's"), new IntWritable(1));
		mapDriver.withOutput(new Text("get"), new IntWritable(1));
		mapDriver.withOutput(new Text("this"), new IntWritable(1));
		mapDriver.withOutput(new Text("bread"), new IntWritable(1));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testMorePuncMapper() {
		String input = "I love testing! This is fun!!! non-compliant?";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		
		mapDriver.withOutput(new Text("I"), new IntWritable(1));
		mapDriver.withOutput(new Text("love"), new IntWritable(1));
		mapDriver.withOutput(new Text("testing"), new IntWritable(1));
		mapDriver.withOutput(new Text("This"), new IntWritable(1));
		mapDriver.withOutput(new Text("is"), new IntWritable(1));
		mapDriver.withOutput(new Text("fun"), new IntWritable(1));
		mapDriver.withOutput(new Text("non-compliant"), new IntWritable(1));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testGenericReduce() {
		List<IntWritable> list = new ArrayList<IntWritable>();
		list.add(new IntWritable(1));
		list.add(new IntWritable(1));
		list.add(new IntWritable(1));
		
		
		reduceDriver.withInput(new Text("code"), list);
		reduceDriver.withOutput(new Text("code"), new IntWritable(3));
		reduceDriver.runTest();
	}
	

	@Test
	public void testNonOnesReduce() {
		List<IntWritable> list = new ArrayList<IntWritable>();
		list.add(new IntWritable(1));
		list.add(new IntWritable(3));
		list.add(new IntWritable(4));
		
		
		reduceDriver.withInput(new Text("code"), list);
		reduceDriver.withOutput(new Text("code"), new IntWritable(8));
		reduceDriver.runTest();
	}
	

	@Test
	public void testNegitiveReduce() {
		List<IntWritable> list = new ArrayList<IntWritable>();
		list.add(new IntWritable(-1));
		list.add(new IntWritable(1));
		list.add(new IntWritable(-2));
		
		
		reduceDriver.withInput(new Text("code"), list);
		reduceDriver.withOutput(new Text("code"), new IntWritable(-2));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		String input = "code code food food sleep code code sleep";
		mapReduceDriver.addInput(new LongWritable(0), new Text(input));
		mapReduceDriver.addOutput(new Text("code"), new IntWritable(4));
		mapReduceDriver.addOutput(new Text("food"), new IntWritable(2));
		mapReduceDriver.addOutput(new Text("sleep"), new IntWritable(2));
		
		mapReduceDriver.runTest();
	}
}
