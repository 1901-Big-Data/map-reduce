package driver;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Bad_Input;
import com.revature.map.DelayCount;

public class Driver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		/*
		 * The expected command-line arguments are the paths containing
		 * input and output data. Terminate the job if the number of
		 * command-line arguments is not exactly 2.
		 */
		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}

		/*
		 * Instantiate a Job object for your job's configuration.  
		 */
		Job job = new Job();
		job.setNumReduceTasks(0);

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(Driver.class);

		/*
		 * Specify an easily-decipherable name for the job.
		 * This job name will appear in reports and logs.
		 */
		job.setJobName("Delay");

		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(DelayCount.class);

		/*
		 * For the word count application, the input file and output 
		 * files are in text format - the default format.
		 * 
		 * In text format files, each record is a line delineated by a 
		 * by a line terminator.
		 * 
		 * When you use other input formats, you must call the 
		 * SetInputFormatClass method. When you use other 
		 * output formats, you must call the setOutputFormatClass method.
		 */

		/*
		 * For the word count application, the mapper's output keys and
		 * values have the same data types as the reducer's output keys 
		 * and values: Text and IntWritable.
		 * 
		 * When they are not the same data types, you must call the 
		 * setMapOutputKeyClass and setMapOutputValueClass 
		 * methods.
		 */

		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		/*
		 * Start the MapReduce job and wait for it to finish.
		 * If it finishes successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);
		
		Counters cn = job.getCounters();
		
		// Find the specific counters that you want to print
		Counter c1=cn.findCounter(Bad_Input.NO_DELAY_FOUND);
		System.out.println(c1.getDisplayName()+":"+c1.getValue());
		System.exit(success ? 0 : 1);

	}
}
