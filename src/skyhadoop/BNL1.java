package skyhadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

// the difference here are:
// the mapper does not parition the input
//but this is the 
public class BNL1 extends BNL {

	public String name = "BNL1";

	public static class MapnoDivision extends 
			Mapper<LongWritable, Text, LongWritable, PointWritable> {
		final LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			PointWritable p = new PointWritable(value.toString());

			context.write(one, p);
		}
	}

	public static void Divide(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJobName("BNL1-divide");

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PointWritable.class);
		job.setJarByClass(BNL1.class);
		job.setMapperClass(MapnoDivision.class);
		if (combiner)
			job.setCombinerClass(SkyReducer_PP.class);
	//	job.setPartitionerClass(PointParitioner.class);
		job.setReducerClass(SkyReducer_PT.class);
		job.setNumReduceTasks(reducers);
		//conf.setNumMapTasks(mappers);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/tmp"));

		try {
			//JobClient.runJob(conf);
			job.waitForCompletion(true);
		} catch (Exception e) {
			success = false;

		}
	}

	public static void run(String[] args) throws Exception {
		Divide(args);
		Gather(args);
	}
}
