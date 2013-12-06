package skyhadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

//
public class BNL_binary extends Experiment {
	public String name = "BNL";
	

	public static class MapDivision extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, PointWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, PointWritable> output,
				Reporter reporter) throws IOException {
			PointWritable p = new PointWritable(value.toString());
			p.id = (int) key.get();
			int divison = Partitioner.getpart(p);
			output.collect(new LongWritable(divison), p);
		}
	}

	public static class IdentityMapper extends MapReduceBase implements
			Mapper<LongWritable, PointWritable, LongWritable, PointWritable> {

		final LongWritable one = new LongWritable(1);

		public void map(LongWritable key, PointWritable value,
				OutputCollector<LongWritable, PointWritable> output,
				Reporter reporter) throws IOException {

			output.collect(one, value);
		}
	}

	// Point --> text
	public static class SkyReducer_PT extends MapReduceBase implements
			Reducer<LongWritable, PointWritable, LongWritable, Text> {

		public void reduce(LongWritable n, Iterator<PointWritable> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {

			Vector<Point> points = new Vector<Point>();
			while (values.hasNext()) {
				Point p = new Point(values.next());
				points.add(p);
			}
			Skyline skyline = new Skyline(points);
			skyline.Compute();

			for (int i = 0; i < skyline.skylines.size(); i++) {
				PointWritable s = new PointWritable(skyline.skylines.get(i));
				output.collect(n, new Text(s.toString()));
			}
		}
	}

	public static class SkyReducer_PP extends MapReduceBase implements
			Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {

		public void reduce(LongWritable n, Iterator<PointWritable> values,
				OutputCollector<LongWritable, PointWritable> output,
				Reporter reporter) throws IOException {

			Vector<Point> points = new Vector<Point>();
			while (values.hasNext()) {
				Point p = new Point(values.next());
				points.add(p);
			}
			Skyline skyline = new Skyline(points);
			skyline.Compute();

			for (int i = 0; i < skyline.skylines.size(); i++) {
				PointWritable s = new PointWritable(skyline.skylines.get(i));
				output.collect(n, s);
			}
		}
	}

	static JobConf conf = new JobConf(BNL.class);
	static JobConf gconf = new JobConf(BNL.class);

	public static void Divide(JobConf conf, String[] args) throws Exception {

		conf.setJobName("BNL-divide");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(PointWritable.class);

		conf.setMapperClass(MapDivision.class);
		if (combiner)
			conf.setCombinerClass(SkyReducer_PP.class);
		conf.setReducerClass(SkyReducer_PP.class);
		conf.setNumReduceTasks(reducers);
		conf.setNumMapTasks(mappers);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/tmp"));
		JobClient.runJob(conf);
	}

	public static void Gather(JobConf conf, String[] args) throws Exception {

		conf.setJobName("BNL-Gather");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(PointWritable.class);

		conf.setMapperClass(IdentityMapper.class);
		if (combiner)
			conf.setCombinerClass(SkyReducer_PP.class);
		conf.setReducerClass(SkyReducer_PT.class);
		conf.setNumReduceTasks(reducers);
		conf.setNumMapTasks(mappers);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[1] + "/tmp"));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/r"));
		JobClient.runJob(conf);
	}

	public static void run(String[] args) throws Exception {
		Divide(conf, args);
		Gather(gconf, args);
	}
}
