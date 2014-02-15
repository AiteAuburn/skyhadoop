package skyhadoop;

import java.net.URI;
import java.io.IOException;
import java.util.Vector;
import java.io.DataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

//hadoop jar skyhadoop.jar skyhadoop.Experiment BNL /home/khalefa/m -m4 

public class vldb extends Experiment {
	public String name = "vldb";

	public static class MapDivision extends
			Mapper<LongWritable, Text, LongWritable, PointWritable> {

		protected void setup(Context context) throws IOException,
				InterruptedException {

			try {
				System.err.println("****************SETUP map");
				Configuration conf = context.getConfiguration();
				FileSystem fs = FileSystem.getLocal(conf);

				Path partFile = new Path("10_2_sampled");
				Vector<Point> pnts = readSamplePoints(fs, partFile, conf);
				Point p = pnts.get(0);
				int dim = p.dim;
				int threshold = 2;
				SkyQuadTree sq = new SkyQuadTree(dim, threshold, 0, 1000);
				sq.addpoints(pnts);
				sq.MarkDominatedNode();
				System.out.print(sq);
			} catch (IOException ie) {
				throw new IllegalArgumentException(
						"can't read partitions file", ie);
			}

		}

		static Vector<Point> readSamplePoints(FileSystem fs, Path p,
				Configuration conf) throws IOException {
			Vector<Point> v = new Vector<Point>();
			FSDataInputStream reader = fs.open(p);

			for (int i = 0; i < 4; i++) {
				PointWritable pp = new PointWritable();
				pp.readFields(reader);
				Point pt = (Point) pp;
				v.add(pt);

			}
			reader.close();
			return v;
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			PointWritable p = new PointWritable(value.toString());

			p.id = (int) key.get();

			int divison = Partitioner.getpart(p);

			context.write(new LongWritable(divison), p);
			System.out.println(divison + " " + p.toString());
		}
	}

	public static class IdentityMapper extends
			Mapper<LongWritable, Text, LongWritable, PointWritable> {

		final LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			context.write(one, new PointWritable(value.toString()));

		}
	}

	// Point --> text
	public static class SkyReducer_PT extends
			Reducer<LongWritable, PointWritable, LongWritable, Text> {
		@Override
		public void reduce(LongWritable n, Iterable<PointWritable> values,
				Context context) throws IOException, InterruptedException {
			if (debug)
				System.out.println("Reducer at PT" + n.toString());
			Vector<Point> points = new Vector<Point>();
			for (PointWritable a : values) {
				Point p = new Point(a);
				points.add(p);

				if (debug)
					System.out.println(n.toString() + '(' + p.toString() + ')');

			}
			Skyline skyline = new Skyline(points);
			skyline.Compute();
			if (debug)
				System.out.println("Skyline " + points.size() + " "
						+ skyline.skylines.size());
			for (int i = 0; i < skyline.skylines.size(); i++) {
				PointWritable s = new PointWritable(skyline.skylines.get(i));
				context.write(n, new Text(s.toString()));
				if (debug)
					System.out.println(n.toString() + '(' + s.toString() + ')');
			}
		}
	}

	public static class SkyReducer_PP extends
			Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {
		@Override
		public void reduce(LongWritable n, Iterable<PointWritable> values,
				Context context) throws IOException, InterruptedException {

			if (debug)
				System.out.println("Combiner");
			Vector<Point> points = new Vector<Point>();
			for (PointWritable pp : values) {
				Point p = new Point(pp);
				points.add(p);

				if (debug)
					System.out.println(n.toString() + '(' + p.toString() + ')');

			}

			System.out.println("End Combiner");
			Skyline skyline = new Skyline(points);
			skyline.Compute();

			for (int i = 0; i < skyline.skylines.size(); i++) {
				PointWritable s = new PointWritable(skyline.skylines.get(i));
				context.write(n, s);
			}
		}
	}

	public static void Divide(String[] args) throws Exception {
		System.out.println("Sample");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(BNL.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PointWritable.class);

		job.setMapperClass(MapDivision.class);
		job.setReducerClass(SkyReducer_PT.class);
		job.setNumReduceTasks(reducers);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/tmp"));

		try {
			Vector<Point> pnts = PointSampler.sample(job, 1000);
			// build the quadtree

			/*
			 * Point p = pnts.get(0); int dim = p.dim; int threshold = 2;
			 * SkyQuadTree sq = new SkyQuadTree(dim, threshold, 0, 1000);
			 * sq.addpoints(pnts); sq.MarkDominatedNode();
			 */
			Path sample = new Path(args[1] + "sampled");
			URI uri = new URI(sample.toString() + "#" + "10_2_sampled");
			job.addCacheFile(uri);

			writeSampleFile(job, sample, pnts);

			job.waitForCompletion(true);
		} catch (Exception ee) {

		} catch (Throwable e) {
			success = false;
		}

	}

	public static void writeSampleFile(final JobContext job, Path partFile,
			Vector<Point> pnts) throws Throwable {
		Configuration conf = job.getConfiguration();

		FileSystem outFs = partFile.getFileSystem(conf);
		System.out.println("*****************" + partFile.toString());
		System.out.println("*****************" + partFile.toUri());
		DataOutputStream writer = outFs.create(partFile, true, 64 * 1024,
				(short) 10, outFs.getDefaultBlockSize(partFile));
		for (Point p : pnts) {
			PointWritable pp = new PointWritable(p);
			pp.write(writer);
		}
		writer.close();
	}

	public static void Gather(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJobName("BNL-Gather");

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PointWritable.class);

		job.setMapperClass(IdentityMapper.class);
		job.setCombinerClass(SkyReducer_PP.class);
		job.setReducerClass(SkyReducer_PT.class);
		job.setNumReduceTasks(reducers);
		job.setJarByClass(BNL.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1] + "/tmp"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/r"));
		try {
			// JobClient.runJob(conf);
			job.waitForCompletion(true);
		} catch (Exception e) {
			success = false;

		}
	}

	public static void main(String[] args) {

	}

	public static void run(String[] args) throws Exception {
		if (debug)
			System.out.println("Debug" + debug + "\n" + reducers);

		Divide(args);
		// Gather(args);
	}
}
