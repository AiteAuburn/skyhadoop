package skyhadoop;

import java.net.URI;
import java.net.URISyntaxException;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
			Mapper<LongWritable, Text, Text, PointWritable> {
		SkyQuadTree sq;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			try {

				Configuration conf = context.getConfiguration();
				int threshold = conf.getInt("threshold", 5);
				FileSystem fs = FileSystem.getLocal(conf);
				sq = null;
				for (URI u : context.getCacheFiles()) {
					System.err.println(u.toString());

					Path partFile = new Path(u.getPath());
					Vector<Point> pnts = PointSampler.readSamplePoints(fs,
							partFile);
					Point p = pnts.get(0);
					int dim = p.dim;

					sq = new SkyQuadTree(dim, threshold, 0, 1000);

					sq.addpoints(pnts);
					sq.MarkDominatedNode();
					// System.out.println(sq.toString());
				}
			} catch (IOException ie) {
				throw new IllegalArgumentException("can't read sample  file",
						ie);
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			PointWritable p = new PointWritable(value.toString());
			QuadTree.Node n = sq.getNode(p);

			context.write(new Text(n.id), p);
			// System.out.println(n.id + " " + p.toString());
		}
	}

	public static class IdentityMapper extends
			Mapper<LongWritable, Text, Text, PointWritable> {

		final Text one = new Text("-");

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(one, new PointWritable(value.toString()));
		}
	}

	public static class vldbmapper extends
			Mapper<LongWritable, Text, Text, PointWritable> {

		final Text one = new Text("-");
		Vector<Rect> rects;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			rects = new Vector<Rect>();

			try {
				Configuration conf = context.getConfiguration();
				FileSystem fs = FileSystem.getLocal(conf);
				for (URI u : context.getCacheFiles()) {
					System.err.println(u.toString());

					Path partFile = new Path(u.getPath());
					FSDataInputStream reader = fs.open(partFile);
					while (reader.available() > 0) {
						Rect r = new Rect();
						r.readFields(reader);
						rects.add(r);
					}
					reader.close();
				}
			} catch (IOException ie) {
				ie.printStackTrace();
				throw new IllegalArgumentException("can't read Rectangle file",
						ie);
			}
			// System.out.println("length" + rects.size());
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// get key
			String v = value.toString();
			// String k = (v.split("\t"))[0];
			String v1 = (v.split("\t"))[1];
			// System.out.println("v:" + v.toString());
			// System.out.println("v1:" + v1.toString());
			PointWritable p = new PointWritable(v);
			// check where the point is needed
			for (Rect r : rects) {
				// System.out.println("p:" + p.toString() + ":" + r.toString()
				// + ":::::" + p.dominate(r.l));

				if (p.dominate(r.l) == 1) {
					context.write(new Text(r.l.toString()), p);
					// System.out.println("p dominate r.l" + r.l + "\t"
					// + p.toString());
				}
			}
			// context.write(one, p);
		}
	}

	// Point --> text
	public static class SkyReducer_PT extends
			Reducer<Text, PointWritable, Text, Text> {
		@Override
		public void reduce(Text n, Iterable<PointWritable> values,
				Context context) throws IOException, InterruptedException {

			int phase = context.getConfiguration().getInt("phase", 1);
			System.out.println("Phase:" + phase);
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

			// System.out.println("Skyline " + points.size() + " "
			// + skyline.skylines.size());
			Rect r = null;
			if (phase == 1)
				r = new Rect();
			for (int i = 0; i < skyline.skylines.size(); i++) {
				Point p = skyline.skylines.get(i);

				if (phase == 1)
					r.expand(p);
				PointWritable s = new PointWritable(p);
				context.write(n, new Text(s.toString()));
				if (debug)
					System.out.println(n.toString() + '(' + s.toString() + ')');
			}
			if (phase == 1)
				if (r.count > 0)
					Write(context.getConfiguration(), n.toString(), r);
		}
	}

	static public void Write(Configuration conf, String n, Rect r) {
		Path partFile = new Path("r/a" + n + "r");
		FileSystem outFs;
		try {
			outFs = partFile.getFileSystem(conf);
			DataOutputStream writer = outFs.create(partFile, true);
			r.write(writer);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
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
		conf.setInt("threshold", threshold);
		conf.setInt("sample", sample);
		Job job = Job.getInstance(conf);
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(args[1] + "sampled");
		if (fs.exists(p) == true)
			fs.delete(p, true);
		p = new Path(args[1]);
		if (fs.exists(p) == true)
			fs.delete(p, true);

		job.setJarByClass(BNL.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PointWritable.class);

		job.setMapperClass(MapDivision.class);
		job.setReducerClass(SkyReducer_PT.class);
		job.setNumReduceTasks(reducers);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/tmp"));

		try {
			PointSampler.WriteSampleFile(job, args[1], 1000);
			job.waitForCompletion(true);
		} catch (Exception ee) {

		} catch (Throwable e) {
			success = false;
		}
	}

	@SuppressWarnings("deprecation")
	public static void Contact(Job job, String input) {
		try {
			Configuration conf = job.getConfiguration();
			Path partFile = new Path("r");
			FileSystem fs = FileSystem.get(conf);

			FileStatus[] status = fs.listStatus(partFile);
			Path[] ps = new Path[status.length];
			Path outputFile = new Path("I/" + input);

			fs = outputFile.getFileSystem(conf);
			DataOutputStream writer = fs.create(outputFile, true);
			for (int i = 0; i < status.length; i++) {
				ps[i] = status[i].getPath();
				// System.out.println(ps[i].toString());
				FSDataInputStream reader = fs.open(ps[i]);
				Rect r = new Rect();
				r.readFields(reader);
				// System.out.println("Rect" + r.toString());
				r.write(writer);
				reader.close();
			}
			writer.close();

			URI uri = new URI(outputFile.toString() + "#" + "I/" + input);
			job.addCacheFile(uri);

			fs.delete(partFile, true);
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public static void Gather(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		Contact(job, args[0]);
		conf.setInt("phase", 2);
		job.setJobName("BNL-Gather");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PointWritable.class);

		job.setMapperClass(vldbmapper.class);
		// job.setCombinerClass(SkyReducer_PP.class);
		job.setReducerClass(SkyReducer_PT.class);
		job.setNumReduceTasks(reducers);
		job.setJarByClass(BNL.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1] + "/tmp"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/r"));
		try {

			job.waitForCompletion(true);
		} catch (Exception e) {
			success = false;
		}
		// deleting
		System.out.println("Deleting");
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path p = new Path("I");
		if (fs.exists(p) == true) {
			System.out.println("Deleting" + p.toUri());
			fs.delete(p, true);
		}
		p = new Path("r");
		if (fs.exists(p) == true) {
			System.out.println("Deleting" + p.toUri());
			fs.delete(p, true);
		}
	}

	public static void main(String[] args) {

	}

	public static void run(String[] args) throws Exception {
		if (debug)
			System.out.println("Debug" + debug + "\n" + reducers);

		Divide(args);
		Gather(args);
	}
}
