package skyhadoop;

import java.net.URI;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
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
				FileSystem fs = FileSystem.getLocal(conf);
				sq = null;
				for (URI u : context.getCacheFiles()) {
					System.err.println(u.toString());

					Path partFile = new Path(u.getPath());
					Vector<Point> pnts = PointSampler.readSamplePoints(fs,
							partFile, conf);
					Point p = pnts.get(0);
					int dim = p.dim;
					int threshold = 2;
					sq = new SkyQuadTree(dim, threshold, 0, 1000);

					sq.addpoints(pnts);
					sq.MarkDominatedNode();
					// System.out.println(sq.toString());
				}
			} catch (IOException ie) {
				throw new IllegalArgumentException(
						"can't read partitions file", ie);
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

	// Point --> text
	public static class SkyReducer_PT extends
			Reducer<Text, PointWritable, Text, Text> {
		@Override
		public void reduce(Text n, Iterable<PointWritable> values,
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
			Rect r = new Rect();
			for (int i = 0; i < skyline.skylines.size(); i++) {
				Point p = skyline.skylines.get(i);
				r.expand(p);
				PointWritable s = new PointWritable(p);
				context.write(n, new Text(s.toString()));
				if (debug)
					System.out.println(n.toString() + '(' + s.toString() + ')');
			}
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
			writer.writeChars(r.toString());
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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

	public static void Contact(Configuration conf) {

		try {
			Path partFile = new Path("r/");
			FileSystem fs = FileSystem.get(conf);

			FileStatus[] status = fs.listStatus(partFile);
			Path[] ps = new Path[status.length];
			Path outputfile = new Path("C/a.txt");
			FileSystem outFs;
			outFs = outputfile.getFileSystem(conf);
			DataOutputStream writer = outFs.create(outputfile, true);
			for (int i = 0; i < status.length; i++) {
				ps[i] = status[i].getPath();
				System.out.println(ps[i].toString());
				FSDataInputStream reader = fs.open(ps[i]);
				String s = reader.readLine();
				System.out.println(s);
				writer.writeChars(s);
				reader.close();
			}
			writer.close();
			// fs.delete(partFile,true);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void Gather(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		// contact r
		Contact(conf);
		job.setJobName("BNL-Gather");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PointWritable.class);

		job.setMapperClass(IdentityMapper.class);
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
