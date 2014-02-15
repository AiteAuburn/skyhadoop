package skyhadoop;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.StringUtils;

public class PointSampler {// implements IndexedSortable {
	public Vector<Point> records = new Vector<Point>();

	public void compute() {
		Skyline s = new Skyline(records);
		s.Compute();
		records = s.skylines;
	}

	/*
	 * public int compare(int i, int j) { Point left = records.get(i); Point
	 * right = records.get(j); return left.compareTo(right); }
	 * 
	 * public void swap(int i, int j) { Point left = records.get(i); Point right
	 * = records.get(j); records.set(j, left); records.set(i, right); }
	 */

	public void addPoint(Text key) {
		synchronized (this) {
			records.add(new Point(key.toString()));
		}
	}

	static class SamplerThreadGroup extends ThreadGroup {

		private Throwable throwable;

		public SamplerThreadGroup(String s) {
			super(s);
		}

		@Override
		public void uncaughtException(Thread thread, Throwable throwable) {
			this.throwable = throwable;
		}

		public Throwable getThrowable() {
			return this.throwable;
		}

	}

	static Vector<Point>  sample(final JobContext job, long sampleSize) throws Throwable {
		long t1 = System.currentTimeMillis();

		final PointSampler sampler = new PointSampler();
		int partitions = job.getNumReduceTasks();
		// long sampleSize = 1000;
		final TextInputFormat inFormat = new TextInputFormat();

		final List<InputSplit> splits;
		splits = inFormat.getSplits(job);

		long t2 = System.currentTimeMillis();
		System.out.println("Computing input splits took " + (t2 - t1) + "ms");
		// print locations of all splits
		/*
		 * for(InputSplit is : splits){ FileSplit fs=(FileSplit)is;
		 * System.out.println(fs.getStart()); for(String loc:fs.getLocations()){
		 * System.out.println("\t"+loc); }
		 * 
		 * }
		 */

		int samples = Math.min(10, splits.size());
		System.out.println("Sampling " + samples + " splits of "
				+ splits.size());
		final long recordsPerSample = sampleSize / samples;
		final int sampleStep = splits.size() / samples;
		Thread[] samplerReader = new Thread[samples];
		SamplerThreadGroup threadGroup = new SamplerThreadGroup(
				"Sampler Reader Thread Group");
		// take N samples from different parts of the input
		for (int i = 0; i < samples; ++i) {
			final int idx = i;
			samplerReader[i] = new Thread(threadGroup, "Sampler Reader " + idx) {
				{
					setDaemon(true);
				}

				public void run() {
					long records = 0;
					try {
						TaskAttemptContext context = new TaskAttemptContextImpl(
								job.getConfiguration(), new TaskAttemptID());
						RecordReader<LongWritable, Text> reader = inFormat
								.createRecordReader(
										splits.get(sampleStep * idx), context);
						reader.initialize(splits.get(sampleStep * idx), context);
						while (reader.nextKeyValue()) {
							// sampler.addPoint(new Text(reader.getC()));
							// System.out.println("Sampled"
							// + reader.getCurrentValue());
							sampler.addPoint(reader.getCurrentValue());
							records += 1;
							if (recordsPerSample <= records) {
								break;
							}
						}
					} catch (IOException ie) {
						System.err
								.println("Got an exception while reading splits "
										+ StringUtils.stringifyException(ie));
						throw new RuntimeException(ie);
					} catch (InterruptedException e) {

					}
				}
			};
			samplerReader[i].start();
		}
		for (int i = 0; i < samples; i++) {
			try {
				samplerReader[i].join();
				if (threadGroup.getThrowable() != null) {
					throw threadGroup.getThrowable();
				}
			} catch (InterruptedException e) {
			}
		}
		long t3 = System.currentTimeMillis();
		/*
		 * sampler.compute(); // System.out.println(sampler.records.size() ); //
		 * for(Point p :sampler.records){ // System.out.println(p.toString());
		 * // }
		 */
		System.out.println("Computing parititions took " + (t3 - t2) + "ms");
		return sampler.records;
	}

	/**
	 * @param args
	 */

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
