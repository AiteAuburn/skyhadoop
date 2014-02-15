package skyhadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;

public class PointParitioner extends Partitioner implements
		org.apache.hadoop.mapred.Partitioner<LongWritable, PointWritable> {

	public void configure(JobConf job) {
		setPartitioner();

	}

	public int getPartition(LongWritable key, PointWritable value,
			int numReduceTasks) {

		System.out.println(numReduceTasks);
		return getpart(value) % numReduceTasks;
	}
}