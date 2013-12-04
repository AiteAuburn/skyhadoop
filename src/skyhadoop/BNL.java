package skyhadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
//
public class BNL {

	
	public static class MapDivision extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, PointWritable> {
		
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, PointWritable> output, Reporter reporter)
				throws IOException {
			PointWritable p= (PointWritable) new Point(value.toString());
			p.id=(int)key.get();
			int divison=Partioner.gridpartioner(p);
			output.collect(new LongWritable(divison),p);
		}		
	}
	
	public static class IdentityMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, Text> {

final LongWritable one = new LongWritable(1);

public void map(LongWritable key, Text value,
		OutputCollector<LongWritable, Text> output, Reporter reporter)
		throws IOException {
	String[] val = value.toString().trim().split("\t");
	output.collect(one, new Text(value));
}
}

	public static class SkyReducer extends MapReduceBase implements
			Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {

		public void reduce(LongWritable n, Iterator<PointWritable> values,
				OutputCollector<LongWritable, PointWritable> output, Reporter reporter)
				throws IOException {
			
			Vector<Point> points=new Vector<Point>();
			while(values.hasNext())
			{
				PointWritable p = values.next();
				points.add(p);	
			}
			Skyline skyline=new Skyline(points);
			skyline.Compute();
							
			for (Point s : skyline.skylines) {
				output.collect(new LongWritable(1), (PointWritable)s);	
			}					
		}		
	}
	static JobConf conf = new JobConf(Skyline.class);
	public static void main(String[] args) throws Exception {
	//	long start = System.currentTimeMillis();


		conf.setJobName("BNL");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapDivision.class);
		//conf.setCombinerClass(GlobalReducer.class);
		conf.setReducerClass(SkyReducer.class);
		conf.setNumReduceTasks(2);
		conf.setNumMapTasks(2);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]+"/res"));
		JobClient.runJob(conf);
	
	}
}
	