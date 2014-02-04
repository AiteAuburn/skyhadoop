package skyhadoop;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

//import org.apache.hadoop.mapred.InputSplit;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class Experiment {
	public static boolean combiner = false;
	public static boolean debug = false;
	public static boolean success = true;
	public static int part_no = 10;
	public static int mappers = 1;
	public static int reducers = 1;
	public static int dim=2;
	public static String input = "";
	public static String output = "";
	public static String alg = "";
	public static long start;
	public static long end;

	/*
	 * public void run(String[] args) throws Exception { }
	 */
	public static void getSample(JobContext job) {
		  
		  
		   
		     try {
		    	 TextInputFormat inFormat = new TextInputFormat();
		    	 List<InputSplit> splits  = inFormat.getSplits(job);
				System.out.println(" splits"+ splits.size());
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		     
	}

	static public String getExperiment() {
		long time = end - start;
		return Version.version + "\t" + alg + "\t" + input + "\t" + mappers
				+ "\t" + combiner + "\t" + reducers + "\t" + Partitioner.policy
				+ "\t" + part_no+ "\t"+time + "\n";
	}

	static public void processargs(String args[]) {
		alg = args[0];
		input = args[1];
		output = input + "_";
		for (int i = 2; i < args.length; i++) {
			if (args[i].equals("-d"))
				debug = true;
			if (args[i].equals("-c"))
				combiner = true;
			if (args[i].startsWith("-pg")) {
				Partitioner.policy = "grid";
				if (args[i].startsWith("-pg="))
					part_no = Integer.parseInt(args[i].replace("-pg=", ""));
				else
					part_no = 100;
			}
			
			if (args[i].startsWith("-pa")) {
				Partitioner.policy = "angle";
				if (args[i].startsWith("-pa="))
					part_no = Integer.parseInt(args[i].replace("-pa=", ""));
				else
					part_no = 8;
			}
			if (args[i].startsWith("-m"))
				mappers = Integer.parseInt(args[i].replace("-m", ""));

			if (args[i].startsWith("-r"))
				reducers = Integer.parseInt(args[i].replace("-r", ""));
		}
	}

	public static void main(String[] args) throws Exception {

		processargs(args);
		start = System.currentTimeMillis();

		if (alg.equals("BNL")) {
			BNL.run(new String[] { input, output });
		} else if (alg.equals("BNLB")) {
			BNL_binary.run(new String[] { input, output });
		} else if (alg.equals("BNL1")) {
			BNL1.run(new String[] { input, output });
		}

		end = System.currentTimeMillis();
		if (success) {
			FileWriter writer2 = new FileWriter("jobTime", true);
			writer2.write(getExperiment());
			writer2.close();
		} else {
			System.out
					.println("**********************Job failed*******************");
		}
	}

	public static void Divide(Job job, String[] args) throws Exception {
		throw new Exception("should not be here");
		
	}

	public static void run(String[] args) throws Exception {
		throw new Exception("should not be here");
		
	}
}
