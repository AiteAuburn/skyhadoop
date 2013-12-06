package skyhadoop;

import java.io.FileWriter;

public class Experiment {
	public static boolean combiner = false;
	public static int mappers = 1;
	public static int reducers = 1;
	public static String input = "";
	public static String output = "";
	public static String alg = "";
	public static long start;
	public static long end;

	/*public void run(String[] args) throws Exception {
	}*/

	static public String getExperiment() {
		long time = end - start;
		return Version.version + "\t" + alg + "\t" + input + "\t" + mappers
				+ "\t" + combiner + "\t" + reducers + "\t" + time + "\n";
	}

	static public void processargs(String args[]) {
		alg = args[0];
		input = args[1];
		output = input + "_";
		for(int i=2;i<args.length;i++){
			if (args[i].equals("-c")) combiner=true;
			
		}
	}

	
	public static void main(String[] args) throws Exception {
	
		processargs(args);
		start = System.currentTimeMillis();
		
		if (alg.equals("BNL")) {
			BNL.run(new String[] { input, output });
		} else if (alg.equals("BNLT"))  {
			BNL_binary.run(new String[] { input, output });
		}
		

		end = System.currentTimeMillis();
		FileWriter writer2 = new FileWriter("jobTime", true);
		writer2.write(getExperiment());
		writer2.close();
	}
}