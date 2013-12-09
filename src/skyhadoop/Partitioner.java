package skyhadoop;

public class Partitioner {
	final static int size = 1000;
	//final static int sizeY = 1000;
	
	public static String policy = "grid";

	static Partitioner p = null;

	public static void setPartitioner() {
		if (policy.equals("grid"))
			p = new GridPartitioner();
		else if (policy.equals("angle"))
			p = new AnglePartitioner();
	}

	int execute(Point p) {
		return 0;
	}

	static int getpart(Point b) {
		if (p == null)
			setPartitioner();
		return p.execute(b);
	}
}
