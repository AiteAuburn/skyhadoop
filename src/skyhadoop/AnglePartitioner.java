package skyhadoop;

public class AnglePartitioner extends Partitioner {
	public static int PARTITION = 8; // no of partitions

	public AnglePartitioner() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int execute(Point p) {
		int o = 0;

		double pi = Math.PI / 2;
		if (p.d[0] != 0) {
			o = (int) Math.floor(Math.atan(p.d[1] / p.d[0]) / (pi / PARTITION));
			if (o == PARTITION) {
				o = PARTITION - 1;
			}
		}
		return o;
	}
}
