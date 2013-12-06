package skyhadoop;

public class Partitioner {
	final static int sizeX = 1000;
	final static int sizeY = 1000;
	public static int gridX = 100;
	public static int gridY = 100;

	public static int gridpartioner(Point p) {
		int x = (int) (p.d[0] / gridX);
		int y = (int) (p.d[1] / gridY);

		return (int) (x * sizeX / gridX + y);

	}
}
