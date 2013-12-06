package skyhadoop;

public class GridPartitioner extends Partitioner {

	public GridPartitioner() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int execute(Point p) {
		int x = (int) (p.d[0] / gridX);
		int y = (int) (p.d[1] / gridY);

		return (int) (x * sizeX / gridX + y);
	}

}
