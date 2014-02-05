package skyhadoop;

public class GridPartitioner extends Partitioner {
	public static double grid_d;//new double [Experiment.dim];
	
	public GridPartitioner() {
		double d=Math.pow(Experiment.part_no, 1.0/Experiment.dim);
		//for(int i=0;i<Experiment.dim;i++){
		  grid_d=size/d;
		//}
	}
	public GridPartitioner(int s) {
		size=s;
		double d=Math.pow(Experiment.part_no, 1.0/Experiment.dim);
		//for(int i=0;i<Experiment.dim;i++){
		  grid_d=size/d;
		//}
	}
	@Override
	public int execute(Point p) {
		int x = (int) (p.d[0] / grid_d);
		int y = (int) (p.d[1] / grid_d);

		return (int) (x * size / grid_d + y);
	}

}
