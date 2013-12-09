package skyhadoop;

public class Point implements Comparable<Point> {
	public int id;
	public double[] d;// value of dimensions
	public int dim;// dimentionality

	public Point(String str) {
		String[] temp;
		// get the id

		// temp = str.split(":");
		// id= Integer.parseInt(temp[0]);
		// temp = temp[1].split(",");
		if (str.contains(" ")) {
			str = (str.split(" "))[1];
		}
		if (str.contains("\t")) {
			str = (str.split("\t"))[1];
		}

		temp = str.split(",");
		dim = temp.length;
		d = new double[dim];
		for (int i = 0; i < dim; i++) {
			d[i] = Double.parseDouble(temp[i]);
		}
	}

	public Point() {

	}

	@Override
	public String toString() {

		String s = "";
		int i = 0;
		for (; i < dim - 1; i++) {
			s = s + d[i] + ",";
		}
		s = s + d[i];
		return s;
	}

	public Point(Point p) {

		this.id = p.id;
		this.dim = p.dim;
		d = new double[dim];
		for (int i = 0; i < dim; i++) {
			d[i] = p.d[i];
		}
	}

	// greater is better
	// 0: not comparable
	// 1: this dominates P
	// -1: P dominates this
	public int dominate(Point p) {
		int greater = 0;
		int lesser = 0;
		for (int i = 0; i < dim; i++) {
			if (d[i] > p.d[i])
				greater = 1;
			else if (d[i] < p.d[i])
				lesser = 1;
			if (greater > 0 && lesser > 0)
				return 0;
		}
		return greater > 0 ? 1 : -1;
	}
    double val=-1;
	public double value() {
		if(val>=0)return val;
		
		for (int i = 0; i < dim; i++) {
			if (d[i] == 0)	{val=0; break;}			
			val +=  Math.log(d[i]);
		}
				return val;
	}

	@Override
	public int compareTo(Point o) {
		double o_value=o.value();
		double mvalue=value();
		if(mvalue> o_value){
		  return -1;
		}else if(o_value>mvalue)return 1;
		return 0;
	}

}
