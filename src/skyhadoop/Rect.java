package skyhadoop;

public class Rect {
	Point l;
	Point u;
public	int count;

	public Rect(Point p) {
		l = p;
		u = p;
		count = 1;
	}

	public Rect() {
		count = 0;
	}

	private double Min(double d, double e) {
		if (d > e)
			return e;
		return d;
	}

	private double Max(double d, double e) {
		if (d > e)
			return d;
		return e;
	}

	public void expand(Point p) {
		if (count == 0) {
			l = new Point(p.dim);
			u = new Point(p.dim);
			for (int i = 0; i < p.dim; i++) {
				l.d[i] = p.d[i];
				u.d[i] = p.d[i];
			}
			count = 1;
		} else {
			count++;
			for (int i = 0; i < p.dim; i++) {
				l.d[i] = Min(p.d[i], l.d[i]);
				u.d[i] = Max(p.d[i], u.d[i]);
			}
		}
	}

	@Override
	public String toString() {
		if (count>0)
			return "["+l.toString()+":"+u.toString()+"]";
		else return "empty";
	}
}