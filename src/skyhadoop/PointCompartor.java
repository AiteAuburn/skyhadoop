package skyhadoop;

import java.util.Comparator;

public class PointCompartor implements Comparator<Point> {

	public static int comp(Point x, Point o) {
		double o_value = o.value();
		double mvalue = x.value();
		if (mvalue > o_value) {
			return 1;
		} else if (o_value > mvalue)
			return -1;
		return 0;
	}

	@Override
	public int compare(Point x, Point o) {
		return comp(x, o);
	}
}
