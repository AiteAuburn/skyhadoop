package skyhadoop;

import java.io.Console;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Rect implements Writable {
	Point l;
	Point u;
	public int count;
public int id;
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
		if (count > 0)
			return "[" + l.toString() + ":" + u.toString() + "]"+count;
		else
			return "empty";
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		count = in.readInt();
		PointWritable a = new PointWritable();
		PointWritable b = new PointWritable();
		a.readFields(in);
		l = (Point) a;
		b.readFields(in);
		u = (Point) b;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
		PointWritable a = new PointWritable(l);
		a.write(out);
		PointWritable b = new PointWritable(u);
		b.write(out);
	}
}