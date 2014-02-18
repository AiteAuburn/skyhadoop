package skyhadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class PointWritable extends Point implements
		WritableComparable<PointWritable> {

	public PointWritable() {
	}

	public PointWritable(PointWritable p) {
		super((Point) p);
	}

	public PointWritable(String str) {
		super(str);
	}

	public PointWritable(Point p) {
		super(p);
	}

	@Override
	public int compareTo(PointWritable o) {
		return PointCompartor.comp(this, o);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		// id=in.readInt();
		dim = in.readInt();
		d = new double[dim];
		for (int i = 0; i < dim; i++)
			d[i] = in.readDouble();
	}

	@Override
	public String toString() {

		return super.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		// out.writeInt(id);
		out.writeInt(dim);
		for (int i = 0; i < dim; i++)
			out.writeDouble(d[i]);

	}
	/** A Comparator optimized for PointWritable. */
	/*
	 * public static class Comparator extends WritableComparator { public
	 * Comparator() { super(PointWritable.class); }
	 * 
	 * @Override public int compare(byte[] b1, int s1, int l1, byte[] b2, int
	 * s2, int l2) { double thisValue = readDouble(b1, s1); double thatValue =
	 * readDouble(b2, s2); return (thisValue < thatValue ? -1 : (thisValue ==
	 * thatValue ? 0 : 1)); } }
	 * 
	 * static { // register this comparator
	 * WritableComparator.define(PointWritable.class, new Comparator()); }
	 */
}
