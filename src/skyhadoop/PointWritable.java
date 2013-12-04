package skyhadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PointWritable extends Point implements Writable {

	//public PointWritable(Point s) {
		// TODO Auto-generated constructor stub
	//}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		//id=in.readInt();
		dim=in.readInt();
		d=new double[dim];
		for(int i=0;i<dim;i++)
			d[i]=in.readDouble();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		//out.writeInt(id);
		out.writeInt(dim);
		for(int i=0;i<dim;i++)
			out.writeDouble(d[i]);
		
	}

	
}
