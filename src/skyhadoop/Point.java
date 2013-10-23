package skyhadoop;
//TODO extend it to writable
public class Point {
	public double []d;//value of dimensions
	public int dim ;// dimentionality
	public Point(String str){
		String []temp = str.split(",");
		dim=temp.length;
		d = new double[dim];
		for (int i = 0; i < dim; i++) {
			d[i] = Double.parseDouble(temp[i]);
		}	
	}
	public Point(){
	
	}
	// greater is better
	//0: not comparable 
	//1: this dominates P
	//-1: P dominates this
	public int dominate(Point p){
		int greater=0;
		int lesser=0;
		for(int i=0;i<dim;i++)
		{
			if(d[i]>p.d[i]) greater=1;
			else if(d[i]<p.d[i]) lesser=1;
			if(greater>0 && lesser >0)return 0;
		}
		return greater>0?1:-1 ;
	}
}


