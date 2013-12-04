package skyhadoop;

import java.util.Vector;

//TODO SFS
//TODO Asynchrounus 

public class Skyline {
	//
	Vector<Point> points;
	public Vector<Point> skylines;
    Vector<Point> otherskylines;
	public Skyline(Vector<Point> points) {
		this.points = points;
		skylines = new Vector<Point>();
	}
	// filter points with otherskylines
    public void Filter(){
     skylines.addAll(otherskylines);    	
    	
    }
	//  Basic compute skyline
	public void Compute() {
		Vector<Point> todel = new Vector<Point>();
		for (int i = 0; i < points.size(); i++) {
			Point p = (Point) points.get(i);
			boolean isskyline = true;
			for (int j = 0; j < skylines.size(); j++) {
				Point q = (Point) skylines.get(j);
				int dom = p.dominate(q);
				if (dom == -1) {
					isskyline = false;
					break;
				} else if (dom == 1) {
					todel.add(q);
				}
			}
			if (todel.size() > 0) {
				for (Point q : todel)
					skylines.remove(q);
				todel.clear();
			}
			if (isskyline) {
				skylines.add(p);

			}
		}
	}
}
