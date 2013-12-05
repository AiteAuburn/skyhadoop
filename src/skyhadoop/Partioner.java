package skyhadoop;

public class Partioner {
	static final int sizeX=1000;
	static final int sizeY=1000;
	public static int gridX=100;
	public static int gridY=100;
	
	
	public static int gridpartioner(Point p) {
        double x=p.d[0]/gridX;
        double y=p.d[1]/gridY;
        
        return (int)(x*sizeX/gridX + y);
		
	}
}
