import java.io.Console;
import java.util.Collections;
import java.util.Vector;

import org.junit.*;
import  static org.junit.Assert.*;

import skyhadoop.*;

public class TestQuadTree {

	@Test
	public void test_point() {
			QuadTree q = new QuadTree(2,2);
			Point p = new Point("10,10");
			q.Insert(p);
			q.Insert(new Point("10,60"));
			q.Insert(new Point("60,10"));
			q.Insert(new Point("60,60"));
			assertTrue( q.find(new Point("10,60")));
			assertFalse( q.find(new Point("10,50")));
		}
	
	
}
