import java.io.Console;
import java.util.Collections;
import java.util.Vector;

import org.junit.*;
import  static org.junit.Assert.*;

import skyhadoop.*;

public class TestQuadTree {

	@Test
	public void test_quadtree() {
			QuadTree q = new QuadTree(2,2);
			Point p = new Point("10,10");
			q.Insert(p);
			q.Insert(new Point("10,60"));
			q.Insert(new Point("60,10"));
			q.Insert(new Point("60,60"));
			assertTrue( q.find(new Point("10,60")));
			assertFalse( q.find(new Point("10,50")));
		}
	
	@Test
	public void test_skyquadtree() {
			QuadTree q = new QuadTree(2,2);
			Point p = new Point("10,10");
			q.Insert(p);
			q.Insert(new Point("15,15"));
			q.Insert(new Point("15,25"));
			q.Insert(new Point("10,60"));
			q.Insert(new Point("60,10"));
			q.Insert(new Point("60,60"));
			assertTrue( q.find(new Point("10,60")));
			assertFalse( q.find(new Point("10,50")));
			
		}
	@Test
	public void test_isneeded(){
		SkyQuadTree q = new SkyQuadTree(2, 2, 0, 100);
		Point p = new Point("10,10");
		q.Insert(p);
		q.Insert(new Point("15,15"));
		q.Insert(new Point("15,25"));
		q.Insert(new Point("25,15"));
		q.Insert(new Point("25,25"));

		q.Insert(new Point("10,60"));
		q.Insert(new Point("60,10"));
		q.Insert(new Point("60,60"));
		 assertTrue( q.find(new Point("10,60")));
		 assertFalse( q.find(new Point("10,50")));
		String s1 = q.findNode(new Point("5,5"));
		String s2 = q.findNode(new Point("60,60"));
		String s3 = q.findNode(new Point("15,60"));
		assertTrue(q.needed(s1, s2));
		assertTrue(q.needed(s1, s3));
		assertFalse(q.needed(s2, s1));
		assertFalse(q.needed(s2, s3));
		assertTrue(q.needed(s3, s2));
		//System.out.print(c1+" :"+c2+":"+c3);
	}
}
