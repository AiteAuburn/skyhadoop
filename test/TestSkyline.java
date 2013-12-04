import java.io.Console;
import java.util.Vector;

import org.junit.*;
import static org.junit.Assert.*;

import skyhadoop.*;

public class TestSkyline {

	@Test
	public void test_point() {
		Point p = new Point("1,2.3,4.1");
		assertTrue(p.dim == 3);
		assertTrue(p.d[0] == 1);
		assertTrue(p.d[1] == 2.3);
		assertTrue(p.d[2] == 4.1);
	}

	@Test
	public void test_dominates() {
		// greater is better
		Point p = new Point("1,2,4");
		Point q = new Point("3,2,4");
		Point z = new Point("1,3,2");

		assertTrue(p.dominate(q) == -1);
		assertTrue(q.dominate(p) == 1);
		assertTrue(q.dominate(z) == 0);
		assertTrue(p.dominate(z) == 0);
	}

	@Test
	public void test_skyline() {
		Vector<Point> pnts = new Vector<Point>();
		pnts.add(new Point("1,2"));
		pnts.add(new Point("2,3"));
		pnts.add(new Point("4,1"));

		Skyline s = new Skyline(pnts);
		s.Compute();

		for (int i = 0; i < s.skylines.size(); i++) {
			Point p = s.skylines.get(i);
			System.out.println(p.toString());
		}
	}

}
