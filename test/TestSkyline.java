import java.io.Console;
import java.util.Collections;
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

		assertTrue(p.dominate(q) == 1);
		assertTrue(q.dominate(p) == -1);
		assertTrue(q.dominate(z) == 0);
		assertTrue(p.dominate(z) == 0);
	}

	@Test
	public void test_skyline() {
		Vector<Point> pnts = new Vector<Point>();
		pnts.add(new Point("1,2"));
		pnts.add(new Point("2,3"));
		pnts.add(new Point("4,1"));
		Collections.sort(pnts, new PointCompartor());
		Point p = pnts.get(0);
		assertTrue(p.d[0]==1 && p.d[1]==2);
		p = pnts.get(1);
		assertTrue(p.d[0]==4 && p.d[1]==1);
		p = pnts.get(2);
		assertTrue(p.d[0]==2 && p.d[1]==3);

		Skyline s = new Skyline(pnts);
		s.Compute();

		for (int i = 0; i < s.skylines.size(); i++) {
			Point q = s.skylines.get(i);
			// System.out.println(p.toString());
		}
		assertTrue(s.skylines.size() == 2);
	}

	@Test
	public void test_skyline2() {
		Vector<Point> pnts = new Vector<Point>();
		// System.out.println("Test 2");

		pnts.add(new Point("840,394"));
		pnts.add(new Point("783,798"));
		pnts.add(new Point("911,197"));
		pnts.add(new Point("335,768"));
		pnts.add(new Point("277,553"));
		pnts.add(new Point("477,628"));
		pnts.add(new Point("364,513"));
		pnts.add(new Point("952,916"));
		pnts.add(new Point("635,717"));
		pnts.add(new Point("141,606"));

		Skyline s = new Skyline(pnts);
		s.Compute();

		for (int i = 0; i < s.skylines.size(); i++) {
			Point p = s.skylines.get(i);
			// System.out.println(p.toString());
		}
		assertTrue(s.skylines.size() == 5);
	}

	@Test
	public void test_part() {
		Point p = new Point("10,20");
		Experiment.dim = 2;
		Experiment.part_no = 4;

		skyhadoop.GridPartitioner grid = new GridPartitioner(50);

		assertTrue(0 == grid.execute(p));
		p = new Point("30,30");
		assertTrue(3 == grid.execute(p));
		p = new Point("10,30");
		assertTrue(1 == grid.execute(p));
		p = new Point("30,10");
		assertTrue(2 == grid.execute(p));
	}

}
