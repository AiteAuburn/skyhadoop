import java.io.Console;
import java.util.Collections;
import java.util.Vector;

import org.junit.*;
import static org.junit.Assert.*;

import skyhadoop.*;

public class TestRect {

	@Test
	public void test_rect() {
	 Rect r=new Rect();
	 Point p=new Point("1,2");
	 r.expand(p);
	 System.out.println(r.toString());
	 System.out.println(r.count);
	  p=new Point("2,5");
	  r.expand(p);
	  System.out.println(r.toString());
	}
	
}

