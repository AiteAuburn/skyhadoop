package skyhadoop;

import java.util.*;

public class SkyQuadTree extends QuadTree {
	public boolean needed(String id1, String id2) {
		// trim ids to the shorter string
		if (id1.length() > id2.length()) {
			id1 = id1.substring(0, id2.length());
		} else {
			id2 = id2.substring(0, id1.length());
		}
		int skip = 0;
		for (int i = 0; i < id1.length(); i++) {

			if (id1.charAt(i) == id2.charAt(i))
				skip++;
		}
		// now get the id1 and id2
		int s1 = skip;
		int s2 = skip;

		for (; s1 < id1.length() && id1.charAt(s1) != '-'; s1++)
			;
		for (; s2 < id2.length() && id2.charAt(s2) != '-'; s2++)
			;
		int indx1 = Integer.parseInt(id1.substring(skip, s1));
		int indx2 = Integer.parseInt(id2.substring(skip, s2));
		boolean[] i1 = getFlags(indx1);
		boolean[] i2 = getFlags(indx2);
		boolean[] ii = new boolean[i1.length];
		for (int j = 0; j < i1.length; j++) {
			ii[j] = i1[j] && i2[j];
		}
		for (int j = 0; j < i1.length; j++) {
			if (ii[j] != i1[j])
				return false;
		}
		return true;
	}

	public SkyQuadTree(int dim, int threshold) {
		super(dim, threshold);
	}

	public SkyQuadTree(int dim, int threshold, double low, double high) {
		super(dim, threshold, low, high);
	}

	// iterate the nodes for nodes the
	public void MarkDominatedNode() {
		if (root != null) {
			root.adjust();
			root.MarkDominatedNode();
			get_all_children();
			for (int i = 0; i < all_children.size(); i++) {
				Node n = all_children.get(i);
				if (n.domianted == true)
					continue;
				if (n.count > 0)
					continue;

				for (int j = i + 1; j < all_children.size(); j++) {
					Node m = all_children.get(j);
					if (m.domianted == true)
						continue;
					if (n.midpoint.dominate(m.lowerpoint) == 1)
						m.domianted = true;
				}
			}
		}
	}

	public void addpoints(Vector<Point> pnts) {
		for (Point p : pnts)
			Insert(p);

	}

	@Override
	public String toString() {
		if (root != null)
			return root.toString();
		return "null root";
	}

	public void adjust() {
		if (root != null)
			root.adjust();
	}

	public static void main(String[] args) {
		SkyQuadTree q = new SkyQuadTree(2, 1, 0, 100);
		Point p = new Point("10,10");
		q.Insert(p);
		q.Insert(new Point("15,15"));
		q.Insert(new Point("15,25"));
		q.Insert(new Point("25,15"));
		q.Insert(new Point("25,25"));

		q.Insert(new Point("10,60"));
		q.Insert(new Point("60,10"));
		q.Insert(new Point("28,60"));
		q.Insert(new Point("30,60"));
		q.Insert(new Point("60,60"));
		q.MarkDominatedNode();

		System.out.print(q.toString());
		q.get_all_children();
	}

}
