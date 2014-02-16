package skyhadoop;

import java.util.*;

public class QuadTree {
	int dim;
	int threshold;
	Node root;

	public QuadTree(int dim, int threshold) {
		this(dim, threshold, Integer.MIN_VALUE, Integer.MAX_VALUE);
	}

	public QuadTree(int dim, int threshold, double low, double high) {
		this.dim = dim;
		this.threshold = threshold;
		Point a = new Point(dim);
		Point b = new Point(dim);
		for (int i = 0; i < dim; i++) {
			a.d[i] = low;
			b.d[i] = high;
		}
		root = new Node(a, b);
		root.id = "";
	}

	public Vector<Node> all_children;

	void get_all_children() {
		all_children = new Vector<Node>();
		root.getChild(all_children);
	}

	public void Insert(Point p) {
		if (root == null)
			root = new Node();
		root.insert(p);
	}

	public String findNode(Point p) {

		if (root == null)
			return "";

		Node n = root.findNode(p);
		return n.id;
	}

	public boolean[] getFlags(int i) {
		boolean[] f = new boolean[dim];
		int ind = 0;
		while (i != 0) {
			if (i % 2 == 1)
				f[ind] = true;
			i = i / 2;
			ind++;
		}
		return f;
	}
   public Node getNode(Point p){
	   Node n = root.findNode(p);
		return n;
   }
	public boolean find(Point p) {
		if (root == null)
			return false;

		Node n = root.findNode(p);
		for (Point q : n.points)
			if (q.equals(p))
				return true;
		return false;
	}
	

	public class Node {
		Vector<Point> points;
		Point lowerpoint;
		Point upperpoint;
		Point midpoint;
		String id;
		Node[] children;// list of nodes
		public boolean domianted = false;
		public int count = 0;
		public void getChild(Vector<Node> all_children) {
			if (children == null) {
				all_children.add(this);
			} else
				for (Node ch : children)
					ch.getChild(all_children);
		}

		public int adjust() {
			if (children == null) {
				count = points.size();
				return points.size();
			}
			int c = 0;
			for (Node ch : children) {
				c += ch.adjust();
			}
			count = c;
			return c;
		}

		@Override
		public String toString() {
			String s = id + "[[" + lowerpoint.toString() + "-"
					+ upperpoint.toString() + "](" + domianted + "|" + count
					+ ")\n";
			if (children != null)
				for (Node n : children)
					s = s + "\t" + n.toString();
			else if (points != null) {
				s = s + "{";
				for (Point p : points)
					s = s + p.toString();
				s = s + "}";
			}

			return s;
		}

		public Node() {
			Point pl = new Point(dim);
			Point pu = new Point(dim);
			for (int i = 0; i < dim; i++) {
				pl.d[i] = Integer.MIN_VALUE;
				pu.d[i] = Integer.MAX_VALUE;
			}
			lowerpoint = pl;
			upperpoint = pu;
			initalize(pl, pu);
		}

		public void initalize(Point pl, Point pu) {
			lowerpoint = new Point(dim);
			upperpoint = new Point(dim);
			midpoint = new Point(dim);

			for (int i = 0; i < dim; i++) {
				midpoint.d[i] = (pl.d[i] + pu.d[i]) / 2;
				lowerpoint.d[i] = Min(pl.d[i], pu.d[i]);
				upperpoint.d[i] = Max(pl.d[i], pu.d[i]);
			}

			points = new Vector<Point>();
			children = null;
		}

		private double Min(double d, double e) {
			if (d > e)
				return e;
			return d;
		}

		private double Max(double d, double e) {
			if (d > e)
				return d;
			return e;
		}

		public Node(Point pl, Point pu) {
				initalize(pl, pu);
		}

		int getIndex(Point p) {
			int s = 1;
			int index = 0;
			for (int i = 0; i < p.dim; i++) {

				if (p.d[i] > midpoint.d[i])
					index += s;
				s = s * 2;
			}
			return index;
		}

		// d =4
		// s=1*2*2*2*2
		int getSize() {
			int s = 1;
			for (int i = 0; i < dim; i++) {
				s = s * 2;
			}
			return s;
		}

		public void MarkDominatedNode() {
			if (children == null)
				return;
			if (count == 0)
				return;
			Node n = children[0];
			if (n.count > 0)
				children[getSize() - 1].domianted = true;
			for (int i = 0; i < getSize(); i++) {
				n = children[i];
				if (n.domianted == true)
					continue;
				n.MarkDominatedNode();
			}
		}

		public void divide() {
			children = new Node[getSize()];
			// create the children
			for (int i = 0; i < getSize(); i++) {
				Point p = new Point(dim);
				boolean[] f = getFlags(i);
				for (int j = 0; j < dim; j++)
					if (f[j] == false)
						p.d[j] = lowerpoint.d[j];
					else
						p.d[j] = upperpoint.d[j];

				children[i] = new Node(p, midpoint);
				children[i].id = id + "-" + i;
			}
			// now partition the points
			for (Point p : points) {
				Node n = findNode(p);
				n.points.add(p);
			}
			points = null;
		}

		Node findNode(Point p) {
			if (children != null) {
				int i = getIndex(p);
				Node c = children[i];
				return c.findNode(p);
			}
			return this;
		}

		public void insert(Point p) {
			Node n = this;

			if (children != null) {
				n = findNode(p);
			}
			if (n.points.size() < threshold)
				n.points.add(p);
			else {
				n.points.add(p);
				n.divide();
			}
		}
	}


}
