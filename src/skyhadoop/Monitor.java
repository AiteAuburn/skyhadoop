package skyhadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * 
 * Monitor the skyline execution in the hadoop cluster
 * 
 */
class Monitor extends Thread {

	/**
	 * @TODO How to know port numbers for partitions?
	 * @TODO How to determine size of writable object to allocate ByteBuffer?
	 */
	public boolean Sharing;
	public boolean ComputingGSL;
	public int sharedPointsNumber;
	public String inputFile;
	public String outputFile;
	public int ps;
	public ByteBuffer ports;
	public JobConf conf;

	public void stopThread() {
		Sharing = false;
	}

	public Monitor(String inputFile, String outputFile, int sharedPointsNumber) {
		this.sharedPointsNumber = sharedPointsNumber;
		this.inputFile = inputFile;
		this.outputFile = outputFile;
		Sharing = false;
		ComputingGSL = false;
	}

	public void startComputingSL() {
		Sharing = false;
		ComputingGSL = true;
	}

	public void startSharing() {
		Sharing = true;
	}

	public void stopSharing() {
		Sharing = false;
	}

	public void Share() throws IOException {
		System.out.println("STARTED THREAD");

		Vector<ByteBuffer> msg = new Vector<ByteBuffer>();
		Vector<Integer> par = new Vector<Integer>();
		DatagramChannel channel = DatagramChannel.open();
		channel.configureBlocking(false);
		channel.socket().bind(new InetSocketAddress(9900));
		ByteBuffer rec = ByteBuffer.allocate(64 * 1048);
		while (Sharing) {
			rec.clear();
			SocketAddress sa = channel.receive(rec);
			if (sa != null) {
				int sender = (int) rec.getDouble(0);
				msg.add(rec);
				par.add(sender);
			} else {
				for (int i = 0; i < msg.size(); i++) {
					ByteBuffer bb = ByteBuffer.allocate(64 * 1048);
					bb.clear();
					bb = msg.get(i).putDouble(0, 0.0);
					if (par.get(i) < ps - 1) {
						int bytesSent = channel.send(bb, new InetSocketAddress(
								"127.0.0.1", ports.get(par.get(i) + 1)));

					}
				}
			}
		}
		System.out.println("Done sharing!!");
	}

	public void computeGlobalSL() throws IOException {
		Vector<Point> skylines = new Vector<Point>();
		FileSystem fs = FileSystem.get(conf);
		int oo = 0;
		for (int i = 0;; i++) {
			System.out.println(inputFile + String.format("%05d", i));
			Path inFile = new Path(inputFile + String.format("%05d", i));
			if (!fs.exists(inFile)) {
				break;
			}
			BufferedReader in = new BufferedReader(new InputStreamReader(
					fs.open(inFile)));

			System.out.println("STARTED SOLVING GLOBAL");
			String s;
			Vector<Point> todel = new Vector<Point>();

			while ((s = in.readLine()) != null) {
				System.out.println("READING " + oo++);
				Point p = new Point(s);
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
					for (Point q : todel) {
						skylines.remove(q);
					}
					todel.clear();
				}
				if (isskyline) {
					skylines.add(p);

				}
			}
			in.close();
		}
		System.out.println("SOLVED GLOBAL");
		PrintWriter out = new PrintWriter(outputFile, "UTF-8");
		for (int i = 0; i < skylines.size(); i++) {
			System.out.println(skylines.get(i).toString());
		}
		out.close();
	}

	@SuppressWarnings("deprecation")
	public void run() {
		try {
			if (Sharing) {
				Share();
			}
			if (ComputingGSL) {
				computeGlobalSL();
			}
		} catch (IOException ex) {
			Logger.getLogger(Monitor.class.getName()).log(Level.SEVERE, null,
					ex);
		}
	}
}
