package skyhadoop;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Vector;
/**
 * 
 * Monitor the skyline execution in the hadoop cluster
 *
 *//*
 class Monitor extends Thread {																																																																																																																																																																																																																																																																																																																																														
		private boolean Status;
		int k;
		private boolean kill;																			
		public long end;
		public long start;																																																																																																																																																																									
		public String file;
		private int ps;
		private ByteBuffer ports;
		public   JobConf conf;
		public void stopThread() {
			Status = false;
		}

		public Monitor(String resFile, long start) {
			this.start = start;
			file = resFile;
			Status = true;
			kill = false;
		}

		public void killThread() {
			kill = true;
		}

		@SuppressWarnings("deprecation")
		public void run() {
			try {
				System.out.println("STARTED THREAD");

				end = 0;
				Vector<ByteBuffer> msg = new Vector<ByteBuffer>();
				Vector<Integer> par = new Vector<Integer>();
				DatagramChannel channel = DatagramChannel.open();
				channel.configureBlocking(false);
				channel.socket().bind(new InetSocketAddress(9900));
				ByteBuffer rec = ByteBuffer.allocate(k * 4 * 8 + 8 * 8);
				while (Status && !kill) {
					ByteBuffer send = ByteBuffer.allocate(k * 4 * 8 + 8 * 8);
					rec.clear();
					send.clear();
					SocketAddress sa = channel.receive(rec);
					if (sa != null) {
						Double[][] points = new Double[k][4];
						int sender = (int) rec.getDouble(0);

						for (int i = 0, l = 8; i < k; i++) {
							for (int j = 0; j < 4; j++, l += 8) {
								points[i][j] = rec.getDouble(l);
								// System.out.print(points[i][j] + " ");
								send.putDouble(l - 8, points[i][j]);
							}
							// System.out.println();
						}
						System.out.println("RECIEVED " + k + " POINTS FROM "
								+ sender);
						// for (int i = 0; i < send.array().length; i++) {
						// System.out.println(send.array()[i]);
						// }
						msg.add(send);
						par.add(sender);
					} else {
						for (int i = 0; i < msg.size(); i++) {
							ByteBuffer bb = ByteBuffer.allocate(k * 4 * 8 + 8
									* 8);
							bb.clear();
							bb = msg.get(i).putDouble(k * 4 * 8 + 16, 0.0);
							if (par.get(i) < ps - 1) {
								int bytesSent = channel.send(bb,
										new InetSocketAddress("127.0.0.1",
												ports.get(par.get(i) + 1)));
								// if (bytesSent == 0) {
								// System.out.println("ERROR @PORT: " +
								// ports.get(i));
								// }
							}
						}
					}
				}
				Vector<Double[]> SL = new Vector<Double[]>();
				// Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				int oo = 0;
				for (int i = 0;; i++) {
					System.out.println(file + String.format("%05d", i));
					Path inFile = new Path(file + String.format("%05d", i));
					if (!fs.exists(inFile)) {
						break;
					}
					BufferedReader in = new BufferedReader(
							new InputStreamReader(fs.open(inFile)));

					System.out.println("STARTED SOLVING GLOBAL");
					String s;

					while (!kill && (s = in.readLine()) != null) {
						System.out.println("READING " + oo++);

						String[] ss = s.split("\t");
						String[] temp = ss[1].split(",");
						Double[] t = new Double[temp.length];
						for (int j = 0; j < t.length; j++) {
							t[j] = Double.parseDouble(temp[j]);
						}
						boolean add = true;

						for (int indd = 0; indd < SL.size(); indd++) {
							int dominate = -1;
							for (int j = 0; j < t.length; j++) {
								if (t[j] < SL.get(indd)[j]) {
									if (dominate == -1) {
										dominate = 1;
									} else if (dominate == 2) {
										dominate = 0;
									}
								} else if (t[j] > SL.get(indd)[j]) {
									if (dominate == -1) {
										dominate = 2;
									} else if (dominate == 1) {
										dominate = 0;
									}
								}
							}
							if (dominate == 1) {

								SL.remove(indd--);
							} else if (dominate == 2 || dominate == -1) {
								add = false;
								break;
							}
						}
						if (add) {
							SL.add(t);
						}
					}
					in.close();
				}
				System.out.println("SOLVED GLOBAL");
				PrintWriter out = new PrintWriter("/home/hadoopuser/HADOOPCODE/result", "UTF-8");
				for (int i = 0; i < SL.size(); i++) {
					out.print(SL.get(i)[0]);
					for (int j = 0; j < SL.get(i).length; j++) {
						out.print("," + SL.get(i)[j]);
					}
					out.print("\n");
				}
				out.close();
				end = System.currentTimeMillis();
				FileWriter writer2 = new FileWriter("jobTime", true);
				writer2.write(end - start + "\n");
				writer2.close();
			} catch (Exception e) {
				System.out.println(e.getLocalizedMessage());
			}
		}
	}
*/