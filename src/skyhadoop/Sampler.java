/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package skyhadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 
 * @author waleed
 */
public class Sampler {

	public static List reservoirSampling(List<PointWritable> inp, int k) {
		List<PointWritable> reservoirList = new ArrayList<PointWritable>(k); // reservoirList
																				// is
																				// where
																				// our
																				// selected
																				// lines
																				// stored
		int count = 0; // we will use this counter to count the current line
						// numebr while iterating
		Random ra = new Random();

		int randomNumber;
		for (int i = 0; i < inp.size(); i++) {
			{
				count++; // increase the line number
				if (count <= 10) {
					reservoirList.add(inp.get(i));
				} else {
					randomNumber = (int) ra.nextInt(count);
					if (randomNumber < k) {
						reservoirList.set(randomNumber, inp.get(i));
					}
				}
			}
		}
		return null;
	}
}
