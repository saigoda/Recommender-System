package org.allrecipes.CosineSimilarity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class Similarity {

	/**
	 * @param args
	 */
	public static Multimap<String, String> index = ArrayListMultimap.create();
	public static HashMap<String, Integer> maxFreq = new HashMap<String, Integer>();
	public static HashMap<String, Double> docLength = new HashMap<String, Double>();
	private static int numberOfUsers = 247859;

	public static void configure() throws Exception {
		try {
			BufferedReader br = new BufferedReader(
					new FileReader(
							new File(
									"/homes/saibharath/allrecipes/reviews/ingredient-experiment/fold1/inverted-index/part-00000")));
			String st = "";
			while ((st = br.readLine()) != null) {
				String[] splits = st.split("\\t");
				String key = splits[0].split(":")[0];
				String[] ingredients = splits[1].split(",");
				for (String ingredient : ingredients) {
					index.put(key, ingredient);
				}
			}
			br.close();
			br = new BufferedReader(
					new FileReader(
							new File(
									"/homes/saibharath/allrecipes/reviews/ingredient-experiment/fold1/max-frequency/part-00000")));
			while ((st = br.readLine()) != null) {
				String[] splits = st.split("\\t");
				maxFreq.put(splits[0], Integer.parseInt(splits[1]));
			}
			br.close();
			br = new BufferedReader(
					new FileReader(
							new File(
									"/homes/saibharath/allrecipes/reviews/ingredient-experiment/fold1/doc-length/part-00000")));
			while ((st = br.readLine()) != null) {
				String[] splits = st.split("\\t");
				docLength.put(splits[0], Double.parseDouble(splits[1]));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws Exception {
		configure();
		BufferedReader br = new BufferedReader(
				new FileReader(
						new File(
								"/homes/saibharath/allrecipes/reviews/ingredient-experiment/fold1/weights/part-00000")));
		String value = "";
		BufferedWriter bw = new BufferedWriter(
				new FileWriter(
						new File(
								"/homes/saibharath/allrecipes/reviews/ingredient-experiment/fold1/neighbors/part-00000")));
		while ((value = br.readLine()) != null) {
			String text = value.toString().trim();
			HashMap<String, Double> sumWeight = new HashMap<String,Double>();
			String[] splits = text.split("\\t");
			for (String ingredient : splits[1].split(",")) {
				String[] ingNum = ingredient.split(":");
				Collection<String> users = index.get(ingNum[0]);
				for (String user : users) {
					String[] userNum = user.split(":");
					if(splits[0].equalsIgnoreCase(userNum[0]))
						continue;
					int max = maxFreq.get(userNum[0]);
					double tf = 0, idf = 0, weight = 0, sum = 0;
					int userFreq = Integer.parseInt(userNum[1]);
					if (max != 0)
						tf = userFreq / (double) max;
					idf = Math.log10(numberOfUsers / (double) users.size())
							/ Math.log10(2);
					weight = tf * idf;
					sum = weight * Double.parseDouble(ingNum[1]);
					if (!sumWeight.containsKey(userNum[0])) {
						sumWeight.put(userNum[0], sum);
					} else {
						double val = sumWeight.get(userNum[0]) + sum;
						sumWeight.put(userNum[0], val);
					}
				}
			}
			TreeSet<String> queue = new TreeSet<String>(new Comparator<String>() {

				@Override
				public int compare(String s1, String s2) {
					double d1 = Double.parseDouble(s1.split(":")[1]);
					double d2 = Double.parseDouble(s2.split(":")[1]);
					return d1 > d2 ? -1 : d1 == d2 ? 0 : 1;
				}
			});

			Iterator<String> userIter = sumWeight.keySet().iterator();
			while (userIter.hasNext()) {
				String user = userIter.next();
				double doclen1 = docLength.get(splits[0]), doclen2 = docLength
						.get(user), sumTotal = 0;
				sumTotal = sumWeight.get(user) / (doclen1 * doclen2);
				if (queue.size() < 10) {
					queue.add(user + ":" + sumTotal);
				} else {
					String last = queue.last();
					queue.remove(last);
					queue.add(user + ":" + sumTotal);
				}
			}
			System.out.println("My queue size is" + queue.size());
			Iterator<String> it = queue.iterator();
			StringBuilder sb = new StringBuilder();
			while (it.hasNext()) {
				String val = it.next();
				sb.append(val + ",");
			}
			bw.write(splits[0] + "\t" + sb.toString()+"\n");
		}
		br.close();
		bw.close();
	}
}
