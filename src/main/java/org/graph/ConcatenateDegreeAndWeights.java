package org.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConcatenateDegreeAndWeights extends Configured implements Tool {

	/**
	 * @param args
	 * 
	 * 
	 *            This program concatenates both the networks and neighborhoods
	 *            so that it is easy to compute flow values. This class takes
	 *            both the updated neighborhoods(neighborhoods updated with the
	 *            type of link information.
	 * 
	 */

	public static class ConcatenateDegreeAndWeightsMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * Input:
		 * 
		 * Key---<user:updatedDegree>
		 * 
		 * updatedDegree--degree+_typeofnetwork.toLowerCase().charAt(0);
		 * 
		 * Value--<user1:degree1:updatedcommonneighbors1>,<user3:degree3:
		 * updatedcommonneighbors3
		 * >,<user2:degree2:updatedcommonneighbors2>|distance|Color|parent
		 * 
		 * updatedcommonneighbors(i)--commonneighbors(i)+_typeofnetwork.toLowerCase
		 * ().charAt(0);
		 * 
		 * _typeofnetwork---Is the type of the network that is specified in the
		 * updated neighborhood job
		 * 
		 * Output:
		 * 
		 * Key--<User>
		 * 
		 * Values--updatedDegree and
		 * <user1:degree1:updatedcommonneighbors1>,<user3:degree3:
		 * updatedcommonneighbors3
		 * >,<user2:degree2:updatedcommonneighbors2>|distance|Color|parent
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			String text = value.toString().trim();
			Node node = new Node(text);
			String[] userDegree = node.getId().split(":");
			output.collect(new Text(userDegree[0]), new Text(userDegree[1]));
			output.collect(new Text(userDegree[0]), node.getNodeInfo());
		}
	}

	public static class ConcatenateDegreeAndWeightsReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * Input:
		 * 
		 * Key--<User>
		 * 
		 * Values--updatedDegree1,updatedDegree2 and
		 * <user1:degree1':updatedcommonneighbors1'>,<user3:degree3':
		 * updatedcommonneighbors3'
		 * >,<user2:degree2':updatedcommonneighbors2'>|distance1
		 * |Color1|parent1,<
		 * user1:degree1:updatedcommonneighbors1>,<user3:degree3:
		 * updatedcommonneighbors3
		 * >,<user2:degree2:updatedcommonneighbors2>|distance2|Color2|parent2
		 * 
		 * Output:
		 * 
		 * Key:<User:updatedDegree1:updatedDegree2>
		 * 
		 * Value--<user1:degree1
		 * ':updatedcommonneighbors1':degree1:updatedcommonneighbors1>,<user3:degree3':
		 * updatedcommonneighbors3':degree3:updatedcommonneighbors3
		 * >,<user2:degree2':updatedcommonneighbors2
		 * '::degree2:updatedcommonneighbors2>|distance1
		 * :distance2|Color1:Color2|parent1:parent2
		 */
		private HashMap<String, Integer> degrees = new HashMap<String, Integer>();

		private HashMap<String, String> colors = new HashMap<String, String>();

		private List<String> networkType = new ArrayList<String>();

		private HashMap<String, String> parent = new HashMap<String, String>();

		// remove the last character and test the rest of the string to see of
		// it is the degree or neighbors. If they are neighbors they the try
		// block will throw an exception
		public boolean isDegreeOrNeighbor(String source) {
			boolean returnValue = false;
			try {
				String lastChar = source.charAt(source.length() - 1) + "";
				String test = source.substring(0, source.length() - 1);
				for (String network : networkType) {
					String sourceChar = network.toLowerCase().charAt(0) + "";
					if (lastChar.equals(sourceChar))
						degrees.put(network, Integer.parseInt(test));// Stores
																		// the
																		// degree
																		// value
																		// in
																		// the
																		// hash
																		// map
					returnValue = true;
				}
			} catch (Exception e) {
				returnValue = false;
			}
			return returnValue;
		}

		@Override
		public void configure(JobConf job) {
			try {
				int count = Integer.parseInt(job.get("count"));
				for (int i = 0; i < count; i++) {
					networkType.add(job.get("type" + (i + 1)));
				}
			} catch (Exception e) {

			}
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer buffer = new StringBuffer();
			while (values.hasNext()) {
				String degreeOrNeighborsString = values.next().toString();
				// System.out.println(degreeOrNeighborsString);
				boolean degreeOrNeighbors = isDegreeOrNeighbor(degreeOrNeighborsString);
				// If the value is not a degree then go to this part of the code
				// and store the parent,distance,edges and color
				if (!degreeOrNeighbors) {
					// Split the values by '|' to get the edges,parent,distance
					// and color
					String[] splits = degreeOrNeighborsString.split("\\|");
					char lastChar = degreeOrNeighborsString
							.charAt(degreeOrNeighborsString.length() - 1);
					// if the last char is 'r' store the recipe
					// distance,color and parent
					for (String network : networkType) {
						char sourceChar = network.toLowerCase().charAt(0);
						if (sourceChar == lastChar) {
							colors.put(network, splits[2]);
							parent.put(network, splits[3].substring(0,
									splits[3].length() - 1));
						}
					}
					buffer.append(splits[0]);// append all the edges(ingredient
					// and recipe to the buffer)
				}
			}
			System.out.println(buffer.toString());
			HashMap<String, HashMap<String, String>> neighbors = new HashMap<String, HashMap<String, String>>();
			String updatedKey = key.toString();
			for (String network : networkType) {
				updatedKey += ":" + degrees.get(network);
			}
			for (String s : buffer.toString().split(",")) {
				String[] splits = s.split(":");// store all the edges in a hash
												// map.
				// The key of the hashmap is the node name and the values are
				// the degree and weights in each network respectively.
				// If the node is not present in both the neighborhoods we store
				// only one neighbors degree and weight
				if (splits.length == 3) {
					s = splits[0];// splits[0]--contains the node name
					if (neighbors.containsKey(s)) {
						HashMap<String, String> map = neighbors.get(s);
						for (String network : networkType) {
							if (network.toLowerCase().charAt(0) == splits[2]
									.charAt(splits[2].length() - 1)) {
								map.put(network,
										(splits[1]
												+ ":"
												+ splits[2].substring(0,
														splits[2].length() - 1) + ":"));
								break;
							}
						}
						neighbors.put(s, map);
					} else {
						for (String network : networkType) {
							if (network.toLowerCase().charAt(0) == splits[2]
									.charAt(splits[2].length() - 1)) {
								HashMap<String, String> map = new HashMap<String, String>();
								map.put(network,
										(splits[1]
												+ ":"
												+ splits[2].substring(0,
														splits[2].length() - 1) + ":"));
								neighbors.put(s, map);
								break;
							}
						}
					}
					// neighbors.put(s, (splits[1] + "," + splits[2] +
					// ","));
				}
			}
			// Iterate through the hashmap and store all the values(degree and
			// weights) of the hashmaps without the appended characters
			// The order of storing an is :
			/*
			 * If there is an edge of type ingredient and recipe in the
			 * neighborhood then we store the edge as:
			 * 
			 * edge:recipe_degree:recipe_common_neighbors
			 * :ingredient_degree:ingredient_common_neighbors
			 * 
			 * If there is recipe edge and no ingredient edge then we store the
			 * edge as:
			 * 
			 * edge:recipe_degree:recipe_common_neighbors :0:0
			 * 
			 * If there is a ingredient edge and no recipe edge then we store
			 * the edge as:
			 * 
			 * edge:0:0:ingredient_degree:ingredient_common_neighbors
			 */
			Iterator<String> neighborIterator = neighbors.keySet().iterator();
			StringBuffer edges = new StringBuffer();
			while (neighborIterator.hasNext()) {
				String neighborKey = neighborIterator.next();
				HashMap<String, String> neighborValue = neighbors
						.get(neighborKey);
				String degreeWeight = "";
				for (String network : networkType) {
					if (neighborValue.containsKey(network)) {
						degreeWeight += (neighborValue.get(network));
					} else {
						degreeWeight += ("0:0:");
					}
				}
				edges.append(neighborKey + ":"
						+ degreeWeight.substring(0, degreeWeight.length() - 1)
						+ ",");
			}

			String color = "";
			for (String network : networkType) {
				color += (this.colors.get(network) + ":");
			}
			color = color.substring(0, color.length() - 1);
			edges.append("|" + color);
			String parents = "";
			for (String network : networkType) {
				parents += (this.parent.get(network) + ",");
			}
			parents = parents.substring(0, parents.length() - 1);
			edges.append("|" + parents);
			// emit the node and it's new properties
			output.collect(new Text(updatedKey), new Text(edges.toString()));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new ConcatenateDegreeAndWeights(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Concatenate Degrees and Networks");
		job.setJobPriority(JobPriority.NORMAL);
		job.setMapperClass(ConcatenateDegreeAndWeightsMap.class);
		job.setReducerClass(ConcatenateDegreeAndWeightsReduce.class);
		// job.setNumMapTasks(100);
		job.setNumReduceTasks(30);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		for (int i = 0; i < Integer.parseInt(args[args.length - 1]); i++) {
			MultipleInputs.addInputPath(job, new Path(args[i]),
					TextInputFormat.class);
		}
		job.set("count", args[args.length - 1]);
		FileSystem.get(job).delete(new Path(args[0 + Integer.parseInt(args[args.length - 1])]));
		FileOutputFormat.setOutputPath(job,
				new Path(args[0 + Integer.parseInt(args[args.length - 1])]));
		for (int i = 0; i < args.length
				- (Integer.parseInt(args[args.length - 1]) + 1); i++) {
			job.set("type" + (i + 1),
					args[(Integer.parseInt(args[args.length - 1]) + 1) + i]);// set
																				// the
																				// type
																				// of
																				// the
			// network that
			// we are using to store the
			// neighborhood. The same
			// type
			// is to be used throughout
		}
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
