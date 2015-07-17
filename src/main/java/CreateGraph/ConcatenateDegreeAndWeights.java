package CreateGraph;

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
import org.graph.Node;

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
		private int recipeDegree = Integer.MIN_VALUE;

		private int ingredientDegree = Integer.MIN_VALUE;

		// remove the last character and test the rest of the string to see of
		// it is the degree or neighbors. If they are neighbors they the try
		// block will throw an exception
		public boolean isDegreeOrNeighbor(String source) {
			boolean returnValue = false;
			try {
				String lastChar = source.charAt(source.length() - 1) + "";
				String test = source.substring(0, source.length() - 1);
				if (lastChar.equals("i"))
					ingredientDegree = Integer.parseInt(test);// store the
																// ingredient
																// degree if
																// last char is
																// 'i'
				else
					recipeDegree = Integer.parseInt(test);// store recipe degree
															// if the last char
															// is 'r'
				returnValue = true;
			} catch (Exception e) {
				returnValue = false;
			}
			return returnValue;
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer buffer = new StringBuffer();
			String recipeParent = "", ingredientParent = "";
			int recipeDistance = 0, ingredientDistance = 0;
			String recipeColor = "", ingredientColor = "";
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
					if (degreeOrNeighborsString.charAt(degreeOrNeighborsString
							.length() - 1) == 'r') {
						// if the last char is 'r' store the recipe
						// distance,color and parent
						if (splits[1].equals("Integer.MAX_VALUE")) {
							recipeDistance = Integer.MAX_VALUE;
						} else {
							recipeDistance = Integer.parseInt(splits[1]);
						}
						recipeColor = splits[2];
						recipeParent = splits[3].substring(0,
								splits[3].length() - 1);
					} else {
						// If the last character of the string is 'i' then store
						// the ingredient distance,color and parent.
						if (splits[1].equals("Integer.MAX_VALUE")) {
							ingredientDistance = Integer.MAX_VALUE;
						} else {
							ingredientDistance = Integer.parseInt(splits[1]);
						}
						ingredientColor = splits[2];
						ingredientParent = splits[3].substring(0,
								splits[3].length() - 1);
					}
					buffer.append(splits[0]);// append all the edges(ingredient
												// and recipe to the buffer)
				}
			}
			System.out.println(buffer.toString());
			HashMap<String, String> neighbors = new HashMap<String, String>();
			Node node = new Node();
			String updatedKey = key.toString() + ":" + recipeDegree + ":"
					+ ingredientDegree;
			node.setId(updatedKey);
			node.setParent(recipeParent + ":" + ingredientParent);
			node.setColor(recipeColor + ":" + ingredientColor);
			node.setDistance(recipeDistance + ":" + ingredientDistance);
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
						String newValue = neighbors.get(s);
						// splits[1] is the degree and splits[2] is the weight
						newValue += (splits[1] + "," + splits[2] + ",");
						neighbors.put(s, newValue);
					} else
						neighbors.put(s, (splits[1] + "," + splits[2] + ","));
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
			List<String> edges = new ArrayList<String>();
			while (neighborIterator.hasNext()) {
				String neighborKey = neighborIterator.next();
				String neighborValue = neighbors.get(neighborKey);
				String[] splits = neighborValue.split(",");
				String bufferValue = "";
				if (splits.length == 4) {
					// splits length is 4 of there is ingredient and recipe edge
					// in the graph
					if ((splits[1].charAt(splits[1].length() - 1) + "")
							.equals("r")) {
						bufferValue = splits[0]
								+ ":"
								+ splits[1]
										.substring(0, splits[1].length() - 1)
								+ ":"
								+ splits[2]
								+ ":"
								+ splits[3]
										.substring(0, splits[3].length() - 1);
					} else {
						bufferValue = splits[2]
								+ ":"
								+ splits[3]
										.substring(0, splits[3].length() - 1)
								+ ":"
								+ splits[0]
								+ ":"
								+ splits[1]
										.substring(0, splits[1].length() - 1);
					}
				} else {
					//It is less than 4 if there is only one kind of edge in the graph
					if ((splits[1].charAt(splits[1].length() - 1) + "")
							.equals("r")) {
						bufferValue = splits[0]
								+ ":"
								+ splits[1]
										.substring(0, splits[1].length() - 1)
								+ ":0:0";
					} else {
						bufferValue = "0:0:"
								+ splits[0]
								+ ":"
								+ splits[1]
										.substring(0, splits[1].length() - 1);
					}
				}
				edges.add((neighborKey + ":" + bufferValue));
			}
			//set the new edges in the node
			node.setEdges(edges);
			//emit the node and it's new properties
			output.collect(new Text(node.getId()), node.getNodeInfo());
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

	@SuppressWarnings("deprecation")
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
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class);
		FileSystem.get(job).delete(new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
