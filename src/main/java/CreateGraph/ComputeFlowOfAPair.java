package CreateGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.graph.Node;

public class ComputeFlowOfAPair extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class ComputeFlowOfAPairMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static FileSystem fs;
		private static String _typeOfNetwork;// Store the type of network for
												// which we are computing flow
		private static String _commonNeighbors;// Store the path of the file to
												// the common neighbors
		private static long _commonNeighborsCount;// Store the common neighbors
													// count in this variable
		private static String _scorePath;// Store the path to the score file
		private static double _beta;// Store the beta value which is 0.05 in
									// this variable
		private static long _neighborCount;
		private static HashMap<String, Double> scores = new HashMap<String,Double>();// store
																		// the
																		// score
																		// S(v,u)
																		// in
																		// this
																		// hashmap

		@Override
		public void configure(JobConf job) {
			try {

				_typeOfNetwork = job.get("typeOfNetwork");
				_commonNeighbors = job.get("commonNeighbors");
				_scorePath = job.get("score");
				_beta = Double.parseDouble(job.get("beta"));
				fs = FileSystem.get(job);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path("/user/saibharath/" + _commonNeighbors
								+ "/part-00000"))));
				_commonNeighborsCount = Long.parseLong(br.readLine().trim());
				br.close();
				br = new BufferedReader(new InputStreamReader(fs.open(new Path(
						"/user/saibharath/" + _scorePath + "/part-00000"))));
				String st = "";
				while ((st = br.readLine()) != null) {
					// read each line in the score file and put it in a hashmap
					String[] splits = st.trim().split("\\t");
					if (splits.length == 2)
						scores.put(splits[0], Double.parseDouble(splits[1]));
				}
				br.close();
				br = new BufferedReader(new InputStreamReader(fs.open(new Path(
						"/user/saibharath/" + job.get("neighborCount")
								+ "/part-00000"))));
				_neighborCount = Long.parseLong(br.readLine().trim());
				br.close();
				System.out.println(_beta + "\t" + _commonNeighbors + "\t"
						+ _commonNeighborsCount + "\t" + _neighborCount + "\t"
						+ _scorePath + "\t" + _typeOfNetwork);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

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
		 * Key:<User:updatedDegree1:updatedDegree2>
		 * 
		 * Value--<user1:degree1
		 * ':updatedcommonneighbors1':degree1:updatedcommonneighbors1>,<user3:degree3':
		 * updatedcommonneighbors3':degree3:updatedcommonneighbors3
		 * >,<user2:degree2':updatedcommonneighbors2
		 * '::degree2:updatedcommonneighbors2>|distance1
		 * :distance2|Color1:Color2|parent1:parent2
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				Node node = new Node(text);
				String[] nodeIdSplits = node.getId().split(":");
				int sourceDegreeRecipe = Integer.parseInt(nodeIdSplits[1]);
				int sourceDegreeIngredient = Integer.parseInt(nodeIdSplits[2]);
				String[] parentSplits = node.getParent().split(":");
				String recipeParent = "", ingredientParent = "";
				String recipeColor = "", ingredientColor = "";
				String[] colorSplits = node.getColor().split(":");
				if (colorSplits.length == 2) {
					recipeColor = colorSplits[0];
					ingredientColor = colorSplits[1];
				} else if (colorSplits.length == 1) {
					if (node.getColor().charAt(0) == ':')
						ingredientColor = colorSplits[0];
					else
						recipeColor = colorSplits[0];
				}
				if (parentSplits.length == 2) {
					if (node.getParent().charAt(0) == ':')
						ingredientParent = parentSplits[0] + ":"
								+ parentSplits[1];
					else
						recipeParent = parentSplits[0] + ":" + parentSplits[1];
				} else if (parentSplits.length == 3) {
					if (parentSplits[0].equals("null"))
						ingredientParent = parentSplits[1] + ":"
								+ parentSplits[2];
					else
						recipeParent = parentSplits[0] + ":" + parentSplits[1];
				} else if (parentSplits.length == 4) {
					ingredientParent = parentSplits[2] + ":" + parentSplits[3];
					recipeParent = parentSplits[0] + ":" + parentSplits[1];
				}
				if (_typeOfNetwork.equalsIgnoreCase("recipe")
						&& recipeParent.length() > 0
						&& recipeColor.equalsIgnoreCase("BLACK")) {
					String searchScore = (recipeParent) + ","
							+ (nodeIdSplits[0] + ":" + nodeIdSplits[1]);
					double scoreOfSource = scores.get(searchScore);
					List<String> edges = new ArrayList<String>();
					for (String s : node.getEdges()) {
						String[] splits = s.split(":");
						if (splits.length == 5) {
							int recipeDegree = Integer.parseInt(splits[1]);
							int recipeWeight = Integer.parseInt(splits[2]);
							int ingredientDegree = Integer.parseInt(splits[3]);
							int ingredientWeight = Integer.parseInt(splits[4]);
							double scoreOfvBeta = scoreOfSource * _beta;
							double recipeInfluence = scoreOfvBeta
									* ((double) recipeWeight / sourceDegreeRecipe);
							if (ingredientWeight == 0 || ingredientDegree == 0) {
								s += (":" + recipeInfluence);

							} else {
								double ingredientInfluence = scoreOfvBeta
										* ((double) _commonNeighborsCount / _neighborCount)
										* ((double) ingredientWeight / sourceDegreeIngredient);
								double totalInfluence = recipeInfluence
										+ ingredientInfluence;
								s += (":" + totalInfluence);
							}
							edges.add(s);
						}
					}
					node.setEdges(edges);
					output.collect(new Text(node.getId()), node.getNodeInfo());
				} else {
					if (ingredientParent.length() > 0
							&& ingredientColor.equalsIgnoreCase("BLACK")) {
						String searchScore = (ingredientParent) + ","
								+ (nodeIdSplits[0] + ":" + nodeIdSplits[2]);
						double scoreOfSource = scores.get(searchScore);
						List<String> edges = new ArrayList<String>();
						for (String s : node.getEdges()) {
							String[] splits = s.split(":");
							if (splits.length == 5) {
								int recipeDegree = Integer.parseInt(splits[1]);
								int recipeWeight = Integer.parseInt(splits[2]);
								int ingredientDegree = Integer
										.parseInt(splits[3]);
								int ingredientWeight = Integer
										.parseInt(splits[4]);
								double scoreOfvBeta = scoreOfSource * _beta;
								double ingredientInfluence = scoreOfvBeta
										* ((double) ingredientWeight / sourceDegreeIngredient);
								if (recipeWeight == 0 || recipeDegree == 0) {
									s += (":" + ingredientInfluence);
								} else {
									double recipeInfluence = scoreOfvBeta
											* ((double) _commonNeighborsCount / _neighborCount)
											* ((double) recipeWeight / sourceDegreeRecipe);
									double totalInfluence = recipeInfluence
											+ ingredientInfluence;
									s += (":" + totalInfluence);
								}
								edges.add(s);
							}
						}
						node.setEdges(edges);
						output.collect(new Text(node.getId()),
								node.getNodeInfo());
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ComputeFlowOfAPairReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			output.collect(key, values.next());
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new ComputeFlowOfAPair(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		if (args.length < 7) {
			System.out
					.println("First argument is User Recipe Ingedient network");
			System.out.println("Second is the output");
			System.out.println("Third is the common neighbors count");
			System.out.println("Fourth is the path to score file");
			System.out.println("Fifth is the neighbor count");
			System.out.println("Sixth is the beta value(0.05");
			System.out
					.println("seventh is the type of network for which flow is computed");
			return 1;
		} else {
			JobConf job = new JobConf(super.getConf(), this.getClass());
			job.setJarByClass(this.getClass());
			job.set("commonNeighbors", args[2]);
			job.set("score", args[3]);
			job.set("neighborCount", args[4]);
			job.set("beta", args[5]);
			job.set("typeOfNetwork", args[6]);
			job.setJobName("Compute flow of (u,v) of link type " + args[6]);
			job.setJobPriority(JobPriority.VERY_HIGH);
			job.setMapperClass(ComputeFlowOfAPairMap.class);
			job.setReducerClass(ComputeFlowOfAPairReduce.class);
			// job.setNumMapTasks(50);
			job.setNumReduceTasks(30);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(job, args[0]);
			FileSystem.get(job).delete(new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			JobClient.runJob(job);
			System.out.println("***********DONE********");
			return 0;
		}
	}

}
