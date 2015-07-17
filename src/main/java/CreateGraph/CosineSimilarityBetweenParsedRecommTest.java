package CreateGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

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
import org.crawling.models.ReviewDish;

import com.google.gson.Gson;

public class CosineSimilarityBetweenParsedRecommTest extends Configured
		implements Tool {

	/**
	 * @param args
	 */

	public static class CosineSimilarityBetweenParsedRecommTestMap extends
			MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public int fold;

		@Override
		public void configure(JobConf job) {
			try {
				fold = Integer.parseInt(job.get("fold"));
				try {
					Path recipeIdFilePath = new Path(
							"/user/saibharath/UniqueRecipeJsonFinal/part-00000");
					FileSystem fs = FileSystem.get(new Configuration());
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(recipeIdFilePath)));
					String str = "";
					while ((str = br.readLine()) != null) {
						ReviewDish dish = new Gson().fromJson(str,
								ReviewDish.class);
						recipes.put(dish.getName(), dish.getIngredientsName());
					}
					br.close();
					br = new BufferedReader(new InputStreamReader(
							fs.open(new Path(
									"/user/saibharath/UserAndHisRecipesTrainFold"
											+ fold + "/part-00000"))));
					while ((str = br.readLine()) != null) {
						String[] splits = str.split("\\t");
						String key = splits[0].split(":")[0];
						List<String> list = new ArrayList<String>();
						for (String s : splits[1].split(",")) {
							list.add(s);
						}
						userRecipes.put(key, list);
					}
					br.close();
				} catch (Exception e) {

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		static HashMap<String, List<String>> recipes = new HashMap<String, List<String>>();
		static HashMap<String, List<String>> userRecipes = new HashMap<String,List<String>>();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			PriorityQueue<String> queue = new PriorityQueue<String>(1000000,
					new Comparator<String>() {

						@Override
						public int compare(String o1, String o2) {
							double s1 = Double.parseDouble(o1.split(",")[1]);
							double s2 = Double.parseDouble(o2.split(",")[1]);
							return s2 > s1 ? 1 : s2 == s1 ? 0 : -1;
						}
					});
			try {
				text = text.trim();
				String splits[] = text.split("\\t");
				Iterator<String> keys = recipes.keySet().iterator();
				List<String> usersRecipes = userRecipes.get(splits[0]);
				while (keys.hasNext()) {
					String ingredientKey = keys.next();
					List<String> ingredients = recipes.get(ingredientKey);
					if (usersRecipes.contains(ingredientKey))
						continue;
					StringTokenizer st = new StringTokenizer(splits[1], ",");
					int count = 0, size = 0;
					while (st.hasMoreTokens()) {
						if (ingredients
								.contains(st.nextToken()))
							count++;
						size++;
					}
					double recipeIngreLength = Math.sqrt(ingredients.size());
					double algorithmIngredients = Math.sqrt(size);
					double lengthProduct = recipeIngreLength
							* algorithmIngredients;
					double cosineSim = count / lengthProduct;
					queue.add(ingredientKey + "," + cosineSim);
				}
				for (int i = 0; i < 10; i++) {
					output.collect(new Text(splits[0]), new Text(
							queue.remove().split(",")[0]));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class CosineSimilarityBetweenParsedRecommTestReduce extends
			MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer outputRecipes = new StringBuffer();
			while (values.hasNext()) {
				outputRecipes.append(values.next().toString() + ",");
			}
			output.collect(key, new Text(outputRecipes.toString()));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new CosineSimilarityBetweenParsedRecommTest(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(CosineSimilarityBetweenParsedRecommTest.class);
		job.setJobName("Cosine Similarity for adsorption results and input recipe");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(CosineSimilarityBetweenParsedRecommTestMap.class);
		job.setReducerClass(CosineSimilarityBetweenParsedRecommTestReduce.class);
		// job.setNumMapTasks(50);
		job.setNumReduceTasks(30);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.set("fold", args[2]);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
