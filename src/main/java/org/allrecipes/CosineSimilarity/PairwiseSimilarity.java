package org.allrecipes.CosineSimilarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

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

public class PairwiseSimilarity extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static class ComputeSimilarityMap2 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public static HashMap<String, Double> docLength = new HashMap<String,Double>();

		@Override
		public void configure(JobConf job) {
			try {
				FileSystem fs = FileSystem.get(job);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(job.get("docPath") + "/part-00000"))));
				String st = "";
				while ((st = br.readLine()) != null) {
					String[] splits = st.split("\\t");
					docLength.put(splits[0], Double.parseDouble(splits[1]));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			String[] splits = text.split("\\t");
			int df = Integer.parseInt(splits[0].split(":")[1]);
			if (df < 100000) {
				String[] ingredients = splits[1].split(",");
				HashMap<String, Double> orderedPairs = new HashMap<String, Double>();
				HashMap<String, Double> weights = new HashMap<String, Double>();
				for (String ingredient : ingredients) {
					String[] ingNum = ingredient.split(":");
					weights.put(ingNum[0], Double.parseDouble(ingNum[1]));
				}
				for (String ingredient : ingredients) {
					String[] ingNum = ingredient.split(":");
					for (int i = 0; i < ingredients.length; i++) {
						String ing = ingredients[i];
						if (ing.equals(ingredient))
							continue;
						else {
							String[] ingNum1 = ing.split(":");
							String samePair = ingNum[0] + "," + ingNum1[0];
							String reversePair = ingNum1[0] + "," + ingNum[0];
							if (orderedPairs.containsKey(samePair)
									|| orderedPairs.containsKey(reversePair))
								continue;
							else {
								orderedPairs.put(samePair, (weights
										.get(ingNum[0]) * weights
										.get(ingNum1[0])));
							}

						}
					}
					Iterator<String> it = orderedPairs.keySet().iterator();
					while (it.hasNext()) {
						String key1 = it.next();
						String[] splits1 = key1.split(",");
						String val1 = splits1[0] + ":"
								+ docLength.get(splits1[0]);
						String val2 = splits1[1] + ":"
								+ docLength.get(splits1[1]);
						String updatedKey = val1 + "," + val2;
						output.collect(new Text(updatedKey), new Text(
								orderedPairs.get(key1) + ""));
					}
				}
			}
		}

	}

	public static class ComputeSimilarityReduce2 extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String[] splits = key.toString().split(",");
			String doc1 = splits[0].split(":")[0], doc2 = splits[1].split(":")[0];
			double val1 = Double.parseDouble(splits[0].split(":")[1]), val2 = Double
					.parseDouble(splits[1].split(":")[1]);
			double sum = 0;
			while (values.hasNext()) {
				sum += Double.parseDouble(values.next().toString());
			}
			double weight = sum / (val1 * val2);
			output.collect(new Text(doc1 + "," + doc2), new Text(weight + ""));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new PairwiseSimilarity(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.set("docPath", args[1]);
		job.setJobName("Compute similar neighbors");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ComputeSimilarityMap2.class);
		job.setReducerClass(ComputeSimilarityReduce2.class);
		job.setNumMapTasks(20);
		job.setNumReduceTasks(120);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
