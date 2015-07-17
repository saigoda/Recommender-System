package CreateGraph;

import java.io.IOException;
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

public class CreateNeighbourhoodForBFS extends Configured implements Tool {

	/**
	 * @param args
	 */
	/*
	 * 
	 * This class takes the neighborhood and converts the neighborhood into a
	 * form that is given as an input to Breadth First Search Algorithm.
	 * 
	 * The neighborhood is of the form
	 * <user>\t<user>,<user>,<user>,<user>.... 
	 * Ex:- 10000_user:30	11000_user:30:20,2000_user:10:2,3000_user:10:1....
	 * 
	 * The mapper emits the key and it's corresponding neighbors. We can even
	 * omit the mapper in this case
	 */
	public static class CreateNeighbourhoodForBFSMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter) \
		 * 
		 * output key value pair:
		 * 
		 * Key--<key> (Ex:10000_user:30) Value--List of neighbors(Ex:
		 * 2345_user:30,3456_user:40....)
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String[] splits = value.toString().trim().split("\\t");
			output.collect(new Text(splits[0]), new Text(splits[1]));
		}
	}

	public static class CreateNeighbourhoodForBFSReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		private boolean source = false;

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * So the first key that the reducer receives is taken as the source
		 * node.The paper says any arbitrary node can be chosen as the source
		 * node.
		 * 
		 * There is only one reducer so all the keys come to the same reducer.
		 * The first key is the source.
		 * 
		 * For source node the output is
		 * <Key>\t<neighbor1>,<neighbor2>,<neighbor3>,<neighbor4>|distance|GRAY|source|1
		 * The distance is the distance from
		 * the parent node. If the distance is modified then it means the parent
		 * of the node changedThey source node is GRAY indicating that this is
		 * the node from which the processing should startThe 'source' indicates
		 * that this is the parent node.
		 * 
		 * For other nodes the output is
		 * 
		 * <Key>\t<neighbor1>,<neighbor2>,<neighbor3>,<neighbor4>|Integer.MAX_VALUE
		 * |WHITE|null|0
		 * 
		 * The color of these nodes is white since they are ready to be
		 * processed first or they are in the search frontier.The distance is
		 * set to some arbitrary number since the node is not yet reached.
		 * 
		 * The null indicates that there is no parent for this node.
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			StringBuffer b = new StringBuffer();
			while (values.hasNext())
				b.append(values.next().toString());
			if (!source) {
				output.collect(key, new Text(b.toString() + "|0|GRAY|source|1"));
				source = true;
			} else
				output.collect(key, new Text(b.toString()
						+ "|Integer.MAX_VALUE|WHITE|null|0"));
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new CreateNeighbourhoodForBFS(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Generate Neighbourhood for BFS");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(CreateNeighbourhoodForBFSMap.class);
		job.setReducerClass(CreateNeighbourhoodForBFSReduce.class);
		// job.setNumMapTasks(100);
		job.setNumReduceTasks(1);
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
