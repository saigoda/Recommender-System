import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class Create_Graph_Parallel extends Configured implements Tool {

	/* Path to input folder */
	public static String userInputPath_;
	/* Path to input folder */
	public static String itemInputPath_;

	public static String edgeInputPath_;

	public static String dbName_;

	// public static String host_ = "localhost";

	// public static String host_ =
	// "10.50.0.2,10.50.0.4,10.50.0.5,10.50.0.6,10.50.0.7,10.50.0.8,10.50.0.9,10.50.0.10,10.50.0.11,"
	// +
	// "10.50.0.12,10.50.0.13,10.50.0.14,10.50.0.15,10.50.0.16,10.50.0.17,10.50.0.18,10.50.0.19,10.50.0.20,10.50.0.21,10.50.0.22";

	public static String host_;

	public static void configure(String[] args) throws IOException {

		/*
		 * The first and second arguments will be the output paths from
		 * userSetGeneratorParallel and ItemSetGeneratorParallel jobs
		 */
		userInputPath_ = args[0];
		itemInputPath_ = args[1];
		edgeInputPath_ = args[2];
		dbName_ = args[3];
		System.out.println(userInputPath_);
		System.out.println(itemInputPath_);
		System.out.println(edgeInputPath_);
		System.out.println(dbName_);

		if (args[4].equalsIgnoreCase("local")) {
			host_ = "localhost";
		} else {
			// host_ =
			// "10.50.0.2,10.50.0.4,10.50.0.5,10.50.0.6,10.50.0.7,10.50.0.8,10.50.0.9,10.50.0.10,10.50.0.11,"
			// +
			// "10.50.0.12,10.50.0.13,10.50.0.14,10.50.0.15,10.50.0.16,10.50.0.17,10.50.0.18,10.50.0.19,10.50.0.20,10.50.0.21,10.50.0.22";
			host_ = "theia,titan01,titan02,titan04,titan05,titan06,titan07,titan08,titan09,titan10";
		}

		System.out.println("Host is: " + host_);

	}

	/**
	 * Function to call the mapreduce jobs in this class.
	 */

	public int run(String[] args) throws Exception {
		// System.out.println("\n*****Loading the Configuration for this Job*****\n");
		// configure(args);
		// System.out.println("\n*****Done....*****\n");
		System.out
				.println("Loading the configurations for table creation. BOTH unique");
		configure(args);

		System.out
				.println("Creating a schema for parallel loading of vertices");
		createGraph();
		System.out.println("Constructing the set of user and item vertices");
		System.out.println("Adding the vertices to the graph");
		JobConf conf0 = createJob0(args);
		RunningJob job0 = JobClient.runJob(conf0);
		System.out.println("\n*****Done....*****\n");
		// System.out.println("Printing the Vertices");
		// printVertices();

		System.out.println("Adding the edges to the graph now");
		JobConf conf1 = createJob1(args);
		RunningJob job1 = JobClient.runJob(conf1);

		System.out.println("About to query database");

		queryDatabase();
		System.out.println("\n*****Done....*****\n");

		return 0;
	}

	public static void queryDatabase() {
		TitanGraph g = null;
		try {
			org.apache.commons.configuration.Configuration titanConf = new BaseConfiguration();
			titanConf.setProperty("storage.backend", "hbase");
			titanConf.setProperty("storage.tablename", dbName_);
			titanConf.setProperty("storage.hostname", host_);

			g = TitanFactory.open(titanConf);

			Iterator<Vertex> vertices = g.getVertices().iterator();
			long verticesCount = 1;
			while (vertices.hasNext()) {
				verticesCount++;
			}
			Iterator<Edge> edges = g.getEdges().iterator();
			long edgesCount = 1;
			while (edges.hasNext()) {
				edgesCount++;
			}
			System.out.println("The number of vertices are:" + verticesCount);
			System.out.println("The number of edges in the graph are:"
					+ edgesCount);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Method to set the configuration of the MapReduce Job
	 * 
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public JobConf createJob0(String[] args) throws Exception {
		JobConf jobConf = new JobConf(super.getConf(), this.getClass());

		jobConf.setJarByClass(this.getClass());
		jobConf.setJobName("VertexCreation_" + dbName_);

		Path userInputPath = new Path(userInputPath_);
		Path itemInputPath = new Path(itemInputPath_);

		System.out.println("Input Path on HDFS: " + userInputPath_);
		System.out.println("Input Path on HDFS: " + itemInputPath_);

		jobConf.setNumReduceTasks(0);

		jobConf.set("DBNAME", dbName_);
		jobConf.set("HOST", host_);

		jobConf.set("mapred.task.timeout", "1800000");
		jobConf.set("mapred.job.priority", JobPriority.VERY_HIGH.toString());

		MultipleInputs.addInputPath(jobConf, userInputPath,
				TextInputFormat.class, VertexLoader.class);
		MultipleInputs.addInputPath(jobConf, itemInputPath,
				TextInputFormat.class, VertexLoader.class);

		FileSystem.get(jobConf).delete(
				new Path("/tmp/vertexCreation" + dbName_), true);
		FileOutputFormat.setOutputPath(jobConf, new Path("/tmp/vertexCreation"
				+ dbName_));

		jobConf.setMapOutputKeyClass(NullWritable.class);
		jobConf.setMapOutputValueClass(NullWritable.class);

		return jobConf;
	}

	/**
	 * Method to set the configuration of the MapReduce Job
	 * 
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public JobConf createJob1(String[] args) throws Exception {
		JobConf jobConf = new JobConf(super.getConf(), this.getClass());

		jobConf.setJarByClass(this.getClass());
		jobConf.setJobName("EdgeCreation_" + dbName_);

		jobConf.setInputFormat(TextInputFormat.class);

		Path preferenceInputPath = new Path(edgeInputPath_);

		System.out.println("Input Path on HDFS: " + edgeInputPath_);

		jobConf.setNumReduceTasks(0);

		jobConf.set("mapred.job.priority", JobPriority.VERY_HIGH.toString());

		jobConf.set("DBNAME", dbName_);
		jobConf.set("HOST", host_);

		FileInputFormat.setInputPaths(jobConf, preferenceInputPath);

		FileSystem.get(jobConf).delete(new Path("/tmp/edgeCreation" + dbName_),
				true);
		FileOutputFormat.setOutputPath(jobConf, new Path("/tmp/edgeCreation"
				+ dbName_));

		jobConf.setMapperClass(EdgeLoader.class);

		jobConf.setMapOutputKeyClass(NullWritable.class);
		jobConf.setMapOutputValueClass(NullWritable.class);

		return jobConf;
	}

	public static final class VertexLoader extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, NullWritable> {

		// private IntWritable one = new IntWritable(1);

		String dbName_;
		String host_;
		// int bufferSize = 10000;

		TitanGraph g;

		public static enum VERTEX_COUNTER {
			VertexCounter, UserCounter, ItemCounter, DUPLICATEVERTEX
		};

		public void configure(JobConf jobConf) {

			dbName_ = jobConf.get("DBNAME");
			host_ = jobConf.get("HOST");

			System.out.println("DBNAME is: " + dbName_);
			System.out.println("Host is: " + host_);

			// org.apache.commons.configuration.Configuration titanConf = new
			// BaseConfiguration();
			//
			// titanConf.setProperty("storage.backend","hbase");
			// titanConf.setProperty("storage.tablename", dbName_);
			// titanConf.setProperty("storage.hostname",host_);
			// titanConf.setProperty("storage.batch-loading",true);
			// titanConf.setProperty("storage.buffer-size", bufferSize);

			org.apache.commons.configuration.Configuration titanConf = new BaseConfiguration();

			titanConf.setProperty("storage.backend", "hbase");
			titanConf.setProperty("storage.tablename", dbName_);
			titanConf.setProperty("storage.hostname", host_);

			titanConf.setProperty("storage.batch-loading", true);
			titanConf.setProperty("ids.block-size", 1500000);
			titanConf.setProperty("persist-wait-time", 300000);
			titanConf.setProperty("persist-attempts", 100);
			titanConf.setProperty("storage.idauthority-block-size", 1500000);
			titanConf.setProperty("storage.lock-wait-time", 30000);
			titanConf.setProperty("storage.idauthority-wait-time", 30000);
			titanConf.setProperty("storage.idauthority-retries", 10);
			titanConf.setProperty("ids.renew-timeout", 2000000);
			titanConf.setProperty("ids.flush", true);

			g = TitanFactory.open(titanConf);

		}

		/**
		 * <pre>
		 * Input -
		 *    key - LongWritable
		 *    value - userID/itemID
		 *    
		 * Output - 
		 *    key - NullWritable
		 *    value - NullWritable
		 * </pre>
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, NullWritable> collector,
				Reporter reporter) throws IOException {

			String id = value.toString();

			// System.out.println("Key: " + value.toString());

			try {
				Iterator<Vertex> it = g.getVertices("MyID", id).iterator();

				if (!it.hasNext()) {
					String typeString = id.substring(id.lastIndexOf("_") + 1);
					// System.out.println("Creating new Vertex: " + id + "\t" +
					// typeString);
					Vertex v = g.addVertex(id);
					v.setProperty("MyID", id);
					v.setProperty("type", typeString);

					reporter.getCounter(VERTEX_COUNTER.VertexCounter)
							.increment(1);

					if (typeString.equalsIgnoreCase("user")) {
						reporter.getCounter(VERTEX_COUNTER.UserCounter)
								.increment(1);
					} else {
						reporter.getCounter(VERTEX_COUNTER.ItemCounter)
								.increment(1);
					}

				} else {
					// System.err.println("The id: " + id
					// +" is already added to the graph");
					reporter.getCounter(VERTEX_COUNTER.DUPLICATEVERTEX)
							.increment(1);

					// System.exit(-1);
				}

				if (reporter.getCounter(VERTEX_COUNTER.VertexCounter)
						.getValue() % 5000 == 0) {

					// System.out.println("Committing: " +
					// reporter.getCounter(VERTEX_COUNTER.VertexCounter).getValue());
					g.commit();
				}

			} catch (Exception e) {
				g.rollback();
				e.printStackTrace();
				System.exit(1);
			}
		}

		public void close() {
			g.commit();
			g.shutdown();
		}

	}

	public static final class EdgeLoader extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, NullWritable> {

		// private IntWritable one = new IntWritable(1);

		String dbName_;
		String host_;
		// int bufferSize = 10000;
		public static final Pattern DELIMITER = Pattern.compile("[\t,]");
		TitanGraph g;

		public static enum EDGE_COUNTER {
			EdgeCounter
		};

		public void configure(JobConf jobConf) {

			dbName_ = jobConf.get("DBNAME");
			host_ = jobConf.get("HOST");

			System.out.println("DBNAME is: " + dbName_);
			System.out.println("Host is: " + host_);

			// org.apache.commons.configuration.Configuration titanConf = new
			// BaseConfiguration();
			//
			// titanConf.setProperty("storage.backend","hbase");
			// titanConf.setProperty("storage.tablename", dbName_);
			// titanConf.setProperty("storage.hostname",host_);
			// titanConf.setProperty("storage.batch-loading",true);
			// titanConf.setProperty("storage.buffer-size", bufferSize);

			org.apache.commons.configuration.Configuration titanConf = new BaseConfiguration();

			titanConf.setProperty("storage.backend", "hbase");
			titanConf.setProperty("storage.tablename", dbName_);
			titanConf.setProperty("storage.hostname", host_);

			titanConf.setProperty("storage.batch-loading", true);
			titanConf.setProperty("ids.block-size", 1500000);
			titanConf.setProperty("persist-wait-time", 300000);
			titanConf.setProperty("persist-attempts", 100);
			titanConf.setProperty("storage.idauthority-block-size", 1500000);
			titanConf.setProperty("storage.lock-wait-time", 30000);
			titanConf.setProperty("storage.idauthority-wait-time", 30000);
			titanConf.setProperty("storage.idauthority-retries", 10);
			titanConf.setProperty("ids.renew-timeout", 2000000);
			titanConf.setProperty("ids.flush", true);

			g = TitanFactory.open(titanConf);

		}

		/**
		 * <pre>
		 * Input -
		 *    key - LongWritable
		 *    value - userID,itemID,clickCount
		 *    
		 * Output - 
		 *    key - NullWritable
		 *    value - NullWritable
		 * </pre>
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, NullWritable> collector,
				Reporter reporter) throws IOException {

			String[] temp = DELIMITER.split(value.toString().trim());

			String userID = temp[0].trim() + "_user";
			String itemID = temp[1].trim() + "_item";
			double labelDist_inj = Double.parseDouble(temp[2]);

			// System.out.println("The userID is: " + userID);
			// System.out.println("The itemID is: " + itemID);

			try {
				Vertex user = g.getVertices("MyID", userID).iterator().next();
				Vertex item = g.getVertices("MyID", itemID).iterator().next();

				/* Adding the edge between the vertices */

				Edge edge = g.addEdge(null, user, item, "click_injLabel");
				edge.setProperty("labelDist_inj", labelDist_inj);

				reporter.getCounter(EDGE_COUNTER.EdgeCounter).increment(1);

				if (reporter.getCounter(EDGE_COUNTER.EdgeCounter).getValue() % 100000 == 0) {

					System.out.println("Committing: "
							+ reporter.getCounter(EDGE_COUNTER.EdgeCounter)
									.getValue());
					g.commit();
				}

			} catch (Exception e) {
				System.out.println("Line is: " + value.toString());
				System.out.println("Values are: " + userID + " : " + itemID);
				g.rollback();
				e.printStackTrace();
				System.exit(1);
			}
		}

		public void close() {
			g.commit();
			g.shutdown();
		}

	}

	public static void printVertices() {
		TitanGraph g = null;
		try {
			org.apache.commons.configuration.Configuration titanConf = new BaseConfiguration();
			titanConf.setProperty("storage.backend", "hbase");
			titanConf.setProperty("storage.tablename", dbName_);
			titanConf.setProperty("storage.hostname", host_);

			g = TitanFactory.open(titanConf);

			/* Printing the newly added vertex */
			Iterator<Vertex> it = g.getVertices().iterator();

			while (it.hasNext()) {
				Vertex v1 = it.next();
				System.out.println("type: " + v1.getProperty("type"));
				System.out.println("myID: " + v1.getProperty("MyID"));
			}

			g.commit();
			g.shutdown();
		} catch (Exception e) {
			g.shutdown();

			e.printStackTrace();
		}

	}

	public static void createGraph() throws IOException {
		TitanGraph g = null;
		try {
			org.apache.commons.configuration.Configuration titanConf = new BaseConfiguration();

			/* Set the properties for this configuration */
			int bufferSize = 10000;
			titanConf.setProperty("storage.backend", "hbase");
			titanConf.setProperty("storage.tablename", dbName_);
			titanConf.setProperty("storage.hostname", host_);

			titanConf.setProperty("storage.batch-loading", true);
			titanConf.setProperty("ids.block-size", 1500000);
			titanConf.setProperty("persist-wait-time", 300000);
			titanConf.setProperty("persist-attempts", 100);
			titanConf.setProperty("storage.idauthority-block-size", 1500000);
			titanConf.setProperty("storage.buffer-size", bufferSize);
			titanConf.setProperty("storage.lock-wait-time", 30000);
			titanConf.setProperty("storage.idauthority-wait-time", 30000);
			titanConf.setProperty("storage.idauthority-retries", 10);
			titanConf.setProperty("ids.renew-timeout", 2000000);
			titanConf.setProperty("ids.flush", true);

			g = TitanFactory.open(titanConf);

			/*
			 * Set the Vertex Properties. 1. MyID - Primary - long 2. Type -
			 * String 3. distSum - double
			 */
			g.makeType().name("MyID").dataType(String.class).indexed(Vertex.class).unique(Direction.BOTH).makePropertyKey();
			g.makeType().name("type").dataType(String.class).unique(Direction.OUT).makePropertyKey();
//			g.makeType().name("distSum").dataType(.class).unique(Direction.OUT).makePropertyKey();
			
			
			/*Set the edge labels and properties*/
			g.makeType().name("labelDist_inj").dataType(Double.class).unique(Direction.OUT).makePropertyKey();
			g.makeType().name("labelDist_est").dataType(Double.class).unique(Direction.OUT).makePropertyKey();
			g.makeType().name("click_injLabel").makeEdgeLabel();
			g.makeType().name("click_estLabel").makeEdgeLabel();

			/* Adding Dummy Vertex */

			String dummyID = "__DUMMY__";
			Vertex v = g.addVertex(dummyID);
			v.setProperty("type", "dummy");
			v.setProperty("MyID", dummyID);

			/* Printing the newly added vertex */
			Vertex v1 = g.getVertices("MyID", dummyID).iterator().next();
			System.out.println("type: " + v1.getProperty("type"));
			System.out.println("myID: " + v1.getProperty("MyID"));

			g.commit();
			g.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
			g.shutdown();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.out
					.println("USAGE: \n"
							+ "\t\t The first argument should be the path to user set\n"
							+ "\t\t The second argument should be the item set path\n"
							+ "\t\t The third argument should be the path to the edges file\n"
							+ "\t\t The forth argument should be the name of the database\n"
							+ "\t\t The fifth argument is the host: 'local' for localhost\n");
			System.exit(0);
		}
		Configuration conf = new Configuration();
		ToolRunner.run(conf, new Create_Graph_Parallel(), args);
	}

}
