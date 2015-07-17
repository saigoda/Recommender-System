package heterogeneous.brainStorm;

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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ComputeFlowOfAPair extends Configured implements Tool {

    /**
     * @param args
     */

    public static class ComputeFlowOfAPairMap extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        private static FileSystem fs;
        private static int _typeOfNetwork;// Store the type of network for
                                          // which we are computing flow
        // Stores the conditional probability

        // Conditional probability file path
        private String _condidtionalProbabilityFilePath;
        private static HashMap<String, Double> _conditionalProbability = new HashMap<String, Double>();

        private static double _beta;// Store the beta value which is 0.05 in
                                    // this variable
        // Used to store score of a node which is S(v) (which is computed and
        // stored in a file)
        private static HashMap<String, Double> scores = new HashMap<String, Double>();// store

        private HashMap<String, Integer> degrees = new HashMap<String, Integer>();

        private List<String> networkType = new ArrayList<String>();

        @Override
        public void configure(JobConf job) {
            try {
                // Count stores the number of different networks
                int count = Integer.parseInt(job.get("count"));
                fs = FileSystem.get(job);
                // Stores the index of the network for which we compute flow
                _typeOfNetwork = Integer.parseInt(job.get("typeOfNetwork")) - 1;
                // Store the different network types
                for (int i = 0; i < count; i++) {
                    networkType.add(job.get("type" + (i + 1)));
                }
                BufferedReader br = null;
                _condidtionalProbabilityFilePath = job
                        .get("condiditonalProbability");
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(
                        _condidtionalProbabilityFilePath))));
                String st = "";
                // Store the conditional probability
                while ((st = br.readLine()) != null) {
                    String[] splits = st.split("\\t");
                    _conditionalProbability.put(splits[0],
                            Double.parseDouble(splits[1]));
                }
                br.close();
                // Store the score in the hash map

                _beta = Double.parseDouble(job.get("beta"));
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(
                        job.get("score")))));
                while ((st = br.readLine()) != null) {
                    // read each line in the score file and put it in a hashmap
                    String[] splits = st.trim().split("\\t");
                    scores.put(splits[0], Double.parseDouble(splits[1]));
                }
                System.out.println(scores);
                br.close();

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
         * Key:<User:Degree1:Degree2>
         * 
         * Value--<user1:degree1'
         * :commonneighbors1':degree1:commonneighbors1>,<user3:degree3':
         * commonneighbors3':degree3:commonneighbors3
         * >,<user2:degree2':commonneighbors2 ':degree2:commonneighbors2>
         * 
         * Output:
         * 
         * Key:<User:Degree1:Degree2>
         * 
         * Value--<user1:degree1' :commonneighbors1
         * ':degree1:commonneighbors1:flow(User,user1,networkI)>,<user3:degree3':
         * commonneighbors3':degree3:commonneighbors3:flow(User,user3,networkI)
         * >,<user2:degree2':commonneighbors2
         * ':degree2:commonneighbors2:flow(User,user2,networkI)>
         */
        public void map(LongWritable key, Text value,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            // TODO Auto-generated method stub
            String text = value.toString().trim();

            String[] splits = text.split("\\t");
            String[] node = splits[1].split("\\|");
            String[] nodeIdSplits = splits[0].split(":");
            int i = 1;
            System.out.println("Splits[0] is:" + splits[0]);
            System.out.println("Node id splits:" + nodeIdSplits[0]);
            System.out.println(scores);
            System.out.println(_conditionalProbability);
            for (String network : networkType) {
                degrees.put(network, Integer.parseInt(nodeIdSplits[i]));
                i++;
            }
            // Store the network name for the network we are computing flow.
            String flowType = networkType.get(_typeOfNetwork);
            // Get the score of the source.
            double scoreOfSource = scores.get(nodeIdSplits[0]);

            StringBuilder sb = new StringBuilder();
            for (String s : node[0].split(",")) {
                String[] splits1 = s.split(":");
                // Used to store the distribution for each network which is
                // later used to compute the flow
                HashMap<String, Double> weights = new HashMap<String, Double>();
                int temp = 1;
                int edgeTypes = 0;
                for (String network : networkType) {
                    double weight = 0;
                    for (int j = 0; j < 2; j++) {
                        temp += j;
                        if (j == 1) {
                            weight = Double.parseDouble(splits1[temp]);
                            if (Double.parseDouble(splits1[temp]) != 0
                                    && !network.equals(flowType))
                                edgeTypes++;// used to store the egde types
                                            // for a given node. If for
                                            // example an edge is
                                            // u:2:0.3:4:0.5 there are two
                                            // edges from this node to
                                            // source. if the edge type is
                                            // u:0:0:0.3:0.5 then it has one
                                            // edge type
                            temp++;
                        }
                    }
                    weights.put(network, weight);
                }
                System.out.println("Weights of " + splits1[0] + ":" + weights);
                // Source influence is calculated as
                // S(v)*beta*weightDistributionOfTheSourceNetwork
                double scoreOfvBeta = scoreOfSource * _beta;
                double sourceInfluence = scoreOfvBeta * weights.get(flowType);
                double otherNeighborsInfluence = 0;
                int sourceNetworkIndex = _typeOfNetwork + 1;
                for (String network : networkType) {
                    int networkIndex = networkType.indexOf(network) + 1;
                    if (!flowType.equals(network)) {
                        String conditionalProbKey = sourceNetworkIndex + "_"
                                + networkIndex;
                        // Calculating Sigma( conditional Prob *
                        // weight_distribution for all other combinations
                        // if there are three networks and source index is 1
                        // we calculate conditional Prob(1_2)*weightof(u)
                        // +conditional Prob(1_3)*weight(u)..
                        otherNeighborsInfluence += (_conditionalProbability
                                .get(conditionalProbKey))
                                * (weights.get(network));
                    }
                }

                double totalInfluence = 0;
                if (edgeTypes > 1) {
                    // If there are more than one edge types divide by the
                    // edge types
                    otherNeighborsInfluence = scoreOfvBeta
                            * (otherNeighborsInfluence / edgeTypes);
                    totalInfluence = sourceInfluence + otherNeighborsInfluence;
                } else {
                    totalInfluence = sourceInfluence;
                }
                s += (":" + totalInfluence);
                sb.append(s + ",");
            }
            output.collect(new Text(splits[0]), new Text(sb.toString()));

        }
    }

    @Deprecated
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
            System.out.println("Third is the path to score file");
            System.out.println("Fourth is the path to the probability file");
            System.out
                    .println("From fifth to length-3 you should specify the newtwork names");
            System.out.println("THIRD FROM LAST is the beta value(0.05");
            System.out
                    .println("SECOND FORM LAST is the count of number of networks");
            System.out
                    .println("Last is the type of network for which flow is computed");
            return 1;
        } else {
            JobConf job = new JobConf(super.getConf(), this.getClass());
            job.setJarByClass(this.getClass());
            System.out.println("Score path is:" + args[2]);
            job.set("score", args[2]);
            job.set("condiditonalProbability", args[3]);
            System.out.println("Conditional prob" + args[3]);

            for (int i = 4, k = 1; i < 4 + Integer
                    .parseInt(args[args.length - 3]); i++, k++) {
                job.set("type" + k, args[i]);
            }
            job.set("beta", args[args.length - 4]);
            System.out.println("Beta:" + job.get("beta"));
            job.set("count", args[args.length - 3]);
            job.set("typeOfNetwork", args[args.length - 2]);
            job.setJobName("Compute flow of (u,v) of link type "
                    + job.get("type" + args[args.length - 2]));
            job.setJobPriority(JobPriority.VERY_HIGH);
            job.setMapperClass(ComputeFlowOfAPairMap.class);
            job.setReducerClass(IdentityReducer.class);
            // job.setNumMapTasks(50);
            job.setNumReduceTasks(Integer.parseInt(args[args.length - 1]));
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
