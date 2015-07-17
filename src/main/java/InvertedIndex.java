import java.io.IOException;
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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {

    /**
     * @param args
     */
    public static class InvertedIndexMap extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String text = value.toString();
            // Stores the word count of words in the current document
            HashMap<String, Integer> docCount = new HashMap<String, Integer>();
            try {
                String[] docWords = text.split("\\t");
                String docName = docWords[0];// The first split contains the doc
                                             // name
                for (String word : docWords[1].split("\\s")) {
                    if (docCount.containsKey(word)) {
                        int docWordCount = docCount.get(word) + 1;// increment
                                                                  // the
                                                                  // document
                                                                  // word count
                                                                  // by 1
                        docCount.put(word, docWordCount);
                    } else {
                        docCount.put(word, 1);
                    }
                }
                Iterator<String> keys = docCount.keySet().iterator();
                while (keys.hasNext()) {
                    String word = keys.next();
                    // Output key word
                    // Output value docName,wordCountInDocument
                    output.collect(new Text(word), new Text(docName + ","
                            + docCount.get(word)));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 
     * @author srikar
     * 
     *         Output Key: word,document_frequency
     * 
     *         Output Value:documentName1,wordCountInDoc1||documentName2,
     *         wordCountInDoc2....
     *
     */
    public static class InvertedIndexReduce extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            // Stores the document frequency
            int count = 0;
            StringBuilder sb = new StringBuilder();
            while (values.hasNext()) {
                String valueText = values.next().toString();
                sb.append(valueText + "|");
                count++;
            }
            output.collect(new Text(key.toString() + "," + count),
                    new Text(sb.toString()));
        }

    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new InvertedIndex(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @SuppressWarnings("deprecation")
    public int run(String[] args) throws Exception {
        JobConf job = new JobConf(super.getConf(), this.getClass());
        job.setJarByClass(this.getClass());
        job.setJobName("Compute inverted index");
        job.setMapperClass(InvertedIndexMap.class);
        job.setReducerClass(InvertedIndexReduce.class);
        job.setNumReduceTasks(10);
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
