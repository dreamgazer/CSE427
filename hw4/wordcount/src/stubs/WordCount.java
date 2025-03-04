package stubs;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* 
 * MapReduce jobs are typically implemented by using a driver class.
 * The purpose of a driver class is to set up the configuration for the
 * MapReduce job and to run the job.
 * Typical requirements for a driver class include configuring the input
 * and output data formats, configuring the map and reduce classes,
 * and specifying intermediate data formats.
 * 
 * The following is the code for the driver class:
 */
public class WordCount extends Configured implements Tool{

	  public static void main(String[] args) throws Exception {
		  
		  (new SentimentPartitionTest()).testSentimentPartition();// test the SentimentPartitioner
		  
		  
		  int res = ToolRunner.run(new Configuration(), new WordCount(), args);
	      System.exit(res);

	  }

	@Override
	public int run(String[] args) throws Exception {
		 Configuration conf = getConf();

		    /*
		     * Validate that two arguments were passed from the command line.
		     */
		    if (args.length != 2) {
		      System.out.printf("Usage: WordCount <input dir> <output dir>\n");
		      System.exit(-1);
		    }

		    /*
		     * Instantiate a Job object for your job's configuration. 
		     */
		    Job job = new Job(conf);
		    
		    /*
		     * Specify the jar file that contains your driver, mapper, and reducer.
		     * Hadoop will transfer this jar file to nodes in your cluster running 
		     * mapper and reducer tasks.
		     */
		    job.setJarByClass(WordCount.class);
		    
		    /*
		     * Specify an easily-decipherable name for the job.
		     * This job name will appear in reports and logs.
		     */
		    job.setJobName("WordCount");
		    FileInputFormat.setInputPaths(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    /*
		     * Specify the mapper and reducer classes.
		     */
		    job.setMapperClass(WordMapper.class);
		    job.setReducerClass(SumReducer.class);
		    job.setPartitionerClass(SentimentPartitioner.class);//add the Partitioner Class to the job
		    job.setNumReduceTasks(3);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    /*
		     * Start the MapReduce job and wait for it to finish.
		     * If it finishes successfully, return 0. If not, return 1.
		     */
		    boolean success = job.waitForCompletion(true);
		    System.exit(success ? 0 : 1);
	   	return 0;
	}
  
  
 
}
