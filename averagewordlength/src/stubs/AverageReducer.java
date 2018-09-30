package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  
	  double totalLength=0;
	  double wordNum=0;
		for (IntWritable value : values) {
			totalLength += value.get();
			wordNum++;
		}
		double averageLength= (double) Math.round(totalLength /wordNum*10)/10;
		context.write(key, new DoubleWritable(averageLength));

  }
}