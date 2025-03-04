package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

		    int rateSum = 0;
		    
			for (IntWritable value : values) {
				rateSum += value.get();
			}
			
			context.write(key, new IntWritable(rateSum));

	}
}
