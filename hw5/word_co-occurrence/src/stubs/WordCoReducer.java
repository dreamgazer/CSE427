package stubs;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCoReducer extends Reducer<NameWritable, IntWritable, Text, IntWritable> {

	@Override
	  public void reduce(NameWritable key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
		  
		  	int nameNum=0;
			for (IntWritable value : values) {
				nameNum += value.get();
			}
			context.write(new Text("("+key.firstName+","+key.lastName+")"), new IntWritable(nameNum));
	
	  }
	

}
