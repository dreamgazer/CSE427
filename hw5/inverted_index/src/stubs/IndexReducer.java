package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  String locations="";
	  
	  for (Text value : values) {
		  
		  locations += value.toString();
		  
		  locations += ",";
		}
	  
	  if(locations.length()>0) locations = locations.substring(0, locations.length() - 1); // remove the last comma
	  	
	  context.write(key, new Text(locations));
	  	
    
  }
}