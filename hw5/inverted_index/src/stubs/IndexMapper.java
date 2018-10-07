package stubs;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
	  
	  FileSplit fileSplit = (FileSplit)context.getInputSplit();
	  String filename = fileSplit.getPath().getName();

	  String line = value.toString();
	  
	  String lineNum = line.split("\\W+")[0];
	  
	  String lineContext = line.substring(lineNum.length());
	 
	  
	  
	  for (String word : lineContext.split("\\W+")) {
	      if (word.length() > 0) {
	    	  word = word.toLowerCase();
	    	  context.write(new Text(word), new Text(filename+"@"+lineNum));
	      }
	  }
    
  }
}