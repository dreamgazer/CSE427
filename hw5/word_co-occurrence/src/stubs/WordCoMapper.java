package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class WordCoMapper extends Mapper<LongWritable, Text, NameWritable, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  String firstName = line.split(" ")[0];
	  String lastName = line.split(" ")[1];
	  
	  NameWritable nameKey = new NameWritable(firstName,lastName);
	  context.write(nameKey, new IntWritable(1));
	  /*
     public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  	
	  	String line = value.toString();
	  	
	    for (String word : line.split("\\W+")) {
	      if (word.length() > 0&&Character.isLetter(word.charAt(0))) {
	        String firstCharacter=word.substring(0, 1);
	        int Len=word.length();
	        if(!isCaseSensitive){
	        	firstCharacter=firstCharacter.toLowerCase();
	        }
	 	    
	        
	       //  
	      }
}
     */
    
  }
}
