package stubs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Boolean isCaseSensitive;
	
	
  public void setup(Context context){
	  Configuration conf = context.getConfiguration();

	  String caseSensitive = conf.get("caseSensitive");
	  //"-D caseSensitive=true"
	  if(caseSensitive=="true"){
		  isCaseSensitive=true;
	  }
	  else{
		  isCaseSensitive=false;
	  }
	//determine whether the Mapper class should treat upper and lower case letters as different 
  }
  @Override
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
	 	    context.write(new Text(firstCharacter), new IntWritable(Len));
	        
	       //  
	      }
}
  }
}