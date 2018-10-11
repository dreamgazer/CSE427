package stubs;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNListMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private TreeMap<Integer, Integer> topNItems;
	private Configuration conf;
	private int N;
	public void setup(Context context){
		topNItems = new TreeMap<Integer, Integer>(Collections.reverseOrder());
		conf = context.getConfiguration();
		N = Integer.parseInt(conf.get("N"));
	}
	
	public void map(LongWritable key, Text value, Context context){
		String line = value.toString();
		System.out.println("line:"+line);
	  	int  rate = Integer.parseInt(line.split("\\s+")[1]);
	  	int film_id = Integer.parseInt(line.split("\\s+")[0]);
	  	topNItems.put(rate, film_id);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		int itemNum = 0;
		Iterator it = topNItems.entrySet().iterator();
	    while (itemNum < N && it.hasNext()) {
	    	itemNum++;
	        Map.Entry<Integer,Integer> item = (Map.Entry<Integer,Integer>)it.next();
	        context.write(NullWritable.get(), new Text(item.getKey()+"#"+item.getValue()));
	        it.remove(); 
	    }// only emit the top N greatest rate sum
	    
	}
	
}
