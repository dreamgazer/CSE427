package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

  
public class TopNListReducer extends Reducer<NullWritable, Text, IntWritable, Text> {

	private TreeMap<Integer, Integer> topNItems;
	private Map<Integer, String> filmMap = getFilmMap("movie_titles.txt");
	private Configuration conf;
	private int N;
	public void setup(Context context){
		topNItems = new TreeMap<Integer, Integer>(Collections.reverseOrder());
		conf = context.getConfiguration();
		N = Integer.parseInt(conf.get("N"));
	}

  @Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
	  for(Text value: values){
		  String line = value.toString();
		  	int film_id = Integer.parseInt(line.split("#")[1]);
		  	int rate = Integer.parseInt(line.split("#")[0]);
		  	topNItems.put(rate, film_id);
	  }
	  	
	}
  
  public void cleanup(Context context) throws IOException, InterruptedException{
		int itemNum = 0;
		Iterator it = topNItems.entrySet().iterator();
	    while (itemNum < N && it.hasNext()) {
	    	itemNum++;
	        Map.Entry<Integer,Integer> item = (Map.Entry<Integer,Integer>)it.next();
	        context.write(new IntWritable(item.getKey()), new Text(filmMap.get(item.getValue())));
	        it.remove(); 
	    }
	    
	}
  
  private Map<Integer, String> getFilmMap(String filename){
		Map<Integer, String> filmMap = new HashMap<Integer, String>();
		
		 File file = new File(filename);  
	      BufferedReader reader = null;  
	      try { 
	          reader = new BufferedReader(new FileReader(file));  
	          String line = null;  
	          while ((line = reader.readLine()) != null) {  
	        	 String film_id = line.split(",")[0];
	        	 String film_name = line.split(",")[2];
	        	 filmMap.put(Integer.parseInt(film_id), film_name);
	          }  
	          reader.close();  
	      } catch (IOException e) {  
	          e.printStackTrace();  
	      } finally {  
	          if (reader != null) {  
	              try {  
	                  reader.close();  
	              } catch (IOException e1) {  
	              }  
	          }  
	      }  
		
		
		return filmMap;
	}
}