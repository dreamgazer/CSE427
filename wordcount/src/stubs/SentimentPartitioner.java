package stubs;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();


  @Override
  public void setConf(Configuration configuration) {
	  
	  readWordFile("positive-words.txt",positive);
	  readWordFile("negative-words.txt",negative);
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You need to implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the sentiment 
     * category; use 0 for positive words, 1 for negative words, and 2 for neutral words. 
     * Use the sets of positive and negative words to find out the sentiment of each word.
     *
     * Hint: use positive.contains(key.toString()) and negative.contains(key.toString())
     * If a word appears in both lists assume it is positive. That is, once you found 
     * that a word is in the positive list you do not need to check if it is in the 
     * negative list. 
     */
	  
	 if(positive.contains(key.toString())) return 0;
	 if(negative.contains(key.toString())) return 1;
	 return 2;
  }

 public static void readWordFile(String fileName,Set<String> set) {  
      File file = new File(fileName);  
      BufferedReader reader = null;  
      try { 
          reader = new BufferedReader(new FileReader(file));  
          String tempString = null;  
          while ((tempString = reader.readLine()) != null) {  
        	  if(tempString.charAt(0)==';') continue;
        	  set.add(tempString);
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
  }  
  
  
  
  
}