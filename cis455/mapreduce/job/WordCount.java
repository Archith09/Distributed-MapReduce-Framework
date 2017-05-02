package edu.upenn.cis455.mapreduce.job;

import java.util.Iterator;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
    // Your map function for WordCount goes here
    String noOfOccurences = "1";
  	context.write(value.toLowerCase(), noOfOccurences);
  }
  
  public void reduce(String key, Iterator<String> values, Context context)
  {
    // Your reduce function for WordCount goes here
  	int count = 0;
  	while(values.hasNext()){
  		count = count + Integer.parseInt(values.next());
  	}
  	context.write(key, String.valueOf(count));
  }
  
}
