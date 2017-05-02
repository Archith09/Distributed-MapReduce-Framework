package edu.upenn.cis455.mapreduce.job;

import java.util.*;
import edu.upenn.cis455.mapreduce.*;

public class MyContext implements Context {

	public HashMap<String, ArrayList<String>> content = new HashMap<String, ArrayList<String>>();
	public void write(String k, String v){
		if(content.containsKey(k)){
			ArrayList<String> arrVals = content.get(k);
			arrVals.add(v);
			content.put(k, arrVals);
		} else{
			ArrayList<String> arrVals = new ArrayList<String>();
			arrVals.add(v);
			content.put(k, arrVals);
		}
	}
}
