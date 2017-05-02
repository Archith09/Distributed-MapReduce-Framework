package edu.upenn.cis.stormlite.bolt;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Iterator;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis.stormlite.TopologyContext.STATE;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.mapreduce.job.*;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class ReduceBolt implements IRichBolt {
	MyContext myContext  = new MyContext();
    WordCount wc = new WordCount();
    String pathToDirectory = "";
    DBWrapper database;

    static Logger log = Logger.getLogger(ReduceBolt.class);

	
	Job reduceJob;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	boolean sentEof = false;
	
	/**
	 * Buffer for state, by key
	 */
	Map<String, List<String>> stateByKey = new HashMap<>();

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context;
    
    int neededVotesToComplete = 0;
    
    public ReduceBolt() {
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        if (!stormConf.containsKey("reduceClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("reduceClass");
        	
        	try {
				reduceJob = (Job)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

        // TODO: determine how many EOS votes needed
        String s1 = "noOfWorkers";
        String s2 = "mapExecutors";
        String s3 = "reduceExecutors";
        String s4 = "dbDir";
        int count = Integer.parseInt(stormConf.get(s1));
        neededVotesToComplete = (count - 1) * Integer.parseInt(stormConf.get(s2)) * Integer.parseInt(stormConf.get(s3)) + Integer.parseInt(stormConf.get(s2));
        pathToDirectory = stormConf.get(s4);
    }

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @SuppressWarnings("static-access")
	@Override
    public synchronized void execute(Tuple input) {
    	String slash = "/";
        boolean yes = true;
        StringBuilder stringBuilder = new StringBuilder(pathToDirectory);
        stringBuilder.append(slash);
        stringBuilder.append(executorId);
        File name = new File(stringBuilder.toString());
        if(!name.exists()){
            name.mkdir();
            name.setReadable(yes);
            name.setWritable(yes);
        }
        this.database = new DBWrapper(name);

        if (sentEof) {
	        if (!input.isEndOfStream())
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
    		// Already done!
		} else if (input.isEndOfStream()) {
			
			// TODO: only if at EOS do we trigger the reduce operation and output all state
            neededVotesToComplete--;
            if(neededVotesToComplete == 0)
			    sentEof = true;
    	} else {
    		// TODO: this is a plain ol' hash map, replace it with BerkeleyDB
    		
    		String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        log.debug(getExecutorId() + " received " + key + " / " + value);
	        
	        // synchronized (stateByKey) {
		       //  if (!stateByKey.containsKey(key))
		       //  	stateByKey.put(key, new ArrayList<>());
		       //  else
		       //  	log.debug("Adding item to " + key + " / " + stateByKey.get(key).size());
		       //  stateByKey.get(key).add(value);
	        // }

            if(this.database.service.returnWords().contains(key)){
                Results results = this.database.service.returnWords().get(key);
                results.updateList(value);
                this.database.service.returnWords().put(results);
            } else{
                Results results = new Results();
                results.updateWord(key);
                results.updateList(value);
                this.database.service.returnWords().put(results);
            }
    	}

        if(sentEof){
            context.setState(STATE.REDUCE);
            ArrayList<Results> resultsArr = this.database.returnWordsDatabase();
            for(Results r : resultsArr){
                Iterator<String> iterator = r.returnTask().iterator();
                wc.reduce(r.returnWord(), iterator, myContext);
                context.incReduceOutputs(r.returnWord());
            }
            int index = 1;
            for(String k : myContext.content.keySet()){
                context.setOutcome(k, String.valueOf(myContext.content.get(k)).substring(index, String.valueOf(myContext.content.get(k)).length() - index));
                this.collector.emit(new Values<Object>(k, String.valueOf(myContext.content.get(k))));
            }
            context.setState(STATE.WAITING);
            this.database.removeIndividuals();

            // Delete the directory
            removeDirectoryContents(name);
        }
    }

    public void removeDirectoryContents(File name){
        File[] data = name.listFiles();
        if(data != null){
            for(File i : data)
                removeDirectoryContents(i);
        }
        name.delete();
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
}
