package edu.upenn.cis.stormlite.bolt;

import com.sleepycat.persist.*;

public class Service {
	PrimaryIndex<String, Results> results;

	public Service(EntityStore es){
		results = es.getPrimaryIndex(String.class, Results.class);
	}

	public PrimaryIndex<String, Results> returnWords(){
		return results;
	}

}
