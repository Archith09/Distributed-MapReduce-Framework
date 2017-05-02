package edu.upenn.cis.stormlite.bolt;

import java.util.*;
import com.sleepycat.persist.model.*;

@Entity
public class Results {
	public ArrayList<String> resultsArr = new ArrayList<String>();
	@PrimaryKey
	public String name = "";

	public void updateWord(String s){
		name = s;
	}

	public void updateList(String s){
		resultsArr.add(s);
	}

	public ArrayList<String> returnTask(){
		return resultsArr;
	}

	public String returnWord(){
		return name;
	}
}
