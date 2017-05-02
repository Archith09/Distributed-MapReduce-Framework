package edu.upenn.cis455.mapreduce.master;

import java.util.*;

public class ThreadClass {

	public ArrayList<String> channels = new ArrayList<String>();
	public HashSet<String> finalCals = new HashSet<String>();
	public String identification = "";
	public String address;
	public String task = "";
	public int input = 0;
	public int output = 0;
	public String position = "";

	public void updateAddress(String s){
		address = s;
	}

	public String returnAddress(){
		return address;
	}

	public void updateTask(String s){
		task = s;
	}

	public String returnTask(){
		return task;
	}

	public void updatePosition(String s){
		position = s;
	}

	public String returnPosition(){
		return position;
	}

	public void updateInput(int i){
		input = i;
	}

	public int returnInput(){
		return input;
	}

	public void updateOutput(int i){
		output = i;
	}

	public int returnOutput(){
		return output;
	}

	public void updateFinalCals(String s){
		finalCals.add(s);
	}

	public HashSet<String> returnFinalCals(){
		return finalCals;
	}

	public void emptyHash(){
		finalCals.clear();
	}
}
