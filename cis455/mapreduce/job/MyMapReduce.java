package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.master.*;
import edu.upenn.cis455.mapreduce.worker.*;

public class MyMapReduce {

	@SuppressWarnings("unused")
	public static void main(String[] args){

		try{
			if(args.length == 0){
				MasterServlet ms = new MasterServlet();
			} else if(args.length == 3){
				WorkerServer ws = new WorkerServer(args[0], args[1], Integer.parseInt(args[2]));
			} 
		} catch(Exception e){
			// System.out.println("ERROR: " + e);
		}
	}
}
