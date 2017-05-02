package edu.upenn.cis.stormlite.bolt;

import java.io.*;
import java.util.*;
import com.sleepycat.je.*;
import com.sleepycat.persist.*;

public class DBWrapper {
	File name;
	public static Service service;
	public static EntityStore es;
	public static Environment databaseEnv = null;
	public static boolean yes = true;

	public DBWrapper(File name){
		this.name = name;
		DBSetup();
	}

	public void DBSetup(){
		try{
			StoreConfig sc = new StoreConfig();
			sc.setAllowCreate(yes);
			sc.setTransactional(yes);
			EnvironmentConfig ec = new EnvironmentConfig();
			ec.setAllowCreate(yes);
			ec.setTransactional(yes);

			databaseEnv = new Environment(name, ec);
			es = new EntityStore(databaseEnv, "EntityStore", sc);
			service = new Service(es);
		} catch(DatabaseException e){
			// System.out.println("Database ERROR: " + e);
		} catch(Exception e){
			// System.out.println("ERROR: " + e);
		}
	}

	@SuppressWarnings("resource")
	public ArrayList<Results> returnWordsDatabase(){
		ArrayList<Results> results = new ArrayList<Results>();
		EntityCursor<Results> resultsCur = service.returnWords().entities();
		try{
			for(Results r : resultsCur)
				results.add(r);
			resultsCur.close();
		} catch(Exception e){
			// System.out.println("ERROR: " + e);
		} finally{
			if(resultsCur != null)
				resultsCur = null;
		}
		return results;
	}

	@SuppressWarnings("resource")
	public void removeIndividuals(){
		EntityCursor<Results> resultsCur = service.returnWords().entities();
		try{
			for(Results r : resultsCur){
				String string = r.returnWord();
				service.returnWords().delete(string);
			}
			resultsCur.close();
		} catch(Exception e){
			// System.out.println("ERROR: " + e);
		} finally{
			if(resultsCur != null)
				resultsCur = null;
		}
	}

	public void close(){
		if(es != null){
			try{
				es.close();
			} catch(DatabaseException e){
				System.err.println("Unable to close EntityStore. ERROR: " + e.toString());
				System.exit(-1);
			}
		}
		if(databaseEnv != null){
			try{
				databaseEnv.close();
			} catch(DatabaseException e){
				System.err.println("Unable to close Database Environment. ERROR: " + e.toString());
				System.exit(-1);
			}
		}
	}
}
