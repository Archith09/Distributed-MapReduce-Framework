package edu.upenn.cis455.mapreduce.job;

import java.io.IOException;
import java.net.URL;


import org.w3c.dom.Text;

import edu.upenn.cis455.mapreduce.Context;



public class Host {

	public void map(Object k, Text v, Context c) throws IOException, InterruptedException {
		String url = k.toString();
		String rank = v.toString();
		if (k == null || url == null || url.trim().equals("")) {
			return;
		}

		try {
			String hostName = null;
			URL myURL = new URL(url);
			hostName = myURL.getHost();
			c.write(hostName, rank);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void reduce(Text k, Iterable<Text> v, Context c) throws IOException, InterruptedException{
		double count = 0;
		String url = k.toString();
		if(k == null || url == null || url.trim().equals("")){
			return;
		}
		for(Text i : v){
			String pos = i.toString();
			double rank = Double.parseDouble(pos);
			count += rank;
		}
		c.write(url, String.valueOf(count));
	}

}
