package edu.upenn.cis455.mapreduce;

import org.w3c.dom.Text;

public interface Context {

  void write(String key, String value);
  
}
