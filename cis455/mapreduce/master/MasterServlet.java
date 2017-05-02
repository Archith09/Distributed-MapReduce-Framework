package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import spark.*;
import static spark.Spark.setPort;
import java.net.*;
import java.util.*;
import edu.upenn.cis.stormlite.*;
import edu.upenn.cis.stormlite.bolt.*;
import edu.upenn.cis.stormlite.distributed.*;
import edu.upenn.cis.stormlite.spout.*;
import edu.upenn.cis.stormlite.tuple.*;
import test.edu.upenn.cis.stormlite.mapreduce.*;
import test.edu.upenn.cis.stormlite.*;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MasterServlet{

	static final long serialVersionUID = 455555001;

	// public void doGet(HttpServletRequest request, HttpServletResponse response) 
	//      throws java.io.IOException
	// {
	//   response.setContentType("text/html");
	//   PrintWriter out = response.getWriter();
	//   out.println("<html><head><title>Master</title></head>");
	//   out.println("<body>Hi, I am the master!</body></html>");
	// }

	public HashMap<String, ThreadClass> thread = new HashMap<String, ThreadClass>();
	public final String MAP_BOLT = "MAP_BOLT";
	public final String REDUCE_BOLT = "REDUCE_BOLT";
	public final String WORD_SPOUT = "WORD_SPOUT";
	public final String PRINT_BOLT = "PRINT_BOLT";

	public MasterServlet(){
		init();
	}

	public void init(){
		int initialPortNum = 8000;
		setPort(initialPortNum);
		final ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

		Spark.get(new Route("/workerstatus"){
			@Override
			public Object handle(Request arg0, Response arg1){
				StringBuilder address = new StringBuilder();
				int portNum = 0;
				String task = "";
				int input = 0;
				int output = 0;
				String position = "";
				String finalCals = "";
				String sep = "\\|";
				String resStatus = "OK";

				try{
					portNum = Integer.parseInt(arg0.queryParams("port"));
					task = arg0.queryParams("job");
					input = Integer.parseInt(arg0.queryParams("keysRead"));
					output = Integer.parseInt(arg0.queryParams("keysWritten"));
					position = arg0.queryParams("status");
					finalCals = arg0.queryParams("results");
				} catch(Exception e){
					// System.out.println("ERROR: " + e);
				}

				address.append(arg0.ip() + ":" + String.valueOf(portNum));
				if(thread.containsKey(address.toString())){
					ThreadClass threadClass = thread.get(address.toString());
					threadClass.updateTask(task);
					threadClass.updatePosition(position);
					threadClass.updateInput(input);
					threadClass.updateOutput(output);
					String[] finalCalsSep = finalCals.split(sep);
					threadClass.emptyHash();
					
					for(int index = 0; index < finalCalsSep.length; index++)
						threadClass.updateFinalCals(finalCalsSep[index]);

					thread.put(address.toString(), threadClass);
				} else{
					ThreadClass threadClass = new ThreadClass();
					threadClass.updateAddress(address.toString());
					threadClass.updateTask(task);
					threadClass.updatePosition(position);
					threadClass.updateInput(input);
					threadClass.updateOutput(output);
					String[] finalCalsSep = finalCals.split(sep);
					threadClass.emptyHash();
					
					for(int index = 0; index < finalCalsSep.length; index++)
						threadClass.updateFinalCals(finalCalsSep[index]);

					thread.put(address.toString(), threadClass);
				}
				return resStatus;
			}
		});

		Spark.get(new Route("/status"){
			@Override
			public Object handle(Request arg0, Response arg1){
				String nothing = "";
				StringBuilder res = new StringBuilder();
				arg1.type("text/html");
				try{
					res.append("<html><body><table style=\"width:100%\">\n<tr>\n<th>IP : Port</th>\n<th>Status</th>\n<th>Job</th>\n<th>Keys Read</th>\n<th>Keys Written</th>\n<th>Results</th>\n</tr>\n");
					for(String address : thread.keySet()){
						ThreadClass threadClass = thread.get(address);
						res.append("<tr>\n<th>" + threadClass.returnAddress() + "</th>\n<th>" + threadClass.returnPosition() + "</th>\n<th>" + threadClass.returnTask() + "</th>\n<th>" + threadClass.returnInput() + "</th>\n<th>" + threadClass.returnOutput() + "</th>\n");
						HashSet<String> finalCals = threadClass.returnFinalCals();
						for(String i : finalCals){
							res.append("<th>" + i + "</th>");
						}
						res.append("</tr>");
					}
					res.append("</table><br><form action=\"status\" method=\"post\"><div class=\"container\" style=\"background-color:#D0D0D0\"><br><label>Class name of job: </label><input type=\"text\" placeholder=\"Class name of job\" name=\"job\" required><br><label>Name of job: </label><input type=\"text\" placeholder=\"Name of job\" name=\"jobName\" required><br><label>Input Directory: </label>\n<input type=\"text\" placeholder=\"Input directory\" name=\"inputDirectory\"><br><label>Output Directory: </label>\n<input type=\"text\" placeholder=\"Output directory\" name=\"outputDirectory\"><br><label>Number of Map Threads: </label>\n<input type=\"text\" placeholder=\"Number of Map Threads\" name=\"map\" required><br><label>Number of Reduce Threads: </label><input type=\"text\" placeholder=\"Number of Reduce Threads\" name=\"reduce\" required><br><button type=\"submit\">Confirm</button>\n</form></body></html>");
					return res.toString();
				} catch(Exception e){
					// System.out.println("ERROR: " + e);
					return nothing;
				}
			}
		});

		Spark.post(new Route("/status"){
			@Override
			public Object handle(Request arg0, Response arg1){
				int size = 0;
				int iterator = 0;
				StringBuilder activeThreads = new StringBuilder();
				Config myConfig = new Config();
				String openBracket = "[";
				String closeBracket = "]";
				String comma = ",";
				String nothing = "";
				String userHome = System.getProperty("user.home");
				String intermediate = "/store";
				String slash = "/";
				String homepage = "127.0.0.1:8000";
				String typeOfRequest = "POST";
				String string1 = "definejob";
				String string2 = "runjob";
				String resStatus = "OK";
				
				TopologyBuilder topologyBuilder = new TopologyBuilder();
				MapBolt mapBolt = new MapBolt();
				ReduceBolt reduceBolt = new ReduceBolt();
				FileSpout fileSpout = new WordFileSpout();
				PrintBolt printBolt = new PrintBolt();
				ObjectMapper om = new ObjectMapper();

				activeThreads.append(openBracket);
				for(String i : thread.keySet()){
					activeThreads.append(i);
					size++;
					if(size != thread.size()){
						activeThreads.append(comma);
					}
				}
				activeThreads.append(closeBracket);
				if(!activeThreads.toString().equals(nothing))
					myConfig.put("workerList", activeThreads.toString());
				myConfig.put("workerSize", String.valueOf(thread.size()));
				String task = arg0.queryParams("job");
				String dir1 = userHome + intermediate + slash + arg0.queryParams("inputDirectory");
				String dir2 = userHome + intermediate + slash + arg0.queryParams("outputDirectory");
//				String dir1 = arg0.queryParams("inputDirectory");
//				String dir2 = arg0.queryParams("outputDirectory");
				
				myConfig.put("master", homepage);
				myConfig.put("noOfWorkers", String.valueOf(thread.size()));
				myConfig.put("job", arg0.queryParams("jobName"));
				myConfig.put("mapClass", task);
				myConfig.put("reduceClass", task);
				myConfig.put("dbDir", userHome + intermediate);
				myConfig.put("mapExecutors", arg0.queryParams("map"));
				myConfig.put("reduceExecutors", arg0.queryParams("reduce"));
				myConfig.put("master", homepage);
				myConfig.put("inputDirectory", dir1);
				myConfig.put("outputDirectory", dir2);
				myConfig.put("spoutExecutors", "1");
				
				topologyBuilder.setSpout(WORD_SPOUT, fileSpout, Integer.valueOf(myConfig.get("spoutExecutors")));
				topologyBuilder.setBolt(MAP_BOLT, mapBolt, Integer.valueOf(myConfig.get("mapExecutors"))).fieldsGrouping(WORD_SPOUT, new Fields("value"));
				topologyBuilder.setBolt(REDUCE_BOLT, reduceBolt, Integer.valueOf(myConfig.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT, new Fields("key"));
				topologyBuilder.setBolt(PRINT_BOLT, printBolt, 1).firstGrouping(REDUCE_BOLT);
				Topology topology = topologyBuilder.createTopology();
				WorkerJob threadTask = new WorkerJob(topology, myConfig);
				om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
				String[] newThread = WorkerHelper.getWorkers(myConfig);
				for(String receiver : newThread){
					myConfig.put("workerIndex", String.valueOf(iterator++));
					try{
						if(sendJob(receiver, typeOfRequest, myConfig, string1, om.writerWithDefaultPrettyPrinter().writeValueAsString(threadTask)).getResponseCode() != HttpURLConnection.HTTP_OK){
							throw new RuntimeException("Incorrect job definition");
						}
					} catch(IOException e){
						// System.out.println("ERROR: " + e);
					}
				}
				for(String receiver : newThread){
					try{
						if(sendJob(receiver, typeOfRequest, myConfig, string2, nothing).getResponseCode() != HttpURLConnection.HTTP_OK){
							throw new RuntimeException("Unable to execute request with provided job definition");
						}
					} catch(IOException e){
						// System.out.println("ERROR: " + e);
					}
				}
				return resStatus;
			}
		});

		Spark.get(new Route("/shutdown"){
			@Override
			public Object handle(Request arg0, Response arg1){

				String colon = ":";
				Socket client;
				String resStatus = "OK";

				for(String i : thread.keySet()){
					String[] address = i.split(colon);
					try{
						client = new Socket(address[0].trim(), Integer.parseInt(address[1].trim()));
						StringBuilder message = new StringBuilder();
						DataOutputStream dataOutputStream = new DataOutputStream(client.getOutputStream());
						message.append("GET /shutdown HTTP/1.0");
						message.append("\r\n\r\n");
						dataOutputStream.write(message.toString().getBytes());
						dataOutputStream.flush();
						dataOutputStream.close();
						client.close();
					} catch(NumberFormatException e){
						System.out.println("ERROR: " + e);
					} catch(UnknownHostException e){
						System.out.println("ERROR: " + e);
					} catch(IOException e){
						System.out.println("ERROR: " + e);
					}
				}
				int sleepTime = 500;
				try{
					Thread.sleep(sleepTime * 6);
				} catch(InterruptedException e){
					System.out.println("ERROR: " + e);
				}
				System.exit(0);
				return resStatus;
			}
		});

	}

	static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters)
	throws IOException{
		URL url = new URL(dest + "/" + job);

		// log.info("Sending request to " + url.toString());

		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);
		
		if(reqType.equals("POST")){
			conn.setRequestProperty("Content-Type", "application/json");
			byte[] toSend = parameters.getBytes();
			OutputStream os = conn.getOutputStream();
			os.write(toSend);
			os.flush();
		} else{
			conn.getOutputStream();
		}
		return conn;
	}

}
  
