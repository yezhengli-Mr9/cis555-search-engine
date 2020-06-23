package edu.upenn.cis455.mapreduce.master;

import static spark.Spark.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.crawler.CrawlerBolt;
import edu.upenn.cis455.crawler.DocumentParser;
import edu.upenn.cis455.crawler.FrontierQueue;
import edu.upenn.cis455.crawler.URLFilter;
import edu.upenn.cis455.storage.CrawledContentsReader;
import edu.upenn.cis455.storage.S3Wrapper;

class MasterApp {
	public static void main(String args[]) {
		final String SPOUT = "SPOUT";
	    final String CRAWL_BOLT = "CRAWL_BOLT";
	    final String PARSE_BOLT = "PARSE_BOLT";
	    final String FILTER_BOLT = "FILTER_BOLT";
	    
	    List<String> urls = new ArrayList<String>();
		Map<String, Long> lastStatus = new HashMap<String, Long>();
		Map<String, String[]> workers = new HashMap<String, String[]>();
		Map<String, String> masterConfig = new HashMap<String, String>();
//		
		if (args.length<1) {
			System.out.println("Pleas provide ip:port of this server");
			System.exit(1);
		}
		else {
			masterConfig.put("ip", args[0].split(":")[0]);
			masterConfig.put("port", args[0].split(":")[1]);
		}
		
		//ipAddress(masterConfig.get("ip"));
		port(Integer.parseInt(masterConfig.get("port")));
		//port(8000);

		/* Just in case someone opens the root URL, without /status... */

		get("/", (request,response) -> {
			return "Please go to the <a href=\"/status\">status page</a>!";
		});
		
		get("/refresh", (request,response) -> {
			urls.clear();
			urls.addAll(CrawledContentsReader.getAllUrls());
			return null;
		});
		
		get("/records", (request,response) -> {
			if (urls.isEmpty()) 
				urls.addAll(CrawledContentsReader.getAllUrls());
			int pageNo;
			try {
				pageNo = Integer.parseInt(request.queryParams("page"));
			} catch (NullPointerException | NumberFormatException e_) {
				pageNo = 1;
			}
			if ((pageNo-1)*100 > urls.size()-1) {
				response.status(404);
				return "Page does not exist!";
			}
			String msg = "<html><body>" + "urls in page "+pageNo + ": ("+urls.size()+" urls in total)<br>";
			for (int i = (pageNo-1)*100; i < pageNo*100; i++) {
				if (i >= urls.size()) break;
				msg += "<br>"+(i%100+1)+". <a href=\"https://urlcontents-cis455g18-backup.s3.amazonaws.com/"+S3Wrapper.getSHA(urls.get(i))+"\">"+urls.get(i)+"</a>";
			}
			if ((pageNo)*100 <= urls.size()-1) {
				msg += "<p><a href=\"/records?page="+ ++pageNo +"\">Go to next page.</a>";
			}
			return msg+"</body></html>";
		});
		
		get("/start", (request,response) -> {
			Config config = new Config();
			String workerList = "";
			Set<String> workerTreeSet = new TreeSet<String>();
			workerTreeSet.addAll(workers.keySet());
			for (String workerID: workerTreeSet) {
				if (!lastStatus.containsKey(workerID) || System.currentTimeMillis() - lastStatus.get(workerID) > 30000) {
					continue;
				}
				if (workerList.equals(""))
					workerList = "[" +workerID;
				else workerList += ","+workerID;
			} 
			workerList += "]";
			System.out.println(workerList +" is out for routing");
			config.put("workerList", workerList);
			config.put("job", "crawler");
			config.put("maxPageSize", "1.0");
			config.put("monitorHost", masterConfig.get("ip"));
			config.put("master", masterConfig.get("ip")+":"+masterConfig.get("port"));
			TopologyBuilder builder = new TopologyBuilder();
			
			FrontierQueue spout = new FrontierQueue();
            CrawlerBolt crawler = new CrawlerBolt();
            DocumentParser parser = new DocumentParser();
            URLFilter filter = new URLFilter();
            
            builder.setSpout(SPOUT, spout, 1);
            builder.setBolt(CRAWL_BOLT, crawler, 4).shuffleGrouping(SPOUT);
            builder.setBolt(PARSE_BOLT, parser, 1).shuffleGrouping(CRAWL_BOLT);
            builder.setBolt(FILTER_BOLT, filter, 1).fieldsGrouping(PARSE_BOLT, new Fields("host"));
            
            try {
            	WorkerJob job = new WorkerJob(builder.createTopology(), config);
                ObjectMapper mapper = new ObjectMapper();
                mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
                int i = 0;
                for (String dest: workerTreeSet) {
                	config.put("workerIndex", String.valueOf(i++));
                	if (sendJob(dest, "POST", config, "definejob", 
                			mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() != 
                			HttpURLConnection.HTTP_OK) {
                		throw new RuntimeException("Job definition request failed");
                	}
                }
                for (String dest: workerTreeSet) {
                	if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != 
                			HttpURLConnection.HTTP_OK) {
                		throw new RuntimeException("Job execution request failed");
                	}
                }
			}catch (Exception e) {
				e.printStackTrace();
                //System.exit(0);
				response.status(403);
				return "denied!";
			}
			return "started!";
		});
		
		post("/credentials", (request,response) -> {
			return "deprecated!";
//			FileWriter f = new FileWriter(System.getProperty("user.home")+"/.aws/credentials");
//			f.write(request.queryParams("credentials").replaceAll("\\s+", "\r\n"));
//			f.close();
//			for (String dest: workers.keySet()) {
//				try {
//					sendJob(dest, "POST", null, "credentials", request.queryParams("credentials").replaceAll("\\s+", "\r\n")).getResponseCode();
//				}
//				catch (IOException e) {
//
//				}
//			}
//			return "OK";
		});

		/* Status page, for launching jobs and for viewing the current status */

		get("/status", (request,response) -> {
			String style = "<style>\n" +
					"a:visited {\n" + 
					"  color: blue;\n" + 
					"}"+
					"table {\n" + 
					"  width:100%;\n" + 
					"}\n" + 
					"table, th, td {\n" + 
					"  border: 1px solid black;\n" + 
					"  border-collapse: collapse;\n" + 
					"}\n" + 
					"th, td {\n" + 
					"  padding: 15px;\n" + 
					"  text-align: center;\n" + 
					"}\n" + 
					"table#t01 tr:nth-child(even) {\n" + 
					"  background-color: #eee;\n" + 
					"}\n" + 
					"table#t01 tr:nth-child(odd) {\n" + 
					" background-color: #fff;\n" + 
					"}\n" + 
					"table#t01 th {\n" + 
					"  background-color: black;\n" + 
					"  color: white;\n" + 
					"}\n" + 
					"</style>";
			String webPage = "<!DOCTYPE html><html><head>"+style+"</head><body><small>written by: fxiang@seas.upenn.edu</small><h3>Active Workers:</h3><table id=\"t01\"><tr><th>IP:PORT</th><th>RATE</th><th>IN QUEUE</th><th>REQUESTED</th><th>KEPT</th></tr>";
			for (String workerID: workers.keySet()) {
				if (!lastStatus.containsKey(workerID) || System.currentTimeMillis() - lastStatus.get(workerID) > 30000) {
					continue;
				}
				String[] status = workers.get(workerID);
				webPage += "<tr><td>"+workerID+"</td><td>"+status[1]+"</td><td>"+status[2]+"</td><td>"+status[3]+"</td><td>"+status[4]+"</td></tr>";
			}
			webPage += "</table><br>";
			//webPage += "Set Credentials:<br><form action=\"/credentials\" method=\"POST\">\n<input type=\"text\" name=\"credentials\" placeholder=\"credentials\"/><p><input type=\"submit\" value=\"Submit\"/></form>";
			webPage += "<p><a href=\"/records\">Check all records on AWS S3.</a>";
			webPage += "</body></html>";
			return webPage;
		});
		
//		get("/submit", (request,response) -> {
//			try {
//				String className = request.queryParams("name");
//				String inputDir = request.queryParams("input");
//				String outputDir = request.queryParams("output");
//				String mapThreads = request.queryParams("map");
//				String reduceThreads = request.queryParams("reduce");
//				if (className.equals("") || inputDir.equals("") || outputDir.equals("") || mapThreads.equals("") || reduceThreads.equals(""))
//					throw new NullPointerException();
//				
//				Config config = new Config();
//				String workerList = "";
//				for (String workerID: workers.keySet()) {
//					if (!lastStatus.containsKey(workerID) || System.currentTimeMillis() - lastStatus.get(workerID) > 30000) {
//						continue;
//					}
//					if (workerList.equals(""))
//						workerList = "[" +workerID;
//					else workerList += ","+workerID;
//				} 
//				workerList += "]";
//				config.put("workerList", workerList);
//				config.put("job", className);
//				config.put("master", "127.0.0.1:8000");
//				
//				config.put("jobClass", className);
//				config.put("input", inputDir);
//				config.put("output", outputDir);
//				config.put("spoutExecutors", "1");
//				config.put("mapExecutors", mapThreads);
//				config.put("reduceExecutors", reduceThreads);
//				TopologyBuilder builder = new TopologyBuilder();
//				
//				FileSpout spout = new WordSpout();
//                MapBolt bolt = new MapBolt();
//                ReduceBolt bolt2 = new ReduceBolt();
//                PrintBolt printer = new PrintBolt();
//
//                builder.setSpout(WORD_SPOUT, spout, Integer.valueOf(config.get("spoutExecutors")));
//
//                // Parallel mappers, each of which gets specific words
//                builder.setBolt(MAP_BOLT, bolt, Integer.valueOf(config.get("mapExecutors"))).shuffleGrouping(WORD_SPOUT);;
//
//                // Parallel reducers, each of which gets specific words
//                builder.setBolt(REDUCE_BOLT, bolt2, Integer.valueOf(config.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT, new Fields("key"));
//
//                // Only use the first printer bolt for reducing to a single point
//                builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCE_BOLT);
//                WorkerJob job = new WorkerJob(builder.createTopology(), config);
//                ObjectMapper mapper = new ObjectMapper();
//                mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
//                int i = 0;
//                for (String dest: workers.keySet()) {
//                	config.put("workerIndex", String.valueOf(i++));
//                	if (sendJob(dest, "POST", config, "definejob", 
//                			mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() != 
//                			HttpURLConnection.HTTP_OK) {
//                		throw new RuntimeException("Job definition request failed");
//                	}
//                }
//                for (String dest: workers.keySet()) {
//                	if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != 
//                			HttpURLConnection.HTTP_OK) {
//                		throw new RuntimeException("Job execution request failed");
//                	}
//                }
//			} catch (NullPointerException e) {
//				return "Wrong parameters!<p><a href=\"/status\">Back to status page.</a>";
//			} catch (Exception e2) {
//				e2.printStackTrace();
//                System.exit(0);
//			}
//			return "Submit success!<p><a href=\"/status\">Back to status page.</a>";
//		});

		/* Workers submit requests for /workerstatus; human users don't normally look at this */

		get("/workerstatus", (request,response) -> {
			/* do something with the information in the request */
			synchronized(workers) {
				synchronized(lastStatus) {
					String workerID = request.ip()+":"+request.queryParams("port");
					lastStatus.put(workerID, System.currentTimeMillis());
					String[] workerStatus = workers.get(workerID);
					if (workerStatus == null)
						workerStatus = new String[5];
					workerStatus[0] = request.queryParams("port");
					workerStatus[1] = request.queryParams("status");
					workerStatus[2] = request.queryParams("job");
					workerStatus[3] = request.queryParams("keysRead");
					workerStatus[4] = request.queryParams("keysWritten");
					workers.put(workerID, workerStatus);
				}
			}
			return "OK";
		});
		
		get("/shutdown", (request,response) -> {
			/* do something with the information in the request */
			
			for (String dest: workers.keySet()) {
				try {
					sendJob(dest, "GET", null, "shutdown", null).getResponseCode();
				}
				catch (IOException e) {

				}
			}
			return "Shutdown orders sent.<p><a href=\"/status\">Back to status page.</a>";
		});
	}
	
	static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters) throws IOException {
		URL url = new URL("http://"+dest + "/" + job);
		//System.out.println(dest+job);

		HttpURLConnection conn = (HttpURLConnection)url.openConnection();

		conn.setRequestMethod(reqType);

		if (reqType.equals("POST")) {
			conn.setDoOutput(true);
			conn.setRequestProperty("Content-Type", "application/json");

			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
		}

		return conn;
	}
}
