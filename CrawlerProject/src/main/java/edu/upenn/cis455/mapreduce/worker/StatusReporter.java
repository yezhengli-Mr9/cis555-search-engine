package edu.upenn.cis455.mapreduce.worker;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.stormlite.TopologyContext;

public class StatusReporter implements Runnable{
	
	private Map<String, String> config;
	private List<TopologyContext> contexts;
	
	StatusReporter (Map<String, String> config, List<TopologyContext> contexts) {
		this.config = config;
		this.contexts = contexts;
	}
	
	StatusReporter () {
	}
	

	@Override
	public void run() {
		long lastTap = System.currentTimeMillis();
		int lastKept = 0;
		long currentSpeed = 0;
		while (true) {
			try {
				Thread.sleep(5000);
				String queryString = "";
				queryString += "port=" + config.get("port");
				
				
				if (contexts.size()!=0) {
					queryString += "&keysRead="+contexts.get(contexts.size()-1).numProcessed.get();
					queryString += "&keysWritten="+contexts.get(contexts.size()-1).numWritten.get();
					queryString += "&job="+contexts.get(contexts.size()-1).queueLength.get();
					
					if (System.currentTimeMillis() - lastTap >= 60000) {
						currentSpeed = (contexts.get(contexts.size()-1).numWritten.get() - lastKept) * 65;
						lastTap = System.currentTimeMillis();
						lastKept = contexts.get(contexts.size()-1).numWritten.get();
					}
					
					queryString += "&status="+currentSpeed+"+per+hour";
				} else {
					queryString += "&job=None";
					queryString += "&status=Idle";
				}
				
				URL url = new URL("http://"+config.get("masterIpPort") + "/" + "workerstatus" + "?" + queryString);
				HttpURLConnection conn = (HttpURLConnection)url.openConnection();
				conn.setRequestMethod("GET");
				conn.setRequestProperty("Connection", "close");
				if (conn.getResponseCode() != HttpURLConnection.HTTP_OK)
					throw new RuntimeException();
				Thread.sleep(5000);
			}
			catch (InterruptedException e_) {
				break;
			}
			catch (Exception e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				continue;
			}
		}
		//System.out.println("interrupted");
	}
}

