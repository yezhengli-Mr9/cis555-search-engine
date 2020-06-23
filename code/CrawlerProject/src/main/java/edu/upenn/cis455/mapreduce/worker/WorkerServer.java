package edu.upenn.cis455.mapreduce.worker;

import static spark.Spark.get;
import static spark.Spark.post;

import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

/**
 * Simple listener for worker creation 
 * 
 * @author zives
 *
 */
public class WorkerServer {
    static Logger log = Logger.getLogger(WorkerServer.class);
        
    static DistributedCluster cluster = new DistributedCluster();
    
    List<TopologyContext> contexts = new ArrayList<>();
        
    static List<String> topologies = new ArrayList<>();
    Thread reporterThread;
        
    public WorkerServer(int myPort, Map<String, String> config) throws MalformedURLException {
    	
    	reporterThread = new Thread(new StatusReporter(config, contexts));
    	reporterThread.start();
        log.info("Creating server listener at socket " + myPort);
        
        Spark.port(myPort);
        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        Spark.post("/definejob", new Route() {

                @Override
                public Object handle(Request arg0, Response arg1) {
                    if (!contexts.isEmpty()) {
                    	arg1.status(403);
                    	return null;
                    }
                    WorkerJob workerJob;
                    try {
                        workerJob = om.readValue(arg0.body(), WorkerJob.class);
                        workerJob.getConfig().put("storageDir", config.get("storageDir"));
                        if (config.containsKey("initialUrl"))
                        	workerJob.getConfig().put("initialUrl", config.get("initialUrl"));
                        try {
                            log.info("Processing job definition request" + workerJob.getConfig().get("job") +
                                     " on machine " + workerJob.getConfig().get("workerIndex"));
                            contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(), 
                                                                workerJob.getTopology()));    
                            synchronized (topologies) {
                                topologies.add(workerJob.getConfig().get("job"));
                            }
                        } catch (ClassNotFoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return "Job launched";
                    } catch (IOException e) {
                        e.printStackTrace();
                                        
                        // Internal server error
                        arg1.status(500);
                        return e.getMessage();
                    } 
                        
                }
                
            });
        
        Spark.post("/runjob", new Route() {

                @Override
                public Object handle(Request arg0, Response arg1) {
                    log.info("Starting job!");
                    cluster.startTopology();
                                
                    return "Started";
                }
            });
        
        Spark.post("/push/:stream", new Route() {

                @Override
                public Object handle(Request arg0, Response arg1) {
                    try {
                        String stream = arg0.params(":stream");
                        Tuple tuple = om.readValue(arg0.body(), Tuple.class);
                                        
                        log.debug("Worker received: " + tuple + " for " + stream);
                                        
                        // Find the destination stream and route to it
                        StreamRouter router = cluster.getStreamRouter(stream);
                        
                                        
                        if (contexts.isEmpty())
                            log.error("No topology context -- were we initialized??");
                                        
                        if (!tuple.isEndOfStream())
                            contexts.get(contexts.size() - 1).incSendOutputs(router.getKey(tuple.getValues()));
                                        
                        if (tuple.isEndOfStream()) {
                        	//System.out.println("sender eos received:" + stream);
                            //System.out.println(router.getBolts());
                            router.executeEndOfStreamLocally(contexts.get(contexts.size() - 1));
                        }
                        else
                            router.executeLocally(tuple, contexts.get(contexts.size() - 1));
                                        
                        return "OK";
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                                        
                        arg1.status(500);
                        return e.getMessage();
                    }
                                
                }
                
            });

        // TODO: Handle /shutdown to shut down the worker
        
        get("/shutdown", (request,response) -> {
        	reporterThread.interrupt();
        	WorkerServer.shutdown();
        	Spark.stop();
        	return "ok";
		});
        
        post("/credentials", (request,response) -> {
			FileWriter f = new FileWriter(System.getProperty("user.home")+"/.aws/credentials");
			f.write(request.body());
			f.close();
			return "OK";
		});
        
        
    }
    
        
    public static void createWorker(Map<String, String> config) {
    	try {
			new WorkerServer(Integer.parseInt(config.get("port")), config);
		} catch (NumberFormatException | MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public static void shutdown() {
        synchronized(topologies) {
            for (String topo: topologies)
                cluster.killTopology(topo);
        }
                
        cluster.shutdown();
    }
}
