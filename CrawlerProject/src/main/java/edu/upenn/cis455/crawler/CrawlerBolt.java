package edu.upenn.cis455.crawler;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

public class CrawlerBolt implements IRichBolt {
	static Logger log = Logger.getLogger(CrawlerBolt.class);
	
	Fields schema = new Fields("content", "type", "url", "host");
    
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private XPathCrawler crawler;
    
    private TopologyContext context;
    
    public CrawlerBolt() {
    }

	@Override
	public String getExecutorId() {
		// TODO Auto-generated method stub
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(schema);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public synchronized void execute(Tuple input) {
		//System.out.println("crawler: " + input.toString());
		// TODO Auto-generated method stub
		URL url = (URL) input.getObjectByField("url");
		Values<Object> res = crawler.run(url);
		if (res != null) {
			this.context.numProcessed.getAndSet(crawler.numProcessed.get());
			this.context.numWritten.getAndSet(crawler.numUploaded.get());
			this.context.queueLength.getAndSet(crawler.urlQueue.size());
			collector.emit(res);
		} else {
			//System.out.println("failed :"+input.toString());
		}
		Thread.yield();
		//System.out.println("c out");
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.context = context;
		this.collector = collector;
		crawler = XPathCrawler.getInstance(stormConf);
	}

	@Override
	public void setRouter(StreamRouter router) {
		// TODO Auto-generated method stub
		this.collector.setRouter(router);
	}

	@Override
	public Fields getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

}
