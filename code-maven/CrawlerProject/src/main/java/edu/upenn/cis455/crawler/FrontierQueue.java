package edu.upenn.cis455.crawler;

import java.io.BufferedReader;
import java.net.URL;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

public class FrontierQueue implements IRichSpout{
	static Logger log = Logger.getLogger(FrontierQueue.class);

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    /**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;
	TopologyContext context;
	
	/**
	 * This is a simple file reader for words.txt
	 */
	
	private XPathCrawler crawler;

    public FrontierQueue() {
    	//log.debug("Starting queue");
    }

	@Override
	public String getExecutorId() {
		// TODO Auto-generated method stub
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("url", "host"));
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		crawler = XPathCrawler.getInstance(config);
		this.context = topo;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public synchronized void nextTuple() {
		//System.out.println("q in");
		// TODO Auto-generated method stub
		if (context.getTaskNum() > 300) {
			//System.out.print(context.getTaskNum());
			return ;
		}
		URL url = crawler.urlQueue.poll();
		if (url!=null) {
			//log.debug(getExecutorId() + " emitting " + url.toString());
			this.collector.emit(new Values<Object>(url, url.getHost()));
		}
		//System.out.println("q out");
	}

	@Override
	public void setRouter(StreamRouter router) {
		// TODO Auto-generated method stub
		this.collector.setRouter(router);
	}

}
