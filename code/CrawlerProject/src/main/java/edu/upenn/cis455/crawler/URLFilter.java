package edu.upenn.cis455.crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.storage.RDSWrapper;

public class URLFilter implements IRichBolt {
	static Logger log = Logger.getLogger(URLFilter.class);
	
	Fields schema = new Fields();
    
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
    
    private List<String> linkSrc;
    private List<String> linkTar;
    private LinkedList<String> bufSrc;
    private LinkedList<String> bufTar;
    
    public URLFilter() {
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
		bufSrc.clear();
		bufTar.clear();
		new Thread(new RDSWrapper(linkSrc, linkTar)).start();
		linkSrc.clear();
		linkTar.clear();
	}

	@Override
	public synchronized void execute(Tuple input) {
		// TODO Auto-generated method stub
		//System.out.println("f in");
		String arg0 = ((URL) input.getObjectByField("srcurl")).toString();
		URL arg1 = (URL) input.getObjectByField("url");
		bufSrc.add(arg0);
		bufTar.add(arg1.toString());
		if (bufSrc.size()< 500) {
			//System.out.println("f ou");
			return ;
		} else {
			if (XPathCrawler.getInstance().urlQueue.size() > 100000) {
				bufSrc.clear();
				bufTar.clear();
				//System.out.println("f ou");
				return ;
			}
			while (!bufSrc.isEmpty()) {
				String srcurl = bufSrc.poll();
				URL newUrl;
				try {
					newUrl = new URL(bufTar.poll());
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}
				if (crawler.urlSeen.contains(newUrl))
					continue;
				else {
					crawler.urlSeen.add(newUrl);
					crawler.urlQueue.add(newUrl);
					if (!srcurl.endsWith("REDIRECT.REDIRECT")) {
						//System.out.println(srcurl +" -> "+ newUrl.toString());
						linkSrc.add(srcurl);
						linkTar.add(newUrl.toString());
						if (linkSrc.size() >= 1000) {
							new Thread(new RDSWrapper(linkSrc, linkTar)).start();
							linkSrc.clear();
							linkTar.clear();
						}
						//new Thread(new RDSWrapper(srcurl, newUrl.toString())).start();
					}
				}
			}
			bufSrc.clear();
			bufTar.clear();
		}
		//System.out.println("f ou");
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		crawler = XPathCrawler.getInstance(stormConf);
		linkSrc = new ArrayList<String>();
		linkTar = new ArrayList<String>();
		bufSrc = new LinkedList<String>();
		bufTar = new LinkedList<String>();
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
