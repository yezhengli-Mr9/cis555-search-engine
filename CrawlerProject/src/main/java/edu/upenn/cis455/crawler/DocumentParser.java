package edu.upenn.cis455.crawler;

import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.helper.W3CDom;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.InputSource;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.storage.RDSWrapper;

public class DocumentParser implements IRichBolt {
	static Logger log = Logger.getLogger(DocumentParser.class);
	
	Fields schema = new Fields("srcurl", "url", "host");
    
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
    
    public DocumentParser() {
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
		// TODO Auto-generated method stub
	    //System.out.println("p in");
		String content = input.getStringByField("content");
		String type = input.getStringByField("type");
		URL url = (URL) input.getObjectByField("url");
		
		if (type.equals("REDIRECT")) {
			String host = input.getStringByField("host");
			try {
				this.collector.emit(new Values<Object>(new URL("http://REDIRECT.REDIRECT"), new URL(content), host));
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				content = null;
			}
		}
		
		//List<URL> urls = new ArrayList<URL>();
		if (content != null) {
			if (type.toLowerCase().contains("text/html")) {
				int sent = 0;
				org.jsoup.nodes.Document jsoup = Jsoup.parse(content);
				Elements links = jsoup.select("a");
				for (Element link:links) {
					try {
						URL newUrl = new URL(url, link.attr("href"));
						newUrl = newUrl.toURI().normalize().toURL();
						//urls.add(newUrl);
						if (XPathCrawler.isGoodURL(newUrl) && sent<100) {
							sent ++;
							this.collector.emit(new Values<Object>(url, newUrl, newUrl.getHost()));
						}
					} catch (Exception e) {
						continue;
					}
				}
				//this.collector.emit(new Values<Object>(url, urls, url.getHost()));
			}
		}
		//System.out.println("p out");
		Thread.yield();
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
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
