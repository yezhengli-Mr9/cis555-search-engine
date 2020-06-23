package edu.upenn.cis455.storage;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class SQLWorker implements Runnable{
	private static List<String> srcs;
	private static List<String> dsts;
	private static BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(50);
	private static ExecutorService executor = new ThreadPoolExecutor(12, 12, 0L, TimeUnit.MILLISECONDS, queue
			);
	
	private String urlstr;

	public SQLWorker(String s) {
		this.urlstr = s;
	}
	
	public static void main(String[] args) throws Exception{
		int progress = 0;
		srcs = new ArrayList<String>();
		dsts = new ArrayList<String>();
		
		ResultSet rs = RDSWrapper.executeQuery("SELECT url, retrived_time FROM links.sha2url ORDER BY retrived_time ASC, sha");
		while (rs.next()) {
			System.out.println(++progress);
			if (rs.getString("retrived_time") == null)
				continue;
			if (progress < 450000) continue;
			String urlstr = rs.getString("url");
			
			try {
				executor.execute(new SQLWorker(urlstr));
			} catch(RejectedExecutionException e) {
				while (queue.size() > 20) {
					Thread.yield();
				}
				executor.execute(new SQLWorker(urlstr));
			}
			
		}
		new RDSWrapper(srcs, dsts).run();
		srcs.clear();
		dsts.clear();
		
		System.out.println("phase 2 ok");
	}

	@Override
	public void run() {
		URL url;
		try {
			url = new URL(urlstr);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return ;
		}
		
		String content;
		try {
			content = CrawledContentsReader.getContentFromS3Object(CrawledContentsReader.downloadFromS3(urlstr));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return ;
		}
		
		org.jsoup.nodes.Document jsoup = Jsoup.parse(content);
		Elements links = jsoup.select("a");
		for (Element link:links) {
			try {
				URL newUrl = new URL(url, link.attr("href"));
				newUrl = newUrl.toURI().normalize().toURL();
				if (newUrl.toString().toLowerCase().startsWith("http")) {
					synchronized (srcs) {
					srcs.add(urlstr);
					dsts.add(newUrl.toString());
					if (srcs.size() > 5000) {
						new Thread(new RDSWrapper(srcs, dsts)).start();
						srcs.clear();
						dsts.clear();
					}
					}
				}
			} catch (Exception e) {
				//e.printStackTrace();
				continue;
			}
		}
		
	}

}
