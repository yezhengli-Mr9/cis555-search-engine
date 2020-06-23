package edu.upenn.cis455.crawler;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.amazonaws.regions.Regions;

import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.storage.S3Wrapper;
import edu.upenn.cis455.storage.StorageServer;


/** (MS1, MS2) The main class of the crawler.
 */
public class XPathCrawler {
	private static XPathCrawler instance = null; 
	Queue<URL> urlQueue = new ConcurrentLinkedQueue<URL>();
	Set<URL> urlSeen = ConcurrentHashMap.newKeySet();
	ConcurrentHashMap<String, RobotsTxtInfo> robots = new ConcurrentHashMap<String, RobotsTxtInfo>();
	ConcurrentHashMap<String, Long> lastAccess = new ConcurrentHashMap<String, Long>();
	ConcurrentHashMap<String, Integer> hostDelay = new ConcurrentHashMap<String, Integer>();
	StorageServer storageServer;
	int maxSize;
	int maxNumber = 0;
	String monitorHost = "cis455.cis.upenn.edu";
	public AtomicInteger numProcessed = new AtomicInteger(0);
	public AtomicInteger numUploaded = new AtomicInteger(0);
	private final int TIMEOUT = 1500;
	XPathCrawler (String initialUrl, String dbDir, String maxSize){
		try {
			URL url = new URL(initialUrl);
			url = url.toURI().normalize().toURL();
			urlQueue.add(url);
			urlSeen.add(urlQueue.peek());
			storageServer = new StorageServer(dbDir);
			this.maxSize = (int) (Float.parseFloat(maxSize) * 1024 * 1024);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	XPathCrawler() {
	}
	
	XPathCrawler (Map<String, String> config) {
		storageServer = new StorageServer(config.get("storageDir"));
		if (config.containsKey("monitorHost"))
			this.monitorHost = config.get("monitorHost");	
		this.maxSize = (int) (Float.parseFloat(config.get("maxPageSize")) * 1024 * 1024);
		byte[] bytes = storageServer.getBytes("QUEUE", "doc_store");
		if (bytes==null) {
			if (config.containsKey("initialUrl")) {
				try {
					urlQueue.add(new URL(config.get("initialUrl")));
					urlSeen.add(urlQueue.peek());
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
				ObjectInputStream ois = new ObjectInputStream(bis);
				urlQueue = (Queue<URL>) ois.readObject();
				bytes = storageServer.getBytes("SEEN", "doc_store");
				bis = new ByteArrayInputStream(bytes);
				ois = new ObjectInputStream(bis);
				urlSeen = (Set<URL>) ois.readObject();
				System.out.println(urlSeen.size()+" seen targets loaded!");
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static XPathCrawler getInstance(Map<String, String> config) {
		if (instance == null) {
			instance = new XPathCrawler(config);
		}
		return instance;
	}
	
	public static XPathCrawler getInstance() {
		if (instance == null) {
			instance = new XPathCrawler();
		}
		return instance;
	}
	
	public static void main(String args[]) {
		if (args.length < 3) {
			System.err.println("You need to provide correct number of parameters!");
			System.exit(1);
		}
		XPathCrawler crawler = new XPathCrawler(args[0], args[1], args[2]);
		if (args.length >= 4)
			crawler.maxNumber = Integer.parseInt(args[3]);
		if (args.length >= 5)
			crawler.monitorHost = args[4];
		instance = crawler;
	}
	
	private String[] getResponse(URL url) {
		String[] response = new String[2];
		HttpURLConnection con;
		try {
			if (url.getProtocol().equalsIgnoreCase("https"))
				con = (HttpsURLConnection) url.openConnection();
			else if (url.getProtocol().equalsIgnoreCase("http"))
				con = (HttpURLConnection) url.openConnection();
			else return null;
			
			byte[] monitorData = ("fxiang;"+url).getBytes();
			DatagramPacket monitorPacket = new DatagramPacket(monitorData, monitorData.length, InetAddress.getByName(monitorHost), 10455);
			DatagramSocket monitorSocket = new DatagramSocket();
			monitorSocket.send(monitorPacket);
			monitorSocket.close();
			
			con.setInstanceFollowRedirects(false);
			con.setRequestMethod("GET");
			//con.setRequestProperty("Accept-Encoding", "identity");
			con.setRequestProperty("User-Agent", "cis455crawler");
			con.setRequestProperty("Connection", "close");
			con.setConnectTimeout(TIMEOUT);
			con.setReadTimeout(TIMEOUT);
			int responseCode = con.getResponseCode();
			if (responseCode>=300 && responseCode<400) {
				URL newUrl = new URL(url, con.getHeaderField("Location"));
				newUrl = newUrl.toURI().normalize().toURL();
				if (url.getPath().toLowerCase().equals("/robots.txt")) {
					return getResponse(newUrl);
				}
//				if (urlSeen.contains(newUrl)) {
//				}
//				else {
//					if (isGoodURL(newUrl)) {
//						urlSeen.add(newUrl);
//						urlQueue.add(newUrl);
//					}
//				}
				con.disconnect();
				response[0] = "REDIRECT";
				response[1] = newUrl.toString();
				return response;
			}
			else if (responseCode == 200) {
				String type = con.getHeaderField("Content-Type");
				if (type!=null && !type.toLowerCase().contains("text/html") && !type.toLowerCase().contains("text/plain"))
					return null;
				BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream(), "ISO-8859-15"));
				char[] buf = new char[maxSize];
				int length = 0;
				for (int i = 0;i < maxSize;i++) {
					int chr = reader.read();
					buf[i] = (char) chr;
					if (chr ==  -1) break;
					length ++;
				}
				if (length==maxSize) {
					if (reader.read()==-1) {
					}
					else {
						return null;
					}
				}
				String body = new String(Arrays.copyOfRange(buf, 0, length));
				response[1] = body;
				if (type == null) {
					if (url.getPath().toLowerCase().endsWith(".html") || url.getPath().toLowerCase().endsWith(".htm")) type = "text/html; charset=UTF-8";
					else if (body.trim().toLowerCase().endsWith("</html>")) type = "text/html; charset=UTF-8";			
					else return null;
				}
				response[0] = type;
				con.getInputStream().close();
				con.disconnect();
				return response;
			} else {
				con.disconnect();
				//System.out.println("ERROR: "+responseCode);
				return null;
			}
		} catch (SocketTimeoutException e_) {
			return null;
		}	catch (Exception e) {
			e.printStackTrace();
			return null;
		}	
	}
	
	private boolean isMatched(String requesting, String disallowed) {
		if (!disallowed.contains("*")) {
			if (disallowed.charAt(disallowed.length()-1) == '$') {
				disallowed = disallowed.substring(0, disallowed.length()-1);
				return requesting.equals(disallowed);
			}		
			else return requesting.startsWith(disallowed);
		}
		else {
			String reg = "";
			boolean first = true;
			boolean exact = false;
			if (disallowed.charAt(disallowed.length()-1) == '$') {
				disallowed = disallowed.substring(0, disallowed.length()-1);
				exact = true;
			}
			for (String s:disallowed.split(Pattern.quote("*"))) {
				if (first) {
					if (!s.equals(""))
						reg = reg + Pattern.quote(s);
					first = false;
				} else {
					reg = reg + ".*?";
					if (!s.equals(""))
						reg = reg + Pattern.quote(s);
				}
			}
			if (disallowed.charAt(disallowed.length()-1) == '*')
				reg = reg + ".*?";
			if (!exact) reg = reg + "(/.*?)?";
			return requesting.matches(reg);
		}
	}
	
	private boolean isGoodPage (String s) {
		try {
			String html = new String(s.getBytes("ISO-8859-15"));
			Document doc = Jsoup.parse(html);
			if (!doc.select("html").isEmpty() && doc.select("html").get(0).hasAttr("lang")) {
				if (!doc.select("html").get(0).attr("lang").toLowerCase().startsWith("en")) {
					//System.err.println("DISCARD! bad page l");
					return false;
				}
			}
			Elements es = doc.select("p");
			String content = doc.title();
			int maxSegSize = doc.title().length();
			int cnt = 0;
			for (Element e : es) {
				content += " "+ e.text();
				cnt ++;
				maxSegSize = Math.max(e.text().length(), maxSegSize);
			}
			if (content.length() < 5000 || maxSegSize < 650) {
				//System.err.println("DISCARD! bad page s");
				return false;
			}
			if (content.length() / cnt < 300) {
				System.err.print("DISCARD! imbalanced page");
				return false;
			}
			//System.out.println(content.length() / cnt);
//			LanguageDetector detector;
//			detector = new OptimaizeLangDetector().setMixedLanguages(true).loadModels();
//
//			if (!detector.detect(content).getLanguage().equals("en")) {
//				System.err.println("DISCARD! not English");
//				return false;
//			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//System.err.println("DISCARD! bad page e");
			return false;
		}
		return true;
	}
	
	public static boolean isGoodURL(URL url) {
		String urlstr = url.toString().toLowerCase();
		if (!urlstr.startsWith("http")) return false;
		if (urlstr.contains("support") || urlstr.contains("help") || urlstr.contains("review") ||
				urlstr.contains("youtube") || urlstr.contains("twitter") || urlstr.contains("facebook") || urlstr.contains("account")
				|| urlstr.contains("netflix") || urlstr.contains("instagram") || urlstr.contains("apps.apple.com") || urlstr.contains("policy")) {
			//System.out.println("bad url");
			return false;
		}
//		if (url.getPath().contains("/") && url.getPath().split("/").length >= 4)
//			return false;
		if (url.getFile().length() > 100) {
			return false;
		}
		for (char c : urlstr.toCharArray()) {
			if (c=='/'||c=='.'||c=='-'||c==':') continue;
			if ((c >= 48 && c <= 57) || (c >=97 && c<=122))
				continue;
			//System.out.println(url+" Discarded!"+c);
			return false;
		}
		//sorry
		if (Math.random() < 0.5) {
			//System.out.println(url+" Discarded for math!");
			return false;
		}
		return true;
	}

	public Values<Object> run(URL url) {
		//System.out.println("processing "+url.toString());
//		if (numProcessed.get() >= maxNumber && maxNumber > 0)
//			return null;
		try {
			//System.out.println("robots");
			String urlStr = url.toString();
			if (robots.get(url.getHost())==null) {
				URL robotUrl;
				if (url.getPort()==-1)
					robotUrl = new URL(url.getProtocol()+"://"+url.getHost()+"/robots.txt");
				else robotUrl = new URL(url.getProtocol()+"://"+url.getHost()+":"+url.getPort()+"/robots.txt");
				String[] response = getResponse(robotUrl);
				String robotTxt = "";
				if (response!=null)
					robotTxt = response[1];
				RobotsTxtInfo robotTxtInfo = new RobotsTxtInfo();
				String currentAgent = "*";
				if (robotTxt.equals("")) {
					robotTxtInfo.addUserAgent("*");
					robotTxtInfo.addDisallowedLink("*", "");
				}
				else {
					for (String s:robotTxt.split("\\r?\\n")) {
						if (s.startsWith("#") || !s.contains(":")) continue;
						String key = s.split(":")[0];
						String val = s.substring(key.length()+1).trim();
						if (key.toLowerCase().contains("user-agent")) {
							robotTxtInfo.addUserAgent(val);
							currentAgent = val;
						} else if (key.toLowerCase().contains("disallow"))
							robotTxtInfo.addDisallowedLink(currentAgent, val);
						else if (key.toLowerCase().contains("allow"))
							robotTxtInfo.addAllowedLink(currentAgent, val);
						else if (key.toLowerCase().contains("crawl-delay")) {
							double delayInRobots;
							try {
								delayInRobots = Double.parseDouble(val);
							} catch (NumberFormatException e_) {
								delayInRobots = 0.1;
							}
							robotTxtInfo.addCrawlDelay(currentAgent, (int) (delayInRobots * 1000.0));
						}
						else if (key.toLowerCase().contains("sitemap"))
							robotTxtInfo.addSitemapLink(val);
					}
				}
				robots.put(url.getHost(), robotTxtInfo);
			}
			Long lastAccessTime = lastAccess.get(url.getHost());
			if (lastAccessTime != null && hostDelay.get(url.getHost()) != null && System.currentTimeMillis()-lastAccessTime<hostDelay.get(url.getHost())) {
				XPathCrawler.getInstance().urlQueue.add(url);
				return null;
				//Thread.sleep(hostDelay.get(url.getHost())-(System.currentTimeMillis()-lastAccessTime));
			}
			String appliedAgent = null;
			ArrayList<String> disallowed = null;
			if (robots.get(url.getHost()).containsUserAgent("cis455crawler")) 
				appliedAgent = "cis455crawler";
			else if (robots.get(url.getHost()).containsUserAgent("*")) 
				appliedAgent = "*";
			if (appliedAgent!=null) {
				Integer delay = robots.get(url.getHost()).getCrawlDelay(appliedAgent);
				disallowed = robots.get(url.getHost()).getDisallowedLinks(appliedAgent);
				if (delay!=null)
					hostDelay.put(url.getHost(), Math.min(delay, 1500));
				if (disallowed!=null) {
					boolean flag = false;
					for (String s : disallowed) {
						if (s.equals("")) continue;
						if (s.equals("/") || isMatched(url.getPath(), s)) {
							flag = true;
							break;
						}
					}
					if (flag) {
						System.out.println(url.toString()+" DISALLOWED!");
						return null;
					}
				}
			}
			lastAccess.put(url.getHost(), System.currentTimeMillis());
			String[] response = getResponse(url);	
			if (numProcessed.incrementAndGet() % 100 ==0) {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
		        ObjectOutputStream oos = new ObjectOutputStream(bos);
		        oos.writeObject(urlQueue);
				storageServer.putBytes("QUEUE", bos.toByteArray(), "doc_store");
				
				bos = new ByteArrayOutputStream();
		        oos = new ObjectOutputStream(bos);
		        oos.writeObject(urlSeen);
		        storageServer.putBytes("SEEN", bos.toByteArray(), "doc_store");
			}
			
			if (response == null) return null;
			if (response[0].equals("REDIRECT")) return new Values<Object>(response[1], "REDIRECT", url, new URL(response[1]).getHost());
			String type = response[0];
			String body = response[1];
			if (url.getPath().split("/").length>2 && !isGoodPage(body)) {
				return null;
			}
			System.out.println(urlStr+": Downloading");
			//System.out.println(new String(body.getBytes("ISO-8859-15")));
			numUploaded.getAndIncrement();
			new Thread(new S3Wrapper("urlcontents-cis455g18", Regions.US_EAST_1, urlStr, type, body.getBytes("ISO-8859-15"))).start();
			return new Values<Object>(body, type, url, url.getHost());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
