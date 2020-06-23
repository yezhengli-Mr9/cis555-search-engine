package edu.upenn.cis455.storage;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.XPathCrawler;

public class RDSWrapper implements Runnable {
	public static Connection conn = null;
	private static final int FAILURE = -1;
	private static final int SUCCESS = 0;
	
//	private static final String JDBC_URL = "jdbc:mysql://urlinks.crpnw2fluwdp.us-east-1.rds.amazonaws.com:3306/links";
//	private static final String MASTER_USER = "admin";
//	private static final String MASTER_PSW = "admin4links";
	
	// This is database for indexer, table name: "indexer.invertedIndex"
	private static final String JDBC_URL = "jdbc:mysql://r5-xlarge-indexer.cqkrw7o8cny6.us-east-1.rds.amazonaws.com:3306/indexer";
	private static final String MASTER_USER = "cis555";
	private static final String MASTER_PSW = "cis555cis555";
	
	private String src;
	private String singleDst;
	private List<String> dst = null;
	
	private List<String> linkSrc = null;
    private List<String> linkTar = null;
	
	public RDSWrapper(List<String> src, List<String> tar) {
		this.linkSrc = new ArrayList<String>();
		this.linkTar = new ArrayList<String>();
		this.linkSrc.addAll(src);
		this.linkTar.addAll(tar);
	}
	
	public RDSWrapper(String src, List<String> dst) {
		this.src = src;
		this.dst = dst;
	}
	
	public RDSWrapper(String src, String dst) {
		this.src = src;
		this.singleDst = dst;
		this.dst = null;
	}
	
	public synchronized static int executeUpdate(String sql) {
		if (conn == null) {
			try {
				conn = DriverManager.getConnection(JDBC_URL, MASTER_USER, MASTER_PSW);
			} catch (SQLException  e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				conn = null;
				return FAILURE;
			}
		}
		if (conn != null) {
			try {
				Statement st = conn.createStatement();
				st.executeUpdate(sql); 
				return SUCCESS;
			} catch (SQLException e) {
				e.printStackTrace();
				return FAILURE;
			}	
		}
		return FAILURE;
	}
	
	public synchronized static ResultSet executeQuery(String sql) {
		if (conn == null) {
			try {
				conn = DriverManager.getConnection(JDBC_URL, MASTER_USER, MASTER_PSW);
			} catch (SQLException  e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		if (conn != null) {
			try {
				Statement st = conn.createStatement();
				return st.executeQuery(sql); 
			} catch (SQLException e) {
				e.printStackTrace();
				return null;
			}	
		}
		return null;
	}
	
	public static int addAdjLink(String src, String tar) {
		String sql = "INSERT IGNORE INTO links.crawling (src, tar) VALUES ('"+src+"','"+tar+"');";
		return executeUpdate(sql);
	}
	
	public static int addAdjLink(String src, List<String> tar) {
		String sql = "INSERT IGNORE INTO links.crawling (src, tar) VALUES ";
		for (int i = 0;i < tar.size();i++) {
			sql += "('"+src+"','"+tar.get(i)+"')";
			if (i != tar.size()-1)
				sql += ",\n";
			else sql += ";";
		}
		return executeUpdate(sql);
	}
	
	public static int addAdjLink(List<String> src, List<String> tar) {
		if (src.size()==0) return SUCCESS;
		String sql = "INSERT IGNORE INTO links.crawling (src, tar) VALUES ";
		for (int i = 0;i < src.size();i++) {
			sql += "('"+src.get(i)+"','"+tar.get(i)+"')";
			if (i != src.size()-1)
				sql += ",\n";
			else sql += ";";
		}
		//System.out.println(sql);
		return executeUpdate(sql);
	}
	
	public static int addRanks(List<String> src, List<String> tar) {
		if (src.size()==0) return SUCCESS;
		String sql = "INSERT IGNORE INTO pagerank2 (url, rank) VALUES ";
		for (int i = 0;i < src.size();i++) {
			sql += "('"+src.get(i)+"',"+tar.get(i)+")";
			if (i != src.size()-1)
				sql += ",\n";
			else sql += ";";
		}
		//System.out.println(sql);
		return executeUpdate(sql);
	}
	
	public static String showLinks(int limit) {
		String sql = "SELECT * FROM links.crawling";
		if (limit > 0) 
			sql += " LIMIT " + limit;
		ResultSet rs = executeQuery(sql);
		String res = "";
		try {
			while (rs.next()) {
				res += rs.getString("src") + " -> " + rs.getString("tar") + "\n";
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return res;
	}
	
	public static void showResultSet(ResultSet resultSet) throws SQLException {
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.print("COLUMNS: ");
		for (int i = 1; i <= columnsNumber; i++)
			System.out.print(rsmd.getColumnName(i) + "  ");
		System.out.println("\n");
		while (resultSet.next()) {
		    for (int i = 1; i <= columnsNumber; i++) {
		        String columnValue = resultSet.getString(i);
		        //System.out.print(columnValue + " " + rsmd.getColumnName(i));
		        System.out.print(columnValue + "  ");
		    }
		    System.out.println("");
		}
	}
	
	public static void main(String[] args) throws Exception {
		List<String> urls = new ArrayList<String>();
		List<String> ranks = new ArrayList<String>();
		int i = 0;
		Connection conn2 = null;
		if (conn2 == null) {
			conn2 = DriverManager.getConnection("jdbc:mysql://urlinks.crpnw2fluwdp.us-east-1.rds.amazonaws.com:3306/links", "admin", "admin4links");
		}
		if (conn2 != null) {
			try {
				Statement st = conn2.createStatement();
				ResultSet rs = st.executeQuery("SELECT * FROM links.pagerank"); 
				while (rs.next()) {
					urls.add(rs.getString(1));
					double tmp = Double.parseDouble(rs.getString(2));
					if (tmp > 3.0) tmp = 3.0;
					ranks.add(String.valueOf(tmp));
					if (urls.size() >= 5000) {
						System.out.println((++i)*5000);
						new Thread(new RDSWrapper(urls, ranks)).start();
						urls.clear();
						ranks.clear();
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
				return ;
			}	
		}
		
		new Thread(new RDSWrapper(urls, ranks)).start();
		urls.clear();
		ranks.clear();
		
//		executeUpdate("INSERT ignore into links.id2url2\n" + 
//				"		select tar as url from links.crawling");
//		ResultSet rs = executeQuery("SELECT count(*) FROM links.id2url2");
//		showResultSet(rs);
//		List<String> urls = CrawledContentsReader.getAllUrls();
//		for (int i = 0;i < urls.size();i++) {
//			String url = urls.get(i);
//			executeUpdate( "INSERT IGNORE INTO links.sha2url (sha, url) VALUES ('"+CrawledContentsReader.getSHA(url)+"','"+url+"');"  );
//		}
		
		
//		int progress = 0;
//		List<String> srcs = new ArrayList<String>();
//		List<String> dsts = new ArrayList<String>();
//		List<String> allUrls = CrawledContentsReader.getAllUrls();
//		HashSet<String> urls = new HashSet<String>(allUrls);
//		ResultSet rs = executeQuery("SELECT * FROM links.crawling");
//		while (rs.next()) {
//			System.out.println(++progress);
//			String src = rs.getString("src");
//			String tar = rs.getString("tar");
//			if (!urls.contains(src)) {
//				executeUpdate("DELETE FROM links.crawling WHERE src='"+src+"';");
//				executeUpdate("DELETE FROM links.crawling WHERE tar='"+src+"';");
//			}
//			if (!urls.contains(tar)) {
//				executeUpdate("DELETE FROM links.crawling WHERE src='"+tar+"';");
//				executeUpdate("DELETE FROM links.crawling WHERE tar='"+tar+"';");
//			}
//		}
//		
		
		//executeUpdate("ALTER TABLE links.url2id modify id INT NOT NULL");
		//rs.next();
		//System.out.println(rs.getString(1));
//		for (String s : allUrls) {
//			System.out.print(s+" ");
//			ResultSet rs = executeQuery("SELECT id FROM links.queryUrls WHERE url='"+s+"';");
//			if (!rs.next()) {
//				System.out.println("no");
//				continue;
//			}
//			System.out.println(rs.getInt(1));
//		}
		
//		ResultSet rs = executeQuery("SELECT url, retrived_time FROM links.sha2url ORDER BY retrived_time ASC");
//		while (rs.next()) {
//			System.out.println(++progress);
//			if (rs.getString("retrived_time") != null)
//				continue;
//			String urlstr = rs.getString("url");
//			URL url;
//			try {
//				url = new URL(urlstr);
//			} catch (MalformedURLException e) {
//				e.printStackTrace();
//				continue;
//			}
//			String content = CrawledContentsReader.getContentFromS3Object(CrawledContentsReader.downloadFromS3(urlstr));
//			org.jsoup.nodes.Document jsoup = Jsoup.parse(content);
//			Elements links = jsoup.select("a");
//			for (Element link:links) {
//				try {
//					URL newUrl = new URL(url, link.attr("href"));
//					newUrl = newUrl.toURI().normalize().toURL();
//					if (newUrl.toString().toLowerCase().startsWith("http")) {
//						srcs.add(urlstr);
//						dsts.add(newUrl.toString());
//						if (srcs.size() > 5000) {
//							new Thread(new RDSWrapper(srcs, dsts)).start();
//							srcs.clear();
//							dsts.clear();
//						}
//					}
//				} catch (Exception e) {
//					e.printStackTrace();
//					continue;
//				}
//			}
//		}
//		new Thread(new RDSWrapper(srcs, dsts)).start();
//		srcs.clear();
//		dsts.clear();
//		
//		System.out.println("phase 1 ok");
		//SELECT(SELECT sha from links.sha2url WHERE url='https://www.cneret.com') AS src_id, (SELECT sha from links.sha2url WHERE url='https://www.cnet.com/') AS tar_id
//		rs = executeQuery("SELECT * FROM links.crawling");
//		while (rs.next()) {
//			String src = rs.getString("src");
//			String tar = rs.getString("tar");
//			if (!allUrls.contains(src) || !allUrls.contains(tar))
//				continue;
//			ResultSet rs2 = executeQuery("SELECT id FROM links.sha2url WHERE ");
//		}
		
//		List<String> newurls = new ArrayList<String>();
//		newurls.add("http://test.org");
//		newurls.add("http://test.org/bad");
//		addAdjLink("http://test.org", newurls);
		
		//addAdjLink("http://test.org", "http://test.org/extremly_bad");
//		System.out.println(showLinks(0));
//		new Thread(new RDSWrapper("http://test.org", "http://test.org/extremly_bad")).start();
	}

	@Override
	public void run() {
		//System.out.println("rds started");
		// TODO Auto-generated method stub
		if (linkSrc!=null) {
			addAdjLink(linkSrc, linkTar);
		}
		else {
			if (dst!=null)
				addAdjLink(src, dst);
			else addAdjLink(src, singleDst);
		}
		//System.out.println("rds ended");
	}
}
