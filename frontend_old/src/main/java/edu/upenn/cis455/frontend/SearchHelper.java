package edu.upenn.cis455.frontend;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import opennlp.tools.stemmer.PorterStemmer;

public class SearchHelper implements Runnable {
	private static AtomicInteger finished;
	private static String[] cacheResults;
	private static final String JDBC_URL = "jdbc:mysql://r5-xlarge-indexer.cqkrw7o8cny6.us-east-1.rds.amazonaws.com:3306/indexer";
	private static final String MASTER_USER = "cis555";
	private static final String MASTER_PSW = "cis555cis555";
//	private static final String JDBC_URL = "jdbc:mysql://urlinks.crpnw2fluwdp.us-east-1.rds.amazonaws.com:3306/links";
//	private static final String MASTER_USER = "admin";
//	private static final String MASTER_PSW = "admin4links";
	private static Connection conn = null;
	
	private String job;
	private int idx;
	
	public SearchHelper(String job, int idx) {
		this.job = job;
		this.idx = idx;
	}
	
	private static String stemWord(String word) {
        int idx = 0;
        for (;idx!=word.length();idx++){
            if (Character.isLetter(word.charAt(idx))){
                break;
            }
        }


        String newword = word.substring(idx,word.length());
        for (idx = newword.length()-1;idx!=-1;idx--){
            if (Character.isLetter(newword.charAt(idx))){
                break;
            }
        }
        if (idx==-1){
//            System.out.println("[word]\t"+word+"\t[newword]\t"+newword+"\t[idx]\t"+idx);
//                    System.exit(1);
            return null;
        }
        try {
            newword = newword.substring(0, idx+1);
        }catch(Exception e2){e2.printStackTrace();
//            System.out.println("[word]\t"+word+"\t[newword]\t"+newword+"\t[idx]\t"+idx);
            System.exit(1);}
        PorterStemmer stem = new PorterStemmer();
        String target = stem.stem(newword);
        return target;
    }
	
	public static List<String> getCachedPages(List<String> urls, int pageNum) {
		int idx = 0;
		cacheResults = new String[XPathApp.resultsPerPage];
		finished = new AtomicInteger(0);
		for (int i = (pageNum-1)*XPathApp.resultsPerPage; i < pageNum*XPathApp.resultsPerPage;i++) {
			if (i >= urls.size())
				break;
			String url = urls.get(i);
			new Thread(new SearchHelper(url, idx++)).start();
		}
		while (finished.get() != idx) {
			//System.out.println(finished.get());
			Thread.yield();
		}
		return Arrays.asList(cacheResults);
	}
	
	public static List<String> getTestURLs(String keywords) {
		List<String> res = new ArrayList<String>();
		res.add("test4null");
		if (conn == null) {
			try {
				conn = DriverManager.getConnection(JDBC_URL, MASTER_USER, MASTER_PSW);
			} catch (SQLException  e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				conn = null;
			}
		}
		if (conn != null) {
			String sql = "SELECT url FROM links.sha2url LIMIT 24";
			try {
				Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery(sql);
				while (rs.next()) {
					String url = rs.getString("url");
					res.add(url);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
		return res;
	}
	
	public static List<String> SearchByKeywords(String query) {
		List<String> keywords;
		if (query.contains(" "))
			keywords = Arrays.asList(query.split(" "));
		else keywords = Arrays.asList(query);
		List<String> res = new ArrayList<String>();
		int len = keywords.size();
		String keywordsList = "(";
		for (int i = 0;i < len;i++) {
			keywordsList += "'"+stemWord(keywords.get(i).toLowerCase())+"'";
			if (i == len-1) keywordsList +=")";
			else keywordsList += ",";
		}
		if (conn == null) {
			try {
				conn = DriverManager.getConnection(JDBC_URL, MASTER_USER, MASTER_PSW);
			} catch (SQLException  e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				conn = null;
			}
		}
		if (conn != null) {
			String sql = "SELECT\n" + 
					"  rawUrl,\n" + 
					"  id_score + ifnull(rank, 0) as score,\n" + 
					"  id_score,\n" + 
					"  rank\n" + 
					"FROM\n" + 
					"  (\n" + 
					"    SELECT\n" + 
					"      urlid,\n" + 
					"      rawUrl,\n" + 
					"      id_score\n" + 
					"    FROM\n" + 
					"      (\n" + 
					"        SELECT\n" + 
					"          urlid,\n" + 
					"          sum(weight) AS id_score\n" + 
					"        FROM\n" + 
					"          invertedIndexStemmed\n" + 
					"        WHERE\n" + 
					"          word IN "+keywordsList+"\n" + 
					"        GROUP BY\n" + 
					"          urlid\n" + 
					"      ) AS id_tmp\n" + 
					"      LEFT JOIN id2url on id_tmp.urlid = id2url.id\n" + 
					"    WHERE\n" + 
					"      rawUrl is not null\n" + 
					"  ) AS pg_tmp\n" + 
					"  LEFT JOIN pagerank on pg_tmp.rawUrl = pagerank.url\n" + 
					"  ORDER BY score DESC\n" + 
					"  LIMIT 40\n" + 
					"";
			try {
				Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery(sql);
				while (rs.next()) {
					String url = rs.getString("rawUrl");
					System.out.println("[debug] "+url+": "+rs.getString("score")+" / "+rs.getString("id_score")+" / "+rs.getString("rank"));
					res.add(url);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
		return res;
	}
	
	public static void main(String[] args) {
		System.out.println(stemWord("noodle"));
		//System.out.println(getCachedPages(getTestURLs(""), 1).get(1));
//		int idx = 0;
//		cacheResults = new String[10];
//		finished = new AtomicInteger(0);
//		for (int i = (1-1)*10; i < 1*10;i++) {
//			if (i >= urls.size())
//				break;
//			String url = urls.get(i);
//			new Thread(new SearchHelper(url, idx++)).start();
//		}
//		while (finished.get() != idx) {
//			//System.out.println(finished.get());
//			Thread.yield();
//		}
//		//System.out.println(Arrays.asList(cacheResults));
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		//System.out.println("got"+this.job);
		String cachedContent;
		try {
			cachedContent = CrawledContentsReader.getContentFromS3Object(CrawledContentsReader.downloadFromS3(this.job));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			cachedContent = null;
		}
		cacheResults[this.idx] = cachedContent;
		finished.getAndIncrement();
		//System.out.println("finished " + this.idx);
	}
	
}
