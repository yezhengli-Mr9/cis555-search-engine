package edu.upenn.cis455.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SearchHelper {
	private static final String JDBC_URL = "jdbc:mysql://r5-xlarge-indexer.cqkrw7o8cny6.us-east-1.rds.amazonaws.com:3306/indexer";
	private static final String MASTER_USER = "cis555";
	private static final String MASTER_PSW = "cis555cis555";
	private static Connection conn = null;
	
	public List<String> SearchByKeywords(List<String> keywords) {
		List<String> res = new ArrayList<String>();
		int len = keywords.size();
		if (len > 10) len = 10;
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
			String sql = "";
			try {
				Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery(sql);
				while (rs.next()) {
					String url = rs.getString("rawUrl");
					res.add(url);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
		return res;
	}
	
}
