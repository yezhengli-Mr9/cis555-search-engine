package edu.upenn.cis455.indexer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class CrawledContentsReader {
	private static final String JDBC_URL = "jdbc:mysql://urlinks.crpnw2fluwdp.us-east-1.rds.amazonaws.com:3306/links";
	private static final String MASTER_USER = "admin";
	private static final String MASTER_PSW = "admin4links";
	private static Connection conn = null;
	
	public static List<String> getAllUrls() throws SQLException {
		List<String> urls = new ArrayList<String>();
		if (conn == null)
			conn = DriverManager.getConnection(JDBC_URL, MASTER_USER, MASTER_PSW);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT url FROM links.sha2url ORDER BY retrived_time ASC"); 
		while (rs.next()) {
			urls.add(rs.getString("url"));
		}
		return urls;
	}
	
	// @return S3 Object by key
	public static S3Object downloadFromS3(String url) {
		return downloadFromS3("urlcontents-cis455g18", Regions.US_EAST_1, url);
	}
	
	// @return body part of a S3 object
	public static String getContentFromS3Object(S3Object obj) throws IOException {
		return new String(obj.getObjectContent().readAllBytes());
	}
	
	// @return type of a S3 object, e.g. "text/html; charset=utf-8"
	public static String getTypeFromS3Object(S3Object obj) {
		return obj.getObjectMetadata().getContentType();
	}
	
	public static String getSHA(String key) {
		return DigestUtils.sha1Hex(key);
	}
	
	
	public static S3Object downloadFromS3(String bucketName, Regions regions, String url) {
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withRegion(regions)
                .build();
		String key = DigestUtils.sha1Hex(url);
		S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
		return fullObject;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(getContentFromS3Object(downloadFromS3(getAllUrls().get(1))));
	}

}
