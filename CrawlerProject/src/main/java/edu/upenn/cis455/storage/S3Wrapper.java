package edu.upenn.cis455.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;

public class S3Wrapper implements Runnable{
	private String bucket;
	private Regions regions;
	private String url;
	private String type;
	private byte[] content;
	
	public S3Wrapper (String bucketName, Regions regions, String url, String contentType, byte[] content) {
		this.bucket = bucketName;
		this.regions = regions;
		this.url = url;
		this.type = contentType;
		this.content = content;
	}
	
	public static void createBucket(String bucketName, Regions regions) {
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(regions)
                .build();
		if (!s3Client.doesBucketExistV2(bucketName))
			s3Client.createBucket(new CreateBucketRequest(bucketName));
	}
	
	public static void upload2S3(String bucketName, Regions regions, String url, String contentType, byte[] content) {
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withRegion(regions)
                .build();
		
		ByteArrayInputStream contentAsStream = new ByteArrayInputStream(content);
		ObjectMetadata md = new ObjectMetadata();
		md.setContentLength(content.length);
		md.setContentType(contentType);

		String key = DigestUtils.sha1Hex(url);
		s3Client.putObject(new PutObjectRequest(bucketName, key, contentAsStream, md).withCannedAcl(CannedAccessControlList.PublicRead));
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
	
	public static List<String> getAllUrls() throws Exception {
//		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
//                .withRegion(regions)
//                .build();
//		String key = DigestUtils.sha1Hex("URLS");
//		S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
//		List<String> URLS;
//		
//		ByteArrayInputStream bis = new ByteArrayInputStream(fullObject.getObjectContent().readAllBytes());
//		ObjectInputStream ois = new ObjectInputStream(bis);
//		URLS = (List<String>) ois.readObject();
//        
//		return URLS;
		List<String> urls = new ArrayList<String>();
		ResultSet rs = RDSWrapper.executeQuery("SELECT url FROM links.sha2url ORDER BY retrived_time ASC");
		while (rs.next()) {
			urls.add(rs.getString("url"));
		}
		return urls;
		
	}
	
	public static String getSHA(String key) {
		return DigestUtils.sha1Hex(key);
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		// Test: upload url:bytes pair to S3
//		String bucketName = "testurls";
//		Regions regions = Regions.US_EAST_1;
////		createBucket(bucketName, regions);
//		byte[] bytes = "<html><body><p>This is a test page!</body></html>".getBytes();
//		upload2S3(bucketName, regions, "http://test.org", "text/html", bytes);
//		List<String> newurls = new ArrayList<String>();
//		newurls.add("http://test.org");
//		ByteArrayOutputStream bos = new ByteArrayOutputStream();
//		ObjectOutputStream oos = new ObjectOutputStream(bos);
//		oos.writeObject(newurls);
//		upload2S3(bucketName, regions, "URLS", "URLS", bos.toByteArray());
//		
		//System.out.println(java.time.LocalDateTime.now().toString().replace("T", " ").replaceAll("\\..*", ""));
//		
//		List<String> URLS = getAllUrls(bucketName, regions);
//		List<String> newurls = new ArrayList<String>();
//		newurls.addAll(URLS);
//		System.out.println(URLS.size());
//		Iterator<String> URLS1 = URLS.iterator();
//		Set<String> test = new HashSet<String>();
//		while (URLS1.hasNext()) {
//			String url = URLS1.next();
//			if (test.contains(url)) 
//				newurls.remove(url);
//			else {
//				test.add(url);
//			}
//		}
//		System.out.println(newurls.size());
//		
//		ByteArrayOutputStream bos = new ByteArrayOutputStream();
//		ObjectOutputStream oos = new ObjectOutputStream(bos);
//		oos.writeObject(newurls);
//		upload2S3(bucketName, regions, "URLS", "URLS", bos.toByteArray());
		
//		// Test: get bytes and its metadata by key from S3
//		S3Object obj = downloadFromS3(bucketName, regions, "http://test.org");
//		Iterator<String> URLS1 = getAllUrls(bucketName, regions).iterator();
//		System.out.println("Content: " + new String(obj.getObjectContent().readAllBytes()));
//		System.out.println("Content-type(from metadata): " + obj.getObjectMetadata().getContentType());
//		System.out.println("Content-length(from metadata): " + obj.getObjectMetadata().getContentLength());
//		System.out.println("Listing first 10 urls:");
//		int idx = 1;
//		while (URLS1.hasNext()) {
//			System.out.println(idx+". "+URLS1.next());
//			if (idx++==10) break;
//		}
		
//		uploadFromBDB("/home/cis455/git/HW2/db");
	}

	@Override
	public void run() {
		//System.out.println("s3 started");
		// TODO Auto-generated method stub
		upload2S3(bucket, regions, url, type, content);
		String time = java.time.LocalDateTime.now().toString().replace("T", " ").replaceAll("\\..*", "");
		RDSWrapper.executeUpdate("INSERT IGNORE INTO links.sha2url (sha, url, retrived_time) VALUES ('"+getSHA(url)+"','"+url+"','"+time+"');");
//		try {
//			List<String> URLS = getAllUrls(bucket, regions);
//			if (!URLS.contains(url))
//				URLS.add(url);
//			ByteArrayOutputStream bos = new ByteArrayOutputStream();
//	        ObjectOutputStream oos = new ObjectOutputStream(bos);
//	        oos.writeObject(URLS);
//			upload2S3(bucket, regions, "URLS", "URLS", bos.toByteArray());
//		} catch (ClassNotFoundException | IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//System.out.println("s3 ended");
	}
	
//	private static void uploadFromBDB(String dir) throws IOException {
//		List<String> URLS = new ArrayList<String>();
//		StorageServer storageServer = new StorageServer(dir);
//		Map<String, String> records = storageServer.showStringRecords("doc_store");
//		for (String key : records.keySet()) {
//			if (!key.equals("URLS") && !key.endsWith("_type") && !key.endsWith("_modified") && !key.endsWith("_time")) {
//				URLS.add(key);
//				upload2S3("urlcontents", key, storageServer.getUserInfo(key+"_type", "doc_store"), storageServer.getUserInfo(key, "doc_store").getBytes("ISO-8859-15"));
//			}
//		}
//		ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        ObjectOutputStream oos = new ObjectOutputStream(bos);
//        oos.writeObject(URLS);
//		upload2S3("urlcontents", "URLS", "URLS", bos.toByteArray());
//	}
}
