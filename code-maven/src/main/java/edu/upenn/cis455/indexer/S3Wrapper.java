package edu.upenn.cis455.indexer;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Iterator;

import org.apache.commons.codec.digest.DigestUtils;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;



import org.apache.log4j.Logger;
import java.sql.*;
import java.util.Calendar;
import edu.upenn.cis455.URLInfo;
import java.io.FileWriter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
public class S3Wrapper {
    static Logger logger = Logger.getLogger(S3Wrapper.class);
    private static Connection getRemoteConnection() {

//        if (System.getenv("RDS_HOSTNAME") != null) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String dbName = "indexer";//System.getenv("RDS_DB_NAME");
            String userName = "cis555";//System.getenv("RDS_USERNAME");
            String password = "cis555cis555";//System.getenv("RDS_PASSWORD");
            String hostname ="indexer.cqkrw7o8cny6.us-east-1.rds.amazonaws.com";// System.getenv("RDS_HOSTNAME");
            String port = "3306";//System.getenv("RDS_PORT");
            String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password;
            logger.trace("Getting remote connection with connection string from environment variables.");

            Connection con = DriverManager.getConnection(jdbcUrl);
            logger.info("Remote connection successful.");
            System.out.println("[WordCount yezheng] Remote connection successful.");
            return con;
        }
        catch (ClassNotFoundException e) {
            logger.warn(e.toString());
            e.printStackTrace();
        }
        catch (SQLException e) {
            logger.warn(e.toString());
            e.printStackTrace();
        }
//        }

        return null;
    }
    public static void upload2S3(String bucketName, String url, byte[] content) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(Regions.US_WEST_1)
                .build();

        ByteArrayInputStream contentAsStream = new ByteArrayInputStream(content);
        ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(content.length);

        String key = DigestUtils.sha1Hex(url);
        s3Client.putObject(new PutObjectRequest(bucketName, key, contentAsStream, md));
    }

    public static byte[] downloadFromS3(String bucketName, String url) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(Regions.US_WEST_1)
                .build();
        String key = DigestUtils.sha1Hex(url);
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
        try {
            return fullObject.getObjectContent().readAllBytes();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    public static List<String> getAllUrls(String bucketName, Regions clientRegion) throws IOException, ClassNotFoundException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(clientRegion)
                .build();
        String key = DigestUtils.sha1Hex("URLS");
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
        List<String> URLS;

        ByteArrayInputStream bis = new ByteArrayInputStream(fullObject.getObjectContent().readAllBytes());
        ObjectInputStream ois = new ObjectInputStream(bis);
        URLS = (List<String>) ois.readObject();

        return URLS;
    }
//    private static void generateFile(String url, String stringToWrite) {
//        String key = DigestUtils.sha1Hex(url);
//        String outputFile = "trash/URLdebug/" +key, Fileoutput = "trash/URLraw/" +key;
//        try {
//            FileWriter writer = new FileWriter(outputFile);
//            writer.append(stringToWrite);
//            writer.flush();
//            writer.close();
//            logger.debug("New File is generated ==>" + outputFile);
//        } catch (Exception exp) {
//            logger.error("Exception in generateFile ", exp);
//            exp.printStackTrace();
//        }
//        try {
//            FileWriter writer = new FileWriter(Fileoutput);
//            writer.append(url);
//            writer.flush();
//            writer.close();
//            logger.debug("New File is generated ==>" + Fileoutput);
//        } catch (Exception exp) {
//            logger.error("Exception in generateFile ", exp);
//            exp.printStackTrace();
//        }
//
//    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        byte[] bytes = "test msg".getBytes();
        String bucketName = "urlcontents";
        Regions clientRegion = Regions.US_WEST_1;
        List<String> allurls=  getAllUrls(bucketName, clientRegion);
        int cntUrl = 0;
        for (String url: allurls){
            cntUrl++;
            Connection conn = getRemoteConnection();


            byte[] body_ = downloadFromS3(bucketName,url);
            String body = new String(body_, "UTF-8"); //migbase64
            //-------

//            https://jsoup.org/cookbook/extracting-data/attributes-text-html
            Document doc = Jsoup.parse(body);
            String text = doc.body().text(); // "An example link"
//            generateFile(url,text);
            //-------
//            System.out.println("[S3Wrapper] body\t"+body);
            URLInfo urlinfo = new URLInfo(url);
//            https://alvinalexander.com/java/java-mysql-insert-example-preparedstatement/
            String query = " INSERT INTO invertedIndex (word, hostname, portNo, filepath, count,date_created)"
                    + " VALUES (?, ?, ?, ?,?,?) ON DUPLICATE KEY UPDATE count=?, date_created=?";
            Calendar calendar = Calendar.getInstance();
            java.sql.Timestamp timestamp =  new Timestamp(System.currentTimeMillis());
//2016-11-16 06:43:19.77;
            Map <String,Integer> word2cnt = new LinkedHashMap<>();
            String word = "";
            for (char c: text.toCharArray()) {
                if (Character.isLetter(c)||c =='-'){
                    word+=c;
                }
                else if(0!=word.length())
                {
                    word = word.toLowerCase();
                    if (!word2cnt.containsKey(word)) {
                        word2cnt.put(word, 1);
                    } else {
                        word2cnt.put(word, word2cnt.get(word) + 1);
                    }
                    word = "";
                }
            }

            // create the mysql insert preparedstatement
            try{
                for (Map.Entry<String,Integer> entry: word2cnt.entrySet()) {
                    word = entry.getKey();
                    Integer cnt = entry.getValue();
                    PreparedStatement preparedStmt = conn.prepareStatement(query);
                    preparedStmt.setString(1, word);
                    preparedStmt.setString(2, urlinfo.getHostName());
                    preparedStmt.setInt(3, urlinfo.getPortNo());
                    preparedStmt.setString(4, urlinfo.getFilePath());
                    preparedStmt.setInt(5, cnt);
                    preparedStmt.setTimestamp(6, timestamp);
                    preparedStmt.setInt(7, cnt);
                    preparedStmt.setTimestamp(8, timestamp);
//                    System.out.println("[preparedStmt]" + preparedStmt);


                    preparedStmt.execute();
                }
            }
            catch(Exception e ){e.printStackTrace();}
            try{
                conn.close();
            }
            catch(Exception e ){e.printStackTrace();}
            System.out.println("[Done] "+url+"\t"+cntUrl);
        }

    }
}

