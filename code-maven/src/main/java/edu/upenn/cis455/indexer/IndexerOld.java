package edu.upenn.cis455.indexer;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.codec.digest.DigestUtils;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;



import org.apache.log4j.Logger;
import java.sql.*;

import edu.upenn.cis455.URLInfo;
import java.io.FileWriter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
//import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
//import org.deeplearning4j.text.tokenization.tokenizerfactory.UimaTokenizerFactory;
//import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer;
public class IndexerOld {
    static Logger logger = Logger.getLogger(IndexerOld.class);

    private static Connection getRemoteConnection() {

//        if (System.getenv("RDS_HOSTNAME") != null) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String dbName = "indexer";//System.getenv("RDS_DB_NAME");
            String userName = "cis555";//System.getenv("RDS_USERNAME");
            String password = "cis555cis555";//System.getenv("RDS_PASSWORD");
            String hostname ="indexer-db-m4-xlarge.cqkrw7o8cny6.us-east-1.rds.amazonaws.com";// System.getenv("RDS_HOSTNAME");
            String port = "3306";//System.getenv("RDS_PORT");
            String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password;
            logger.trace("Getting remote connection with connection string from environment variables.");

            Connection con = DriverManager.getConnection(jdbcUrl);
            logger.info("Remote connection successful.");
//            System.out.println("[Indexer yezheng] Remote connection successful.");
            return con;
        } catch (ClassNotFoundException e) {
            logger.warn(e.toString());
            e.printStackTrace();
        } catch (SQLException e) {
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
        //yezheng:
        //        https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
//        BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAJZOB4XEN67XQP22Q",
//                "dPB1A0eQjJqJ+4Ge82l0GTl+nB1naKUoPQAGAUfq");
//        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
//                .withRegion(Regions.US_WEST_1)
//                .build();
        // Feng Xiang
//                BasicAWSCredentials awsCreds =
//                        new BasicAWSCredentials("AKIAJZOB4XEN67XQP22Q",
//                "dPB1A0eQjJqJ+4Ge82l0GTl+nB1naKUoPQAGAUfq");
//        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
//                .withRegion(Regions.US_EAST_1)
//                .build();

        //----
        //Feng Xiang
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withRegion(Regions.US_EAST_1)
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
//        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//                .withCredentials(new ProfileCredentialsProvider())
//                .withRegion(clientRegion)
//                .build();
        // Feng Xiang
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withRegion(Regions.US_EAST_1)
                .build();
        String key = DigestUtils.sha1Hex("URLS");
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
        List<String> URLS;

        ByteArrayInputStream bis = new ByteArrayInputStream(fullObject.getObjectContent().readAllBytes());
        ObjectInputStream ois = new ObjectInputStream(bis);
        URLS = (List<String>) ois.readObject();

        return URLS;
    }


    private static void DebugUpload2S3(String bucketName, String url, String stringToWrite) {
        String key = DigestUtils.sha1Hex(url);
        String outputFile = "debug-" +key, outputUrl = "url-" +key;
        //------
//        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//                .withCredentials(new ProfileCredentialsProvider())
//                .withRegion(Regions.US_EAST_1)
//                .build();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withRegion(Regions.US_EAST_1)
                .build();

        {
            ByteArrayInputStream contentAsStream = new ByteArrayInputStream(stringToWrite.getBytes());
            ObjectMetadata md = new ObjectMetadata();
            md.setContentLength(stringToWrite.getBytes().length);
            s3Client.putObject(new PutObjectRequest(bucketName, outputFile, contentAsStream, md).withCannedAcl(CannedAccessControlList.PublicReadWrite));//Feng Xiang
        }


        {
            ByteArrayInputStream contentAsStream = new ByteArrayInputStream(url.getBytes());
            ObjectMetadata md = new ObjectMetadata();
            md.setContentLength(url.getBytes().length);
            s3Client.putObject(new PutObjectRequest(bucketName, outputUrl, contentAsStream, md).withCannedAcl(CannedAccessControlList.PublicReadWrite) );// Feng Xiang
        }



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

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        byte[] bytes = "test msg".getBytes();
        String bucketName = "urlcontents-cis455g18";//"crawled-url";//"urlcontents";//
        String query = " INSERT INTO invertedIndex (word, hostname, portNo, filepath, count,date_created)"
                + " VALUES";
        Regions clientRegion = Regions.US_WEST_1;
        List<String> allurls = getAllUrls(bucketName, clientRegion);
        int cntUrl = 0,cntSqlVal = 0, byte_len = Integer.parseInt(args[2]);
        System.out.println("[indexer main] allurls.size()\t" + allurls.size());
        int start = Integer.parseInt(args[0]), end = Integer.parseInt(args[1]);
        Connection conn = getRemoteConnection();
        String id2urlquery = " INSERT INTO id2url (id, hostname, portNo, filepath,date_created)"
                + " VALUES";

        for (String url : allurls) {
            cntUrl++;
            if (cntUrl <= start) {
                continue;
            }
            if (cntUrl > end) {
                break;
            }
            URLInfo urlinfo = new URLInfo(url);
//            if (!urlinfo.getFilePath().startsWith("/en-us/autos/auto-shows/karma-sc1") ){//"/en-us/video/watch/hoda"
//                continue;
//            }
//            {// https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html
//                Statement stmt = null;
//                String checkquery = "SELECT * FROM invertedIndex WHERE hostname="
//                + "\""+urlinfo.getHostName()+"\""
//                +"AND portNo=" + urlinfo.getPortNo();
////                    System.out.println(query);
//
//                try{
//                    stmt = conn.createStatement(
//                            ResultSet.TYPE_SCROLL_SENSITIVE,
//                            ResultSet.CONCUR_UPDATABLE);
//                    stmt = conn.createStatement();
//                    ResultSet rs = stmt.executeQuery(checkquery);
//                    if (rs.next()) {
////                        String coffeeName = rs.getString("hostname");
////                        int supplierID = rs.getInt("portNo");
////                        System.out.println("[EXIST]"+coffeeName + "\t" + supplierID+"\t"+cntUrl );
//                        continue;
//                    }
//                } catch (SQLException e ) {
//                    e.printStackTrace();
//                } finally {
//                    if (stmt != null) {
//                        try{
//                            stmt.close();
//                        }catch(Exception e){
//                            e.printStackTrace();}
//                    }
//                }
//            }

            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.MILLISECOND, 0);   // zeroize milliseconds
            String timestamp = ""+new Timestamp(System.currentTimeMillis());

            timestamp = timestamp.substring(0, timestamp.length()-4);
            List<String> vals0 = new ArrayList<>();
            vals0.add(""+cntUrl);
            vals0.add("\""+urlinfo.getHostName()+"\"");
            vals0.add("" + urlinfo.getPortNo());
            vals0.add("\""+urlinfo.getFilePath()+"\"");
            vals0.add("STR_TO_DATE(\""+ timestamp+"\",'%Y-%m-%d %H:%i:%s')");
            id2urlquery += "(" + String.join(",", vals0) + "),";
            if (true)
                continue;


            //-------

//            https://jsoup.org/cookbook/extracting-data/attributes-text-html
            Document doc = Jsoup.parse(new String(downloadFromS3(bucketName, url), "UTF-8")); //migbase64);
//            String text = doc.body().text() +" "+ doc.title(); // "An example link"
            String text = "";
            for (Element p : doc.select("p")) {
                text += " " + p.text();
            }
//            System.out.println(url+"[url text]"+text);
//            DebugUpload2S3("debug-url-body", text,url); //expired

            //-------
//            System.out.println("[S3Wrapper] body\t"+body);

//            https://alvinalexander.com/java/java-mysql-insert-example-preparedstatement/

//2016-11-16 06:43:19.77;
//            https://docs.oracle.com/javase/7/docs/api/java/util/StringTokenizer.html
            Map<String, Integer> word2cnt = new LinkedHashMap<>();


            //---------
            //yezheng too slow!!!
            String preword = "";
            for (char c : text.toCharArray()) {
                if (Character.isLetter(c) || c == '-') {
                    preword += c;
                } else if (0 != preword.length()) {
                    preword = preword.toLowerCase();
                    if (!word2cnt.containsKey(preword)) {
                        word2cnt.put(preword, 1);
                    } else {
                        word2cnt.put(preword, word2cnt.get(preword) + 1);
                    }
//            https://deeplearning4j.konduit.ai/language-processing/tokenization
                    //tokenization with lemmatization,part of speech taggin,sentence segmentation
//                    try {
//                        TokenizerFactory tokenizerFactory = new UimaTokenizerFactory();
////                Tokenizer tokenizer = tokenizerFactory.tokenize(body);
////                https://github.com/eclipse/deeplearning4j-examples/blob/ec9ab1b62c410c0d731d3de800ab845ec734cf8b/dl4j-examples/src/main/java/org/deeplearning4j/examples/recurrent/processnews/NewsIterator.java
//                        Tokenizer tokenizer = tokenizerFactory.create(preword);
//                        //iterate over the tokens
//                        while (tokenizer.hasMoreTokens()) {
//                            String word = tokenizer.nextToken();
//                            word = word.toLowerCase();
//                            if (!word2cnt.containsKey(word)) {
//                                word2cnt.put(word, 1);
//                            } else {
//                                word2cnt.put(word, word2cnt.get(word) + 1);
//                            }
//                        }

                    //get the whole list of tokens
                    //            List<String> tokens = tokenizer.getTokens();

                    //iterate over the tokens
                    //            while(tokenizer.hasMoreTokens()) {
                    //                String token = tokenizer.nextToken();
                    //            }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }

                    preword = "";
                }
            }

//---------

//get the whole list of tokens
//            List<String> tokens = tokenizer.getTokens();

            // create the mysql insert preparedstatement

//            String[] l = timestamp.split(".");
//            System.out.println("timestamp\t"+timestamp+"\t"+l.length);
//            timestamp  = timestamp.split(".")[0];


            for (Map.Entry<String, Integer> entry : word2cnt.entrySet()) {
                cntSqlVal++;
                String word = entry.getKey();
                Integer cnt = entry.getValue();
                List<String> vals = new ArrayList<>();
                vals.add("\""+word+"\"");
                vals.add("\""+urlinfo.getHostName()+"\"");
                vals.add("" + urlinfo.getPortNo());
                vals.add("\""+urlinfo.getFilePath()+"\"");
                vals.add("" + cnt);
                vals.add("STR_TO_DATE(\""+ timestamp+"\",'%Y-%m-%d %H:%i:%s')");
//                vals.add("UNIX_TIMESTAMP(\""+ timestamp+"\")");
//                vals.add(null);

                query += "(" + String.join(",", vals) + "),";
            }
            if ((1 <<byte_len) <=cntSqlVal) {//yezheng: large batch process
                cntSqlVal = 0;
//                Connection conn = getRemoteConnection();
                try {
                    query = query.substring(0, query.length() - 1);
                    query += " ON DUPLICATE KEY UPDATE count=VALUES(count), date_created=VALUES(date_created)";
//                    System.out.println(query);

                    PreparedStatement preparedStmt = conn.prepareStatement(query);
                    preparedStmt.execute();
//                    System.out.println("[Done] " + query + "\t" + cntUrl);
                    System.out.println("[Done] sql injection success\t"+cntUrl);
                    query = " INSERT INTO invertedIndex (word, hostname, portNo, filepath, count,date_created)"
                            + " VALUES";
                } catch (Exception e1) {
                    e1.printStackTrace();
                    query = " INSERT INTO invertedIndex (word, hostname, portNo, filepath, count,date_created)"
                            + " VALUES";//debug
                }

            }

        }
        if (false && 0!=cntSqlVal) {//yezheng: large batch process
            cntSqlVal = 0;
//            Connection conn = getRemoteConnection();
            try {
                query = query.substring(0, query.length() - 1);
                query += " ON DUPLICATE KEY UPDATE count=VALUES(count), date_created=VALUES(date_created)";
//                    System.out.println(query);

                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.execute();
//                    System.out.println("[Done] " + query + "\t" + cntUrl);
                System.out.println("[Done] sql injection success\t"+cntUrl);
                query = " INSERT INTO invertedIndex (word, hostname, portNo, filepath, count,date_created)"
                        + " VALUES";
            } catch (Exception e1) {
                e1.printStackTrace();
                query = " INSERT INTO invertedIndex (word, hostname, portNo, filepath, count,date_created)"
                        + " VALUES";//debug
            }
//            try {
//                conn.close();
//            } catch (Exception e2) {
//                e2.printStackTrace();
//            }
        }

        try {
            id2urlquery = id2urlquery.substring(0, id2urlquery.length() - 1);
            id2urlquery += " ON DUPLICATE KEY UPDATE id=VALUES(id), date_created=VALUES(date_created)";
            System.out.println("[Done] sql injection success id2url\t"+id2urlquery);


            PreparedStatement preparedStmt = conn.prepareStatement(id2urlquery);
            preparedStmt.execute();
//                    System.out.println("[Done] " + query + "\t" + cntUrl);


        } catch (Exception e1) {
            e1.printStackTrace();

        }
        try {
            conn.close();
        } catch (Exception e2) {
            e2.printStackTrace();
        }
    }
}
