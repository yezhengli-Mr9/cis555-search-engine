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


import opennlp.tools.stemmer.PorterStemmer;
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
public class Indexer {
    static Logger logger = Logger.getLogger(Indexer.class);
    static PorterStemmer stem = new PorterStemmer();

    private static Connection getRemoteConnection() {

//        if (System.getenv("RDS_HOSTNAME") != null) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String dbName = "indexer";//System.getenv("RDS_DB_NAME");
            String userName = "cis555";//System.getenv("RDS_USERNAME");
            String password = "cis555cis555";//System.getenv("RDS_PASSWORD");
            String hostname ="r5-xlarge-indexer.cqkrw7o8cny6.us-east-1.rds.amazonaws.com";
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

    public static byte[] downloadFromS3REST(String bucketName, String url) {
        // @return S3 Object by key
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

    public static List<String> getAllUrlsREST(String bucketname, Regions clientRegion) throws SQLException {
        String JDBC_URL = "jdbc:mysql://urlinks.crpnw2fluwdp.us-east-1.rds.amazonaws.com:3306/links";
        String MASTER_USER = "admin";
        String MASTER_PSW = "admin4links";
        Connection conn = null;
        List<String> urls = new ArrayList<String>();
        if (conn == null)
            conn = DriverManager.getConnection(JDBC_URL, MASTER_USER, MASTER_PSW);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT url FROM links.sha2url ORDER BY retrived_time,url ASC");
        while (rs.next()) {
            urls.add(rs.getString("url"));
        }
        return urls;
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
    private static void insertQuery(String query, String id2urlquery, Connection conn, int cntUrl, int cntSqlVal_ud2url, int basecntUrl){
        try {

            query = query.substring(0, query.length() - 1);
            query += " ON DUPLICATE KEY UPDATE count=VALUES(count)";
//                    System.out.println(query);
            PreparedStatement preparedStmt = conn.prepareStatement(query);
            preparedStmt.execute();
//                    Saystem.out.println("[Done] " + query + "\t" + cntUrl);
            System.out.println("[Done] sql injection success\t"+(cntUrl-basecntUrl));

        } catch (Exception e1) {
            e1.printStackTrace();
            System.out.println(conn+"[Fail] sql injection fail\t"+(cntUrl-basecntUrl));

        }


        try {
            id2urlquery = id2urlquery.substring(0, id2urlquery.length() - 1);
            id2urlquery += " ON DUPLICATE KEY UPDATE hostname=VALUES(hostname), filepath=VALUES(filepath),portNo=VALUES(portNo), date_created=VALUES(date_created)";



            PreparedStatement preparedStmt = conn.prepareStatement(id2urlquery);
            preparedStmt.execute();
            System.out.println(conn+"[Done] sql injection success cntUrl\t" + (cntUrl - basecntUrl)+"\tcntSqlVal_ud2url\t"+cntSqlVal_ud2url);
//                    System.out.println("[Done] " + query + "\t" + cntUrl);


        } catch (Exception e1) {
            e1.printStackTrace();
            System.out.println("[Fail] sql injection fail cntUrl\t" + (cntUrl - basecntUrl)+"\tcntSqlVal_ud2url\t"+cntSqlVal_ud2url);

        }

    }
    private static String stemWord(String word){
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
        String target = Indexer.stem.stem(newword);
        return target;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        byte[] bytes = "test msg".getBytes();
        String bucketName = "urlcontents-cis455g18";//"crawled-url";//"urlcontents";//
        String query = " INSERT INTO invertedIndexStemmed (word, urlid, count)"
                + " VALUES";
        Regions clientRegion = Regions.US_WEST_1;
        List<String> allurls = new ArrayList<>();
        try {
            allurls = getAllUrlsREST(bucketName, clientRegion);
        }catch(Exception e){e.printStackTrace();}
        int basecntUrl = 132218+1;
        int cntUrl = basecntUrl,cntSqlVal = 0,cntSqlVal_ud2url=0, byte_len = Integer.parseInt(args[2]);

        int start = Integer.parseInt(args[0]), end = Integer.parseInt(args[1]);
        int channel = Integer.parseInt(args[2]);
        Connection conn = getRemoteConnection();
        System.out.println("[indexer main] allurls.size()\t" + allurls.size());
        String id2urlquery = " INSERT INTO id2url (id, hostname, portNo, filepath,date_created, max_count)"
                + " VALUES";

        for (String url : allurls) {

            cntUrl++;
            if (cntUrl - basecntUrl <= start+channel ) {//+126000
                continue;
            }
            if (cntUrl - basecntUrl> end +channel) {//+126000
                break;
            }
            URLInfo urlinfo = new URLInfo(url);



            byte[] b = null;
            //-------
            try {
                b = downloadFromS3REST(bucketName, url); }catch(Exception e){e.printStackTrace();continue;}
//            https://jsoup.org/cookbook/extracting-data/attributes-text-html
            Document doc = Jsoup.parse(new String(b, "UTF-8")); //migbase64);
//            String text = doc.body().text() +" "+ doc.title(); // "An example link"
            String text = "";
            for (Element p : doc.select("p")) {
                text += " " + p.text();
            }

            Map<String, Integer> word2cnt = new LinkedHashMap<>();


            //---------
            //yezheng too slow!!!
            String preword = "";
            for (char c : text.toCharArray()) {
                if (Character.isLetter(c) || c == '-') {
                    preword += c;
                } else if (0 != preword.length()) {
                    preword = preword.toLowerCase();
                    preword = stemWord(preword);
                    if (preword == null){
                        preword = "";
                        continue;
                    }
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

            int maxCount = 0;
            for (Map.Entry<String, Integer> entry : word2cnt.entrySet()) {
                cntSqlVal++;
                String word = entry.getKey();
                Integer cnt = entry.getValue();
                if (cnt>maxCount){maxCount = cnt;}
                List<String> vals = new ArrayList<>();
                vals.add("\""+word+"\"");
                vals.add(""+cntUrl);
                vals.add("" + cnt);

                query += "(" + String.join(",", vals) + "),";
            }


            //
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
            vals0.add(""+maxCount);
            id2urlquery += "(" + String.join(",", vals0) + "),";
//            System.out.println(cntSqlVal+url);
            {
                cntSqlVal_ud2url ++;

            }

            if ((1 <<byte_len) <=cntSqlVal) {//yezheng: large batch process
                cntSqlVal = 0;
                insertQuery(query, id2urlquery, conn, cntUrl , cntSqlVal_ud2url, basecntUrl);

                query = " INSERT INTO invertedIndexSmall (word, urlid, count)"
                        + " VALUES";
                id2urlquery = " INSERT INTO id2url (id, hostname, portNo, filepath,date_created)"
                        + " VALUES";

            }

        }
        if (0!=cntSqlVal) {//yezheng: large batch process
            insertQuery(query,id2urlquery, conn,  cntUrl , cntSqlVal_ud2url, basecntUrl);
        }


        try {
            conn.close();
        } catch (Exception e2) {
            e2.printStackTrace();
        }

    }
}
