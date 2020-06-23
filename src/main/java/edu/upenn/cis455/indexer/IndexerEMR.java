package edu.upenn.cis455.indexer;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Iterator;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
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


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import java.sql.*;
import java.util.Calendar;
import edu.upenn.cis455.URLInfo;
import java.io.FileWriter;

//import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer;
//import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
//import org.deeplearning4j.text.tokenization.tokenizerfactory.UimaTokenizerFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

//https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.jsoup.nodes.Element;

public class IndexerEMR {
    static Logger logger = Logger.getLogger(Indexer.class);
    static URLInfo urlinfo = null;
    static String query = " INSERT INTO invertedIndex (word, hostname, portNo, filepath, count,date_created)"
            + " VALUES (?, ?, ?, ?,?,?) ON DUPLICATE KEY UPDATE count=?, date_created=?";

    public static class MapReducer extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String preword = "";
            //            https://jsoup.org/cookbook/extracting-data/attributes-text-html
            Document doc = Jsoup.parse(value.toString());

//            if (cntUrl<=11235){
//                continue;
//            }
            String text = "";
            for (Element p:doc.select("p")){
                text+= " "+p.text();
            }


            for (char c : text.toCharArray()) {
                if (Character.isLetter(c) || c == '-') {
                    preword += c;
                } else if (0 != preword.length()) {
                    preword = preword.toLowerCase();
                    word.set(preword);
                    output.collect(word, one);
                }
                preword = "";
            }
        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }



            Calendar calendar = Calendar.getInstance();
            java.sql.Timestamp timestamp = new Timestamp(System.currentTimeMillis());
//2016-11-16 06:43:19.77;
//            https://docs.oracle.com/javase/7/docs/api/java/util/StringTokenizer.html
//            try{
//                // create the mysql insert preparedstatement
//                String word = key.toString();
//                PreparedStatement preparedStmt = conn.prepareStatement(IndexerEMR.query);
//                preparedStmt.setString(1, word);
//                preparedStmt.setString(2, IndexerEMR.urlinfo.getHostName());
//                preparedStmt.setInt(3, IndexerEMR.urlinfo.getPortNo());
//                preparedStmt.setString(4, IndexerEMR.urlinfo.getFilePath());
//                preparedStmt.setInt(5, sum);
//                preparedStmt.setTimestamp(6, timestamp);
//                preparedStmt.setInt(7, sum);
//                preparedStmt.setTimestamp(8, timestamp);
////                    System.out.println("[preparedStmt]" + preparedStmt);
//                preparedStmt.execute();
//            }catch(Exception e){e.printStackTrace();}
//
//
//
//
//
//
//
//            output.collect(key, new IntWritable(sum));
//                    try {
//                    conn.close();
//                    } catch (Exception e2) {
//                    e2.printStackTrace();
//                    }

        }
    }



    private static Connection getRemoteConnection() {


//        if (System.getenv("RDS_HOSTNAME") != null) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String dbName = "indexer";//System.getenv("RDS_DB_NAME");
            String userName = "cis555";//System.getenv("RDS_USERNAME");
            String password = "cis555cis555";//System.getenv("RDS_PASSWORD");
            String hostname = "indexer-db-m4-xlarge.cqkrw7o8cny6.us-east-1.rds.amazonaws.com";// System.getenv("RDS_HOSTNAME");
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
//        https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
        BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAJZOB4XEN67XQP22Q",
                "dPB1A0eQjJqJ+4Ge82l0GTl+nB1naKUoPQAGAUfq");
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
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
//    private static void generateFile(String filename, String stringToWrite) {
//        String outputFile = "trash/" +filename;
//        try {
//            FileWriter writer = new FileWriter(outputFile);
//            writer.append(stringToWrite);
//            writer.flush();
//            writer.close();
//            logger.debug("New File is generated ==>" + outputFile);
//        } catch (Exception exp) {
//            logger.error("Exception in generateFile ", exp);
//            exp.printStackTrace();
//        }s
//    }



    public static void main(String[] args) throws IOException, ClassNotFoundException {
        byte[] bytes = "test msg".getBytes();
        String bucketName = "urlcontents";
        Regions clientRegion = Regions.US_WEST_1;
        List<String> allurls = getAllUrls(bucketName, clientRegion);
        Connection conn  = getRemoteConnection();



        int cntUrl = 0;
        for (String url : allurls) {
            cntUrl++;
//            if (cntUrl<=42){
//                continue;
//            }

            //-------




            JobConf conf = new JobConf(IndexerEMR.class);
            conf.setJobName("indexerEMR");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);
            conf.setMapperClass(WordCountEMR.Map.class);
            conf.setCombinerClass(WordCountEMR.Reduce.class);
            conf.setReducerClass(WordCountEMR.Reduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
//            s3n://<location_of_text_file>/Your_text_file
            FileInputFormat.setInputPaths(conf, "s3://"+bucketName+"/"+DigestUtils.sha1Hex(url));
//            FileOutputFormat.setOutputPath(conf, new Path(args[1]));
            JobClient.runJob(conf);
            System.out.println("[Done] " + IndexerEMR.urlinfo.getHostName() + "\t" + IndexerEMR.urlinfo.getFilePath()+"\t" + cntUrl);

        }

//            https://alvinalexander.com/java/java-mysql-insert-example-preparedstatement/



//get the whole list of tokens
//            List<String> tokens = tokenizer.getTokens();



    }
}