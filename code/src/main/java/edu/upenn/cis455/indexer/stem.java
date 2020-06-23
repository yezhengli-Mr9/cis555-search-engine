package edu.upenn.cis455.indexer;

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

import java.util.Date;
import org.apache.log4j.Logger;
import java.sql.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class stem {
    static Logger logger = Logger.getLogger(edu.upenn.cis455.indexer.Indexer.class);
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
    public static void main(String[] args) {//throws IOException, ClassNotFoundException
        String JDBC_URL = "jdbc:mysql://r5-xlarge-indexer.cqkrw7o8cny6.us-east-1.rds.amazonaws.com:3306/indexer";
        String MASTER_USER = "cis555";
        String MASTER_PSW = "cis555cis555";
        List<String> wordList = new ArrayList<>();
        Connection conn = null;
        try {
            if (conn == null)
                conn = DriverManager.getConnection(JDBC_URL, MASTER_USER, MASTER_PSW);
            Statement st = conn.createStatement();
        }catch(Exception e){e.printStackTrace();}
        Statement st = null;

        try{
            st = conn.createStatement();
        }catch(Exception e){e.printStackTrace();}
        ResultSet rs = null;
        try{
            rs = st.executeQuery("SELECT DISTINCT word FROM invertedIndexSmall ORDER BY word ASC");
            while (rs.next()) {
                wordList.add(rs.getString("word"));
            }
        }catch(Exception e){e.printStackTrace();}

        System.out.println(wordList.size());

        //------------------
        byte[] bytes = "test msg".getBytes();
        String bucketName = "urlcontents-cis455g18";//"crawled-url";//"urlcontents";//
        Regions clientRegion = Regions.US_WEST_1;
        String query = " INSERT INTO stemmer (raw,target) VALUES";
//        https://www.codota.com/code/java/classes/opennlp.tools.stemmer.PorterStemmer
        PorterStemmer stem = new PorterStemmer();
//        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Date start = new Date();
        int cnt =0,cntAll=0;


        try{
            for(String word:wordList){
                cntAll++;
//                if (cntAll>=10){break;}
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
                    System.out.println("[word]\t"+word+"\t[newword]\t"+newword+"\t[idx]\t"+idx);
//                    System.exit(1);
                    continue;
                }
                try {
                    newword = newword.substring(0, idx+1);
                }catch(Exception e2){e2.printStackTrace();
                System.out.println("[word]\t"+word+"\t[newword]\t"+newword+"\t[idx]\t"+idx);
                System.exit(1);}
                String target = stem.stem(newword);
//                if (word!=target) {
                    cnt++;
                    query += "(\"" + word + "\",\"" + target + "\"),";
                    if (0 == cnt % (1 << 10)) {
//                        System.out.print(query);
                        System.out.println("[word]\t"+word+"\t[newword]\t"+newword+"\t[target]\t"+target+"\t[idx]\t"+idx+"\t[cntAll]\t"+cntAll+"/"+wordList.size());

                        //Waiting for 10 seconds
                        Date end = new Date();
                        long diff = end.getTime() - start.getTime();
                        String TimeTaken = String.format("[%s] hours : [%s] mins : [%s] secs",
                                Long.toString(TimeUnit.MILLISECONDS.toHours(diff)),
                                TimeUnit.MILLISECONDS.toMinutes(diff),
                                TimeUnit.MILLISECONDS.toSeconds(diff));
                        System.out.println(cnt + "\t" + String.format("Time taken %s", TimeTaken));


                        query = query.substring(0, query.length() - 1);
                        query += " ON DUPLICATE KEY UPDATE target=VALUES(target)";


                        try{
                            PreparedStatement preparedStmt = conn.prepareStatement(query);
                            preparedStmt.execute();
//                    Saystem.out.println("[Done] " + query + "\t" + cntUrl);
                            System.out.println("[Done] sql injection success\t"+cnt );

                        }catch(Exception e){e.printStackTrace();}


                        query = " INSERT INTO stemmer (raw,target) VALUES";

                    }
//                }
            }

        }catch(Exception e){e.printStackTrace(); }




        try {
            conn.close();
        } catch (Exception e2) {
            e2.printStackTrace();
        }



    }
}
