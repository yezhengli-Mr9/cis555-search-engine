package edu.upenn.cis455.indexer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
//https://github.com/awslabs/rds-support-tools/blob/master/mysql/diag/shell/RDSDatabaseConnection.java
import java.sql.*;
public class WordCount {
    static Logger logger = Logger.getLogger(WordCount.class);
//    https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/java-rds.html
    private static Connection getRemoteConnection() {

//        if (System.getenv("RDS_HOSTNAME") != null) {
            try {
                Class.forName("org.mysql.Driver");
                String dbName = "indexer";//System.getenv("RDS_DB_NAME");
                String userName = "cis555";//System.getenv("RDS_USERNAME");
                String password = "cis555cis555";//System.getenv("RDS_PASSWORD");
                String hostname ="indexer.cqkrw7o8cny6.us-east-1.rds.amazonaws.com";// System.getenv("RDS_HOSTNAME");
                String port = System.getenv("RDS_PORT");
                String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password;
                logger.trace("Getting remote connection with connection string from environment variables.");
                Connection con = DriverManager.getConnection(jdbcUrl);
                logger.info("Remote connection successful.");
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
//    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
//        private final static IntWritable one = new IntWritable(1); private Text word = new Text();
//        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { String line = value.toString();
//            StringTokenizer tokenizer = new StringTokenizer(line);
//            while (tokenizer.hasMoreTokens()) {
//                word.set(tokenizer.nextToken());
//                output.collect(word, one); }
//        } }
//    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
//        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
//            int sum = 0;
//            while (values.hasNext()) {
//                sum += values.next().get(); }
//            output.collect(key, new IntWritable(sum)); }
//    }
    public static void main(String[] args) throws Exception {
//        JobConf conf = new JobConf(WordCountEMR.class);
//        conf.setJobName("wordcount");
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(IntWritable.class);
//        conf.setMapperClass(WordCountEMR.Map.class);
//        conf.setCombinerClass(WordCountEMR.Reduce.class);
//        conf.setReducerClass(WordCountEMR.Reduce.class);
//        conf.setInputFormat(TextInputFormat.class);
//        conf.setOutputFormat(TextOutputFormat.class);
//        FileInputFormat.setInputPaths(conf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//        JobClient.runJob(conf);
        //-----
        getRemoteConnection();
    }
}
