package edu.upenn.cis455.pagerank;

import org.apache.spark.sql.execution.columnar.INT;
import scala.Tuple2;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class RDSWrapper {
    public static Connection conn = null;
    private static final int FAILURE = -1;
    private static final int SUCCESS = 0;

    // Hey try not execute DELETE, data WILL be gone

    // This is database for page rank, table name: "links.crawling"
    private static final String JDBC_URL = "jdbc:mysql://urlinks.crpnw2fluwdp.us-east-1.rds.amazonaws.com:3306/links";
    private static final String MASTER_USER = "admin";
    private static final String MASTER_PSW = "admin4links";

//     This is database for indexer, table name: "indexer.invertedIndex"
//     private static final String JDBC_URL = "jdbc:mysql://indexer-db-m4-xlarge.cqkrw7o8cny6.us-east-1.rds.amazonaws.com:3306/indexer";
//     private static final String MASTER_USER = "cis555";
//     private static final String MASTER_PSW = "cis555cis555";

    public synchronized static int executeUpdate(String sql) {
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(JDBC_URL, MASTER_USER, MASTER_PSW);
            } catch (SQLException  e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                conn = null;
                return FAILURE;
            }
        }
        if (conn != null) {
            try {
                Statement st = conn.createStatement();
                st.executeUpdate(sql);
                return SUCCESS;
            } catch (SQLException e) {
                e.printStackTrace();
                return FAILURE;
            }
        }
        return FAILURE;
    }

    public synchronized static ResultSet executeQuery(String sql) {
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(JDBC_URL, MASTER_USER, MASTER_PSW);
            } catch (SQLException  e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        }
        if (conn != null) {
            try {
                Statement st = conn.createStatement();
                return st.executeQuery(sql);
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }

    public static void showResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        System.out.print("COLUMNS: ");
        for (int i = 1; i <= columnsNumber; i++)
            System.out.print(rsmd.getColumnName(i) + "  ");
        System.out.println("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = resultSet.getString(i);
                System.out.print(rsmd.getColumnName(i) + " " + columnValue + " ");
            }
            System.out.println("");
        }
    }

    public static void writeWrapper(ResultSet resultSet, int index) throws IOException, SQLException {
        Writer writer = new FileWriter("links.csv", true);
        StringBuilder sb = new StringBuilder("");
        while (resultSet.next()) {
            sb.append(resultSet.getString(1) + "," + resultSet.getString(2) + "\n");
        }
        writer.append(sb);
        writer.close();
    }

    public static int getDbSize() throws SQLException {
        ResultSet rs = executeQuery("SELECT COUNT(*) FROM links.adj2");
        int size = 0;
        while (rs.next()){
            size = Integer.parseInt(rs.getString(1));
        }
        return size;
    }

    public static void dumpSQLData() throws IOException, SQLException {
        int dbSize = getDbSize();
        Writer writer = new FileWriter("links.csv", false);
        // save db to disk by execute sql queries
        for (int i = 1; i <dbSize;i += 1000000){
            System.out.println("start: " + i);
            String query = String.format("SELECT srcid, tarid FROM links.adj2 limit %d, %d", i, 1000000);
            ResultSet rs = executeQuery(query);
            writeWrapper(rs, i);
            rs.close();
        }
        writer.close();
    }

    public static void initRank(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        System.out.print("COLUMNS: ");
        for (int i = 1; i <= columnsNumber; i++)
            System.out.print(rsmd.getColumnName(i) + "  ");
        System.out.println("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = resultSet.getString(i);
                String query = String.format("INSERT INTO links.pagerank VALUE ('%s', 1)", columnValue);
                executeUpdate(query);
            }
            System.out.println("");
        }
    }

    public static void deleteRecords() throws Exception{
        List<String> extensions = new LinkedList<String>();

        // common image format
        extensions.add("png");
        extensions.add("jpeg");
        extensions.add("jpg");
        extensions.add("gif");
        extensions.add("tiff");
        extensions.add("psd");
        extensions.add("eps");
        extensions.add("raw");

        // Microsoft document format
        extensions.add("docx");
        extensions.add("doc");
        extensions.add("xlsx");
        extensions.add("xls");
        extensions.add("pptx");
        extensions.add("ppt");

        // Document format
        extensions.add("pdf");
        extensions.add("txt");
        extensions.add("tex");
        extensions.add("bib");
        extensions.add("dta");
        extensions.add("csv");
        extensions.add("tsv");
        extensions.add("db");
        extensions.add("mp3");
        extensions.add("mp4");

        // compressed format
        extensions.add("zip");
        extensions.add("7z");
        extensions.add("rar");


        for(int num=0; num<extensions.size(); num++)
        {
            System.out.println(extensions.get(num));
            String query = String.format("DELETE FROM links.crawling where tar like '%%.%s'", extensions.get(num));
            executeUpdate(query);
            query = String.format("DELETE FROM links.crawling where src like '%%.%s'", extensions.get(num));
            executeUpdate(query);
        }
    }

    public static void updateRecords() throws Exception{
        // insert two src id and tar id columns to the table
        String alterQuery = "ALTER TABLE links.crawling\n" + "  ADD src_id INT,\n" + "  ADD tar_id INT \n";
        executeUpdate(alterQuery);

        // update values of the id by left join
        String updateSrcQuery = "UPDATE links.crawling c LEFT JOIN links.url2id u ON c.src = u.tar\n" +" SET c.src_id = u.id";
        executeUpdate(updateSrcQuery);
        String updateTarQuery = "UPDATE links.crawling c LEFT JOIN links.url2id u ON c.tar = u.tar\n" +" SET c.tar_id = u.id";
        executeUpdate(updateSrcQuery);

        // delete rows where either of the id is missing, because these are the rows that we have filtered out (see delete record section above)

    }

    public static void createInsertQuery( List<Tuple2<Integer, Double>> output){
        // first clear the page ranks in the table, if there is any
        RDSWrapper.executeUpdate("DELETE FROM links.numrank");

        int count = 0;
        boolean toFlush = true;
        StringBuilder query = null;
        int totalQuery = output.size();
        for (Tuple2<Integer,Double> tuple : output) {
            if (count > 1000000){
                System.out.println("Insert " + count + "records: Total"+ totalQuery);
            }
            count += 1;
            if (toFlush){
                query = new StringBuilder("INSERT IGNORE INTO links.numrank VALUES ");
                toFlush = false;
            }
            String value = null;
            if (count % 10000 == 0 || count == totalQuery){
                // start batch insertion
                value = String.format("(%d, %4.3f) \n", tuple._1(), tuple._2());
                query.append(value);
                RDSWrapper.executeUpdate(query.toString());
                toFlush = true;
            }
            else{
                value = String.format("(%d, %4.3f), \n", tuple._1(), tuple._2());
                query.append(value);
            }

        }
    }

    public static void writeToDB() throws IOException, SQLException {
        ResultSet rs = executeQuery("SELECT DISTINCT(srcid) FROM links.adj2");
        HashSet<Integer> srcids = new HashSet<>();
        List<Tuple2<Integer, Double>> output = new LinkedList<>();
        while (rs.next()) {
            srcids.add(rs.getInt(1));
        }
        rs.close();
        BufferedReader br = new BufferedReader(new FileReader("numrank.txt"));
        try{
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                String[] record = line.split(",");
                if (srcids.contains(Integer.parseInt(record[0]))) {
                    output.add(new Tuple2<Integer, Double>(Integer.parseInt(record[0]), Double.parseDouble(record[1])));
                }
                line = br.readLine();
            }
        }
        finally {
            br.close();
        }
        createInsertQuery(output);
        System.out.println(output.size());
    }

    public static void main(String[] args) throws Exception {

//        executeUpdate("DELETE FROM links.adj WHERE srcid = 0 or tarid = 0");
//        ResultSet rs = executeQuery("SELECT * FROM links.adj limit 10");
//        ResultSet rs = executeQuery("SELECT * FROM links.url2id where id = 487233");
//        ResultSet rs = executeQuery("SELECT COUNT(srcid) as c FROM links.adj GROUP BY tarid ORDER BY c DESC limit 10");
//        ResultSet rs = executeQuery("SELECT srcid, tarid FROM links.adj limit 1001, 1000");
//         ResultSet rs = executeQuery("SELECT COUNT(*) FROM links.adj2");
//         showResultSet(rs);
//        ResultSet rs = executeQuery("SELECT Count(DISTINCT(srcid)) FROM links.adj2");
//        showResultSet(rs);
//        dumpSQLData();
//        writeToDB();
//        ResultSet rs = executeQuery("SELECT * FROM links.numrank ORDER BY rank DESC limit 100");
//        ResultSet rs = executeQuery("SELECT * FROM links.url2id limit 100");
//        showResultSet(rs);
//        SELECT DISTINCT t1.* FROM t1
//        INNER JOIN t2 ON t1.color = t2.color;
        //ResultSet  rs = executeQuery("SELECT * FROM links.numrank t1 INNER JOIN links.url2id t2 ON t1.urlid = t2.id ORDER BY t1.rank DESC LIMIT 1000");
//        ResultSet rs = executeQuery("SELECT COUNT(srcid) FROM links.adj GROUP BY tarid LIMIT 100");
        ResultSet  rs = executeQuery("SELECT COUNT(*) FROM links.numrank");
        showResultSet(rs);
//        writeToDB();
    }
}
