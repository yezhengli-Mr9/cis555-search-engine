package edu.upenn.cis455.pagerank;


import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;

public class ExperimentResults {

    // collect experiment results on pagerank algorithm

    // First, vary the input size (Number of edges in the link graph)
    // The set of values chosen {1000, 10000, 100000, 1000000, 10000000, 20000000, 30000000, 45000000}
    // The set of cores chosen 2 4 6
    // the metrics collected number of iterations for convergence, the running time

    public static void writeWrapper(ResultSet resultSet, int N) throws IOException, SQLException {
        String fname = String.format("%d.csv", N);
        Writer writer = new FileWriter(fname, true);
        StringBuilder sb = new StringBuilder("");
        while (resultSet.next()) {
            sb.append(resultSet.getString(1) + "," + resultSet.getString(2) + "\n");
        }
        writer.append(sb);
        writer.close();
    }

    public static void dumpSQLData(int N) throws IOException, SQLException {
        String fname = String.format("%d.csv", N);
        Writer writer = new FileWriter(fname, false);
        // save db to disk by execute sql queries
        if (N <= 1000000){
            String query = String.format("SELECT srcid, tarid FROM links.adj2 limit %d, %d", 1, N);
            ResultSet rs = RDSWrapper.executeQuery(query);
            writeWrapper(rs, N);
            rs.close();
        }
        else {
            for (int i = 1; i < N; i += 1000000) {
                String query = String.format("SELECT srcid, tarid FROM links.adj2 limit %d, %d", i, 1000000);
                ResultSet rs = RDSWrapper.executeQuery(query);
                writeWrapper(rs, N);
                rs.close();
            }
            writer.close();
        }
    }

    public static void prepareInput() throws IOException, SQLException {
        int[] inputSizes = new int[]{1000, 10000, 100000, 1000000, 10000000, 20000000, 30000000, 45000000};
        for(int i = 0; i < inputSizes.length; i++){
            System.out.println(inputSizes[i]);
            dumpSQLData(inputSizes[i]);
        }
    }

    public static void main(String[] args) throws Exception{
        prepareInput();
    }
}
