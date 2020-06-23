package edu.upenn.cis455.crawler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DBWrapper;

/** (MS1, MS2) The main class of the crawler.
  */
public class XPathCrawler {
	
  public static void main(String args[]) throws DatabaseException, FileNotFoundException
  {
      boolean sendUDP = false;
      String startPage = args[0];
      DBWrapper db = new DBWrapper(args[1]);
      int maxFileSize = Integer.parseInt(args[2]);
      int maxNumFiles = -1;
      String monitorHost = "";
      if(args.length > 3) {
          maxNumFiles = Integer.parseInt(args[3]);
      }
      InetAddress host = null;
      DatagramSocket s = null;
      if(args.length > 4) {
          sendUDP = true;
          try {
            host = InetAddress.getByName(args[4]);
            s = new DatagramSocket();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SocketException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
      }
      
      LinkedList<String> queue = new LinkedList<String>();
      Map<String, Long> delayTimes = new HashMap<String, Long>();
      SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      queue.add(startPage);
      
      while(queue.size() != 0 || queue.size() < maxNumFiles) {
          try {
          URI page = new URI(queue.poll());
            Socket connection = null;
            if(page.getScheme().equals("http")) {
                connection = new Socket(InetAddress.getByName(page.getHost()), page.getPort());
                
                // write request
                PrintWriter out = new PrintWriter(connection.getOutputStream());
                out.write("HEAD " + page.getPath() + " HTTP/1.1\r\n");
                out.write("User-Agent: cis455crawler\r\n");
                out.write("Accept: text/html,text/xml,application/xml\r\n");
                out.write("Connection: keep-alive\r\n");
                if(db.getTime(page.toString()) != null) {
                    long time = db.getTime(page.toString());
                    out.write("If-Modified-Since: " + dateFormat.format(new Date(time)) + "\r\n");
                }
                out.write("\r\n");
                out.flush();
                
                // read response
                InputStream in = connection.getInputStream();
                Map<String, String> headers = getHeaders(in);
                if(headers.get("status-code").equals("200") && headers.containsKey("content-length")) {
                    int numBytes = Integer.parseInt(headers.get("content-length"));
                    if(numBytes <= maxFileSize) {
                        out.write("GET " + page.getPath() + " HTTP/1.1\r\n");
                        out.write("User-Agent: cis455crawler\r\n");
                        out.write("Accept: text/html,text/xml,application/xml\r\n");
                        out.write("Connection: keep-alive\r\n");
                        if(db.getTime(page.toString()) != null) {
                            long time = db.getTime(page.toString());
                            out.write("If-Modified-Since: " + dateFormat.format(new Date(time)) + "\r\n");
                        }
                        out.write("\r\n");
                        out.flush();
                        
                        Map<String, String> headers2 = getHeaders(in);
                        byte[] body = null;
                        // reading body
                        if(headers2.containsKey("content-length")) {
                            int numBytes2 = Integer.parseInt(headers.get("content-length"));
                            body = new byte[numBytes2];
                            in.read(body);
                        }
                        
                        // store page in database
                        if(body.length > 0) {
                            db.addPage(page.toString(), body, new Date().getTime());
                        }
                        
                        // parse page
                        if(headers.containsKey("content-type") && headers.get("content-type").equals("text/html")) {
                            String html = new String(body, "UTF-8");
                            Document doc = Jsoup.parse(html);
                            Element content = doc.getElementById("content");
                            Elements links = content.getElementsByTag("a");
                            for (Element link : links) {
                              String url = link.absUrl("href");
                              queue.add(url);
                            }
                        }
                    }
                }
                
                in.close();
                out.close();
                connection.close();
            } else if (page.getScheme().equals("https")){
                
            }
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
      }
  }
  
  private static Map<String, String> getHeaders(InputStream in) throws IOException {
      String line = "";
      int lineNum = 0;
      int c;
      String endOfHeaders = "";
      Map<String, String> headers = new HashMap<String, String>();
      
      while((c = in.read()) > 0) { 
          if(c != 13 && c != 10) {   // only add to line if not CRLF
              line += (char) c;
              endOfHeaders = "";
          } else {
              endOfHeaders += c + ".";
          }
          if (c == 10) {  // 10 = LF
              // parse line that was just read
              if(lineNum == 0) {  // first line
                  String[] requestLine = line.split(" ");
                  headers.put("status-code", requestLine[0]);
                  headers.put("status-text", requestLine[1]);
              }
              // reading headers
                  String header = "", value = "";
                  Matcher m = Pattern.compile("([a-zA-Z-]+):\\s*(\\S.*\\S|\\S)\\s*").matcher(line);

                  if (m.find()) {
                      //System.out.println(m.group(1) + " | " + m.group(2));
                      header = m.group(1);
                      value = m.group(2);

                  } else { // check if line is part of previous header content
                      Matcher m2 = Pattern.compile("[ \t]+(\\S.*\\S|\\S)\\s*").matcher(line);

                      if (m2.find()) {
                          value += m2.group(1);
                      }
                  }
                  headers.put(header.toLowerCase(), value);
              line = "";
              lineNum++;
          }
          
          if (endOfHeaders.equals("13.10.13.10.")) {
              break;
          }
      }
      return headers;
  }
	
}
