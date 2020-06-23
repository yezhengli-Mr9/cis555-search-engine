package edu.upenn.cis455.frontend;

import static spark.Spark.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class XPathApp {
    final static int resultsPerPage = 10;
    
    public static void main(String args[]) throws IOException {
        port(2019);
        String mainPage = readFile("mainPage.html", StandardCharsets.UTF_8);
        String resultsPageCSS = readFile("resultsPage.html", StandardCharsets.UTF_8);
        
        //adding example URLs
        
        get("/", (request, response) -> {
            return mainPage;
        });

        post("/redirect", (request, response) -> {
            String query = request.queryParams("query");
            // get links for query here
            
            // here should go all the fetching work
            List<String> results = SearchHelper.SearchByKeywords(query);
            List<String> cache = SearchHelper.getCachedPages(results, 1);
            System.out.println(results);
            request.session().attribute("query", query.replaceAll("\\s+", "+"));
            request.session().attribute("urls", results);
            request.session().attribute("page1", cache);
            response.redirect("/" + query.replaceAll("\\s+", "+") + "/page/1");
            return null;
        });

        get("/:query/page/:pageNum", (request, response) -> {
        	String query = request.params(":query");
        	String queryInSession = request.session().attribute("query");
        	if (queryInSession == null || !query.equals(queryInSession)) {
        		response.status(403);
        		return "<html><body>Query corrupted error: please search through <a><href=\"/\">main page</a></body></html>.";
        	}
            String page = resultsPageCSS;
            List<String> urls = request.session().attribute("urls");
            int pageNum;
            try{
            	pageNum = Integer.parseInt(request.params(":pageNum"));
            	if (pageNum <= 0 || pageNum > ((urls.size() + resultsPerPage-1) / resultsPerPage))
            		throw new Exception();
            } catch (Exception e) {
            	response.status(404);
            	return null;
            }
            List<String> cache = request.session().attribute("page"+pageNum);
           
            //topnav
            page += "<body>"+
            "<div class=\"topnav\">" +
            "<a href=\"/\">G18 Search</a>" +
            "<form action=\"/redirect\" method=\"POST\">" +
            "   <input type=\"text\" placeholder=\""+query+"\" id=\"query\" name=\"query\">" +
            "</form>" +
            "</div>" + 
            "<div class=\"row\">" +
            "<div class=\"leftcolumn\">";
            
            int i = 0;
            int id = (pageNum-1) * resultsPerPage + i;

            if (cache == null) {
            	cache = SearchHelper.getCachedPages(urls, pageNum);
            	request.session().attribute("page"+pageNum, cache);
            }
            
            //System.out.println(cache);

            while (i < resultsPerPage && id < urls.size()) {
            	if (id >= urls.size() || urls.get(id) == null || i>=cache.size() ||cache.get(i)==null) {
            		// do something that is needed for this condition!!
            		i++;
            		id = (pageNum-1)*resultsPerPage+i;
            		continue;
            	} else {
                String URL = urls.get(id);
                Document doc = Jsoup.parse(cache.get(i));
                String title = doc.title();
                String paragraph = "";
                Elements paragraphs = doc.select("p");
                for(Element p : paragraphs) {
                    String[] words = paragraph.split("\\s");
                    int newWords = 0;
                    String[] pageWords = p.text().split("\\s");
                    while (words.length+newWords <= 140 && newWords < pageWords.length) {
                        paragraph += pageWords[newWords] + " ";
                        newWords++;
                    }
                }
                paragraph += "...";
                String html = "<div class=\"card\">" +
                "<h5><a href=\""+URL+"\">"+URL+"</a></h5>" +
                "<h2><a href=\""+URL+"\">"+title+"</a></h2>" +
                "<p>"+paragraph+"</p>" +"</div>";
                page += html;
                i++;
                id = (pageNum-1)*resultsPerPage+i;
            	}
            }
            
            //pagination
            int pages = urls.size()/resultsPerPage + 1;
            page += "</div></div>";
            page += "<div class=\"pagination\">";
            if (pageNum > 1) {
                page += "<a href=\"/"+query+"/page/"+(pageNum-1)+"\">&laquo;</a>";
            }
            for (int j = 1; j <= pages; j++) {
                if(j == pageNum) {
                    page += "<a class=\"active\" href=\"/"+query+"/page/"+j+"\">"+j+"</a>";
                } else {
                    page += "<a href=\"/"+query+"/page/"+j+"\">"+j+"</a>";
                }
            }
            if (pageNum < pages) {
                page += "<a href=\"/"+query+"/page/"+(pageNum+1)+"\">&raquo;</a>";
            }
            page += "</div></body></html>";
            
            return page;
        });
    }
    
    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}