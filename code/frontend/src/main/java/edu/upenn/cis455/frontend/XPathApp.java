package edu.upenn.cis455.frontend;

import static spark.Spark.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class XPathApp {
    final static int resultsPerPage = 10;
    
    public static void main(String args[]) throws IOException {
    	if (args.length == 1)
    		try {
    			port(Integer.parseInt(args[0]));
    		} catch (Exception e) {
    			port(8080);
    		}
    	else port(8080);
        String mainPage = readFile("mainPage.html", StandardCharsets.UTF_8);
        String resultsPageCSS = readFile("resultsPage.html", StandardCharsets.UTF_8);
        
        //adding example URLs
        
        get("/", (request, response) -> {
            return mainPage;
        });

        post("/redirect", (request, response) -> {
            String query = request.queryParams("query");
            for (char c : query.toLowerCase().toCharArray()) {
    			if (c==' ') continue;
    			if ((c >= 48 && c <= 57) || (c >=97 && c<=122))
    				continue;
    			//System.out.println(url+" Discarded!"+c);
    			response.redirect("/");
    			return null;
    		}
            // get links for query here
            query = query.replaceAll("\\s+", " ").trim();
            // here should go all the fetching work
            List<String> results = SearchHelper.SearchByKeywords(query);
            if (results.isEmpty()) {
            	response.redirect("/");
    			return null;
            }
            List<String> cache = SearchHelper.getCachedPages(results, 1);
            request.session().attribute("page1", cache);
//            List<String> cache = SearchHelper.getCachedPages(results);
//            List<String> tmp = new ArrayList<String>();
//            int page = 1;
//            for (int i = 0;i<cache.size();i++) {
//            	if (cache.get(i)==null) continue;
//            	tmp.add(cache.get(i));
//            	if (tmp.size()>=10) {
//            		request.session().attribute("page"+page, tmp);
//            		tmp = new ArrayList<String>();
//            		page ++;
//            	}  	
//            }
//            if (!tmp.isEmpty()) {
//            	request.session().attribute("page"+page, tmp);
//            }
            //System.out.println(results);
            request.session().attribute("query", query);
            request.session().attribute("urls", results);
            
            response.redirect("/" + query.replace(" ", "+") + "/page/1");
            return null;
        });

        get("/:query/page/:pageNum", (request, response) -> {
        	String oriquery = request.params(":query");
        	String query = oriquery.replace("+", " ");
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
            if (pageNum==1) {
            	for (int i = 1;i<=8;i++)
            	request.session().removeAttribute("page"+i);
            }
            List<String> cache = request.session().attribute("page"+pageNum);
            int pages = (urls.size() + resultsPerPage-1) / resultsPerPage;
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
            int shown = 0;
            String results = "";
//            String legacy = request.session().attribute("legacy");
//            if (legacy!=null) {
//            	shown += 3;
//            	results += legacy;
//            }
            
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
                int newWords = 0;
                Elements paragraphs = doc.select("p");
                for(Element p : paragraphs) {
                    String[] pageWords = p.text().split("\\s");
                    if (pageWords.length < 20) continue;
                    for (String word : pageWords) {
                    	paragraph += word + " ";
                    	newWords++;
                    	if (newWords >= 80) break;
                    }
                    if (newWords >= 80) break;
                }
                if (newWords < 80) {
                	i++;
            		id = (pageNum-1)*resultsPerPage+i;
            		continue;
                }
                shown ++;
                paragraph += "...";
                String html = "<div class=\"card\">" +
                "<h5><a href=\""+URL+"\">"+URL+"</a></h5>" +
                "<h2><a href=\""+URL+"\">"+title+"</a></h2>" +
                "<p>"+paragraph+"</p>" +"</div>";
                results += html;
                i++;
                id = (pageNum-1)*resultsPerPage+i;
            	}
            }
           // System.out.println(shown);
            if (shown < 1 && pageNum+1 <= pages) {
//            	int skipped = 0;
//            	if (request.session().attribute("skipped")!=null)
//            		skipped = Integer.parseInt(request.session().attribute("skipped"));
//            	request.session().attribute("skipped", String.valueOf(skipped+1));
            	//request.session().attribute("legacy", results);
            	response.redirect("/"+query.replace(" ", "+")+"/page/"+(pageNum+1));
            	return null;
            }
            page += results;
            //request.session().removeAttribute("legacy");
            
//            int skipped = 0;
//        	if (request.session().attribute("skipped")!=null)
//        		skipped = Integer.parseInt(request.session().attribute("skipped"));
//            request.session().removeAttribute("skipped");
//            if (skipped > 0) {
//            	pageNum -= skipped;
//            	pages -= skipped;
//            }
            //pagination
            
            page += "</div></div>";
            page += "<div class=\"pagination\">";
            if (pageNum > 1) {
                page += "<a href=\"/"+query.replace(" ", "+")+"/page/"+(pageNum-1)+"\">&laquo;</a>";
            }
            for (int j = 1; j <= pages; j++) {
                if(j == pageNum) {
                    page += "<a class=\"active\" href=\"/"+query.replace(" ", "+")+"/page/"+j+"\">"+j+"</a>";
                } else {
                    page += "<a href=\"/"+query.replace(" ", "+")+"/page/"+j+"\">"+j+"</a>";
                }
            }
            if (pageNum < pages) {
                page += "<a href=\"/"+query.replace(" ", "+")+"/page/"+(pageNum+1)+"\">&raquo;</a>";
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