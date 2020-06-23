package edu.upenn.cis455.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import edu.upenn.cis455.crawler.Crawler;
import edu.upenn.cis455.frontend.WebApp;
import edu.upenn.cis455.indexer.Indexer;
import edu.upenn.cis455.pagerank.PageRank;

public class UtilsTest {
    @Before
    public void setUp() throws Exception {
    }



    @Test
    public void CrawlerTest() {
        Crawler crawler = new Crawler();
        }




    @Test
    public void FrontEndTest() {
        WebApp xpathapp = new WebApp();
    }

    @Test
    public void IndexerTest() {
        Indexer indexer = new Indexer();

    }


    @Test
    public void PageRankTest() {
        PageRank pagerank = new PageRank();
    }
}