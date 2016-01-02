package org.mj.textcrawler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;


public class Controller {
	
	private static final Logger logger = LoggerFactory.getLogger(Controller.class);
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			logger.info("Need parameters: ");
			logger.info("\t rootFolder (for intermediate crawl data)");
			logger.info("\t numberOfCrawlers (number of concurrent threads)");
			return;
		}
		
		String crawlStorageFolder = args[0];
		int numberOfCrawlers = Integer.parseInt(args[1]);
		
		CrawlConfig config = new CrawlConfig();
		
		config.setCrawlStorageFolder(crawlStorageFolder);
		config.setPolitenessDelay(1000);
		config.setMaxDepthOfCrawling(2);
		config.setMaxPagesToFetch(-1);
		config.setIncludeBinaryContentInCrawling(false);
		config.setResumableCrawling(true);
		
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);
		
		//controller.addSeed("https://en.wikipedia.org/wiki/Almirante_Latorre-class_battleship");
		controller.addSeed("http://www.ics.uci.edu/");
	    controller.addSeed("http://www.ics.uci.edu/~lopes/");
	    controller.addSeed("http://www.ics.uci.edu/~welling/");
		
		controller.start(TextCrawlerMongoDB.class, numberOfCrawlers);
		
	}

}













