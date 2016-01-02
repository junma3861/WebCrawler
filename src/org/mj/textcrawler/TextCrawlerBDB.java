package org.mj.textcrawler;


import java.io.FileNotFoundException;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.DatabaseException;


import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.BasicDBObject;


import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

import java.io.File;

public class TextCrawlerBDB extends WebCrawler {
	
	private static final Logger logger = LoggerFactory.getLogger(TextCrawlerBDB.class);
	
	private static final Pattern IMAGE_EXTENSIONS = Pattern.compile(".*\\.(bmp|gif|jpg|png)$");
	
	private static final String INDEX_DB_NAME = "WebCrawlerIndexDB";
	private static final String URL_DB_NAME = "OutgoingUrlDB";
	
	private boolean resumable;
	private DatabaseConfig dbConfig;
	private Environment indexDBEnv, outgoingDBEnv;
	private Database indexDB, outgoingUrlDB;
	//private Transaction txnIndexDB,txnOutgoingUrlDB;

	String indexDBPath, outgoingDBPath;
	
	protected final Object mutex = new Object();
	/*
	 * (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#onStart()
	 */
	@Override
	public void onStart() {
		
		resumable = true;
		indexDBPath = "D:\\workspace\\java\\WebCrawler\\tmp\\indexDB";
		outgoingDBPath = "D:\\workspace\\java\\WebCrawler\\tmp\\outgoingDB";
		
		try {
			
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setTransactional(true);
			indexDBEnv = new Environment(new File(indexDBPath), envConfig);
			outgoingDBEnv = new Environment(new File(outgoingDBPath), envConfig);
			
			
			dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			dbConfig.setTransactional(resumable);
			dbConfig.setDeferredWrite(!resumable);
			
			indexDB = indexDBEnv.openDatabase(null, INDEX_DB_NAME, dbConfig);
			outgoingUrlDB = outgoingDBEnv.openDatabase(null, URL_DB_NAME, dbConfig);
			
			logger.info("Successfully initialized two databases.");
			//txnIndexDB = indexDBEnv.beginTransaction(null, null);
			//txnOutgoingUrlDB = outgoingDBEnv.beginTransaction(null, null);
			
		} catch (DatabaseException dbe) {
			shutDown();
			logger.error("Error while openining index or outgoingUrlDB database.");
			dbe.printStackTrace();			
			
		}
	}
	
	
	@Override
	public void onBeforeExit() {
		logger.info("Finishing.");
		shutDown();
	}
	
	/*
	 * 
	 */
	public void shutDown() {
		try {
			if (indexDB != null) {
				indexDB.close();
			}
		} catch (DatabaseException dbe) {
			logger.error("Error while shutting down the index database.");
			dbe.printStackTrace();
		}
		
		try {
			if (outgoingUrlDB != null) {
				outgoingUrlDB.close();
			}
		} catch (DatabaseException dbe) {
			logger.error("Error while shutting down the outgoingUrl database.");
			dbe.printStackTrace();
		}
		
		try {
			if (indexDBEnv != null) {
				indexDBEnv.close();
			}
			if (outgoingDBEnv != null) {
				outgoingDBEnv.close();
			}
			
			
		} catch (DatabaseException dbe) {
			dbe.printStackTrace();
		}
	}
	
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		
		if (IMAGE_EXTENSIONS.matcher(href).matches()) {
			return false;
		}
		
		return true;
		
	}
	

	@Override
	public void visit(Page page) throws DatabaseException {
		
		/*
		 * crawl for search engine repository. 
		 * 
		 * Index
		 * sleepycat db
		 * k = docId, v = HashMap<String, Integer>, where String as word, Integer as count
		 * 
		 * Link
		 * sleepycat db
		 * k = docId, v = HashSet<String>, String as docId of the outgoing urls. 
		 * */
		
		HashMap<String, Integer> wordCountMap = new HashMap<>();
		
		int docid = page.getWebURL().getDocid();
		String url = page.getWebURL().getURL();
		
		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			String text = htmlParseData.getText();
			//String html = htmlParseData.getHtml();
			Set<WebURL> links = htmlParseData.getOutgoingUrls();
			Set<Integer> linkDocId = new HashSet<Integer>();
			
			logger.info("URL: {}", url);
			
			String[] words = text.split(" ");
			for (int i = 0; i < words.length; i++) {
				// TO-DO skip nonsense words
				if (!wordCountMap.containsKey(words[i])) {
					wordCountMap.put(words[i], 1);
				} else {
					wordCountMap.put(words[i], wordCountMap.get(words[i])+1);
				}	
			}
			
			synchronized(mutex) {
				try {
					Transaction txnIndexDB = indexDBEnv.beginTransaction(null, null);
					DatabaseEntry key = new DatabaseEntry(Integer.toString(docid).getBytes());
					DatabaseEntry valueIndexDB = new DatabaseEntry(wordCountMap.toString().getBytes());
					//logger.info("{},{}", Integer.toString(docid), wordCountMap.toString());
					indexDB.put(txnIndexDB, key, valueIndexDB);
					txnIndexDB.commit();
					
				} catch (DatabaseException e) {
					logger.error("Error while working with indexDB, {}", e.getMessage());
					throw e;
				}
			}
			
			
			
			
			for (WebURL outgoingUrl : links) {
				linkDocId.add(outgoingUrl.getDocid());
			}
			
			
			synchronized(mutex) {
				try {
					
					DatabaseEntry key2 = new DatabaseEntry(Integer.toString(docid).getBytes());
					Transaction txnOutgoingUrlDB = outgoingDBEnv.beginTransaction(null, null);
					DatabaseEntry valueOutgoingUrlDB = new DatabaseEntry(linkDocId.toString().getBytes());
					//logger.info("{},{}", Integer.toString(docid), linkDocId.toString());
					outgoingUrlDB.put(txnOutgoingUrlDB, key2, valueOutgoingUrlDB);
					txnOutgoingUrlDB.commit();
				} catch (DatabaseException e) {
					logger.error("Error while working with outgoingDB, {}", e.getMessage());
					throw e;
				}
			}
			
			
		}
	}
	
}
