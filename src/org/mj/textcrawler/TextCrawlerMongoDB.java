/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mj.textcrawler;

import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;



import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;


/**
 * 
 * Crawling Web Text using crawler4j
 * 
 * @author Jun
 *
 */
public class TextCrawlerMongoDB extends WebCrawler {
	
	private static final Logger logger = LoggerFactory.getLogger(TextCrawlerMongoDB.class);
	
	private static final Pattern IMAGE_EXTENSIONS = Pattern.compile(".*\\.(bmp|gif|jpg|png)$");
	
	private static final String INDEX_DB_NAME = "WebCrawlerIndexDB";
	private static final String URL_DB_NAME = "OutgoingUrlDB";
	private static final String DOC_DB_NAME = "DocUrlDB";
	
	private MongoClient mongoClient;
	private MongoDatabase indexDB, outgoingUrlDB, docIdUrlDB;

	
	protected final Object mutex = new Object();
	/*
	 * (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#onStart()
	 */
	@Override
	// TO-DO add index
	public void onStart() {
		
		
		try {
			
			mongoClient = new MongoClient();
			indexDB = mongoClient.getDatabase(INDEX_DB_NAME);
			outgoingUrlDB = mongoClient.getDatabase(URL_DB_NAME);
			docIdUrlDB = mongoClient.getDatabase(DOC_DB_NAME);
			
			logger.info("Successfully initialized two databases.");
			//txnIndexDB = indexDBEnv.beginTransaction(null, null);
			//txnOutgoingUrlDB = outgoingDBEnv.beginTransaction(null, null);
			
		} catch (Exception dbe) {
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
			mongoClient.close();
			
		} catch (Exception e) {
			logger.error("Error while shutting down MongoDB Client.");
			e.printStackTrace();
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
	public void visit(Page page) {
		
		/*
		 * crawl for search engine repository. 
		 * 
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
			
			String[] words = text.split("\\W");
			for (int i = 0; i < words.length; i++) {
				// TO-DO skip nonsense words
				String word = words[i];
				if (word.endsWith(".")) {
					word = word.substring(0, word.length()-1);
				}
				if (!wordCountMap.containsKey(word)) {
					wordCountMap.put(word, 1);
				} else {
					wordCountMap.put(word, wordCountMap.get(word)+1);
				}	
			}
			
			
			// Index DB dump
			synchronized(mutex) {
				try {
					
					indexDB.getCollection("DocId_WordCount").insertOne(new Document().append("doc_id", docid)
							.append("word_count", Arrays.asList()));
					for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
						indexDB.getCollection("DocId_WordCount").updateOne(new Document("doc_id", docid), 
								new Document("$push", new Document("word_count", new Document(entry.getKey(), entry.getValue()))));
					}
							
					
				} catch (Exception e) {
					logger.error("Error while working with indexDB, {}", e.getMessage());
					throw e;
				}
			}
			
			
			
			
			for (WebURL outgoingUrl : links) {
				linkDocId.add(outgoingUrl.getDocid());
			}
			
			
			// outgoing Url DB dump
			synchronized(mutex) {
				try {
					
					outgoingUrlDB.getCollection("DocId_LinkDocId").insertOne(new Document().append("doc_id", docid)
							.append("link_docId", Arrays.asList()));
					for (int url_item : linkDocId) {
						outgoingUrlDB.getCollection("DocId_LinkDocId").updateOne(new Document("doc_id", docid),
								new Document("$push", new Document("link_docId", url_item)));
					}
					
				} catch (Exception e) {
					logger.error("Error while working with outgoingDB, {}", e.getMessage());
					throw e;
				}
			}
			
			
			// docId DB dump
			synchronized(mutex) {
				try {
					
					docIdUrlDB.getCollection("DocId_Url").insertOne(new Document().append("doc_id", docid)
							.append("url", url));
					
				} catch (Exception e) {
					logger.error("Error while working with docIdUrlDB, {}", e.getMessage());
					throw e;
				}
			}
			
			
		}
	}
}
