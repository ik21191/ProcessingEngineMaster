package com.mps.data.feed.pageview;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mps.utils.MyLogger;

public class EnginePageView {
	JavaSparkContext jsc = null;
	public EnginePageView(JavaSparkContext jsc) {
		this.jsc = jsc;
	}
	 public Map<Document, Long> initializePageViewFilters(String uri, String pageViewDatabase, String pageViewCollection, String client) throws Exception{
	    	HashMap<String, String> readOverrides = new HashMap<String, String>();
	    	ReadConfig readConfig = null;
	    	double iTracker = 0.0;
	    	try {
	    		//************************ PAGE TYPE PATTERN LOGIC *************************
	    		//Get all page page patterns from log_page_types_memory table
	    		MyLogger.log("PageViewEngine : initializePageViewFilters() : Page View Filters : Start");
	    		iTracker = 1.0;
	    		String database = pageViewDatabase;
	            String collection = pageViewCollection;
	            iTracker = 2.0;
	            readOverrides.put("database", database);
	            readOverrides.put("collection", collection);
	            readOverrides.put("uri", uri);
	            iTracker = 3.0;
	            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
	            iTracker = 4.0;
	            MyLogger.log("Page View Filters ReadConfig : readOverrides : "+ readOverrides.toString());
	            MyLogger.log("Page View Filters ReadConfig : Done");
	            
	            iTracker = 5.0;
	            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDpageTypeAll : Start");
	            JavaRDD<Document> jMRDDpageViewFilterAll = MongoSpark.load(jsc,readConfig).filter(doc -> doc.getString("webmart_code").trim().equalsIgnoreCase(client));
	            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDpageViewFilterAll : Done");            
	       
	            MyLogger.log("mapPageType : jMRDDPageTypeAggregated.countByValue() : Start");
	            Map<Document, Long> mapPageViewFilter = jMRDDpageViewFilterAll.countByValue();
	            MyLogger.log("jMRDDpageViewFilterAll.countByValue() : Done");
	            if(mapPageViewFilter == null){
	            	iTracker = 8.0;
	            	throw new Exception("mapPageType : NULL");
	            }else{
	            	MyLogger.log("mapPageType : Size : " + mapPageViewFilter.size() + " : Done");
	            }
	            
	            //initialing page type Class for filter doc operation
	            iTracker = 9.0;
	            //PageViewFilter.getPageViewTypeMap(mapPageViewFilter.keySet());
	            
	            //jMRDDpageViewFilterAll.unpersist();
	            iTracker = 10.0;
	            return mapPageViewFilter;
	            //******************************************************************************
			} catch (Exception e) {
				throw new Exception("PageViewEngine : initializePageViewFilters() : " + iTracker + " : " + e.toString());
			}
	    }   
	 
	 public Map<Document, Long> initializePageViewIPFilters(String uri, String pageViewIPDatabase, String pageViewIPCollection, String client) throws Exception{
	    	HashMap<String, String> readOverrides = new HashMap<String, String>();
	    	ReadConfig readConfig = null;
	    	double iTracker = 0.0;
	    	try {
	    		//************************ PAGE TYPE PATTERN LOGIC *************************
	    		//Get all page page patterns from log_page_types_memory table
	    		MyLogger.log("PageViewEngine : initializePageViewIPFilters() : Page View Filters : Start");
	    		iTracker = 1.0;
	    		String database = pageViewIPDatabase;
	            String collection = pageViewIPCollection;
	            iTracker = 2.0;
	            readOverrides.put("database", database);
	            readOverrides.put("collection", collection);
	            readOverrides.put("uri", uri);
	            iTracker = 3.0;
	            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
	            iTracker = 4.0;
	            MyLogger.log("Page View Filters ReadConfig : readOverrides : "+ readOverrides.toString());
	            MyLogger.log("Page View Filters ReadConfig : Done");
	            
	            iTracker = 5.0;
	            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDpageTypeAll : Start");
	            JavaRDD<Document> jMRDDpageViewIPFilterAll = MongoSpark.load(jsc,readConfig).filter(doc -> doc.getString("webmart_code").trim().equalsIgnoreCase(client));
	            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDpageViewFilterAll : Done");            
	       
	            MyLogger.log("mapPageType : jMRDDPageTypeAggregated.countByValue() : Start");
	            Map<Document, Long> mapPageViewIPFilter = jMRDDpageViewIPFilterAll.countByValue();
	            MyLogger.log("jMRDDpageViewFilterAll.countByValue() : Done");
	            if(mapPageViewIPFilter == null){
	            	iTracker = 8.0;
	            	throw new Exception("mapPageType : NULL");
	            }else{
	            	MyLogger.log("mapPageType : Size : " + mapPageViewIPFilter.size() + " : Done");
	            }
	            
	            //initialing page type Class for filter doc operation
	            iTracker = 9.0;
	           
	            //PageViewFilter.getPageViewIPMap(mapPageViewIPFilter.keySet());
	            //jMRDDpageViewIPFilterAll.unpersist();
	            iTracker = 10.0;
	            return mapPageViewIPFilter;
	            //******************************************************************************
			} catch (Exception e) {
				throw new Exception("PageViewEngine : initializePageViewIPFilters() : " + iTracker + " : " + e.toString());
			}
	    }
}
