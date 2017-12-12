package com.mps.data.pagetype;

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mps.utils.MyLogger;

public class EnginePageType {
	JavaSparkContext jsc = null;	
	
	
	public EnginePageType(JavaSparkContext jsc) {
		this.jsc = jsc;
	}
	
	public List<Document> run(String pageTypeHostPort, String pageTypeDatabase, String pageTypeCollection) throws Exception{
    	HashMap<String, String> readOverrides = new HashMap<String, String>();
    	ReadConfig readConfig = null;
    	double iTracker = 0.0;
    	try {
    		//************************ PAGE TYPE PATTERN LOGIC *************************
    		//Get all page page patterns from log_page_types_memory table
    		MyLogger.log("PageTypeEngine : run() : Page Type Pattern : Start");
    		iTracker = 1.0;
            String database = pageTypeDatabase;
            String collection = pageTypeCollection;
            iTracker = 2.0;
            readOverrides.put("database", database);
            readOverrides.put("collection", collection);
            readOverrides.put("uri", pageTypeHostPort);
            iTracker = 3.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            iTracker = 4.0;
            MyLogger.log("Page Type ReadConfig : readOverrides : "+ readOverrides.toString());
            MyLogger.log("Page Type ReadConfig : Done");
            
            iTracker = 5.0;
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDpageTypeAll : Start");
            JavaMongoRDD<Document> jMRDDpageTypeAll = MongoSpark.load(jsc,readConfig);
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDpageTypeAll : Done");
            
            iTracker = 6.0;
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDPageTypeAggregated withPipeline & singletonList: Start");
            //JavaMongoRDD<Document> jMRDDPageTypeAggregated = jMRDDpageTypeAll.withPipeline(singletonList(Document.parse("{ $project: {page_type_pattern : 1, page_type_id:1}}")));
            JavaMongoRDD<Document> jMRDDPageTypeAggregated = jMRDDpageTypeAll.withPipeline(singletonList(Document.parse("{$sort: {precedence : 1}}")));
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDPageTypeAggregated withPipeline & singletonList: Done");
            
            iTracker = 7.0;
            MyLogger.log("mapPageType : jMRDDPageTypeAggregated.collect() : Start");
            //Map<Document, Long> mapPageType = jMRDDPageTypeAggregated.countByValue();
            List<Document> mapPageType = jMRDDPageTypeAggregated.collect();
            MyLogger.log("jMRDDPageTypeAggregated.countByValue() : Done");
            if(mapPageType == null){
            	iTracker = 8.0;
            	throw new Exception("mapPageType : NULL");
            }else{
            	MyLogger.log("mapPageType : Size : " + mapPageType.size() + " : Done");
            }
            
            //initialing page type Class for filter doc operation
            iTracker = 9.0;
            return mapPageType;
            //PageType.initialize(mapPageType);
            
            //******************************************************************************
		} catch (Exception e) {
			throw new Exception("PageTypeEngine : run() : " + iTracker + " : " + e.toString());
		}
    }
}
