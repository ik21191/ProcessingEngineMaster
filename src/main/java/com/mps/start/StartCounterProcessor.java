/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mps.start;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mps.logs.counter.WebLogCounter;
import com.mps.utils.Methods;
import com.mps.utils.MyLogger;

/**
 *
 * @author kapil.verma
 */
public class StartCounterProcessor {
	
	//private String processingType = null;
    
    private String insightConfigHost = null;
    private String insightConfigDataBase = null;
    private String insightConfigCollection = null;
    private int appSleepTime = 1000 * 60 * 2;
    private Methods methods = new Methods("KKM-Spark");
    List<Document> taskList = null;
    
    //
    public StartCounterProcessor() throws Exception{
    	MyLogger.setUser("Spark-CS");
        run();
    }
    
    //
    public void run()throws Exception{
        int taskProcessed = 0;
        int taskSkippedCompleted = 0;
        try {
        	
        	MyLogger.log("StartCounterProcessor : run() : Start");
        	
        	while(true){
        		taskList = getTaskList();
        		taskProcessed = 0;
        		taskSkippedCompleted = 0;
        		
        		MyLogger.log("Total Task Found : " + taskList.size());
        		//processing all the task defined in insight config collection
	        	for(Document taskDoc : taskList) {
	        		try {
	        			Integer taskStatus = (Integer)taskDoc.getOrDefault("status", 0);
		        		String taskState = (String)taskDoc.getOrDefault("state", "");
		        		
		        		if(taskStatus == -2 && taskState.equalsIgnoreCase("Active")) {
		        			taskProcessed++;
		        			MyLogger.log("Task InProgress : " + taskDoc.getObjectId("_id").toString()  + " : " + taskList.size() + "/" +taskSkippedCompleted + "/" +  taskProcessed);
		        			
		        			WebLogCounter webLogCounter = new WebLogCounter();
		        			webLogCounter.setInsightConfigHost(insightConfigHost);
		        			webLogCounter.setInsightConfigDataBase(insightConfigDataBase);
		        			webLogCounter.setInsightConfigCollection(insightConfigCollection);
		            		webLogCounter.processTask(taskDoc);
		            		
		            		MyLogger.log("Task Processed : Total/Skipped/Processd : " + taskList.size() + "/" +taskSkippedCompleted + "/" +  taskProcessed + " : SUCCESS");
		        		}else{
		        			taskSkippedCompleted++;
		        		}
	        			
					} catch (Exception e) {
						MyLogger.log("Task Exception :  Total/Skipped/Processd : " + taskList.size() + "/" +taskSkippedCompleted + "/" +  taskProcessed + " : " + e.toString() + " : FAIL");
					}
	        	}
        	
	        	MyLogger.log("Total Task Processed :  Total/Skipped/Processd : " + taskList.size() + "/" +taskSkippedCompleted + "/" +  taskProcessed);
	        	MyLogger.log("Sleeping app for " + (appSleepTime / 1000) + " secs.");
	        	//MongoUtility.closeJavaSparkContext();
	        	Thread.sleep(appSleepTime);
        	}
        		
        	
        } catch (Exception e) {
            throw e;
        }
    }
    
    //
    private List<Document> getTaskList() throws Exception {
    	Map<String, String> configMap = new HashMap<>();
    	MongoClient client = null;
    	try {
    		String rootPath = methods.getClassPath(this);
            String configPath = rootPath + File.separator + "config" + File.separator + "config.mps";
            configMap = methods.readPropertyFile(configPath);
            
            insightConfigHost = configMap.get("mongo.insight.config.host");
            insightConfigDataBase = configMap.get("mongo.insight.config.database");
            insightConfigCollection = configMap.get("mongo.insight.config.counter.task.collection");
            
            if(insightConfigHost == null){throw new NullPointerException("NULL insightConfigHost in App Config");}
			if(insightConfigHost.trim().equalsIgnoreCase("")){throw new Exception("BLANK insightConfigHost in App Config");}
			
			if(insightConfigDataBase == null){throw new NullPointerException("NULL insightConfigDataBase in App Config");}
			if(insightConfigDataBase.trim().equalsIgnoreCase("")){throw new Exception("BLANK insightConfigDataBase in App Config");}
			
			if(insightConfigCollection == null){throw new NullPointerException("NULL insightConfigCollection in App Config");}
			if(insightConfigCollection.trim().equalsIgnoreCase("")){throw new Exception("BLANK insightConfigCollection in App Config");}
            
            client = new MongoClient(insightConfigHost);
    		MongoDatabase database = client.getDatabase(insightConfigDataBase);
    		
    		//Creating where clause
    		BasicDBObject andQuery = new BasicDBObject();
    	    List<BasicDBObject> whereConditionList = new ArrayList<>();
    	    whereConditionList.add(new BasicDBObject("status", -2));
    	    whereConditionList.add(new BasicDBObject("state", "Active"));
    	    andQuery.put("$and", whereConditionList);
    		
    		MongoCollection<Document> collection = database.getCollection(insightConfigCollection);
    		//Fetching data from Mongo with where clause
    		return (List<Document>) collection.find(andQuery).into(new ArrayList<Document>());
    		
    	}catch(Exception e) {
    		throw e;
    	} finally {
    		if(client != null) {
    			client.close();
    		}
    	}
    }
    
}
