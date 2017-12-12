package com.mps.data.feed.account;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mps.utils.MyException;
import com.mps.utils.MyLogger;

public class EngineAccountInstChildParent{
	private JavaSparkContext jsc = null;	
	
	public EngineAccountInstChildParent(JavaSparkContext jsc) throws Exception {
		if(jsc == null){
			throw new MyException("AccountEngine : JavaSparkContext : NULL");
		}
		this.jsc = jsc;
	}
	
	public ArrayList<DataAccountInstChildParent> run(String accountParentHostPort, String accountDatabase, String accountCollection) throws Exception{
		HashMap<String, String> readOverrides = new HashMap<>();
		JavaRDD<Document> jRDDAccParentChild = null;
		ArrayList<DataAccountInstChildParent> accountDataList = null;
		ReadConfig readConfig = null;
		Set<Document> docSet = null;
		int accountCount = 0;
		int docNo = 0;
		double iTracker = 0.0;
		try {
			
			//************************ ACCOUNT PARENT CHILD DETAILS LOGIC *************************
            MyLogger.log("WebLogFilter : initializeAccountParentChild() : Account Parent Child : Start");
            
            if(accountDatabase == null || accountDatabase.trim().equalsIgnoreCase("")){
            	throw new MyException("");
            }
            
            
            String database = accountDatabase.trim();
            String collection = accountCollection.trim();
            readOverrides.put("database", database);
            readOverrides.put("collection", collection);
            readOverrides.put("uri", accountParentHostPort);
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            MyLogger.log("Account Parent Child  ReadConfig : readOverrides : "+ readOverrides.toString());
            MyLogger.log("Account Parent Child ReadConfig : Done");
            
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jRDDAccParentChild : Start");
            jRDDAccParentChild = MongoSpark.load(jsc,readConfig);
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jRDDAccParentChild : Done");
            
            //
            MyLogger.log("jRDDAccParentChild.countByValue().keySet() : Start");
            docSet = jRDDAccParentChild.countByValue().keySet();
            accountCount = docSet.size();
            MyLogger.log("jRDDAccParentChild.countByValue().keySet() : " + accountCount+ " : Accounts Found : Done");
			
            //By Megha on 2017-10-13for binary search on String Array for account parent list            
            accountDataList = new ArrayList<DataAccountInstChildParent>();
            
            for(docNo = 0; docNo < accountCount; docNo++){
            	Document tempDoc = (Document)docSet.toArray()[docNo];
            	accountDataList.add(new DataAccountInstChildParent(tempDoc.get("institution_id").toString().trim(), tempDoc.get("parent_id").toString().trim()));          	
	    	}
            
            MyLogger.log("MongoSpark Reading Account Parent Details :: Size : "+ accountDataList.size() +" : Done");
            return accountDataList;  
			
		} catch (Exception e) {
			throw new MyException("run : " + iTracker + " : " + e.toString());
		}		
	}
	
}
