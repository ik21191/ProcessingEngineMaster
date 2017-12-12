package com.mps.data.feed.ip;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mps.utils.MyLogger;

public class EngineIPAccount {
	JavaSparkContext jsc = null;	
	private ArrayList<DataIPAccount> ipDataList = new ArrayList<>();
	
	public EngineIPAccount(JavaSparkContext jsc) {
		this.jsc = jsc;
	}
	
	public ArrayList<DataIPAccount> run(String ipAccountHostPort, String ipAccountDatabase, String ipAccountCollection) throws Exception{
    	HashMap<String, String> readOverrides = new HashMap<>();
    	ReadConfig readConfig = null;
    	double iTracker = 0.0;
    	int size = 0;
    	int docNo = 0;
    	Document doc = null;
    	try {
    		//************************ PAGE TYPE PATTERN LOGIC *************************
    		//Get all page page patterns from log_page_types_memory table
    		MyLogger.log("EngineIPAccount : run() : Ip Account : Start");
    		iTracker = 1.0;
            String database = ipAccountDatabase;
            String collection = ipAccountCollection;
            iTracker = 2.0;
            readOverrides.put("database", database);
            readOverrides.put("collection", collection);
            readOverrides.put("uri", ipAccountHostPort);
            iTracker = 3.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            iTracker = 4.0;
            MyLogger.log("IP Account ReadConfig : readOverrides : "+ readOverrides.toString());
            MyLogger.log("IP Account ReadConfig : Done");
            
            iTracker = 5.0;
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDIPAccount : Start");
            JavaMongoRDD<Document> jMRDDIPAccount = MongoSpark.load(jsc,readConfig);
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDIPAccount : Done");
            
            iTracker = 6.0;
            MyLogger.log("mapIPAccount : jMRDDIPAccount.collect() : Start");
            List<Document> listIPAccount = jMRDDIPAccount.collect();
            MyLogger.log("jMRDDPageTypeAggregated.countByValue() : Done");
            if(listIPAccount == null){
            	iTracker = 8.0;
            	throw new Exception("listIPAccount : NULL");
            }else{
            	MyLogger.log("mapPageType : Size : " + listIPAccount.size() + " : Done");
            }
            
            //initialing DataIPAccount Class for filter doc operation
            iTracker = 7.0;
            size = listIPAccount.size();
            for(docNo = 0; docNo < size; docNo++){
	    		doc = null;
	    		iTracker = 8.0;
	    		doc = (Document) listIPAccount.get(docNo);
	    		if(doc.getString("low_ip_range").contains(".") && !doc.getString("low_ip_range").contains(":") && doc.getString("high_ip_range").contains(".") && !doc.getString("high_ip_range").contains(":")){
	    			iTracker = 9.0; 
	    			DataIPAccount ipDataV4 = new DataIPAccount();
                     ipDataV4.ipType = 4;
                     ipDataV4.ipBegin = BigInteger.valueOf(doc.getLong("ip_low"));
                     ipDataV4.ipEnd = BigInteger.valueOf(doc.getLong("ip_high"));
                     ipDataV4.startIP = doc.get("low_ip_range").toString();
                     ipDataV4.endIP = doc.get("high_ip_range").toString();
                     ipDataV4.range = "";
                     ipDataV4.data =  doc.get("code").toString();
                     ipDataList.add(ipDataV4);
                     
	    		}
	    		else if(doc.getString("low_ip_range").contains(":") && doc.getString("high_ip_range").contains(":")){
	    			iTracker = 10.0; 
	    			DataIPAccount ipDataV4 = new DataIPAccount();
                    ipDataV4.ipType = 6;
                    ipDataV4.ipBegin = new BigInteger(doc.getString("ip_low"));
                    ipDataV4.ipEnd = new BigInteger(doc.getString("ip_high"));
                    ipDataV4.startIP = doc.getString("low_ip_range");
                    ipDataV4.endIP = doc.getString("high_ip_range");
                    ipDataV4.range = "";
                    ipDataV4.data =  doc.getString("code");
                    ipDataList.add(ipDataV4);
	    		}
	    		else{
	    			MyLogger.log("Invalid IP Type! Logic Fail in Else" + doc.toString());
	    		}	    		
	    	}
            iTracker = 11.0;
            return ipDataList;
            
            //******************************************************************************
		} catch (Exception e) {
			throw new Exception("EngineIPAccount : run() : " + iTracker + " : " + e.toString() + " : " + doc.toString());
		}
    }
}
