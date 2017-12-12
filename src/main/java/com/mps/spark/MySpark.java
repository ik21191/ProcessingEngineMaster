package com.mps.spark;

import java.io.File;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkSqlAstBuilder;

import com.mps.utils.MyLogger;
import com.mps.utils.Methods;


public class MySpark {

	private Methods methods = new Methods();
	
	private String sparkConfigFilePath = "";
	private String sparkMaster="";
	private String sparkAppName="";
	private String sparkMongodbInputUri = "";
	private String sparkMongodbOutputUri = "";
	private SparkSession session;
	
	//MySpark Class constructor
	public MySpark() throws Exception{
		HashMap<String, String> sparkConfig = new HashMap<>();
		try {
			MyLogger.log("MySpark : Start");
			
			sparkConfigFilePath = methods.getClassPath(this) + File.separator + "config" + File.separator + "spark.mps";
			File sparkFile = new File(sparkConfigFilePath);
			
			if(!sparkFile.exists()){
				throw new Exception("MySpark : sparkConfigFilePath : Does Not Exists : " + sparkConfigFilePath);
			}
			MyLogger.log("MySpark : sparkConfigFilePath : " + sparkConfigFilePath);
			
			//
			sparkConfig = methods.readPropertyFile(sparkConfigFilePath);
			//reading Spark Master
			sparkMaster = sparkConfig.get("spark.master");
			if(sparkMaster == null || sparkMaster.trim().equalsIgnoreCase("")){
				sparkMaster = "local";
			}
			MyLogger.log("SparkConf sparkMaster : " + sparkMaster);
			
			//reading Spark AppName
			sparkAppName = sparkConfig.get("spark.appname");
			if(sparkAppName == null || sparkAppName.trim().equalsIgnoreCase("")){
				sparkAppName = "KSV-Spark";
			}
			MyLogger.log("SparkConf sparkAppName : " + sparkAppName);
			
			//reading Spark MongoDB input Uri
			sparkMongodbInputUri = sparkConfig.get("spark.mongodb.input.uri");
			if(sparkMongodbInputUri == null || sparkMongodbInputUri.trim().equalsIgnoreCase("")){
				throw new Exception("sparkMongodbInputUri : Invalid : " + sparkMongodbInputUri);
			}
			MyLogger.log("SparkConf spark.mongodb.input.uri : " + sparkMongodbInputUri);
			
			//reading Spark MongoDB Output Uri
			sparkMongodbOutputUri = sparkConfig.get("spark.mongodb.output.uri");
			if(sparkMongodbOutputUri == null || sparkMongodbOutputUri.trim().equalsIgnoreCase("")){
				throw new Exception("sparkMongodbOutputUri : Invalid : " + sparkMongodbOutputUri);
			}
			MyLogger.log("SparkConf spark.mongodb.output.uri : " + sparkMongodbOutputUri);
			
			//creating session
			session = SparkSession.builder()
					.master(sparkMaster)
					.appName(sparkAppName)
					.config("spark.mongodb.input.uri", sparkMongodbInputUri)
					.config("spark.mongodb.output.uri", sparkMongodbOutputUri)
					.getOrCreate();
			MyLogger.log(session.sparkContext().toString());
			
			MyLogger.log("SparkSession : Generation : Done");
			
		} catch (Exception e) {
			MyLogger.log("MySpark : Exception : " + e.toString());
			throw e;
		}
		MyLogger.log("MySpark : Start");
	}
	
	//returning JavaSparkContext
    public JavaSparkContext getJavaSparkContext(){
    	JavaSparkContext jsc = null;
    	double iTracker = 0.0;
    	try{
	        //
	        MyLogger.log("JavaSparkContext Generation : Start");
	    	jsc = new JavaSparkContext(session.sparkContext());	    	
	    	MyLogger.log("JavaSparkContext Generation : Done");
    	}
    	catch(Exception e){
    		MyLogger.exception("getJavaSparkContext : " + iTracker + " : " +  e.toString());
    	} 
        return jsc;
    }
	
	//returning SparkSession
    public SparkSession getSparkSession(){
        return this.session;
    }
	
	
	
}
