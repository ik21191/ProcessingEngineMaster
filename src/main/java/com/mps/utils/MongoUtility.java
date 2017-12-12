package com.mps.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mps.spark.MySpark;

public class MongoUtility implements Serializable {

	private static final long serialVersionUID = 1L;
	
	// JavaSparkcontext
	private static JavaSparkContext jsc = null;

	static {
		try {
			jsc = new MySpark().getJavaSparkContext();
		} catch (Exception e) {
			MyLogger.error("Mongo Utility : Exception while initializing MongoUtility");
		}
	}

	public static JavaSparkContext getJavaSparkContext() {
		try {
			if (jsc == null) {
				return new MySpark().getJavaSparkContext();
			} else {
				return jsc;
			}
		} catch (Exception e) {
			MyLogger.error("Mongo Utility : Exception while getting JavaSparkContext : " + e);
		}
		return null;
	}
	
	public static void closeJavaSparkContext() {
		try {
			if(jsc != null) {
				jsc.close();
			}
		}catch(Exception e) {
			MyLogger.error("MongoUtility : Exception closing JavaSparkContext : " + e);
		}
	}
	
	public static JavaRDD<Document> getJavaRDDFromMongo(String database, String collection, String host) {
		JavaRDD<Document> javaRdd = null;
		Map<String, String> readOverrides = new HashMap<>();
		readOverrides.put(Constants.DATABASE, database);
		readOverrides.put(Constants.COLLECTION, collection);
		readOverrides.put(Constants.MONGO_HOST_URI, host);
		try {
			MyLogger.log("MongoUtility : Creating JavaRDD with values : " + readOverrides);
			javaRdd = MongoSpark.load(jsc, ReadConfig.create(jsc).withOptions(readOverrides));
		}catch(Exception e) {
			MyLogger.error("MongoUtility : Exception while creating JavaRDD from : " + readOverrides);
		}
		
		return javaRdd;
	}
	
	public static JavaMongoRDD<Document> getJavaMongoRDDFromMongo(String host, String database, String collection) {
		JavaMongoRDD<Document> javaRdd = null;
		Map<String, String> readOverrides = new HashMap<>();
		readOverrides.put(Constants.DATABASE, database);
		readOverrides.put(Constants.COLLECTION, collection);
		readOverrides.put(Constants.MONGO_HOST_URI, host);
		try {
			MyLogger.log("MongoUtility : Creating JavaRDD with values : " + readOverrides);
			javaRdd = MongoSpark.load(jsc, ReadConfig.create(jsc).withOptions(readOverrides));
		}catch(Exception e) {
			MyLogger.error("MongoUtility : Exception while creating JavaRDD from : " + readOverrides);
		}
		
		return javaRdd;
	}
	
	public static boolean saveJavaRDDtoMongoDB(String uri, String database, String collection, JavaRDD<Document> javaRdd) {
		boolean statusFlag = false;
		Map<String, String> writeOverrides = new HashMap<>();
		writeOverrides.put(Constants.MONGO_DATABASE, database);
		writeOverrides.put(Constants.MONGO_COLLECTION, collection);
		writeOverrides.put(Constants.MONGO_HOST_URI, uri);
		writeOverrides.put("writeConcern.w", "majority");
		try {
			WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
			MyLogger.log("MongoUtility : Saving javaRDD to  : " + writeOverrides);
			MongoSpark.save(javaRdd, writeConfig);
			statusFlag = true;
		}catch(Exception e) {
			MyLogger.error("MongoUtility : Exception while saving JavaRDD : " + writeOverrides);
		}
		return statusFlag;
	}
}
