package com.mps.log.filter;

//java Packages
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//apache packages
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//bson pacakges
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
//Mongo packages
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mps.data.feed.account.DataAccountInstChildParent;
import com.mps.data.feed.account.EngineAccountInstChildParent;
//custom packages
import com.mps.data.feed.account.MapperAccountInstChildParent;
import com.mps.data.feed.ip.DataIPAccount;
import com.mps.data.feed.ip.EngineIPAccount;
import com.mps.data.feed.ip.MapperIPAccount;
import com.mps.data.feed.pageview.EnginePageView;
import com.mps.data.feed.pageview.MapperPageView;
import com.mps.data.pagetype.EnginePageType;
import com.mps.data.pagetype.MapperPageType;
import com.mps.log.filter.client.FilteredDocumentASTM;
import com.mps.log.filter.client.FilteredDocumentIEEE;
import com.mps.spark.MySpark;
import com.mps.utils.Constants;
import com.mps.utils.MyException;
import com.mps.utils.MyLogger;


public class WebLogFilter implements Serializable{
	
	public static Document taskDoc = null;
	
	
	private static final long serialVersionUID = 1L;
	private JavaSparkContext jsc = null;
	private MongoClient mongoClient = null;
	
	private String feedPageTypeHostPort = "";
    private String feedPageTypeDatabase = "";
	private String feedPageTypeCollection = "";
	
	private String feedFilterIpAccountHostPort = "";
	private String feedFilterIpAccountDatabase = "";
    private String feedFilterIpAccountCollection = "";
	
    private String logsHostPort = "";
    private String logsDatabase = "";
    private String logsCollection = "";
    
    private String filterHostPort = "";
    private String filterDatabase = "";
    private String filterCollectionPrefix = "";
    private String filterCollection = "";
    private String filterCollectionSuffix = "";
    
    private String feedFilterMasterHostPort = "";
    private String feedFilterMasterDatabase = "";
    private String feedFilterMasterCollection = "";
    
    private String feedIpHostPort = "";
    private String feedIpDatabase = "";
    private String feedIpCollection = "";
    
    private String feedAccountParentHostPort = "";
    private String feedAccountParentDatabase = "";
    private String feedAccountParentCollection = "";
    
    private String filterStatsHostPort = "";
    private String filterStatsDatabase = "";
    private String filterStatsCollection = "";
    
    private String insightConfigHost = null;
    private String insightConfigDataBase = null;
    private String insightConfigCollection = null;
    
    private int feedPerformanceRecordsInterval = 0;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String startTime = "";
    private String endTime = "";
    public static int performanceCouter = 100000;
	
    private String client = "";
    
    //method to initialize the application and validating all pre requisites data
    
    public void initializeTask(Document task) {
    	try {
    	WebLogFilter.taskDoc = task;
    	//set log page type
    	setFeedPageTypeHostPort((String)task.getOrDefault("feed_pagetype_host_port", ""));
    	setFeedPageTypeDatabase((String)task.getOrDefault("feed_pagetype_database", ""));
        setFeedPageTypeCollection((String)task.getOrDefault("feed_pagetype_collection", ""));
        
        //IP Account memory database and collection
        setFeedFilterIpAccountHostPort((String)task.getOrDefault("feed_filter_ip_account_host_port", ""));
        setFeedFilterIpAccountDatabase((String)task.getOrDefault("feed_filter_ip_account_database", ""));
        setFeedFilterIpAccountCollection((String)task.getOrDefault("feed_filter_ip_account_collection", ""));
        
        //log statistics database and collection
        setLogsHostPort((String)task.getOrDefault("log_host_port", ""));
        setLogsDatabase((String)task.getOrDefault("log_database", ""));
        setLogsCollection((String)task.getOrDefault("log_collection", ""));
        
        //set filter database and collection
        setFilterHostPort((String)task.getOrDefault("filter_host_port", ""));
        setFilterDatabase((String)task.getOrDefault("filter_database", ""));
        setFilterCollectionPrefix((String)task.getOrDefault("filter_collection_prefix", ""));
        setFilterCollection((String)task.getOrDefault("filter_collection", ""));
        setFilterCollectionSuffix((String)task.getOrDefault("filter_collection_suffix", ""));
        
        //log filter memory database and collection
        setFeedFilterMasterHostPort((String)task.getOrDefault("feed_filter_master_host_port", ""));
        setFeedFilterMasterDatabase((String)task.getOrDefault("feed_filter_master_database", ""));
        setFeedFilterMasterCollection((String)task.getOrDefault("feed_filter_master_collection", ""));
        
        //log filter ip memory database and collection
        setFeedIpHostPort((String)task.getOrDefault("feed_ip_host_port", ""));
        setFeedIpDatabase((String)task.getOrDefault("feed_ip_database", ""));
        setFeedIpCollection((String)task.getOrDefault("feed_ip_collection", ""));
        
        //log institution account parent database and collection
        setFeedAccountParentHostPort((String)task.getOrDefault("feed_account_parent_host_port", ""));
        setFeedAccountParentDatabase((String)task.getOrDefault("feed_account_parent_database", ""));
        setFeedAccountParentCollection((String)task.getOrDefault("feed_account_parent_collection", ""));
        
        //set filter stats database and collection
        setFilterStatsHostPort((String)task.getOrDefault("filter_stats_host_port", ""));
        setFilterStatsDatabase((String)task.getOrDefault("filter_stats_database", ""));
        setFilterStatsCollection((String)task.getOrDefault("filter_stats_collection", ""));
        
        setClient((String)task.getOrDefault("processing_client", ""));
        
        //set feed performance record interval
        setFeedPerformanceRecordsInterval((Integer)task.getOrDefault("filter_performance_records_interval", ""));
    	}catch(Exception e) {
    		MyLogger.error("WebLogFilter : EXCEPTION : initializeTask(Document task)  " + e);
    		taskDoc.replace("status", -3);
    		String description = taskDoc.getString("description");
    		taskDoc.replace("description", description + "WebLogFilter : Exception while initialization method initializeTask(Document task) #");
    		updateTask();
    	}

        //no. of docs after which performace will be checked
        //setPerformanceCounter(noOfDocs);
    	
    }
    
    public void initialize() throws Exception{ 
        boolean processForPageType = true;
        boolean processForAccInstChildParent = true;
        boolean processForIPAccount = true;
        boolean processForPageViewFilter = true;
        double iTracker = 0.0;
    	try {
    		iTracker = 1.0;
    		
    		startTime = dateFormat.format(new Date());
    		MyLogger.log("WebLogFilter : initialize() : Start for " + client);
    		
    		iTracker = 2.0;
    		MyLogger.log("*******");
    		MyLogger.log("WebLogFilter : initialize() : JavaSaprkContext : Start");
    		this.jsc = new MySpark().getJavaSparkContext();
    		MyLogger.log("WebLogFilter : initialize() : JavaSaprkContext : Done");
    		
    		//calling method to Initialize PageType content, if Fails application will Exit
    		iTracker = 3.0;
    		if(processForPageType){
    			MyLogger.log("*******");
    			initializePageType();
			}else{
				MyLogger.log("initializePageType : Disabled");
			}
    		
    		//calling method to Initialize Account Child Parent, if Fails application will Exit
    		iTracker = 4.0;
    		if(processForAccInstChildParent){
    			MyLogger.log("*******");
    			initializeAccountParentChild();
			}else{
				MyLogger.log("processForAccInstChildParent : Disabled");
			}
    		
    		//calling method to Initialize IP Account Details Map, if Fails application will Exit
    		iTracker = 5.0;
    		if(processForIPAccount){
    			MyLogger.log("*******");
    			initializeIpAccountDetails();
    		}else{
				MyLogger.log("processForIPAccount : Disabled");
			}
    		
    		//calling method to Initialize Page View Type Flags, if Fails application will Exit
    		iTracker = 6.0;
    		if(processForPageViewFilter){
    			MyLogger.log("*******");
    			initializePageViewFilters();
    		}else{
				MyLogger.log("initializePageViewFilters : Disabled");
			}
    		MyLogger.log("WebLogFilter : initialize() : End  for " + client);
		} catch (Exception e) {
			MyLogger.error("WebLogFilter : EXCEPTION : initialize() : iTracker=" + iTracker + " : " +e.toString());
			taskDoc.replace("status", -3);
			String description = taskDoc.getString("description");
            taskDoc.replace("description", description + "WebLogFilter : There was some exception in initialize() method #");
            //update status of this task
			updateTask();
            throw new MyException("WebLogFilter : initialize() : iTracker=" + iTracker + " : " +e.toString());
		}
    }
    
    //method to start the application
    public void run() throws Exception {
    	Map<String, String> readOverrides = new HashMap<>();
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	
    	try{
        	MyLogger.log("*****************************************************************************");
        	MyLogger.log("WebLogFilter : run() : Start"); 
        	
        	//initializing the application
        	iTracker = 1.0;
        	MyLogger.log("*******");
        	initialize();
        	
        	iTracker = 2.0;
        	MyLogger.log("*******");
        	
			FilterDocStats.refreshTableStats();
    		
    		//MyLogger.log("******* : Prcoessing : " + processingType.trim().toUpperCase() + " : Batch");
        	readOverrides.put(Constants.DATABASE, logsDatabase);
            readOverrides.put(Constants.COLLECTION, logsCollection);
            readOverrides.put(Constants.MONGO_HOST_URI, logsHostPort);
            ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            MyLogger.log("Source ReadConfig : "+ readOverrides.toString());
            
            iTracker = 2.0;
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDDocAll : Start");
            JavaMongoRDD<Document> jMRDDDocAll = MongoSpark.load(jsc,readConfig);
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDDocAll : Done");
	            
	            
            //Lambda for Filter of Document
            iTracker = 3.0;
            MyLogger.log("jMRDDDocAll.map : jRDDDocFilter : Start");
            JavaRDD<Document> jRDDDocFilter = null;
            if(client.equalsIgnoreCase("astm")){
            	jRDDDocFilter = jMRDDDocAll.map((final Document doc) -> 
                //method to get log table document converted into log filtered document
            	FilteredDocumentASTM.getFilteredDoc(doc)
                );
            }
            if(client.equalsIgnoreCase("ieee")){
                jRDDDocFilter = jMRDDDocAll.map((final Document doc) -> 
                //method to get log table document converted into log filtered document
            	FilteredDocumentIEEE.getFilteredDoc(doc)
                );  
            }
            
            MyLogger.log("jMRDDDocAll.map : jRDDDocFilter : Done");
            
            //RDD for Skipping of Unwanted records in filter
            iTracker = 4.0;
            MyLogger.log("jRDDDocFilter.filter : jRDDDocFilteredFinal : skipping unwanted records : Start");
            JavaRDD<Document> jRDDDocFilteredFinal = jRDDDocFilter.filter(doc -> !doc.containsKey("skipRecord"));
            MyLogger.log("jRDDDocFilter.filter : jRDDDocFilteredFinal : Done");
            
            // Create a custom WriteConfig to write data to log filter table
            iTracker = 5.0;
            String filterCollectionName = filterCollectionPrefix + filterCollection + filterCollectionSuffix;
            
            //getting stats after processing of all data
            MyLogger.log(FilterDocStats.getStats());
	            
            //saving final filtered docs in MongoDB
            iTracker = 6.0;
            MyLogger.log("MongoSpark.save(jRDDDocFilteredFinal, writeConfig) : Start");
            
            writeOverrides.put(Constants.DATABASE, filterDatabase);
            writeOverrides.put(Constants.MONGO_HOST_URI, filterHostPort);
            writeOverrides.put(Constants.COLLECTION, filterCollectionName);
            //check whether existing collection needs truncation
            String existingFilterCollectionAction = (String)taskDoc.getOrDefault("existing_filter_collection_action", "");
            if(existingFilterCollectionAction.equalsIgnoreCase("truncate")) {
            	MyLogger.log("WebLogFilter : existing_filter_collection_action : " + existingFilterCollectionAction + " : " + writeOverrides.toString());
            	truncateCollection(filterHostPort, filterDatabase, filterCollectionName);
            	writeOverrides.put(Constants.COLLECTION, filterCollectionName);
            } else if(existingFilterCollectionAction.equalsIgnoreCase("append")) {
            	writeOverrides.put(Constants.COLLECTION, filterCollectionName);
            	MyLogger.log("WebLogFilter : existing_filter_collection_action : " + existingFilterCollectionAction + " : " + writeOverrides.toString());
            } else {
            	MyLogger.log("WebLogFilter : No existing_filter_collection_action is defined so creating default filter collection : " + writeOverrides.toString());
            	filterCollectionName = filterCollectionName + "_" + (new SimpleDateFormat("yyyy M dd hh mm ss").format(new Date())).replace(" ", "");
            	writeOverrides.put(Constants.COLLECTION, filterCollectionName);
            }
       
            writeOverrides.put("writeConcern.w", "majority");
            WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
            MyLogger.log("Destination WriteConfig : "+ writeOverrides.toString());
            
            MongoSpark.save(jRDDDocFilteredFinal, writeConfig);
	            
            MyLogger.log("MongoSpark.save(jRDDDocFilteredFinal, writeConfig) : Done");
            
            taskDoc.replace("status", 1);
            String description = taskDoc.getString("description");
            taskDoc.replace("description", description + "WebLogFilter : LogFilter created successfully for this task #");
           
            //logging the statistics and performance in mongoDB
            iTracker = 7.0;
            MyLogger.log("Putting processing Statistics in Tracker/Audit collection : Start");
    		endTime = dateFormat.format(new Date());
    		iTracker = 8.0;
    		Document doc = new Document("client_id",2)
	        		.append("client_code", client)
	        		.append("client_name", client.toUpperCase())
	        		.append("log_filter_records_processed", FilterDocStats.getRecordsProcessed())
	        		.append("log_filter_records_success", FilterDocStats.getRecordSuccess())                		
	        		.append("log_filter_pgty_nf",FilterDocStats.getPatternNotFound())
	        	    .append("log_filter_blank_url",FilterDocStats.getBlankUrl())
	        	    .append("log_filter_invalid_statuscode",FilterDocStats.getInvalidStatusCode())
	        		.append("log_filter_valid_records", FilterDocStats.getRecordSuccess() - FilterDocStats.getPatternNotFound())
	        		.append("log_filter_invalid_ip",FilterDocStats.getSkipRecordsPageViewIP())
	        		.append("log_filter_skipped_images",FilterDocStats.getSkipRecordsResourceType())
	        		.append("start_time", startTime)
	        		.append("end_time", endTime)
	        		.append("host", filterHostPort)
	                .append("database", filterDatabase)
	                .append("collection", filterCollectionName);
            
            iTracker = 9.0;
            mongoClient = new MongoClient(filterStatsHostPort);
            iTracker = 10.0;
    		MongoDatabase mongoDB = mongoClient.getDatabase(filterStatsDatabase);
    		iTracker = 11.0;
    		MongoCollection<Document> mongoCollection = mongoDB.getCollection(filterStatsCollection);
    		iTracker = 12.0;
    		mongoCollection.insertOne(doc);
    		iTracker = 13.0;
    		MyLogger.log("Processing Statistics : " + FilterDocStats.getStats());
    		MyLogger.log("Putting Processing Statistics in Tracker/Audit collection : End  for " + client);
    		MyLogger.log("WebLogFilter : run() : End  for " + client); 
        } catch (Exception e) {
            MyLogger.log("run : iTracker=" + iTracker + " : " + e.toString());
            taskDoc.replace("status", -3);
            String description = taskDoc.getString("description");
            taskDoc.replace("description", description + "WebLogFilter : There was some exception in run() method.");
            throw new MyException("run : iTracker=" + iTracker + " : " + e.toString());
        } finally {
        	//closing MOngoClient
            if(mongoClient != null){mongoClient.close();}
            //closing java spark context
            if(jsc != null){jsc.close();}
            //update status for this task
            updateTask();
            MyLogger.log("*******************************************************************");
        }
	}
    
    private void truncateCollection(String hostPort, String database, String collection) {
		MongoClient client = null;
		MyLogger.log("WebLogFilter : Deleting all Documents from " + hostPort + "  " + database + "  " + collection);
    	try {
    		client = new MongoClient(hostPort.replace("mongodb://", ""));
    		MongoDatabase mongoDB = client.getDatabase(database);
    		MongoCollection<Document> mongoCollection = mongoDB.getCollection(collection);
    		
    		//To delete all the documents form the collection
    		mongoCollection.deleteMany(new BasicDBObject());
    	}catch(Exception e) {
    		MyLogger.error("WebLogFilter: EXCEPTION : truncateCollection : " + e);
    	} finally {
    		if(client != null) {
    			client.close();
    		}
    	}
	}

	private void updateTask() {
    	MongoClient client = null;
    	try {
    		client = new MongoClient(insightConfigHost);
    		MongoDatabase database = client.getDatabase(insightConfigDataBase);
    		MongoCollection<Document> collection = database.getCollection(insightConfigCollection);
    		
    		Bson filter = new Document("_id", taskDoc.getObjectId("_id"));
    		Bson updateOperationDocument = new Document("$set", taskDoc);
    		collection.updateOne(filter, updateOperationDocument);

    	}catch(Exception e) {
    		MyLogger.error("WebLogFilter: EXCEPTION : truncateCollection()" + e);
    	} finally {
    		if(client != null) {
    			client.close();
    		}
    	}
	}
    
    //method to initialize Page Type and fill respective data collection
    //****** Method Added by KSV on 2017-10-04
    //Method to Initialize PageType pattern and classes 
    private void initializePageType() throws Exception{
    	EnginePageType pageTypeEngine = null;
    	List<Document> pageTypeMap = null;
    	double iTracker = 0.0;
    	try {
    		
    		//Get all page page patterns from log_page_types_memory table
    		iTracker = 1.0;
    		MyLogger.log("WebLogFilter : initializePageType() : Start");
    		pageTypeEngine = new EnginePageType(jsc);
    		pageTypeMap = pageTypeEngine.run(feedPageTypeHostPort, feedPageTypeDatabase, feedPageTypeCollection);
    		
    		iTracker = 2.0;
            MyLogger.log("WebLogFilter : initializePageType() : Start");
            MapperPageType.initialize(pageTypeMap);
    		MyLogger.log("WebLogFilter : initializePageType() : Done");
    
		} catch (Exception e) {
			taskDoc.replace("status", -3);
			String description = taskDoc.getString("description");
			taskDoc.replace("description", description + "WebLogFilter : There was some exception in initializePageType() #");
			updateTask();
			throw new MyException("WebLogFilter : initializePageType() : " + iTracker + " : " + e.toString());
		} 
    }
    
  //Method to Initialize PageView Filters and classes 

    //method to initialize Page View Filters and fill respective data collection    
    private void initializePageViewFilters() throws Exception{
    	EnginePageView pageViewEngine = null;
    	Map<Document, Long> pageViewMap = null;
    	double iTracker = 0.0;
    	try {
    		//************************ PAGE TYPE PATTERN LOGIC *************************
    		//Get all page page patterns from log_page_types_memory table
    		MyLogger.log("WebLogFilter : initializePageViewFilters() : Page View Filters : Start");
    		pageViewEngine = new EnginePageView(jsc);
    		pageViewMap = pageViewEngine.initializePageViewFilters(feedFilterMasterHostPort,feedFilterMasterDatabase , feedFilterMasterCollection, client);
    		iTracker = 2.0;
            MyLogger.log("PageTypeMapper.initialize : Start");
            MapperPageView.initializeDataPageView(pageViewMap.keySet());
    		MyLogger.log("PageTypeMapper.initialize : Done");
    		iTracker = 3.0;
    		
    		MyLogger.log("WebLogFilter : initializePageViewFilters() : Page View Filters : Start");
    		pageViewEngine = new EnginePageView(jsc);
    		pageViewMap = pageViewEngine.initializePageViewIPFilters(feedFilterIpAccountHostPort, feedFilterIpAccountDatabase, feedFilterIpAccountCollection, client);
    		iTracker = 4.0;
            MyLogger.log("PageTypeMapper.initialize : Start");
            MapperPageView.initializePageViewIP(pageViewMap.keySet());
    		MyLogger.log("PageTypeMapper.initialize : Done");
		} catch (Exception e) {
			taskDoc.replace("status", -3);
			String description = taskDoc.getString("description");
			taskDoc.replace("description", description + "WebLogFilter : There was some exception in initializePageViewFilters()");
			updateTask();
			throw new MyException("WebLogFilter : initializePageViewFilters() : " + iTracker + " : " + e.toString());
		}
    }    

    //****** Method Added by KSV on 2017-10-04
    //Method to Initialize Account Parent Child map and 
    
    //method to initialize Account Institution Child Parent and fill respective data collection    
    private void initializeAccountParentChild() throws Exception{
    	EngineAccountInstChildParent accountEngine = null;
    	ArrayList<DataAccountInstChildParent> accountDataList = null;
    	
    	double iTracker = 0.0;    	
    	try {
    		
    		//************************ ACCOUNT PARENT CHILD DETAILS LOGIC *************************
            MyLogger.log("WebLogFilter : initializeAccountParentChild() : Account Parent Child : Start");
            iTracker = 1.0;
            accountEngine = new EngineAccountInstChildParent(jsc);
    		accountDataList = accountEngine.run(feedAccountParentHostPort, feedAccountParentDatabase, feedAccountParentCollection);
    		
    		iTracker = 2.0;
            MyLogger.log("AccountChildParentMapper.initialize : Start");
            MapperAccountInstChildParent.initialize(accountDataList);
            MyLogger.log("AccountChildParentMapper.initialize : Total " + accountDataList.size() + " Accounts Found : Done");
            
		} catch (Exception e) {
			taskDoc.replace("status", -3);
			String description = taskDoc.getString("description");
			taskDoc.replace("description", description + "WebLogFilter : There was some exception in initializeAccountParentChild() #");
			//Update status for this task
			updateTask();
			throw new MyException("WebLogFilter : initializeAccountParentChild() : " + iTracker + " : " + e.toString());
		}
    }    
    
    //****** Method Added by KSV on 2017-10-04
    //Method to Initialize IP and Account Details Mapping 
    
    //method to initialize IP Account Mapping and fill respective data collection
    private void initializeIpAccountDetails() throws Exception{
    	EngineIPAccount ipEngine = null;
    	ArrayList<DataIPAccount> ipDataList = null;
    	double iTracker = 0.0;
    	try {
    		//************************ IP ACCOUNT DETAILS *************************
    		MyLogger.log("WebLogFilter : initializeIpAccountDetails() : IP Acoount Details Mapper : Start");
    		
    		//IpFeedEngine to read IP Feed file and provide a IpData Class list
    		iTracker = 8.0;
    		MyLogger.log("IpEngine : Start");
    		ipEngine = new EngineIPAccount(jsc);
    		ipDataList = ipEngine.run(feedIpHostPort, feedIpDatabase, feedIpCollection);
    		MyLogger.log("IpEngine ipDataList Size : " + ipDataList.size() + " : Done");
    		
    		//initializing IPAccountMapper Class
    		iTracker = 9.0;
    		MyLogger.log("IPAccountMapper.initialize : Start");
    		MapperIPAccount.initialize(ipDataList);
    		MyLogger.log("IPAccountMapper.initialize : Done");
    		
    		iTracker = 10.0;
    		MyLogger.log("WebLogFilter : initializeIpAccountDetails() : IP Acount Details Mapper : Done");
            //******************************************************************************
		} catch (Exception e) {
			taskDoc.replace("status", -3);
			String description = taskDoc.getString("description");
			taskDoc.replace("description", description + "WebLogFilter : There was some exception in initializeIpAccountDetails() #");
			//Update status for this task
			updateTask();
			throw new Exception("WebLogFilter : initializeIpAccountDetails() : " + iTracker + " : " + e.toString());
		}
    }    
    

   
    
    //********************** GETTER & SETTER *************************  
    

	public String getLogsHostPort() {
		return logsHostPort;
	}

	public void setLogsHostPort(String logsHostPort) {
		this.logsHostPort = "mongodb://" + logsHostPort;
	}

	public String getFilterHostPort() {
		return filterHostPort;
	}

	public void setFilterHostPort(String filterHostPort) {
		this.filterHostPort = "mongodb://" + filterHostPort;
	}

	public String getFilterDatabase() {
		return filterDatabase;
	}

	public void setFilterDatabase(String filterDatabase) {
		this.filterDatabase = filterDatabase;
	}

	public String getFilterCollectionPrefix() {
		return filterCollectionPrefix;
	}

	public void setFilterCollectionPrefix(String filterCollectionPrefix) {
		this.filterCollectionPrefix = filterCollectionPrefix;
	}

	public String getFilterCollection() {
		return filterCollection;
	}

	public void setFilterCollection(String filterCollection) {
		this.filterCollection = filterCollection;
	}

	public String getFilterCollectionSuffix() {
		return filterCollectionSuffix;
	}

	public void setFilterCollectionSuffix(String filterCollectionSuffix) {
		this.filterCollectionSuffix = filterCollectionSuffix;
	}

	public void setLogsDatabase(String logsDatabase) {
			this.logsDatabase = logsDatabase;
	}

	public void setLogsCollection(String logsCollection) {
			this.logsCollection = logsCollection;
	}
	
	    
    
	public String getFilterStatsHostPort() {
		return filterStatsHostPort;
	}

	public void setFilterStatsHostPort(String filterStatsHostPort) {
		this.filterStatsHostPort = filterStatsHostPort;
	}

	public String getFilterStatsDatabase() {
		return filterStatsDatabase;
	}

	public void setFilterStatsDatabase(String filterStatsDatabase) {
		this.filterStatsDatabase = filterStatsDatabase;
	}

	public String getFilterStatsCollection() {
		return filterStatsCollection;
	}

	public void setFilterStatsCollection(String filterStatsCollection) {
		this.filterStatsCollection = filterStatsCollection;
	}

	public String getInsightConfigHost() {
		return insightConfigHost;
	}

	public void setInsightConfigHost(String insightConfigHost) {
		this.insightConfigHost = insightConfigHost;
	}

	public String getInsightConfigDataBase() {
		return insightConfigDataBase;
	}

	public void setInsightConfigDataBase(String insightConfigDataBase) {
		this.insightConfigDataBase = insightConfigDataBase;
	}

	public String getInsightConfigCollection() {
		return insightConfigCollection;
	}

	public void setInsightConfigCollection(String insightConfigCollection) {
		this.insightConfigCollection = insightConfigCollection;
	}

	public String getFeedPageTypeHostPort() {
		return feedPageTypeHostPort;
	}

	public void setFeedPageTypeHostPort(String feedPageTypeHostPort) {
		this.feedPageTypeHostPort = "mongodb://" + feedPageTypeHostPort;
	}

	public String getFeedPageTypeDatabase() {
		return feedPageTypeDatabase;
	}

	public void setFeedPageTypeDatabase(String feedPageTypeDatabase) {
		this.feedPageTypeDatabase = feedPageTypeDatabase;
	}

	public String getFeedPageTypeCollection() {
		return feedPageTypeCollection;
	}

	public void setFeedPageTypeCollection(String feedPageTypeCollection) {
		this.feedPageTypeCollection = feedPageTypeCollection;
	}

	public String getFeedIpHostPort() {
		return feedIpHostPort;
	}

	public void setFeedIpHostPort(String feedIpHostPort) {
		this.feedIpHostPort = "mongodb://" + feedIpHostPort;
	}

	public String getFeedIpDatabase() {
		return feedIpDatabase;
	}

	public void setFeedIpDatabase(String feedIpDatabase) {
		this.feedIpDatabase = feedIpDatabase;
	}

	public String getFeedIpCollection() {
		return feedIpCollection;
	}

	public void setFeedIpCollection(String feedIpCollection) {
		this.feedIpCollection = feedIpCollection;
	}

	public String getFeedFilterMasterHostPort() {
		return feedFilterMasterHostPort;
	}

	public void setFeedFilterMasterHostPort(String feedFilterMasterHostPort) {
		this.feedFilterMasterHostPort = "mongodb://" + feedFilterMasterHostPort;
	}

	public String getFeedFilterMasterDatabase() {
		return feedFilterMasterDatabase;
	}

	public void setFeedFilterMasterDatabase(String feedFilterMasterDatabase) {
		this.feedFilterMasterDatabase = feedFilterMasterDatabase;
	}

	public String getFeedFilterMasterCollection() {
		return feedFilterMasterCollection;
	}

	public void setFeedFilterMasterCollection(String feedFilterMasterCollection) {
		this.feedFilterMasterCollection = feedFilterMasterCollection;
	}

	public String getFeedFilterIpAccountHostPort() {
		return feedFilterIpAccountHostPort;
	}

	public void setFeedFilterIpAccountHostPort(String feedFilterIpAccountHostPort) {
		this.feedFilterIpAccountHostPort = "mongodb://" + feedFilterIpAccountHostPort;
	}

	public String getFeedFilterIpAccountDatabase() {
		return feedFilterIpAccountDatabase;
	}

	public void setFeedFilterIpAccountDatabase(String feedFilterIpAccountDatabase) {
		this.feedFilterIpAccountDatabase = feedFilterIpAccountDatabase;
	}

	public String getFeedFilterIpAccountCollection() {
		return feedFilterIpAccountCollection;
	}

	public void setFeedFilterIpAccountCollection(String feedFilterIpAccountCollection) {
		this.feedFilterIpAccountCollection = feedFilterIpAccountCollection;
	}

	public String getFeedAccountParentHostPort() {
		return feedAccountParentHostPort;
	}

	public void setFeedAccountParentHostPort(String feedAccountParentHostPort) {
		this.feedAccountParentHostPort = "mongodb://" + feedAccountParentHostPort;
	}

	public String getFeedAccountParentDatabase() {
		return feedAccountParentDatabase;
	}

	public void setFeedAccountParentDatabase(String feedAccountParentDatabase) {
		this.feedAccountParentDatabase = feedAccountParentDatabase;
	}

	public String getFeedAccountParentCollection() {
		return feedAccountParentCollection;
	}

	public void setFeedAccountParentCollection(String feedAccountParentCollection) {
		this.feedAccountParentCollection = feedAccountParentCollection;
	}

	public int getFeedPerformanceRecordsInterval() {
		return feedPerformanceRecordsInterval;
	}

	public void setFeedPerformanceRecordsInterval(int feedPerformanceRecordsInterval) {
		this.feedPerformanceRecordsInterval = feedPerformanceRecordsInterval;
	}
	
	public String getClient() {
		return client;
	}

	public void setClient(String client) {
		this.client = client;
	}
	
 //************************************** Extra Code **********************************************
	//*************** Commenting Megha Code for IP Account Details Mapping
    /*
    MyLogger.log("********************** IP's Initialization :: Start ***********************");
    database = ipDatabase;
    collection = ipCollection;
    readOverrides = new HashMap<String, String>();
    readOverrides.put("database", ipDatabase);
    readOverrides.put("collection", ipCollection);
    ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
    MyLogger.log("IP readOverrides : "+ readOverrides.toString());
    JavaRDD<Document> jRDDIPs = MongoSpark.load(jsc,readConfig).cache();
    MyLogger.log("MongoSpark Reading IP's : Done");
    
    //JavaMongoRDD<Document> jRddProjIP = jRDDIPs.withPipeline(singletonList(Document.parse("{ $project: {code : 1, low_ip_range : 1, high_ip_range : 1, ip_low : 1, ip_high : 1}}")));
    //JavaRDD<Document> jRddProjIP = jRDDIPs.filter(doc1 -> doc1.getString("low_ip_range").contains(":")).filter(doc1 -> doc1.getString("high_ip_range").contains(":"));
    //MyLogger.log("jRddProjIP withPipeline singletonList to project required fields : Done");
    JavaRDD<Document> jRddIPv4 = jRDDIPs.filter(doc1 -> doc1.get("low_ip_range").toString().trim().contains(".")).filter(doc1 -> doc1.get("high_ip_range").toString().trim().contains("."));
    Set<Document> set = jRddIPv4.countByValue().keySet();
    MyLogger.log("jRddProjIP4.countByValue() : Done");
    
    ipv4Map = new HashMap<String, String>();
    ipv6Map = new HashMap<String, String>();
    Document doc = null;
    ipLow = new Long[set.size()];
    ipHigh = new Long[set.size()];
    code = new String[set.size()];
    for(i=0;i<set.size();i++){
		doc = (Document)set.toArray()[i]; 
		//if(doc.get("low_ip_range").toString().trim().contains(".") && doc.get("high_ip_range").toString().trim().contains(".")){ // add later  && !doc.get("low_ip_range").toString().trim().contains(":") && !doc.get("high_ip_range").toString().trim().contains(":")
			//ipv4Map.put(doc.get("ip_low").toString().trim()+"-"+doc.get("ip_high").toString().trim(), doc.get("code").toString().trim());	
			ipLow[i]=Long.parseLong(doc.get("ip_low").toString().trim());
			ipHigh[i]=Long.parseLong(doc.get("ip_high").toString().trim());
			code[i]=doc.get("code").toString().trim();
		//}
		/*if(doc.get("low_ip_range").toString().trim().contains(":") && doc.get("high_ip_range").toString().trim().contains(":")){
			ipv6Map.put(doc.get("low_ip_range").toString().trim()+"-"+doc.get("high_ip_range").toString().trim(), doc.get("code").toString().trim());	
		}*/
	
    /*
	} 
    //IPDetails.setIpv4(ipv4Map);
    //IPDetails.setIpv6(ipv6Map);
    IPDetails.setIpLow(ipLow);
    IPDetails.setIpHigh(ipHigh);
    IPDetails.setCode(code);
   
    JavaRDD<Document> jRddIPv6 = jRDDIPs.filter(doc1 -> doc1.get("low_ip_range").toString().trim().contains(":")).filter(doc1 -> doc1.get("high_ip_range").toString().trim().contains(":"));
    Set<Document> setIP6 = jRddIPv6.countByValue().keySet();
    MyLogger.log("jRddProjIPv6.countByValue() : Done");
    ipLowRange = new String[setIP6.size()];
    ipHighRange = new String[setIP6.size()];
    codeRange = new String[setIP6.size()];
    for(i=0;i<setIP6.size();i++){
		doc = (Document)setIP6.toArray()[i];
		//if(doc.get("low_ip_range").toString().trim().contains(".") && doc.get("high_ip_range").toString().trim().contains(".")){ // add later  && !doc.get("low_ip_range").toString().trim().contains(":") && !doc.get("high_ip_range").toString().trim().contains(":")
			//ipv4Map.put(doc.get("ip_low").toString().trim()+"-"+doc.get("ip_high").toString().trim(), doc.get("code").toString().trim());	
    		ipLowRange[i]=doc.get("low_ip_range").toString().trim();
    		ipHighRange[i]=doc.get("high_ip_range").toString().trim();
    		codeRange[i]=doc.get("code").toString().trim();
		//}
		/*if(doc.get("low_ip_range").toString().trim().contains(":") && doc.get("high_ip_range").toString().trim().contains(":")){
			ipv6Map.put(doc.get("low_ip_range").toString().trim()+"-"+doc.get("high_ip_range").toString().trim(), doc.get("code").toString().trim());	
		}*/
	/*
	} 
    IPDetails.setIpLowRange(ipLowRange);
    IPDetails.setIpHighRange(ipHighRange);
    IPDetails.setCodeRange(codeRange);
    
    jRDDIPs.unpersist();
    MyLogger.log("IPv4 list size :: "+ set.size() + " :: Done");
    MyLogger.log("IPv6 list size :: "+ setIP6.size() + " :: Done");
    MyLogger.log("************** IP's Initialization :: End *********************");
    */
    //End of commenting Megha Code
    
    
    //MyLogger.log("******************** Set JavaRDD for IPv4 :: Start *****************************");
    //jRDDIPs = MongoSpark.load(jsc,readConfig);
    //JavaRDD<Document> jRddIPv4 = jRDDIPs.filter(doc1 -> doc1.get("low_ip_range").toString().trim().contains(".")).filter(doc1 -> doc1.get("high_ip_range").toString().trim().contains(".")).filter(doc1 -> !doc1.get("low_ip_range").toString().trim().contains(":")).filter(doc1 -> !doc1.get("low_ip_range").toString().trim().contains(":"));
    //MyLogger.log("Read ipv4 list size :: "+jRddIPv4.count());
    //IPDetails.setIpv4RDD(jRddIPv4);
    //Map<Document, Long> ipv4map2 = jRddIPv4.countByValue();
    //MyLogger.log("jRddIPv4.countByValue() : Done");
   /* if(ipv4map2 != null){
    	set = ipv4map2.keySet();
    	//MyLogger.log("set : " + page_type_map.toString());
	}else{
		MyLogger.log("ip_map : NULL");
	}
    ipv4Map = new HashMap<String, String>();
    for(i=0;i<set.size();i++){
		doc = (Document)set.toArray()[i]; 
		//if(doc.get("low_ip_range").toString().trim().contains(".") && doc.get("high_ip_range").toString().trim().contains(".") && !doc.get("low_ip_range").toString().trim().contains(":") && !doc.get("high_ip_range").toString().trim().contains(":")){
			ipv4Map.put(doc.get("ip_low").toString().trim()+"-"+doc.get("ip_high").toString().trim(), doc.get("code").toString().trim());	
		//}
		//if(doc.get("low_ip_range").toString().trim().contains(":") && doc.get("high_ip_range").toString().trim().contains(":")){
			ipv6Map.put(doc.get("low_ip_range").toString().trim()+"-"+doc.get("high_ip_range").toString().trim(), doc.get("code").toString().trim());	
		//}
	} 
    MyLogger.log("IPv4 list size :: "+ ipv4Map.size() + " :: Done");*/
    
	/*private void initializeMemory() throws Exception{
    	HashMap<String, String> readOverrides = new HashMap<String, String>();
    	ReadConfig readConfig = null;
    	int i=0;
    	Set set = null;
    	MyLogger.log("********************** Get Filter Memory Data :: Start ***********************");
	    database = "insight_memory_tables";
	    collection = "log_filters_memory";
	    readOverrides = new HashMap<String, String>();
	    readOverrides.put("database", database);
	    readOverrides.put("collection", collection);
	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
	    MyLogger.log("Log Filter Memory : "+ readOverrides.toString());            
	    JavaMongoRDD<Document> jRDDLogFilter = MongoSpark.load(jsc,readConfig);
	    //######################## Type 1 Array : Start ######################################
	    JavaRDD<Document> jRDDLogFilterType = jRDDLogFilter.filter(doc -> doc.getInteger("type") == 1);
	    set = jRDDLogFilterType.countByValue().keySet();
	    Document filter = null;
	    String[] type1Reg = new String[set.size()];
	    int[] type1Id = new int[set.size()];
	    for(i=0;i<set.size();i++){
	    	filter = (Document)set.toArray()[i]; 
	    	type1Reg[i] = filter.getString("reg_exp").trim();
	    	type1Id[i] = filter.getInteger("id");
		} 
	  //######################## Type 1 Array : End ######################################
	    
	  //######################## Type 2 Array : Start ######################################
	    jRDDLogFilterType = jRDDLogFilter.filter(doc -> doc.getInteger("type") == 2);
	    set = jRDDLogFilterType.countByValue().keySet();
	    filter = null;
	    String[] type2Reg = new String[set.size()];
	    int[] type2Id = new int[set.size()];
	    for(i=0;i<set.size();i++){
	    	filter = (Document)set.toArray()[i]; 
	    	type2Reg[i] = filter.getString("reg_exp").trim();
	    	type2Id[i] = filter.getInteger("id");
		} 
	  //######################## Type 2 Array : End ######################################
	    
	  //######################## Type 3 Array : Start ######################################
	    jRDDLogFilterType = jRDDLogFilter.filter(doc -> doc.getInteger("type") == 3);
	    set = jRDDLogFilterType.countByValue().keySet();
	    filter = null;
	    String[] type3Reg = new String[set.size()];
	    String[] type3Flag = new String[set.size()];
	    Set<String> type3RegSet = new TreeSet<String>();
	    int[] type3Id = new int[set.size()];
	    for(i=0;i<set.size();i++){
	    	filter = (Document)set.toArray()[i]; 
	    	type3Reg[i] = filter.getString("reg_exp").trim();
	    	type3RegSet.add(filter.getString("reg_exp").trim());
	    	type3Flag[i] = filter.getString("type_flag").trim();
	    	type3Id[i] = filter.getInteger("id");
		} 
	  //######################## Type 3 Array : End ######################################   
	    
	  //######################## Type 5 Array : Start ######################################
	    //jRDDLogFilterType = jRDDLogFilter.filter(doc -> doc.getInteger("type") == 5);
	    //set = jRDDLogFilterType.countByValue().keySet();
	    //PageViewFlag.initialize(jRDDLogFilterType.countByValue());
	    /*filter = null;
	    Pattern[] type5Reg = new Pattern[set.size()];
	    String[] type5Flag = new String[set.size()];
	    int[] type5Id = new int[set.size()];
	    String abc ="";
	    for(i=0;i<set.size();i++){
	    	filter = (Document)set.toArray()[i]; 
	    	abc = (".*" +filter.getString("reg_exp").trim()+ ".*").replaceAll("%",".*");
	    	//type5Reg[i] = Pattern.compile(".*" +filter.getString("reg_exp").trim().replaceAll("%",".*") , Pattern.CASE_INSENSITIVE);
	    	type5Reg[i] = Pattern.compile(abc , Pattern.CASE_INSENSITIVE);
	    	type5Flag[i] = filter.getString("type_flag").trim();
	    	type5Id[i] = filter.getInteger("id");
	    	
		} */
	  //######################## Type 5 Array : End ######################################
	   
	   
	    
	   /* MyLogger.log("********************** Get Filter Memory Data  :: End ***********************"); 
	    
	    
	    MyLogger.log("********************** Get Filter Memory IPs Data  :: Start ***********************");  
	  //######################## Type 4 Array : Start ######################################
	    database = "insight_memory_tables";
	    collection = "log_filters_ips_memory";
	    readOverrides = new HashMap<String, String>();
	    readOverrides.put("database", database);
	    readOverrides.put("collection", collection);
	    readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
	    MyLogger.log("Log Filter Memory : "+ readOverrides.toString());            
	    JavaRDD<Document> jRDDLogFilterIPs = MongoSpark.load(jsc,readConfig);
	    jRDDLogFilterType = jRDDLogFilterIPs.filter(doc -> doc.getInteger("type") == 4);            
	    set = jRDDLogFilterType.countByValue().keySet();
	    filter = null;
	    BigInteger[] type4LowIP = new BigInteger[set.size()];
	    BigInteger[] type4HighIP = new BigInteger[set.size()];
	    String[] type4Flag = new String[set.size()];
	    String[] type4InOutFlag = new String[set.size()];
	    int[] type4Id = new int[set.size()];
	    for(i=0;i<set.size();i++){
	    	filter = (Document)set.toArray()[i]; 
	    	type4LowIP[i] = new BigInteger(filter.getString("low_ip").trim());
	    	type4HighIP[i] = new BigInteger(filter.getString("high_ip").trim());
	    	type4Flag[i] = filter.getString("type_flag").trim();
	    	type4Id[i] = filter.getInteger("id");
	    	type4InOutFlag[i] = filter.getString("in_out_flag");
		} 
	  //######################## Type 4 Array : End ######################################
	    MyLogger.log("********************** Get Filter Memory IPs Data  :: End ***********************");
	    PageViewFlag.setType1Reg(type1Reg);
	    PageViewFlag.setType1Flag(type1Id);
	    
	    PageViewFlag.setType2Reg(type2Reg);
	    PageViewFlag.setType2Flag(type2Id);
	    
	    PageViewFlag.setType3Reg(type3Reg);
	    PageViewFlag.setType3Flag(type3Flag);
	    PageViewFlag.setType3Id(type3Id);
	    PageViewFlag.setType3RegSet(type3RegSet);
	    PageViewFlag.setType4Flag(type4Flag);
	    PageViewFlag.setType4Id(type4Id);
	    PageViewFlag.setType4HighIP(type4HighIP);
	    PageViewFlag.setType4LowIP(type4LowIP);
	    PageViewFlag.setType4InOutFlag(type4InOutFlag);
	    
	    /*PageViewFlag.setType5Reg(type5Reg);
	    PageViewFlag.setType5Flag(type5Flag);
	    PageViewFlag.setType5Id(type5Id);
}
    
    private void initializePageView5() throws Exception{
    	HashMap<String, String> readOverrides = new HashMap<String, String>();
    	ReadConfig readConfig = null;
    	double iTracker = 0.0;
    	try {
    		//************************ PAGE TYPE PATTERN LOGIC *************************
    		//Get all page page patterns from log_page_types_memory table
    		MyLogger.log("WebLogFilter : initializePageView() : Page View Pattern : Start");
    		iTracker = 1.0;
            database = "insight_memory_tables";
            collection = "log_filters_memory";
            iTracker = 2.0;
            readOverrides.put("database", database);
            readOverrides.put("collection", collection);
            iTracker = 3.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            iTracker = 4.0;
            MyLogger.log("Page View ReadConfig : readOverrides : "+ readOverrides.toString());
            MyLogger.log("Page View ReadConfig : Done");
            
            iTracker = 5.0;
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDpageTypeAll : Start");
            JavaMongoRDD<Document> jMRDDpageTypeAll = MongoSpark.load(jsc,readConfig);
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDpageTypeAll : Done");
            
            iTracker = 6.0;
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDPageTypeAggregated withPipeline & singletonList: Start");
            JavaMongoRDD<Document> jMRDDPageTypeAggregated = jMRDDpageTypeAll.withPipeline(singletonList(Document.parse("{ $project: { type:1, reg_exp : 1, type_flag:1}}")));
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDPageTypeAggregated withPipeline & singletonList: Done");
            
            iTracker = 7.0;
            MyLogger.log("mapPageType : jMRDDPageTypeAggregated.countByValue() : Start");
            Map<Document, Long> mapPageType = jMRDDPageTypeAggregated.filter(doc -> (doc.getInteger("type")) == 5).countByValue();
            MyLogger.log("jMRDDPageTypeAggregated.countByValue() : Done");
            if(mapPageType == null){
            	iTracker = 8.0;
            	throw new Exception("mapPageType : NULL");
            }else{
            	MyLogger.log("mapPageType : Size : " + mapPageType.size() + " : Done");
            }
            
            //initialing page type Class for filter doc operation
            iTracker = 9.0;
            PageViewFlag.initialize(mapPageType);
            iTracker = 10.0;
            //******************************************************************************
		} catch (Exception e) {
			throw new Exception("WebLogFilter : initializePageView5() : " + iTracker + " : " + e.toString());
		}
    }*/
	

	//Method to Initialize PageView Filters and classes 
	   /* private void initializePageViewIPFilters() throws Exception{
	    	HashMap<String, String> readOverrides = new HashMap<String, String>();
	    	ReadConfig readConfig = null;
	    	double iTracker = 0.0;
	    	try {
	    		//************************ PAGE TYPE PATTERN LOGIC *************************
	    		//Get all page page patterns from log_page_types_memory table
	    		MyLogger.log("WebLogFilter : initializePageViewIPFilters() : Page View Filters : Start");
	    		iTracker = 1.0;
	    		database = filterIPMemDatabase;
	            collection = filterIPMemCollection;
	            iTracker = 2.0;
	            readOverrides.put("database", database);
	            readOverrides.put("collection", collection);
	            iTracker = 3.0;
	            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
	            iTracker = 4.0;
	            MyLogger.log("Page View Filters ReadConfig : readOverrides : "+ readOverrides.toString());
	            MyLogger.log("Page View Filters ReadConfig : Done");
	            
	            iTracker = 5.0;
	            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDpageTypeAll : Start");
	            JavaRDD<Document> jMRDDpageViewIPFilterAll = MongoSpark.load(jsc,readConfig).filter(doc -> doc.getString("webmart_code").trim().equalsIgnoreCase("ieee"));
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
	            PageViewFilter.getPageViewIPMap(mapPageViewIPFilter.keySet());
	            //jMRDDpageViewIPFilterAll.unpersist();
	            iTracker = 10.0;
	            //******************************************************************************
			} catch (Exception e) {
				throw new Exception("WebLogFilter : initializePageViewIPFilters() : " + iTracker + " : " + e.toString());
			}
	    }*/
	
}
