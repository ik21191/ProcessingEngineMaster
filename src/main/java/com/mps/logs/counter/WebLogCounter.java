package com.mps.logs.counter;


import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.Counters;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mps.utils.Constants;
import com.mps.utils.MongoUtility;
import com.mps.utils.MyLogger;

import scala.Tuple2;

public class WebLogCounter implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private String logPageTypesMemoryHost = null;
    private String logPageTypesMemoryDatabase = null;
    private String logPageTypesMemoryCollection = null;
    
    private String logFilterHost = null;
    private String logFilterDatabase = null;
    private String logFilterCollection = null;
    
    private String logCounterHost = null;
    private String logCounterDatabase = null;
    private String logCounterCollectionPrefix = null;
    private String logCounterCollection = null;
    private String logCounterCollectionSuffix = null;
    
    private String counterStatsHost = null;
    private String counterStatsDatabase = null;
    private String counterStatsCollection = null;
    
    private String insightConfigHost = null;
    private String insightConfigDataBase = null;
    private String insightConfigCollection = null;
    
    
    private static Document taskDoc = null;
    
	long logFilterCount = 0;
    
    JavaRDD<Document> jRDDAfterDoubleClickPreProcessing;
    
    private String counterReconcileDatabase = null;
    private String counterReconcileCollection = null;
    
    private boolean doubleClickProcessingFlag = false;
    
	public void initialize() throws Exception{
    	try {
    		//Set counter processing start time
    		CounterStats.setProcessingStartTime();
    		
    		MyLogger.log("WebLogCounter : initialize() : Start");
    		
    		//method to initialize pageType duration for double click
    		initializePageTypeDoubleClickDuration();
    		MyLogger.log("WebLogCounter : initialize() : End");
		} catch (Exception e) {
			taskDoc.replace("status", -3);
			String description = taskDoc.getString("description");
			taskDoc.replace("description", description + "WebLogCounter : Exception while initialization method initialize() #");
			//update status for this task
			updateTask();
			MyLogger.log("WebLogCounter : EXCEPTION : initialize()");
			throw e;
		}
    }
    
	private void initializeTask() throws Exception{
    	try {
        	
        	this.setLogPageTypesMemoryHost((String) taskDoc.getOrDefault("log_page_types_memory_host_port", ""));
            this.setLogPageTypesMemoryDatabase((String) taskDoc.getOrDefault("log_page_types_memory_database", ""));
            this.setLogPageTypesMemoryCollection((String) taskDoc.getOrDefault("log_page_types_memory_collection", ""));
            
            this.setLogFilterHost((String) taskDoc.getOrDefault("filter_host_port", ""));
            this.setLogFilterDatabase((String) taskDoc.getOrDefault("filter_database", ""));
            this.setLogFilterCollection((String)taskDoc.getOrDefault("filter_collection", ""));
            
            this.setLogCounterHost((String) taskDoc.getOrDefault("counter_host_port", ""));
            this.setLogCounterDatabase((String) taskDoc.getOrDefault("counter_database", ""));
            this.setLogCounterCollectionPrefix((String) taskDoc.getOrDefault("counter_collection_prefix", ""));
            this.setLogCounterCollection((String) taskDoc.getOrDefault("counter_collection", ""));
            this.setLogCounterCollectionSuffix((String) taskDoc.getOrDefault("counter_collection_suffix", ""));
            
            this.setCounterStatsHost((String) taskDoc.getOrDefault("counter_stats_host_port", ""));
            this.setCounterStatsDatabase((String) taskDoc.getOrDefault("counter_stats_database", ""));
            this.setCounterStatsCollection((String) taskDoc.getOrDefault("counter_stats_collection", ""));
            
            this.setInsightConfigHost(insightConfigHost);
            this.setInsightConfigDataBase(insightConfigDataBase);
            this.setInsightConfigCollection(insightConfigCollection);
            
        } catch (Exception e) {   
        	taskDoc.replace("status", -3);
        	String description = taskDoc.getString("description");
        	taskDoc.replace("description", description + "WebLogCounter : Exception while initialization method initializeTask(Document task) #");
        	//update status for this task
        	updateTask();
            throw e;
        } 
    }
    
    
    //
    public void processTask(Document taskDoc) throws Exception {
    	
    	try {
        	MyLogger.log("WebLogCounter : processTask() : Start");
        	
        	WebLogCounter.taskDoc = taskDoc;
        	
        	//initializing task
        	initializeTask();
        	//initializing class
        	initialize();
        	
        	/**processing for intermediate steps before running DoubleClick*/
        	doubleClickPreProcessing();
        	
        	/**actually running Double Click Logic*/
        	//do double click processing if doubleClickPreProcessing is done successfully
        	
        	if(doubleClickProcessingFlag) {
        		doubleClickProcessing();
        	}
        	
        	MyLogger.log("WebLogCounter : processTask() : Start");
        } catch (Exception e) {
            MyLogger.error("WebLogCounter: EXCEPTION : processTask(Document taskDoc) " + e.toString());
            taskDoc.replace("status", -3);
    		String description = taskDoc.getString("description");
    		taskDoc.replace("description", description + "WebLogCounter : There was some exception in processTask(Document taskDoc) #");
    		//update status for this task
        	updateTask();
            throw e;
        }
	}

    private void insertCounterStats() {
		
    	Document counterStats = new Document();
    	counterStats.append("filter_host_port", CounterStats.getFilterHostPort())
    	.append("filter_database", CounterStats.getFilterDatabase())
    	.append("filter_collection", CounterStats.getFilterCollection())
    	
    	.append("counter_host_port", CounterStats.getCounterHostPort())
    	.append("counter_database", CounterStats.getCounterDatabase())
    	.append("counter_collection", CounterStats.getCounterCollection())
    	
    	.append("total_records", CounterStats.getTotalRecords())
    	.append("skipped_records", CounterStats.getSkippedRecords())
    	.append("double_click_records", CounterStats.getDoubleClickRecords())
    	
    	.append("processing_start_time", CounterStats.getProcessingStartTime())
    	.append("processing_end_time", CounterStats.getProcessingEndTime())
    	.append("load_filter_with_query", CounterStats.getLoadFilterWithQuery());
    	
    	MongoClient client = null;
    	try {
    		client = new MongoClient(counterStatsHost);
    		MongoDatabase database = client.getDatabase(counterStatsDatabase);
    		MongoCollection<Document> collection = database.getCollection(counterStatsCollection);
    		
    		collection.insertOne(counterStats);
    	}catch(Exception e) {
    		MyLogger.error("WebLogCounter : EXCEPTION : insertCounterStats() : " + taskDoc + "\n" + e);
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
    		MyLogger.error("WebLogCounter: EXCEPTION : updateTask : " + taskDoc + "\n" + e);
    	} finally {
    		if(client != null) {
    			client.close();
    		}
    	}
	}
	
	private void truncateCollection(String hostPort, String database, String collection) {
		MongoClient client = null;
		MyLogger.log("WebLogCounter : Deleting all Documents from " + hostPort + "  " + database + "  " + collection);
    	try {
    		client = new MongoClient(hostPort.replace("mongodb://", ""));
    		MongoDatabase mongoDB = client.getDatabase(database);
    		MongoCollection<Document> mongoCollection = mongoDB.getCollection(collection);
    		
    		//To delete all the documents form the collection
    		mongoCollection.deleteMany(new BasicDBObject());
    	}catch(Exception e) {
    		MyLogger.error("WebLogCounter : truncateCollection : EXCEPTION : " + e);
    	} finally {
    		if(client != null) {
    			client.close();
    		}
    	}
	}

	private void doubleClickPreProcessing() {
		setLogFilterCount(0);
		jRDDAfterDoubleClickPreProcessing = null;
    	JavaMongoRDD<Document> javaRddFilterLogs;
    	
    	try {
    		MyLogger.log("WebLogCounter: doubleClickPreProcessing : Start");
        	
        	/**Reading Filter*/
    		CounterStats.setFilterHostPort(logFilterHost);
    		CounterStats.setFilterDatabase(logFilterDatabase);
    		CounterStats.setFilterCollection(logFilterCollection);
    		
    		javaRddFilterLogs = MongoUtility.getJavaMongoRDDFromMongo(logFilterHost, logFilterDatabase, logFilterCollection);
    	    
    	    if(javaRddFilterLogs == null) {
    	    	throw new Exception();
    	    }
    	    
    	    JavaRDD<Document> javaRddFilterLogsForDoubleClick = null;
    	    
    	    String loadFilterWithQuery = (String)taskDoc.getOrDefault("load_filter_with_query", "");
    	    /**Below if condition is written to check count difference*/
    	    if(loadFilterWithQuery.equalsIgnoreCase("Yes")) {
    	    	CounterStats.setLoadFilterWithQuery(loadFilterWithQuery);
    	    	MyLogger.log("WebLogCounter: Value of load_filter_with_query = " + loadFilterWithQuery);
    	    	MyLogger.log("WebLogCounter: Querying mongoDB to fetch records where pageType != 0 and page_view_flag==counter : Start");
    	    	BasicDBObject basicDBOObject = new BasicDBObject("$match", new BasicDBObject().append("page_view_flag", "counter").
    		    		append(Constants.PAGE_TYPE, new BasicDBObject("$ne", 0)));
    	    	javaRddFilterLogsForDoubleClick = javaRddFilterLogs.withPipeline(Collections.singletonList(basicDBOObject));
    	    	MyLogger.log("WebLogCounter: Querying mongoDB to fetch LogFilter where pageType != 0 and page_view_flag==counter : End");
    	    } else if(loadFilterWithQuery.equalsIgnoreCase("No")) {
    	    	CounterStats.setLoadFilterWithQuery(loadFilterWithQuery);
    	    	MyLogger.log("WebLogCounter: Value of load_filter_with_query = " + loadFilterWithQuery);
    	    	MyLogger.log("WebLogCounter: Remove records where pageType != 0 and page_view_flag==counter : through filter() method of javaRDD");
    	    	javaRddFilterLogsForDoubleClick = javaRddFilterLogs.filter(doc -> DoubleClickDocument.filterJRDD(doc));
    	    } else {
    	    	javaRddFilterLogsForDoubleClick = javaRddFilterLogs.filter(doc -> DoubleClickDocument.filterJRDD(doc));
    	    }
    	    
            //sorting rdd
            MyLogger.log("WebLogCounter: Sorting javaRDD : Start");
            
            JavaPairRDD<Document, Integer> javaPairRdd = javaRddFilterLogsForDoubleClick.mapToPair(document->new Tuple2<>(document, 1));
            
            Comparator<Document> comparator = new DocumentComparator();
            JavaPairRDD<Document, Integer> javaPairRddSorted = javaPairRdd.sortByKey(comparator);
		    
            jRDDAfterDoubleClickPreProcessing = javaPairRddSorted.map(tuple->tuple._1);
            
	    	MyLogger.log("WebLogCounter: Sorting javaRDD : End");
	    	MyLogger.log("WebLogCounter: doubleClickPreProcessing : End");
	    	doubleClickProcessingFlag = true;
	    	
		} catch (Exception e){
			taskDoc.replace("status", -3);
			String description = taskDoc.getString("description");
			taskDoc.replace("description", description + "WebLogCounter : Exception in doubleClickPreProcessing() #");
			MyLogger.log("WebLogCounter: EXCEPTION : doubleClickPreProcessing() : " + e);
			doubleClickProcessingFlag = false;
		} finally {
			//update status for this task
			updateTask();
		}
    }
    

	public void doubleClickProcessing() {
		boolean isSuccessfullySaved = false;
		JavaRDD<Document> jRDDLogsAfterDoubleClick = null;
		
		String logCounterCollectionName = logCounterCollectionPrefix + logCounterCollection + logCounterCollectionSuffix;
		try {
			//initializing stats
			CounterStats.refresh();
			
			MyLogger.log("WebLogCounter: Double Click processing : Start");
			jRDDLogsAfterDoubleClick = jRDDAfterDoubleClickPreProcessing.flatMap((final Document doc) -> DoubleClickDocument.getDoubleClickDocument(doc).iterator());
			
			MyLogger.log("WebLogCounter: Counter Data MongoSpark.save : Start");
			
			String existingCounterCollectionAction = (String)taskDoc.getOrDefault("existing_counter_collection_action", "");
            if(existingCounterCollectionAction.equalsIgnoreCase("truncate")) {
            	MyLogger.log("WebLogCounter : Value of existing_counter_collection_action : " + existingCounterCollectionAction);
            	truncateCollection(logCounterHost, logCounterDatabase, logCounterCollectionName);
            	isSuccessfullySaved = MongoUtility.saveJavaRDDtoMongoDB(logCounterHost, logCounterDatabase, logCounterCollectionName, jRDDLogsAfterDoubleClick);
            } else if(existingCounterCollectionAction.equalsIgnoreCase("append")) {
            	MyLogger.log("WebLogCounter : Value of existing_counter_collection_action : " + existingCounterCollectionAction);
            	isSuccessfullySaved = MongoUtility.saveJavaRDDtoMongoDB(logCounterHost, logCounterDatabase, logCounterCollectionName, jRDDLogsAfterDoubleClick);
            } else {
            	MyLogger.log("WebLogCounter : No existing_counter_collection_action is defined so creating default counter collection");
            	logCounterCollectionName = logCounterCollectionName + "_" + (new SimpleDateFormat("yyyy M dd hh mm ss").format(new Date())).replace(" ", "");
            	isSuccessfullySaved = MongoUtility.saveJavaRDDtoMongoDB(logCounterHost, logCounterDatabase, logCounterCollectionName, jRDDLogsAfterDoubleClick);
            }
			
			CounterStats.setCounterHostPort(logCounterHost);
			CounterStats.setCounterDatabase(logCounterDatabase);
			CounterStats.setCounterCollection(logCounterCollectionName);

			//Log processing End time
			CounterStats.setProcessingEndTime();
			if(isSuccessfullySaved) {
				taskDoc.replace("status", 1);
				taskDoc.replace("description", "Counter has been created successfully.");
			} else {
				taskDoc.replace("status", -3);
				taskDoc.replace("description", "There was a problem while creating counter.");
			}
			
			MyLogger.log("WebLogCounter: Counter Data MongoSpark.save : End");
			
			//Save counter stats
			insertCounterStats();
			
			MyLogger.log("WebLogCounter: Double Click processing : " + CounterStats.getStats() + " :  End");
		} catch (Exception e) {
			taskDoc.replace("status", -3);
			String description = taskDoc.getString("description");
			taskDoc.replace("description", description + "WebLogCounter : Exception in method doubleClickProcessing() #");
			MyLogger.error("WebLogCounter: EXCEPTION : doubleClickProcessing() : " + e);
		} finally {
			//update status of this task
			updateTask();
		}
	}
    
	

    //method to set DocumentDoubleClick Duration Map
	private void initializePageTypeDoubleClickDuration() throws Exception {
		Map<Integer, Integer> pageTypeDurationMap = new HashMap<>();

		MyLogger.log("WebLogCounter: initializePageTypeDoubleClickDuration : Start");
		try {
			
			JavaRDD<Document> jRDDLogPageTypes = MongoUtility.getJavaRDDFromMongo(logPageTypesMemoryDatabase, logPageTypesMemoryCollection, logPageTypesMemoryHost);
			
			if(jRDDLogPageTypes == null) {
				throw new Exception();
			}
			
			JavaRDD<Document> webMartSpecificRdd = jRDDLogPageTypes.filter(doc -> doc.get("webmart_code").toString().equals("ieee"));
			List<Document> documentList = webMartSpecificRdd.collect();
			for (Document document : documentList) {
				pageTypeDurationMap.put(document.getInteger("page_type_id"), document.getInteger("duration"));
			}

			// setting list in DoubleClickDocument Class
			DoubleClickDocument.pageTypeDurationMap = pageTypeDurationMap;

			MyLogger.log("WebLogCounter: initializePageTypeDoubleClickDuration : Total + " + pageTypeDurationMap.size() + ": Records Found : End");

		} catch (Exception e) {
			taskDoc.replace("status", -3);
			String description = taskDoc.getString("description");
			taskDoc.replace("description", description + "WebLogCounter : Exception in method initializePageTypeDoubleClickDuration() #");
			MyLogger.log("WebLogCounter: EXCEPTION : initializePageTypeDoubleClickDuration() : " + e);
			throw e;
		} finally {
			//update status for this task
			updateTask();
		}
	}
        
	
	
    //*******************
    // GETTER & SETTER ***
	
	public String getLogPageTypesMemoryDatabase() {
		return logPageTypesMemoryDatabase;
	}

	public void setLogPageTypesMemoryDatabase(String logPageTypesMemoryDatabase) {
		this.logPageTypesMemoryDatabase = logPageTypesMemoryDatabase;
	}

	public String getLogPageTypesMemoryCollection() {
		return logPageTypesMemoryCollection;
	}

	public void setLogPageTypesMemoryCollection(String logPageTypesMemoryCollection) {
		this.logPageTypesMemoryCollection = logPageTypesMemoryCollection;
	}

	public String getLogPageTypesMemoryHost() {
		return logPageTypesMemoryHost;
	}

	public void setLogPageTypesMemoryHost(String logPageTypesMemoryHost) {
		this.logPageTypesMemoryHost = "mongodb://" + logPageTypesMemoryHost;
	}

	public String getLogFilterHost() {
		return logFilterHost;
	}

	public void setLogFilterHost(String logFilterHost) {
		this.logFilterHost = "mongodb://" + logFilterHost;
	}

	public String getLogFilterDatabase() {
		return logFilterDatabase;
	}

	public void setLogFilterDatabase(String logFilterDatabase) {
		this.logFilterDatabase = logFilterDatabase;
	}

	public String getLogFilterCollection() {
		return logFilterCollection;
	}

	public void setLogFilterCollection(String logFilterCollection) {
		this.logFilterCollection = logFilterCollection;
	}

	public String getLogCounterHost() {
		return logCounterHost;
	}

	public void setLogCounterHost(String logCounterHost) {
		this.logCounterHost = "mongodb://" + logCounterHost;
	}

	public String getLogCounterDatabase() {
		return logCounterDatabase;
	}

	public void setLogCounterDatabase(String logCounterDatabase) {
		this.logCounterDatabase = logCounterDatabase;
	}

	public String getLogCounterCollectionPrefix() {
		return logCounterCollectionPrefix;
	}

	public void setLogCounterCollectionPrefix(String logCounterCollectionPrefix) {
		this.logCounterCollectionPrefix = logCounterCollectionPrefix;
	}

	public String getLogCounterCollection() {
		return logCounterCollection;
	}

	public void setLogCounterCollection(String logCounterCollection) {
		this.logCounterCollection = logCounterCollection;
	}

	public String getLogCounterCollectionSuffix() {
		return logCounterCollectionSuffix;
	}

	public void setLogCounterCollectionSuffix(String logCounterCollectionSuffix) {
		this.logCounterCollectionSuffix = logCounterCollectionSuffix;
	}

	public String getCounterStatsHost() {
		return counterStatsHost;
	}

	public void setCounterStatsHost(String counterStatsHost) {
		this.counterStatsHost = counterStatsHost;
	}

	public String getCounterStatsDatabase() {
		return counterStatsDatabase;
	}

	public void setCounterStatsDatabase(String counterStatsDatabase) {
		this.counterStatsDatabase = counterStatsDatabase;
	}

	public String getCounterStatsCollection() {
		return counterStatsCollection;
	}

	public void setCounterStatsCollection(String counterStatsCollection) {
		this.counterStatsCollection = counterStatsCollection;
	}

	public long getLogFilterCount() {
		return logFilterCount;
	}

	public void setLogFilterCount(long logFilterCount) {
		this.logFilterCount = logFilterCount;
	}

	public String getCounterReconcileDatabase() {
		return counterReconcileDatabase;
	}

	public void setCounterReconcileDatabase(String counterReconcileDatabase) {
		this.counterReconcileDatabase = counterReconcileDatabase;
	}

	public String getCounterReconcileCollection() {
		return counterReconcileCollection;
	}

	public void setCounterReconcileCollection(String counterReconcileCollection) {
		this.counterReconcileCollection = counterReconcileCollection;
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
}

//////################### ***********REFERENCE CODE ******************* ############################



/////////////////////////Code previously used///////////////////
/*public void run() throws Exception {
	//String reconcileCollection = counterReconcileCollection + currentProcessingYear + currentProcessingMonth;
	try {
    	MyLogger.log("***************** START ********************");
    	MyLogger.log("WebLogCounter : run() : Start");
    	
    	//initializing class
    	//initialize();
    	
    	*//**Setting source and destination database and collection*//*
    	if(processingType.trim().equalsIgnoreCase("Monthly")){
    		sourceFilterColl = sourceLogFilterCollection + currentProcessingYear + currentProcessingMonth;
			destinationCounterColl = destinationCounterLogCollection + currentProcessingYear + currentProcessingMonth;
			MyLogger.log("WebLogCounter: Processing Mode : MONTHLY");
    	}else if(processingType.trim().equalsIgnoreCase("Daily")){
    		Calendar monthStart = new GregorianCalendar(Integer.parseInt(currentProcessingYear), Integer.parseInt(currentProcessingMonth) - 1, 1);
    		noOfDays = monthStart.getActualMaximum(Calendar.DAY_OF_MONTH);
    		MyLogger.log("WebLogCounter: Processing Mode : DAILY");
    	}
    	*//**Loop through all days of the month*//*
    	for(dayNo = 1; dayNo <= noOfDays; dayNo++){
    		MyLogger.log("WebLogCounter: **********************************************");
    		if(processingType.trim().equalsIgnoreCase("Daily")){
        		 if(dayNo < 10){
        			 sourceFilterColl = sourceLogFilterCollection + currentProcessingYear + currentProcessingMonth + "0" + (dayNo);
        			 destinationCounterColl = destinationCounterLogCollection + currentProcessingYear + currentProcessingMonth + "0" + (dayNo);
        		 }else{
        			 sourceFilterColl = sourceLogFilterCollection + currentProcessingYear + currentProcessingMonth + (dayNo);
        			 destinationCounterColl = destinationCounterLogCollection + currentProcessingYear + currentProcessingMonth  + (dayNo);
        		 }
        	}
    		
        	doubleClickPreProcessing();
        	if(getLogFilterCount() == 0) {
        		MyLogger.log("WebLogCounter: No data found in the logFilter : " + sourceFilterColl + " hence skipping double click processing.");
        		continue;
        	}
        	*//**actually running Double Click Logic*//*
        	//do double click processing if doubleClickPreProcessing is done successfully
        	
        	if(doubleClickProcessingFlag) {
        		doubleClickProcessing(destinationCounterColl);
        	} 
        	
        	
       }
    	*//**processing for intermediate steps before running DoubleClick*//*
    	counterStatsDoc.append("preprocessing_start_time", Helper.getCurrentDateTime());
    	doubleClickPreProcessing();
    	counterStatsDoc.append("preprocessing_end_time", Helper.getCurrentDateTime());
    	
    	*//**actually running Double Click Logic*//*
    	//do double click processing if doubleClickPreProcessing is done successfully
    	
    	if(doubleClickProcessingFlag) {
    		counterStatsDoc.append("counterprocessing_start_time", Helper.getCurrentDateTime());
    		doubleClickProcessing();
    		counterStatsDoc.append("counterprocessing_end_time", Helper.getCurrentDateTime());
    	}

    	//update status of current task
    	
    	
    	//insert counter stats for this task
    	insertCounterStats(counterStatsDoc);
    	
    	MyLogger.log("WebLogCounter: ***************** END ********************");
    } catch (Exception e) {
    	taskDoc.replace("status", -3);
    	String description = taskDoc.getString("description");
    	taskDoc.replace("description", description + "WebLogCounter : Exception in method run() #");
    	//update status for this task
    	updateTask();
        MyLogger.log("WebLogCounter: run : " + e.toString());
        throw e;
    }
}*/

/*private void createAndSaveReconcileData(JavaRDD<Document> javaRddAllDayWiseCounter, String counterReconcileCollection) {
try {
	JavaPairRDD<Document, Integer> javaPairRdd = javaRddAllDayWiseCounter.mapToPair(document -> {
		Document doc = new Document();
		for (String value : groupByList) {
			value = value.trim();
			if (value.equalsIgnoreCase(Constants.PAGE_TYPE)) {
				doc.append(Constants.PAGE_TYPE, document.getInteger(Constants.PAGE_TYPE));
			} else {
				doc.append(value, document.getString(value));
			}
		}
		return new Tuple2<>(doc, 1);
	});

	JavaPairRDD<Document, Integer> javaRddWithDocCount = javaPairRdd.reduceByKey((val1, val2) -> val1 + val2);
	
	JavaRDD<Document> javaRdd = javaRddWithDocCount.map(tuple-> {
		CounterStats.updateReconcileCount();
		return tuple._1.append("count", tuple._2);
	});
	
	//If sorting is required then uncomment below line
	//JavaRDD<Document> sortedRddBasedOnCount = javaRdd.sortBy(doc -> doc.getInteger("count"), false, 1);

	Map<String, String> writeOverrides = new HashMap<>();
	writeOverrides.put(Constants.MONGO_DATABASE, counterReconcileDatabase);
	writeOverrides.put(Constants.MONGO_COLLECTION, counterReconcileCollection);
	writeOverrides.put("writeConcern.w", "majority");
	WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
	
	MyLogger.log("WebLogCounter: Saving reconcile data to ==> " + writeOverrides + " Start.");
	MongoSpark.save(javaRdd, writeConfig);
	MyLogger.log("WebLogCounter: Saving reconcile data End. Total Reconcile data count is " + CounterStats.getReconcileCount() + ".");
} catch (Exception e) {
	MyLogger.log("WebLogCounter: There was an exception while creating Reconcile table. " + e);
}
}*/