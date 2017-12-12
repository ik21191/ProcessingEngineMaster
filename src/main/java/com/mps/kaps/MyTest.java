/**
 * 
 */
package com.mps.kaps;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mps.mysql.MySqlConnection;
import com.mps.spark.MySpark;
import com.mps.utils.Constants;
import com.mps.utils.MyDataTable;
import com.mps.utils.MyException;
import com.mps.utils.MyLogger;

import static org.apache.spark.sql.functions.col;

/**
 * @author kapil.verma
 *
 */
public class MyTest {

	/**
	 * @param args
	 */
	//JavaSparkContext jsc;
	SparkSession session;
	static MyDataTable mdt = null;
	static Connection connection; 
	private static long recordsAffected = 0;
	private static int batch = 0;  
	private static Dataset<Row> outputDS = null;
	private static int recordCommitBatchSize = 0;
	private static String columnName = "";
	private static HashMap<String, MyDataTable> mdtMap = new HashMap<>();
	
	private void initialize() throws Exception{
		this.session = new MySpark().getSparkSession();
		//this.jsc = new MySpark().getJavaSparkContext();
	}
	
	public static void main(String[] args) {
		try {
			new MyTest().run();
			//new MyTest().createTableColumns();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void run() throws MyException{
		HashMap<String, String> options = new HashMap<>();
		Dataset<Row> dataSet;
		String database = "ieee_new_201710";
		String collection = "counter_201710";
		
		String inputUri = "mongodb://10.50.8.218:27017/web_log.web_log";
		String outputUri = "mongodb://10.50.8.218:27017/web_log.web_log";
		String dd = "";
		int endDay =31;
		int startDay = 1;
		double iTracker = 0.0;
		try {
			MyLogger.log("***************** JOURNAL_REPORT_1 *****************");

			//initializing app
			iTracker = 1.0;
			initialize();

			//
			database = "ieee_201710"; //for local testing
			collection = "counter_201710"; //for local testing
			inputUri = "mongodb://127.0.0.1:27017/web_log.web_log"; //for local testing
			outputUri = "mongodb://127.0.0.1:27017/web_log.web_log"; //for local testing
			startDay = 9; //for local testing
			endDay = 10; //for local testing
			//
			
			//looping for each day of month
			int day = 1;
			mdt = new MyDataTable("c4_jr1");
			for(day = startDay; day <= endDay ; day++){
				try{
					//clean up
					//mdt = null;
					dataSet = null;
					outputDS = null;
					recordsAffected = 0;
					batch = 0;
					//
					
					if(day < 10){dd = "0" + day;}else{dd = "" + day;}
					columnName = collection.replaceAll("counter", "D") + dd;
					
					MyLogger.log("*****Processing for : " + columnName);
					
					iTracker = 2.0;
					options.put(Constants.MONGO_INPUT_URI , inputUri);
					options.put(Constants.MONGO_OUTPUT_URI, outputUri);
					options.put(Constants.DATABASE, database);
		            options.put(Constants.COLLECTION, collection + dd);
		            ReadConfig readConfig = ReadConfig.create(this.session).withOptions(options);
		            MyLogger.log("Source ReadConfig : "+ options.toString());
		            
		            //
		            iTracker = 4.0;
		            MyLogger.log("MongoSpark.loadAndInferSchema : dataSet : Start");
					dataSet = MongoSpark.loadAndInferSchema(this.session, readConfig);
					//MyLogger.log("counterRDD : count : " + dataSet.count());
					MyLogger.log("MongoSpark.loadAndInferSchema : dataSet : End");
					
					iTracker = 5.0;
					//applying group by on DataSet
					//output = dataSet.groupBy("institution_id", "journal_id", "page_type").count().withColumnRenamed("count", columnName);
					//.where("page_type=10 ");
					
					iTracker = 6.0;
					//Query for JR1
					//Dataset<Row> ds1 = dataSet.filter(col("page_type").lt(13));
					outputDS = dataSet.groupBy("institution_id", "journal_id", "page_type").count().withColumnRenamed("count", columnName).filter(col("page_type").lt(13));
					
					iTracker = 7.0;
					//getting mysqlConnection
					connection = MySqlConnection.getMariaDBConnection("", "", "", "");
					//connection = DriverManager.getConnection("jdbc:mariadb://localhost:3306/report_master?user=root&password=root");
					MyLogger.log("MariaDB : is Connection Closed() : " + connection.isClosed());
					
					iTracker = 8.0;
					//setting records commit batch size for Database entry
					recordCommitBatchSize = 0;
					mdt.addColumn("INSTITUTION_ID", "-");
					mdt.addColumn("JOURNAL_ID", "-");
					mdt.addColumn("PAGE_TYPE", "0");
					mdt.addColumn(columnName, "0");
					
					MyLogger.log("Dataset Record Iteration : Batch Size : " + recordCommitBatchSize + " : Start");
					outputDS.foreach((ForeachFunction<Row>) row -> fillMyDataTable(row));
					//commit for final left out/ALL records if any
					
					iTracker = 10.0;
					// ZERO means all records will be updated in all in bulk insert step
					MyLogger.log("MyDataTable Ready : Rows=" + mdt.getRowCount() + " : Columns=" + mdt.getColumnCount());
					if(recordCommitBatchSize == 0){
						//recordsAffected = recordsAffected + mdt.commit(connection);
					}else if(recordsAffected != batch * recordCommitBatchSize){
						//recordsAffected = recordsAffected + mdt.commit(connection);
					}
					MyLogger.log("Dataset Record Iteration: End");
					
					//commiting hashmap
					MyDataTable dt = null;
					String tableName = "";
					for (Entry entry : mdtMap.entrySet()){
						tableName = entry.getKey().toString();
						MyLogger.log("Processing for table : " + tableName);
						//check and create table
						MyLogger.log("Check for table : " + tableName + " : " + MySqlConnection.checkAndCreateTable(tableName , "c4_jr1_template", connection));
						dt = (MyDataTable)entry.getValue();
						int recordsupdated = dt.commit(connection);
						MyLogger.log("Processing Done for table : " + tableName + " : Records Updated : " + recordsupdated);
					}
					mdtMap = new HashMap<>();
					
					//
					iTracker = 12.0;
					MyLogger.log("Total : Batch Processed : " + batch + " : Records Affected : "  + recordsAffected + "  : Reporting Database : " + columnName + " : for Collection : " + collection + dd);
				}catch(Exception e){
					MyLogger.log("EXCEPTION : " + e.toString());
				}finally{
					//mdt = null;
					outputDS = null;
					dataSet = null;
					recordsAffected = 0;
					batch = 0;
					if(connection != null){connection.close();}
					session.clearDefaultSession();
					session.clearActiveSession();
					System.gc();
				}
			}
			
			//commiting hashmap
			MyDataTable dt = null;
			String tableName = "";
			for (Entry entry : mdtMap.entrySet()){
				tableName = entry.getKey().toString();
				MyLogger.log("Processing for table : " + tableName);
				//check and create table
				MySqlConnection.checkAndCreateTable(tableName , "c4_jr1_template", connection);
				dt = (MyDataTable)entry.getValue();
				int recordsupdated = dt.commit(connection);
				MyLogger.log("Processing for table : " + entry.getKey().toString() + " : Records Updated : " + recordsupdated);
			}
			
			//DT to be committed here
			MyLogger.log("Writing MDT to CSV..");
			MyLogger.log(mdt.getCsvData());
			
			/*
			//Hell Hell Hell
			//MyLogger.log(output.collectAsList().toString());
			
			//property for MySql connection
			String url= "jdbc:mysql://localhost:3306/report_master";
			String mysqlTable = "c4_journal_report_1";
			String user = "root";
			String password = "root";
			Properties mysqlConf =  new Properties();
			mysqlConf.put("user", user);
			mysqlConf.put("password", password);
			
			//Saving Dataset MySql Database
			output.write().mode(SaveMode.Ignore).jdbc(url, mysqlTable, mysqlConf);
			//dataSet.groupBy("institution_id", "journal_id", "article_id", "page_type").count().write().csv("C:/Users/kapil.verma/Desktop/ieee_9_oct_counter_inst/dataset_output");
			
			
			//
			MyDataTable mdt = new MyDataTable("c4_journal_report_1");
			mdt.addColumn("INSTITUTION_ID", "-");
			mdt.addColumn("JOURNAL_ID", "-");
			mdt.addColumn("PAGE_TYPE", "-");
			mdt.addColumn("DAY_9", "0");
			
			//filling via MDT
			mdt = new MyDataTable("c4_journal_report_1");
			String[] columns = output.columns();
			int columnIndex = 0;
			for(columnIndex = 0; columnIndex < columns.length; columnIndex++){
				mdt.addColumn(columns[columnIndex].trim().toUpperCase(), "");
			}
			
			output.foreach((ForeachFunction<Row>) row -> System.out.println(row));
			
			List<Row> rowList =output.collectAsList();
			int rowIndex = 1;
			int listCount = rowList.size();
			for(rowIndex = 1 ; rowIndex <= listCount; rowIndex++){
				Row row = rowList.get(rowIndex);
				mdt.addRow();
				mdt.updateData(rowIndex, columns[0].trim().toUpperCase(), row.getString(0));
				mdt.updateData(rowIndex, columns[1].trim().toUpperCase(), row.getString(1));
				mdt.updateData(rowIndex, columns[2].trim().toUpperCase(), row.getString(2));
				mdt.updateData(rowIndex, columns[3].trim().toUpperCase(), row.getString(3));
				mdt.updateData(rowIndex, columns[4].trim().toUpperCase(), row.getString(4));
			}
			
			
			mdt.commit(connection);
			*/
			
			
            //
			/*
            iTracker = 3.0;
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDDocAll : Start");
            counterRDD = MongoSpark.load(jsc,readConfig);
            MyLogger.log("jMRDDDocAll : count : " + counterRDD.count());
            MyLogger.log("MongoSpark.load(jsc,readConfig) : jMRDDDocAll : End");
            */
            
		} catch (Exception e) {
			throw new MyException(e.toString());
		}
	}
	
	//
	private static void fillMyDataTable(Row row){
		String rowKey = "";
		int rowIndex = 0;
		try{
			//adding data in mdt row
			rowKey = (row.get(0).toString().trim()+"~#~" + row.get(1).toString().trim()+"~#~" + row.get(2).toString().trim()).toUpperCase();
			rowIndex = mdt.getRowIndex(rowKey);
			
			if(rowIndex > 0){
				mdt.updateData(rowIndex, columnName, "" + row.get(3));
			}else if(rowIndex <= 0){
				mdt.addRow();
				rowIndex = mdt.getRowCount();
				mdt.updateData(rowIndex, "INSTITUTION_ID", "" + row.get(0));
				mdt.updateData(rowIndex, "JOURNAL_ID", "" + row.get(1));
				mdt.updateData(rowIndex, "PAGE_TYPE", "" + row.get(2));
				mdt.updateData(rowIndex, columnName, "" + row.get(3));
				mdt.setRowKey(rowIndex, rowKey);
			}else{
				MyLogger.log("eachRowAction : Logical FAIL");
			}
					
			
			
			//storing data in different logical tables
			String tableName = row.get(0).toString().trim();
			tableName = "acc_" + tableName.toLowerCase();
			
			MyDataTable mapMdt = mdtMap.get(tableName);
			if(mapMdt != null){
				int r = 1;
				r = mapMdt.getRowIndex(rowKey);
				
				if(r > 0){
					mapMdt.updateData(r, columnName, "" + row.get(3));
				}else if(r <= 0){
					mapMdt.addRow();
					r = mapMdt.getRowCount();
					mapMdt.updateData(r, "INSTITUTION_ID", "" + row.get(0));
					mapMdt.updateData(r, "JOURNAL_ID", "" + row.get(1));
					mapMdt.updateData(r, "PAGE_TYPE", "" + row.get(2));
					mapMdt.updateData(r, columnName, "" + row.get(3));
					mapMdt.setRowKey(r, rowKey);
				}else{
					MyLogger.log("mapMdt : eachRowAction : Logical FAIL");
				}
			}else{
				mapMdt = new MyDataTable(tableName);
				mapMdt.addColumn("INSTITUTION_ID", "-");
				mapMdt.addColumn("JOURNAL_ID", "-");
				mapMdt.addColumn("PAGE_TYPE", "0");
				mapMdt.addColumn(columnName, "0");
				
				mapMdt.addRow();
				int r = mapMdt.getRowCount();
				mapMdt.updateData(r, "INSTITUTION_ID", "" + row.get(0));
				mapMdt.updateData(r, "JOURNAL_ID", "" + row.get(1));
				mapMdt.updateData(r, "PAGE_TYPE", "" + row.get(2));
				mapMdt.updateData(r, columnName, "" + row.get(3));
				mapMdt.setRowKey(r, rowKey);
			}
			mdtMap.put(tableName, mapMdt);
			
		}catch(Exception e){
			MyLogger.log("eachRowAction : " + e.toString());
		}
	} 
	
	//
	public static void initilaizeMyDataTable(String ReportType) throws Exception{
		try {
			
			//JR1
			if(ReportType.trim().equalsIgnoreCase("JR1")){
				//mdt = new MyDataTable("c4_jr1");
				mdt.addColumn("INSTITUTION_ID", "-");
				mdt.addColumn("JOURNAL_ID", "-");
				mdt.addColumn("PAGE_TYPE", "0");
			}else if(ReportType.trim().equalsIgnoreCase("BR2")){
				
			}
			
		} catch (Exception e) {
			throw new Exception("initilaizeMyDataTable : " + e.toString());
		}
	}


	//
	private void createTableColumns(){
		StringBuffer sb = new StringBuffer();
		String yyyy = "2016";
		String mm = "";
		String dd = "";
		
		for(int i = 11 ; i<= 12 ; i++){
			if(i< 10){mm = "0"+i;}else{mm = ""+i;}
			for(int j = 1 ; j<= 31 ; j++){
				if(j< 10){dd = "0"+j;}else{dd = ""+j;}
				sb.append("`D_")
				.append(yyyy).append(mm).append(dd)
				.append("` int(10) NOT NULL DEFAULT '0',\n");
			}
		}
		MyLogger.log(sb.toString());
		
	}
	
	//
	private long copyMySqlTableToMongoCollection(){
		
		long recordsAffected = 0L;
		try {
			
			
			
		} catch (Exception e) {
		
		}
		return recordsAffected;
	}
	

	/*
	private static void upsertInMysql(Row row){
		
		try{
			if(mdt == null){
				mdt = new MyDataTable("c4_journal_report_1");
				rowIndex = 0;
				int columnIndex = 0;
				for(columnIndex = 0; columnIndex < columns.length; columnIndex++){
					mdt.addColumn(columns[columnIndex].trim().toUpperCase(), "");
				}
			}
			
			if(mdt != null && row != null){
				//adding data in mdt row
				rowIndex = rowIndex + 1;
				mdt.addRow();
				
				mdt.updateData(rowIndex, columns[0].trim().toUpperCase(), "" + row.get(0));
				mdt.updateData(rowIndex, columns[1].trim().toUpperCase(), "" + row.get(1));
				mdt.updateData(rowIndex, columns[2].trim().toUpperCase(), "" + row.get(2));
				mdt.updateData(rowIndex, columns[3].trim().toUpperCase(), "" + row.get(3));
				
				if(rowIndex == 500){
					batch++;
					recordsAffected = recordsAffected + mdt.commit(connection);
					mdt = null;
					rowIndex = 0;
					
					if(recordsAffected % 10000 == 0){
						MyLogger.log("upsertInMysql : Batch No : " + batch + " : Records Affected : "  + recordsAffected);
					}
				}
			}else if(mdt != null && row == null){
				batch++;
				recordsAffected = recordsAffected + mdt.commit(connection);
				mdt = null;
				rowIndex = 0;
			}else{
				MyLogger.log("upsertInMysql : Logical FAIL");
			}
			
		}catch(Exception e){
			MyLogger.log("upsertInMysql : " + e.toString());
		}
	} 
	*/
	
	
}
