package com.mps.logs.counter;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.mps.utils.MyLogger;

public class CounterStats {
	private static final long serialVersionUID = 1L;
	
	private static String filterHostPort = "";
	private static String filterDatabase = "";
	private static String filterCollection = "";
			
	private static String counterHostPort = "";
	private static String counterDatabase = "";
	private static String counterCollection = "";
	
	private static long totalRecords = 0L;
	private static long doubleClickRecords = 0L;
	private static long skippedRecords = 0L;
	
	private static String processingStartTime = null;
	private static String processingEndTime = null;
	
	private static String loadFilterWithQuery = "";
	
	public static void refresh(){
		totalRecords = 0L;
		doubleClickRecords = 0L;
		skippedRecords = 0L;
	}
	
	public static String getStats(){
		StringBuffer sbStats = new StringBuffer();
		sbStats.append(" : Records Processed = ")
		.append(totalRecords)
		.append(" : Records Skipped = ")
		.append(skippedRecords)
		.append(" : Double Clicked Records = ")
		.append(doubleClickRecords);
		return sbStats.toString();
	}
	
	////////////////////////////// Getter Setter//////////////
	
	public static long getTotalRecords() {
		return totalRecords;
	}

	public static void processRecord() {
		CounterStats.totalRecords++;
	}

	public static long getDoubleClickRecords() {
		return doubleClickRecords;
	}

	public static void updateDoubleClickRecords(int doubleClickRecords) {
		CounterStats.doubleClickRecords = CounterStats.doubleClickRecords + doubleClickRecords;
	}

	public static long getSkippedRecords() {
		return skippedRecords;
	}

	public static void skipRecord() {
		CounterStats.skippedRecords++;
	}
	
	public static String getFilterHostPort() {
		return filterHostPort;
	}

	public static void setFilterHostPort(String filterHostPort) {
		CounterStats.filterHostPort = filterHostPort;
	}

	public static String getFilterDatabase() {
		return filterDatabase;
	}

	public static void setFilterDatabase(String filterDatabase) {
		CounterStats.filterDatabase = filterDatabase;
	}

	public static String getFilterCollection() {
		return filterCollection;
	}

	public static void setFilterCollection(String filterCollection) {
		CounterStats.filterCollection = filterCollection;
	}

	public static String getCounterHostPort() {
		return counterHostPort;
	}

	public static void setCounterHostPort(String counterHostPort) {
		CounterStats.counterHostPort = counterHostPort;
	}

	public static String getCounterDatabase() {
		return counterDatabase;
	}

	public static void setCounterDatabase(String counterDatabase) {
		CounterStats.counterDatabase = counterDatabase;
	}

	public static String getCounterCollection() {
		return counterCollection;
	}

	public static void setCounterCollection(String counterCollection) {
		CounterStats.counterCollection = counterCollection;
	}

	public static String getProcessingStartTime() {
		return processingStartTime;
	}

	public static void setProcessingStartTime() {
		String currentDateTime = "";
		try {
			currentDateTime = new SimpleDateFormat("yyyy-M-dd hh:mm:ss").format(new Date());
		} catch(Exception e) {
			MyLogger.error("CounterStats : Exception while getting current time : " + e);
		}
		CounterStats.processingStartTime = currentDateTime;
	}

	public static String getProcessingEndTime() {
		return processingEndTime;
	}

	public static void setProcessingEndTime() {
		String currentDateTime = "";
		try {
			currentDateTime = new SimpleDateFormat("yyyy-M-dd hh:mm:ss").format(new Date());
		} catch(Exception e) {
			MyLogger.error("CounterStats : Exception while getting current time : " + e);
		}
		CounterStats.processingEndTime = currentDateTime;
	}

	public static String getLoadFilterWithQuery() {
		return loadFilterWithQuery;
	}

	public static void setLoadFilterWithQuery(String loadFilterWithQuery) {
		CounterStats.loadFilterWithQuery = loadFilterWithQuery;
	}
	
}
