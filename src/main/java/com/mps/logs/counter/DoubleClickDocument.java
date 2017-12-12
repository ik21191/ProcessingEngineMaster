/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mps.logs.counter;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;

import com.mps.utils.Constants;
import com.mps.utils.MyLogger;

/**
 *
 * @author kapil.kumar
 */
public class DoubleClickDocument implements Serializable {

	private static final long serialVersionUID = 1L;
	static Document previousDoc = null; 
	static Map<Integer, Integer> pageTypeDurationMap = new HashMap<>();
	//static JavaRDD<Document> jRDDLogPageTypes;
	
	
	public static List<Document> getDoubleClickDocument(final Document currentDoc) {
		String overlapFlag = "n";
		List<Document> counterDocList = new ArrayList<>();
		
		try {
			//updating stats for processing of records
			CounterStats.processRecord();
			
			//step1: If this is first document then add it to counter
			if(previousDoc == null) {
				currentDoc.append(Constants.DOUBLE_CLICK_FLAG_NAME, "Y");
			}else{
				//step2: Check if there is any double click document then set current document as previous document 
				if(isLongIpAddressUrlPageTypeEqual(currentDoc, previousDoc)){
				
					long timeDiff = getTimeDiffInSeconds(previousDoc.getString(Constants.REQUEST_DATE_TIME) != null ? previousDoc.getString(Constants.REQUEST_DATE_TIME) : "", 
							currentDoc.getString(Constants.REQUEST_DATE_TIME) != null ? currentDoc.getString(Constants.REQUEST_DATE_TIME) : "");
					
					int pageTypeId = currentDoc.getInteger(Constants.PAGE_TYPE) != null ? currentDoc.getInteger(Constants.PAGE_TYPE) : 0;
					
					int timeDurationPageTypesMemory = getPageTypeDurationMap(pageTypeId);
					
					if (timeDiff > timeDurationPageTypesMemory) {
						currentDoc.append(Constants.DOUBLE_CLICK_FLAG_NAME, "Y");
					}else{
						CounterStats.skipRecord();
					}
					//Step3: if document is not double clicked then add it to counter
				}else if(currentDoc.getString(Constants.LONG_IP_ADDRESS) != null && currentDoc.getString(Constants.URL) != null) {
					currentDoc.append(Constants.DOUBLE_CLICK_FLAG_NAME, "Y");
				}
			}
		} catch (Exception e) {
				MyLogger.error("DoubleClickDocument: There was some problem while applying double click trigger : " + e);
		} 
		// Step4: add unique double clicked document to counter
		String doubleClickFlag = currentDoc.getString(Constants.DOUBLE_CLICK_FLAG_NAME);
		try{
			if (doubleClickFlag != null && doubleClickFlag.equalsIgnoreCase("Y")) {
				Document doc = null;
				String tmpTxt = currentDoc.getString(Constants.INSTITUTION_DETAILS) != null ? currentDoc.getString(Constants.INSTITUTION_DETAILS) : "";
				if (tmpTxt != "") {
					String[] institutes = tmpTxt.split(",");
					if(institutes.length > 1) {
						overlapFlag = "y";
						for (int i = 0; i < institutes.length; i++) {
							String element = institutes[i] != null ? institutes[i] : "";
							doc = getUpdatedCounterDoc(currentDoc);
							doc.append("institution_id", element);
							doc.append("overlap_flag", overlapFlag);
							doc.append("load_date", new SimpleDateFormat(Constants.DATE_FORMAT).format(new Date()));
							counterDocList.add(doc);
						}
					} else {
						String element = institutes[0];
						doc = getUpdatedCounterDoc(currentDoc);
						doc.append("institution_id", element);
						doc.append("overlap_flag", overlapFlag);
						doc.append("load_date", new SimpleDateFormat(Constants.DATE_FORMAT).format(new Date()));
						counterDocList.add(doc);
					}
				}
				previousDoc = currentDoc;
			} else {
				// what to do
			}
			}catch(Exception e) {
			MyLogger.error("DoubleClickDocument: There was some exception : " + e);
		}
		CounterStats.updateDoubleClickRecords(counterDocList.size());
		return counterDocList;
	}

	//
	private static Document getUpdatedCounterDoc(Document doc) {
		Document counterDoc = new Document();
		
		counterDoc.append("ip_address", ((doc.getString("ip_address") != null ? doc.getString("ip_address") : "")));
		counterDoc.append("long_ip_address", doc.getString("long_ip_address") != null ? doc.getString("long_ip_address") : "");
		counterDoc.append("request_date_time", doc.getString("request_date_time") != null ? doc.getString("request_date_time") : "");
		counterDoc.append("user_id", doc.getString("user_id") != null ? doc.getString("user_id") : "");
		counterDoc.append("url", doc.getString("url") != null ? doc.getString("url") : "");
		counterDoc.append("referer_url", doc.getString("referer_url") != null ? doc.getString("referer_url") : "");
		counterDoc.append("user_agent", doc.getString("user_agent") != null ? doc.getString("user_agent") : "");
		counterDoc.append("filename", doc.getString("filename") != null ? doc.getString("filename") : "");
		counterDoc.append("institution_codes", doc.getString("institution_codes") != null ? doc.getString("institution_codes") : "");
		counterDoc.append("institution_details", doc.getString("institution_details") != null ? doc.getString("institution_details") : "");
		counterDoc.append("page_type", doc.getInteger("page_type") != null ? doc.getInteger("page_type") : 0);
		counterDoc.append("journal_id", doc.getString("journal_id") !=null ? doc.getString("journal_id") : "");
		counterDoc.append("webmart_code", doc.getString("webmart_code") != null ? doc.getString("webmart_code") : "");
		counterDoc.append("article_id", doc.getString("article_id") != null ? doc.getString("article_id") : "");
		counterDoc.append("session_id", doc.getString("session_id") !=null ? doc.getString("session_id") : "");
		counterDoc.append("search_term", doc.getString("search_term") != null ? doc.getString("search_term") : "");
		return counterDoc;
	}

	//
	private static long getTimeDiffInSeconds(String prevRequestDate, String currentRequestDate) {
		long difference = 0;
    	long diffSeconds = 0;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constants.DATE_FORMAT);
    	Date previousDate = null;
    	Date currentDate = null;
		try {
			previousDate = simpleDateFormat.parse(prevRequestDate);
			currentDate = simpleDateFormat.parse(currentRequestDate);
			difference = currentDate.getTime() - previousDate.getTime();
			diffSeconds = difference / 1000;
		} catch (Exception e) {
			MyLogger.error("DoubleClickDocument : getTimeDiffInSeconds : "  + prevRequestDate + " : " + currentRequestDate + " : " + e.toString());
		}
    	return diffSeconds;
	}

	//
	private static Integer getPageTypeDurationMap(Integer key) {
		return pageTypeDurationMap.get(key) !=null ? pageTypeDurationMap.get(key) : 0;
	}

	//
	private static boolean isLongIpAddressUrlPageTypeEqual(Document currentDoc, Document previousDoc) {
		boolean status = false; 
		/*String currentLongIpAddress = (String)currentDoc.getOrDefault(Constants.LONG_IP_ADDRESS, "");
		String previousLongIpAddress = (String)previousDoc.getOrDefault(Constants.LONG_IP_ADDRESS, "");
		String currentURL = (String)currentDoc.getOrDefault(Constants.URL, "");
		String previousURL = (String)previousDoc.getOrDefault(Constants.URL, "");
		Integer currentPageType = (Integer)currentDoc.getOrDefault(Constants.PAGE_TYPE, 0);
		Integer previousPageType = (Integer)previousDoc.getOrDefault(Constants.PAGE_TYPE, 0);*/
		
		try{
			String currentLongIpAddress = currentDoc.getString(Constants.LONG_IP_ADDRESS);
			if(currentLongIpAddress == null){currentLongIpAddress = "";}
			String previousLongIpAddress = previousDoc.getString(Constants.LONG_IP_ADDRESS);
			if(previousLongIpAddress == null) {previousLongIpAddress = "";}
			
			String currentURL = currentDoc.getString(Constants.URL);
			if(currentURL == null) {currentURL = "";}
			String previousURL = previousDoc.getString(Constants.URL);
			if(previousURL == null) {previousURL = "";}
			
			Integer currentPageType = currentDoc.getInteger(Constants.PAGE_TYPE);
			if(currentPageType == null){currentPageType = 0;}
			Integer previousPageType = previousDoc.getInteger(Constants.PAGE_TYPE);
			if(previousPageType == null) {previousPageType = 0;}
			
			if(currentLongIpAddress.equalsIgnoreCase(previousLongIpAddress) && currentURL.equalsIgnoreCase(previousURL) && currentPageType == previousPageType) {
				status = true;
			} 
			
		}catch(Exception e){
			
			MyLogger.error("DoubleClickDocument : Exception while comparing longIp, URL and pageType" + e);
		}
		
		
		/*if((currentDoc.getString(Constants.LONG_IP_ADDRESS) != null ? currentDoc.getString(Constants.LONG_IP_ADDRESS).
				equals(previousDoc.getString(Constants.LONG_IP_ADDRESS) != null ? previousDoc.getString(Constants.LONG_IP_ADDRESS) : "") : false) &&
				(currentDoc.getString(Constants.URL) != null ? currentDoc.getString(Constants.URL).
						equals(previousDoc.getString(Constants.URL) != null ? previousDoc.getString(Constants.URL) : "") : false) && 
				(currentDoc.getInteger(Constants.PAGE_TYPE) != null ? currentDoc.getInteger(Constants.PAGE_TYPE).
						equals(previousDoc.getInteger(Constants.PAGE_TYPE) != null ? previousDoc.getInteger(Constants.PAGE_TYPE) : "") : false)) {
			return true;
		}
		return false;*/
		
		return status;
	}
	
	static boolean filterJRDD(Document doc){
		try {
			return doc.getInteger(Constants.PAGE_TYPE) != 0 && doc.getString("page_view_flag").equalsIgnoreCase("counter");
		} catch (Exception e) {
			return false;
		}
	}
}


/////////////////////////////////////Extra methods////////////////////////////////



/*private static void initializePreviousDocument(Document previous, Document current) {
	previous.append("ip_address", ((current.getString("ip_address") != null ? current.getString("ip_address") : "")));
	previous.append("long_ip_address", current.getString("long_ip_address") != null ? current.getString("long_ip_address") : "");
	previous.append("request_date_time", current.getString("request_date_time") != null ? current.getString("request_date_time") : "");
	previous.append("user_id", current.getString("user_id") != null ? current.getString("user_id") : "");
	previous.append("url", current.getString("url") != null ? current.getString("url") : "");
	previous.append("referer_url", current.getString("referer_url") != null ? current.getString("referer_url") : "");
	previous.append("user_agent", current.getString("user_agent") != null ? current.getString("user_agent") : "");
	previous.append("filename", current.getString("filename") != null ? current.getString("filename") : "");
	previous.append("institution_codes", current.getString("institution_codes") != null ? current.getString("institution_codes") : "");
	previous.append("institution_details", current.getString("institution_details") != null ? current.getString("institution_details") : "");
	previous.append("page_type", current.getInteger("page_type") != null ? current.getInteger("page_type") : 0);
	previous.append("journal_id", current.getString("journal_id") !=null ? current.getString("journal_id") : "");
	previous.append("webmart_code", current.getString("webmart_code") != null ? current.getString("webmart_code") : "");
	previous.append("article_id", current.getString("article_id") != null ? current.getString("article_id") : "");
	previous.append("session_id", current.getString("session_id") !=null ? current.getString("session_id") : "");
	previous.append("search_term", current.getString("search_term") != null ? current.getString("search_term") : "");
}*/




/*static JavaRDD<Document> sortJRDD(JavaRDD<Document> unSortedJRDD){
	JavaRDD<Document> sortTedRddDoc = null;
	try {
		sortTedRddDoc = unSortedJRDD.sortBy(doc -> (doc.getString(Constants.LONG_IP_ADDRESS) != null ? doc.getString(Constants.LONG_IP_ADDRESS) : "") + 
				(doc.getString(Constants.URL) != null ? doc.getString(Constants.URL) : "") +
				(doc.getString(Constants.REQUEST_DATE_TIME) != null ? doc.getString(Constants.REQUEST_DATE_TIME) : "") , true, 1);
		
	} catch (Exception e) {
		MyLogger.error("DoubleClickDocument: There was some exceptions while sorting document : " + e);
	}
	return sortTedRddDoc;
}*/