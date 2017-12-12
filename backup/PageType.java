package com.mps.backup;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bson.Document;
import com.mps.utils.MyLogger;

public class PageType {
	//private static List<PageTypePatterns> list = null;	
	private static Pattern[] pageTypePattern;
	private static int[] pageTypes;
	
	//
	public static void initialize(final Map<Document, Long> docMap) throws Exception{
		Set<Document> set = null;
		Document doc = null;
		int size = 0;
    	int docNo = 0;
    	int[] pageTypes = null;
    	Pattern[] patterns = null;
    	double iTracker = 0.0;
    	try{
    		MyLogger.log("PageType : initialize() : Start");
    		
    		iTracker = 1.0;
    		//check for NUll
    		if(docMap == null){
    			throw new Exception("PageType : initialize() : docMap : NULL");
    		}
    		iTracker = 2.0;
    		//getting set from docMap for iteration
    		set = docMap.keySet();
    		size = set.size();
    		
    		iTracker = 3.0;
    		//initializing arrays
    		pageTypes = new int[size];
    		patterns = new Pattern[size];
    		
	    	for(docNo = 0; docNo < size; docNo++){
	    		doc = null;
	    		iTracker = 5.0;
	    		doc = (Document) set.toArray()[docNo];
	    		iTracker = 6.0;
	    		//patterns[docNo] = Pattern.compile(doc.get("page_type_pattern").toString().trim().replaceAll("%",".*"), Pattern.CASE_INSENSITIVE);
	    		patterns[docNo] = Pattern.compile(doc.get("page_type_pattern").toString().trim(), Pattern.CASE_INSENSITIVE);
	    		iTracker = 7.0;
	    		pageTypes[docNo] = Integer.parseInt(doc.get("page_type_id").toString());
	    	}
	    	doc = null;
	    	
	    	//setting page type patterns
	    	PageType.setPageTypePattern(patterns);
	    	PageType.setPageTypes(pageTypes);
	    	MyLogger.log("PageType : initialize() : Page Type : Pattern Size=" + pageTypePattern.length + " : pageTypes size=" + pageTypes.length + " : Done");
    	}
    	catch(Exception e){
    		throw new Exception("PageType : initialize() : iTracker : " + iTracker + " : " + doc.toJson() + " : " + e.toString());
    	}
    }
	
	//
	public static int[] getPageTypes() {
		return pageTypes;
	}

	public static void setPageTypes(int[] pageTypes) {
		PageType.pageTypes = pageTypes;
	}

	public static Pattern[] getPageTypePattern() {
		return pageTypePattern;
	}

	public static void setPageTypePattern(Pattern[] pageTypePattern) {
		PageType.pageTypePattern = pageTypePattern;
	}
	
	public static int getPageTypeFromPattern(String url){
		Matcher matcher = null;
		int i;
		try {
        	if(pageTypePattern != null){
    			for(i=0;i<pageTypePattern.length;i++) 	{			
    	            matcher = pageTypePattern[i].matcher(url);
    	            if( matcher.matches() == true){
    	            	return pageTypes[i];
    	            }
    			}
    		}
        } catch (RuntimeException e) {
        	//return 0;
        }
    	return 0;
	}
}
