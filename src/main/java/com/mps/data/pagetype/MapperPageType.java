package com.mps.data.pagetype;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;
import com.mps.utils.MyLogger;

public class MapperPageType {
	private static Pattern[] pageTypePattern;
	private static int[] pageTypesID;
	private static String[] pageTypesJournalPattern;
	public static void initialize(final List<Document> docMap) throws Exception{
		Document doc = null;
		int size = 0;
    	int docNo = 0;
    	int[] pageTypes = null;
    	String[] pageTypesJournal = null;
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
    		//set = docMap.keySet();
    		
    		size = docMap.size();
    		
    		iTracker = 3.0;
    		//initializing arrays
    		pageTypes = new int[size];
    		patterns = new Pattern[size];
    		pageTypesJournal = new String[size];
	    	for(docNo = 0; docNo < size; docNo++){
	    		doc = null;
	    		iTracker = 5.0;
	    		//doc = (Document) set.toArray()[docNo];
	    		doc = (Document) docMap.get(docNo);
	    		iTracker = 6.0;
	    		//patterns[docNo] = Pattern.compile(doc.get("page_type_pattern").toString().trim().replaceAll("%",".*"), Pattern.CASE_INSENSITIVE);
	    		patterns[docNo] = Pattern.compile(doc.get("page_type_pattern").toString().trim(), Pattern.CASE_INSENSITIVE);
	    		iTracker = 7.0;
	    		pageTypes[docNo] = Integer.parseInt(doc.get("page_type_id").toString());
	    		iTracker = 8.0;
	    		pageTypesJournal[docNo] = doc.get("journal_pattern").toString();
	    		
	    	}
	    	doc = null;
	    	pageTypePattern = patterns;
	    	pageTypesID = pageTypes;
	    	pageTypesJournalPattern = pageTypesJournal;
	    	//setting page type patterns
	    	//PageTypeMapper.setPageTypePattern(patterns);
	    	//PageTypeMapper.setPageTypes(pageTypes);
	    	MyLogger.log("PageType : initialize() : Page Type : Pattern Size=" + pageTypePattern.length + " : pageTypes size=" + pageTypes.length + " : Done");
    	}
    	catch(Exception e){
    		throw new Exception("PageType : initialize() : iTracker : " + iTracker + " : " + doc.toJson() + " : " + e.toString());
    	}
    }
	
	public static List<String> getPageTypeFromPattern(String url){
		List<String> list = null;
		Matcher matcher = null;
		int i;
		try {
        	if(pageTypePattern != null){
    			for(i=0;i<pageTypePattern.length;i++) 	{			
    	            matcher = pageTypePattern[i].matcher(url);
    	            if( matcher.matches() == true){
    	            	list = new ArrayList<String>();
    	            	list.add(pageTypesID[i]+"");
    	            	list.add(pageTypesJournalPattern[i]);
    	            	return list;
    	            }
    			}
    		}
        } catch (RuntimeException e) {
        	MyLogger.log("PageType Error");
        }
    	return list;
	}
	
	public static String fetchIDFromUrl(String journalPattern, String urlPattern, String keyword, int loc) {
		try{
		String journalId = "";
		int index3 = 0;
		int index4 = 0;
		int index1 = journalPattern.indexOf(keyword);
	    int index2 = index1 + keyword.length();
	    String suffix = journalPattern.substring(index2);
	    String prefix = journalPattern.substring(0, index1);
		if (prefix == null) {
			prefix = "";
		}
		if (suffix == null) {
			suffix = "";
		}

		String suffixPart;
		String prefixPart;
		
		if (suffix.indexOf(".*") == -1) {
			suffixPart = suffix;
		} else {
			suffixPart = suffix.substring(0, suffix.indexOf(".*"));
		}
		
		if (prefix.lastIndexOf(".*") == -1) {
			prefixPart = prefix;
		} else {
			prefixPart = prefix.substring(prefix.lastIndexOf(".*") + 2, prefix.length());
		}

		int prefixPathLength = prefixPart.length();
		int suffixPathLength = suffixPart.length();
		
		if (suffixPart.equalsIgnoreCase("/") && prefixPart.equalsIgnoreCase("/")){	
			int counter = 0;
			int var = 0;
			String tmpTxt = null;
			String element = "";
			tmpTxt = prefix;
			var = prefix.length() - 1;
			while(var != -1){
				element = tmpTxt.substring(var);
				tmpTxt = tmpTxt.substring(0, var);
				if(element.equalsIgnoreCase("/"))
					counter= counter+1;
				var = var -1;
			}
			if(urlPattern.indexOf('/') != -1 ){
				journalId = subStringOrdinalIndex(urlPattern,"/",counter+1);
				journalId = subStringOrdinalIndex(journalId,"/",-loc);
				
			}
			return journalId;			
		}
		else if(suffixPathLength >= prefixPathLength){
			if(suffixPart.indexOf("_")!=-1){
				int foundPos = 0;
				String tempUrl;
				String tmpTxt = null;
				String element = "";
				String prevElement = "";
				int suffixPartLength = 0;
				tmpTxt = suffixPart;
				foundPos = tmpTxt.indexOf("_");
				while(foundPos !=-1 ){
					if(foundPos == 0){
						element = "";
						tmpTxt = tmpTxt.substring(1);
					}
					else{
						element = tmpTxt.substring(0,foundPos);
						tmpTxt = tmpTxt.substring(element.length() + 1);
					}
					if(element.length() > prevElement.length())
						prevElement = element;
				    foundPos = tmpTxt.indexOf("_");
				}
				if(tmpTxt.length() > prevElement.length()) {
					prevElement = tmpTxt;
				}
				suffixPartLength = suffixPart.substring(0,suffixPart.indexOf(prevElement)).length();
				
				index3 = urlPattern.indexOf(prevElement);
				index4 = index3 - suffixPartLength - 1;
				tempUrl = urlPattern.substring(0,index4);
				
				if (prefixPart.equalsIgnoreCase("")  &&  tempUrl.indexOf('&')!=-1) 
					journalId = subStringOrdinalIndex(tempUrl,"&",1);
				else if (prefixPart.equalsIgnoreCase("")) 
					journalId = tempUrl;
				else if (prefixPart.indexOf("_") !=-1){						
					journalId = tempUrl.substring(prefixPart.length());
				}
				else
					journalId = subStringOrdinalIndex(tempUrl,prefixPart,-loc);
				return journalId;
				
			}
			else{
				if(urlPattern.indexOf(suffixPart) != -1){
					index1 = urlPattern.indexOf(suffixPart);
					String tempUrl = urlPattern.substring(0, index1);
					if (prefixPart.equalsIgnoreCase("")  && tempUrl.indexOf('&')!=-1) {
						journalId = subStringOrdinalIndex(tempUrl,"&",-1);
					}
					else if (prefixPart.equalsIgnoreCase("")){
						journalId = tempUrl;
					}
					else if (prefixPart.indexOf("_") !=-1){						
						journalId = tempUrl.substring(prefixPart.length());
					}
					else
						journalId = subStringOrdinalIndex(tempUrl,prefixPart,-loc);
					return journalId;
				}
			}
		}
		else{
			if(prefixPart.indexOf("_") != -1){
				int foundPos = 0;
				String tempUrl;
				String tmpTxt = null;
				String element = "";
				String prevElement = "";
				int prefixPartLength = 0;
				tmpTxt = prefixPart;
				foundPos = tmpTxt.indexOf("_");
				while(foundPos !=-1 ){
					if(foundPos == 0){
						element = "";
						tmpTxt = tmpTxt.substring(1);
					}
					else{
						element = tmpTxt.substring(0,foundPos);
						tmpTxt = tmpTxt.substring(element.length());
					}
					if(element.length() > prevElement.length())
						prevElement = element;
				    foundPos = tmpTxt.indexOf("_");
				}
				if(tmpTxt.length() > prevElement.length()) {
					prevElement = tmpTxt;
				}
				prefixPartLength = prefixPart.substring(prefixPart.indexOf(prevElement)).length();
				
				index3 = urlPattern.indexOf(prevElement);
				index4 = index3 + prefixPartLength;
				tempUrl = urlPattern.substring(index4);
				
				if (suffixPart.equalsIgnoreCase("")  &&  tempUrl.indexOf('&')!=-1) 
					journalId = subStringOrdinalIndex(tempUrl,"&",1);
				else if (suffixPart.equalsIgnoreCase("")) 
					journalId = tempUrl;
				else if (suffixPart.indexOf("_") !=-1)						
					journalId = tempUrl.substring(0,tempUrl.length() - suffixPart.length());
				else
					journalId = subStringOrdinalIndex(tempUrl,suffixPart,loc);
				return journalId;
				
			}
			else{
				int in =-1 ;
				if(urlPattern.indexOf(prefixPart) != -1){
					in = urlPattern.indexOf(prefixPart);
					in = in+ prefixPathLength;
					String tempUrl = urlPattern.substring(in);
					if (suffixPart.equalsIgnoreCase("")  && tempUrl.indexOf('&')!=-1) {
						journalId = subStringOrdinalIndex(tempUrl,"&",1);
					}
					else if (suffixPart.equalsIgnoreCase("")){
						journalId = tempUrl;
					}
					else if (suffixPart.indexOf("_") !=-1)	{					
						journalId = tempUrl.substring(0,tempUrl.length() - suffixPart.length());
					}
					else
						journalId = subStringOrdinalIndex(tempUrl,suffixPart,loc);
				}
				return journalId;				
			}
		}
		
		}catch(Exception e){
			MyLogger.log("Exception in fetchIDFromUrl() method " + e.toString() + ":: " + journalPattern + " :: " + urlPattern);
		}
		return "";
	}
	public static String subStringOrdinalIndex(String str, String c, int n) {
		try{
		int pos=0;
		int i;
		String substring = str;
		if(n == 0){
			return str;
		}
	    if(n < 0){
	    	pos = str.lastIndexOf(c); 
		   for (i=n;i<-1 && pos != -1;i++)
		        pos = str.lastIndexOf(c,pos+1); 
		   if(pos != -1)
			   substring = str.substring(pos + 1);
	    }
	    else{
	    	 pos = str.indexOf(c);
		    for (i=1;i<n && pos != -1;i++)
		        pos = str.indexOf(c,pos+1);
		    if(pos != -1)
		    	substring = str.substring(0, pos);
	    }
	   

	    return substring;
	}
		 
	catch(Exception e){
		MyLogger.log("Exception in subStringOrdinalIndex() method " + e.toString() + ":: " + str + " :: " + c);
	}
		return str;
	}	
}
