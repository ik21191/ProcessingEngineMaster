package com.mps.backup;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;

import com.mps.utils.MyLogger;

public class PageViewFlag {
	private static String[] type1Reg;
	public static Set<String> getType3RegSet() {
		return type3RegSet;
	}
	public static void setType3RegSet(Set<String> type3RegSet) {
		PageViewFlag.type3RegSet = type3RegSet;
	}

	private static int[] type1Id;
	private static String[] type2Reg;
	private static int[] type2Id;
	private static String[] type3Reg;
	private static String[] type3Flag;
	private static int[] type3Id;
	private static BigInteger[] type4LowIP;
	private static BigInteger[] type4HighIP;
	private static String[] type4Flag;
	private static String[] type4InOutFlag;
	private static int[] type4Id;
	private static Pattern[] type5Reg;
	private static String[] type5Flag;
	private static int[] type5Id;
	private static Set<String> type3RegSet;
	
	
	public static String[] getType1Reg() {
		return type1Reg;
	}
	public static void setType1Reg(String[] type1Reg) {
		PageViewFlag.type1Reg = type1Reg;
	}
	public static String[] getType2Reg() {
		return type2Reg;
	}
	public static void setType2Reg(String[] type2Reg) {
		PageViewFlag.type2Reg = type2Reg;
	}
	public static String[] getType3Reg() {
		return type3Reg;
	}
	public static void setType3Reg(String[] type3Reg) {
		PageViewFlag.type3Reg = type3Reg;
	}
	
	public static int[] getType1Id() {
		return type1Id;
	}
	public static void setType1Id(int[] type1Id) {
		PageViewFlag.type1Id = type1Id;
	}
	public static int[] getType2Id() {
		return type2Id;
	}
	public static void setType2Id(int[] type2Id) {
		PageViewFlag.type2Id = type2Id;
	}
	public static int[] getType3Id() {
		return type3Id;
	}
	public static void setType3Id(int[] type3Id) {
		PageViewFlag.type3Id = type3Id;
	}
	public static BigInteger[] getType4LowIP() {
		return type4LowIP;
	}
	public static void setType4LowIP(BigInteger[] type4LowIP) {
		PageViewFlag.type4LowIP = type4LowIP;
	}
	public static BigInteger[] getType4HighIP() {
		return type4HighIP;
	}
	public static void setType4HighIP(BigInteger[] type4HighIP) {
		PageViewFlag.type4HighIP = type4HighIP;
	}
	public static String[] getType4InOutFlag() {
		return type4InOutFlag;
	}
	public static void setType4InOutFlag(String[] type4InOutFlag) {
		PageViewFlag.type4InOutFlag = type4InOutFlag;
	}
	public static int[] getType4Id() {
		return type4Id;
	}
	public static void setType4Id(int[] type4Id) {
		PageViewFlag.type4Id = type4Id;
	}
	public static Pattern[] getType5Reg() {
		return type5Reg;
	}
	public static void setType5Reg(Pattern[] type5Reg) {
		PageViewFlag.type5Reg = type5Reg;
	}
	public static String[] getType5Flag() {
		return type5Flag;
	}
	public static void setType5Flag(String[] type5Flag) {
		PageViewFlag.type5Flag = type5Flag;
	}
	public static int[] getType5Id() {
		return type5Id;
	}
	public static void setType5Id(int[] type5Id) {
		PageViewFlag.type5Id = type5Id;
	}
	public static int[] getType1Flag() {
		return type1Id;
	}
	public static void setType1Flag(int[] type1Id) {
		PageViewFlag.type1Id = type1Id;
	}
	public static int[] getType2Flag() {
		return type2Id;
	}
	public static void setType2Flag(int[] type2Id) {
		PageViewFlag.type2Id = type2Id;
	}
	public static String[] getType3Flag() {
		return type3Flag;
	}
	public static void setType3Flag(String[] type3Flag) {
		PageViewFlag.type3Flag = type3Flag;
	}
	public static String[] getType4Flag() {
		return type4Flag;
	}
	public static void setType4Flag(String[] type4Flag) {
		PageViewFlag.type4Flag = type4Flag;
	}
	
	public static int getImagecount(String resourceType){
		int i;
		try {
			if(type1Reg != null && type1Id != null){
				for(i=0;i<type1Reg.length;i++) 	{
					if(resourceType.trim().equalsIgnoreCase(type1Reg[i])){
						return type1Id[i];
					}
				}
			}
			return 0;
        } catch (RuntimeException e) {
        	return 0;
        }
        	
	}
	
	public static int getStatusCodeCount(String statusCode){
		int i;
		try {
			if(type2Reg != null && type2Id != null){
				for(i=0;i<type1Reg.length;i++) 	{
					if(statusCode.trim().equalsIgnoreCase(type2Reg[i])){
						return type2Id[i];
					}
				}
			}
			return 0;
        } catch (RuntimeException e) {
        	return 0;
        }
        
	}
	
	public static String getIPCount(BigInteger longIpAddress){
		int i;
		try {
			if(type4LowIP != null){
				for(i=0;i<type4LowIP.length;i++) 	{
					//if(longIpAddress >= type4LowIP[i] && longIpAddress <= type4HighIP[i]){
					if((longIpAddress.compareTo(type4LowIP[i]) == 0 || longIpAddress.compareTo(type4LowIP[i]) == 1) && (longIpAddress.compareTo(type4HighIP[i]) == 0 || longIpAddress.compareTo(type4HighIP[i]) == -1)){
						//li.add(0,type4Id.toString());
						//li.add(1,type4InOutFlag.toString());
						//li.add(2,type4Flag.toString());
						if(type4Id[i] == 0 || Integer.parseInt(type4InOutFlag[i]) != 0)
							return type4Flag[i].toString();
						else
							return "Exclude";
					}
				}
			}
        } catch (RuntimeException e) {
        	return "NA";
        }
        	return "NA";
	}
	
	public static String getRobotCount(String userAgent){
		int i=0;
		try {
			if(type3Reg != null){
				for(i=0;i<type3Reg.length;i++) 	{
					if(userAgent.trim().contains(type3Reg[i])){
						return type3Flag[i].toString();
					}
				}
			}
			/*if(type3RegSet != null){
				for(String s : type3RegSet)	{
					if(userAgent.trim().contains(s)){
						return "exclude_patterns";
					}
				}
				
			}*/
        } catch (RuntimeException e) {
        	return "NA";
        }
        	return "NA";
	}
	
	public static String getOtherUrlCount(String url){
		Matcher matcher = null;
		int i;
		try {
			if(type5Reg != null){
				for(i=0;i<type5Reg.length;i++) 	{
					matcher = type5Reg[i].matcher(url);
		            if( matcher.matches() == true){
		            	return type5Flag[i];
		            }
				}
			}
        } catch (RuntimeException e) {
        	return "NA";
        }
        	return "NA";
	}
	
	public static void initialize(final Map<Document, Long> docMap) throws Exception{
		Set<Document> set = null;
		Document doc = null;
		int size = 0;
    	int docNo = 0;
    	double iTracker = 0.0;
    	try{
    		MyLogger.info("PageView : initialize() : Start");
    		
    		iTracker = 1.0;
    		//check for NUll
    		if(docMap == null){
    			throw new Exception("PageView : initialize() : docMap : NULL");
    		}
    		iTracker = 2.0;
    		//getting set from docMap for iteration
    		set = docMap.keySet();
    		size = set.size();
    		
    		iTracker = 3.0;
    		//initializing arrays
    		type5Reg = new Pattern[size];
    		type5Flag = new String[size];
    		String pageView = "";
	    	for(docNo = 0; docNo < size; docNo++){
	    		doc = null;
	    		iTracker = 5.0;
	    		doc = (Document) set.toArray()[docNo];
	    		iTracker = 6.0;
	    		if(doc.get("reg_exp").toString().trim().startsWith("%") && doc.get("reg_exp").toString().trim().endsWith("%")){
	    			pageView = doc.getString("reg_exp").trim().replaceAll("%",".*");
	    		}
	    		else if(doc.get("reg_exp").toString().trim().startsWith("%")){
	    			pageView = doc.getString("reg_exp").trim().replaceAll("%",".*") + ".*";
	    		}
	    		else if(doc.get("reg_exp").toString().trim().endsWith("%")){
	    			pageView = ".*" + doc.getString("reg_exp").trim().replaceAll("%",".*");
	    		}
	    		else{
	    			pageView = ".*" + doc.getString("reg_exp").trim().replaceAll("%",".*") + ".*";
	    		}
	    		
	    		type5Reg[docNo] = Pattern.compile(pageView, Pattern.CASE_INSENSITIVE);
	    		iTracker = 7.0;
	    		type5Flag[docNo] = doc.get("type_flag").toString().trim();
	    	}
	    	doc = null;
	    	
	    	//setting page type patterns
	    	//PageViewFlag.setType5Reg(type5Reg);
	    	//PageViewFlag.setType5Flag(type5Flag);
	    	MyLogger.info("PageView : initialize() : Page View :  Size=" + type5Reg.length + " : pageView size=" + type5Flag.length + " : Done");
    	}
    	catch(Exception e){
    		throw new Exception("PageType : initialize() : iTracker : " + iTracker + " : " + doc.toJson() + " : " + e.toString());
    	}
    }
	
	
	
}
