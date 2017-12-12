package com.mps.backup;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;

import com.googlecode.ipv6.IPv6Address;
import com.googlecode.ipv6.IPv6AddressRange;
import com.mps.utils.MyLogger;

public class IPDetails {
	private static Map<String, String> ipv4;
	private static Map<String, String> ipv6;
	private static Comparator<AccountParent> comparator;
	

	public static Comparator<AccountParent> getComparator() {
		return comparator;
	}

	public static void setComparator(Comparator<AccountParent> comparator) {
		IPDetails.comparator = comparator;
	}

	private static String institutionFlag ="";
	private static String groupFlag ="";
	private static int webmartId;
	private static List<AccountParent> accParentList;
	private static JavaRDD<Document> ipv4RDD;
	private static Long[] ipLow;
	private static Long[] ipHigh;
	private static String[] code;
	private static String[] ipLowRange;
	private static String[] ipHighRange;
	private static String[] codeRange;

	public static String[] getIpLowRange() {
		return ipLowRange;
	}

	public static void setIpLowRange(String[] ipLowRange) {
		IPDetails.ipLowRange = ipLowRange;
	}

	public static String[] getIpHighRange() {
		return ipHighRange;
	}

	public static void setIpHighRange(String[] ipHighRange) {
		IPDetails.ipHighRange = ipHighRange;
	}

	public static String[] getCodeRange() {
		return codeRange;
	}

	public static void setCodeRange(String[] codeRange) {
		IPDetails.codeRange = codeRange;
	}

	public static List<AccountParent> getAccParentList() {
		return accParentList;
	}

	public static Long[] getIpLow() {
		return ipLow;
	}

	public static void setIpLow(Long[] ipLow) {
		IPDetails.ipLow = ipLow;
	}

	public static Long[] getIpHigh() {
		return ipHigh;
	}

	public static void setIpHigh(Long[] ipHigh) {
		IPDetails.ipHigh = ipHigh;
	}

	public static String[] getCode() {
		return code;
	}

	public static void setCode(String[] code) {
		IPDetails.code = code;
	}

	public static JavaRDD<Document> getIpv4RDD() {
		return ipv4RDD;
	}

	public static void setIpv4RDD(JavaRDD<Document> ipv4rdd) {
		ipv4RDD = ipv4rdd;
	}

	public static void setAccParentList(List<AccountParent> accParentList) {
		IPDetails.accParentList = accParentList;
	}

	public static Map<String, String> getIpv4() {
		return ipv4;
	}

	public static void setIpv4(Map<String, String> ipv4) {
		IPDetails.ipv4 = ipv4;
	}

	public static Map<String, String> getIpv6() {
		return ipv6;
	}

	public static void setIpv6(Map<String, String> ipv6) {
		IPDetails.ipv6 = ipv6;
	}

	public static String getInstitutionFlag() {
		return institutionFlag;
	}

	public static void setInstitutionFlag(String institutionFlag) {
		IPDetails.institutionFlag = institutionFlag;
	}

	public static String getGroupFlag() {
		return groupFlag;
	}

	public static void setGroupFlag(String groupFlag) {
		IPDetails.groupFlag = groupFlag;
	}

	public static int getWebmartId() {
		return webmartId;
	}

	public static void setWebmartId(int webmartId) {
		IPDetails.webmartId = webmartId;
	}

	/*public static String getInstitutionFromIPv4(long IP){
		try{
		Set<String> ipSet = ipv4.keySet();
		for(String str : ipSet){
		    if(Long.parseLong(str.split("-")[0]) >= IP && Long.parseLong(str.split("-")[1]) <= IP){
		    	return ipv4.get(str);
		    }
		}
		return "-2";
		}
		catch(Exception ex){
			return "-3";
			//throw ex;
		}
	}*/
	
	public static String getInstitutionFromIPv4(long IP){
		try{
			for(int i=0;i<ipLow.length;i++){
				if(ipLow[i] >= IP &&  ipHigh[i]<= IP){
			    	return code[i];
			    }
			}
			return "-2";
		}
		catch(Exception ex){
			return "-3";
			//throw ex;
		}
	}
	
	public static String getInstitutionFromIPv6(String IP){
		
		IPv6AddressRange range = null;
		try{
			for(int i=0;i<ipLowRange.length;i++){				
				range = IPv6AddressRange.fromFirstAndLast(IPv6Address.fromString(ipLowRange[i]),IPv6Address.fromString(ipHighRange[i]));
				if(range.contains(IPv6Address.fromString(IP))){
					return codeRange[i];
				}
			}
			return "-2";
		}
		catch(Exception ex){
			return "-3";
			//throw ex;
		}
	}
	
	/*public static String getInstitutionFromIPv6(String IP){
		try{
		Set<String> ipSet = ipv6.keySet();
		IPv6AddressRange range = null;
		for(String str : ipSet){
		    range = IPv6AddressRange.fromFirstAndLast(IPv6Address.fromString(str.split("-")[0]),IPv6Address.fromString(str.split("-")[1]));
			if(range.contains(IPv6Address.fromString(IP))){
				return ipv6.get(str);
			}
		}
		return "-2";
		}
		catch(Exception ex){
			return "-3";
			//throw ex;
		}
	}*/
	
	/*public static String getInstitutionFromIPv4RDD(long IP){
		String institution = "";	
		Document doc = null;
		int size = 0;
		try{
			JavaRDD<Document> rddDoc = ipv4RDD.filter(doc1 -> doc1.getLong("ip_low") >= IP).filter(doc1 -> doc1.getLong("ip_high") <= IP);
			Set<Document> ipSet = rddDoc.countByValue().keySet();
			size = ipSet.size();
			
			for(int i=0;i<size;i++){
				doc = (Document)ipSet.toArray()[i]; 
				institution = institution + "," + doc.get("code");
			}
		
			return institution.substring(1, institution.length());
		}
		catch(Exception ex){
			throw ex;
		}
	}*/
	
	public static String getInstitutionFromUserID(String userID){
		String[] userIds = null;		
		try{
			if(userID.contains(",")){
				userIds = userID.split(",");
				for(int i=0;i<userIds.length;i++){
					int index = Collections.binarySearch(accParentList, new AccountParent(userIds[i], null), comparator);
					if(index >= 0)
						return accParentList.get(index).getInstitution_id();
				}
			}
			else{				
		        int index = Collections.binarySearch(accParentList, new AccountParent(userID, null), comparator);
				if(index >= 0)
					return accParentList.get(index).getInstitution_id();
			}
			return "-2";
		}
		catch(Exception ex){
			return "-3";
		}
	}
	
	public static String getGroupFromInstitutionID(String institutionId){
		String[] institutionIds = null;	
		StringBuilder sb  = new StringBuilder();
		String s ="";
		try{
			if(institutionId.contains(",")){
				institutionIds = institutionId.split(",");
				for(int i=0;i<institutionIds.length;i++){
					int index = Collections.binarySearch(accParentList, new AccountParent(institutionIds[i], null), comparator);
					if(index >= 0){
						if(sb.length() == 0)
							sb.append(accParentList.get(index).getParent_id());
						else{
							//MyLogger.log("Check IPdatails :: " + accParentList.get(index).getParent_id() +" with :: "+sb.toString() + " IN ::: " +sb.toString().contains(","+accParentList.get(index).getParent_id()) +" OR " + sb.toString().contains(accParentList.get(index).getParent_id() +","));
							 s = sb.toString().trim();
							if(!sb.toString().trim().contains(","+accParentList.get(index).getParent_id()) && !(s+",").contains(accParentList.get(index).getParent_id() +",")){
								sb.append(",").append(accParentList.get(index).getParent_id().trim());
							}
						}
					}
				}
			}
			else{				
		        int index = Collections.binarySearch(accParentList, new AccountParent(institutionId, null), comparator);
				if(index >= 0)
					return accParentList.get(index).getParent_id();
			}
			return sb.toString();
		}
		catch(Exception ex){
			return "-3";
		}
	}
}
