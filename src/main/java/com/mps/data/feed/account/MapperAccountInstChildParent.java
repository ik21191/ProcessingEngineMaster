package com.mps.data.feed.account;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import com.mps.data.feed.account.ComparatorAccountInstChildParent;
import com.mps.data.feed.account.DataAccountInstChildParent;
import com.mps.utils.MyLogger;

public class MapperAccountInstChildParent{
	private static ArrayList<DataAccountInstChildParent> accountData = null;
    private static BigInteger[] maxIndex = null;
    private static double iTracker = 0.0;
    
    public static void initialize(final ArrayList<DataAccountInstChildParent> accountDataList) throws Exception{
        
        try {
        	MyLogger.log("MapperAccountInstChildParent : Start");
            if(accountDataList == null){
                throw new Exception("MapperAccountInstChildParent : NUll AccountData ArrayList");
            }
            accountData = accountDataList;
            Collections.sort(accountData, new ComparatorAccountInstChildParent());
            MyLogger.log("MapperAccountInstChildParent : End");
        } catch (Exception e) {
        	MyLogger.log("MapperAccountInstChildParent : Exception : " + e);
            throw new Exception("MapperAccountInstChildParent : Exception : " + e);
        }
        
    }
    
  //method to fin the x in a range in a collection
    private static Set<String> findContainingSet(String institutionId) throws Exception {
    	Set<String> set = new LinkedHashSet<>();
    	String[] institutionIds = null;
        try{
				institutionIds = institutionId.split(",");
				for(int i=0;i<institutionIds.length;i++){
					int index = Collections.binarySearch(accountData, new DataAccountInstChildParent(institutionIds[i], null), new ComparatorAccountInstChildParent());
					if(index >= 0){
						set.addAll(Arrays.asList(accountData.get(index).parentId.split(",")));
						while (index >= 0 && accountData.size() != (index+1) && new ComparatorAccountInstChildParent().compare(accountData.get(index), accountData.get(index+1)) == 0) {
							set.addAll(Arrays.asList(accountData.get(index+1).parentId.split(",")));
						    index++;
						}
					}
				}
			return set;
        } catch (Exception e) {
        	MyLogger.log("MapperAccountInstChildParent : findContainingSet : " + institutionId + e.toString() );
        }
        return set;
    }
    
    public static String getAccountGroupData(String institutionId){
    	StringBuilder accountDataList = new StringBuilder();
        try {
            Set<String> set = findContainingSet(institutionId);            
            //getting list from Account MAP resultMap
            if(!set.isEmpty()){
	            for(String s : set){
	            	accountDataList.append(s).append(",");
	            }
	            return accountDataList.substring(0, accountDataList.length()-1);
            }
            else
            	return "-2";
        } catch (Exception e) {
        	MyLogger.log("MapperAccountInstChildParent : getAccountGroupData : " + institutionId + " : " + e.toString());
            return "-3";
        }
    }
    
    public static String getAccountData(String institutionId){
    	String instID = "";
        try {
        	instID = findContainingInstitutionSet(institutionId);             
            return instID;
        } catch (Exception e) {
        	MyLogger.log("MapperAccountInstChildParent : getAccountData : " + institutionId + " : " + e.toString());
            return null;
        }
    }
    
    private static String findContainingInstitutionSet(String institutionId) throws Exception {
    	Set<String> set = new LinkedHashSet<>();        
    	String[] institutionIds = null;
    	
    	try{
			if(institutionId.contains(",")){
				institutionIds = institutionId.split(",");
				for(int i=0;i<institutionIds.length;i++){
					int index = Collections.binarySearch(accountData, new DataAccountInstChildParent(institutionIds[i], null), new ComparatorAccountInstChildParent());
					if(index >= 0){
						return accountData.get(index).institutionId;
					}
				}
				
			}else{				
		        int index = Collections.binarySearch(accountData, new DataAccountInstChildParent(institutionId, null), new ComparatorAccountInstChildParent());
				if(index >= 0){
					return accountData.get(index).institutionId;
				}
			}
			return "";
        } catch (Exception e) {
        	MyLogger.log("MapperAccountInstChildParent : findContainingInstitutionSet : " + institutionId );
        }
    	return "";
    }
    
    
    ///******************************Extra Code***************************************************
  //method to fin the x in a range in a collection
    /*private static void findContainingSet(String institutionId, int min, int max, ConcurrentHashMap<String, AccountData> resultMap) throws Exception {
        int midIndex = 0;
        int compareval = 0;
        
        try {
            if (min >= max) {
                return;
            }
            midIndex = (min + max) >>> 1;
            AccountData accountSet = accountData[midIndex];

            //Comapring X with BEGIN of IP range
            compareval = institutionId.compareTo(accountSet.institutionId);
           
            if(compareval < 0){
            	 // Comparing institutionId with lesser than mid values
                findContainingSet(institutionId, min, midIndex, resultMap);
                
            }else if(compareval > 0){
            	// Comparing institutionId with greter than mid values
                findContainingSet(institutionId, midIndex + 1, max, resultMap);
                
            }else{
            	//Value found & saved in map
            	resultMap.put(accountSet.parentId, accountSet);
            	// Comparing same institutionId with greter than mid values
            	findContainingSet(institutionId, min, midIndex, resultMap);
            }
        } catch (Exception e) {
           System.out.println("findContainingSet : " + institutionId + " : " + min + " : " + max + " : " + midIndex);
        }
    }*/
    /*private static String findContainingInstitutionSet(String institutionId) throws Exception {
        int midIndex = 0;
        int compareval = 0;
        
        try {
            if (min >= max) {
                return "-";
            }
            midIndex = (min + max) >>> 1;
            AccountData accountSet = accountData[midIndex];

            //Comapring X with BEGIN of IP range
            compareval = institutionId.compareTo(accountSet.institutionId);
            if(compareval < 0){
            //if (x < ipSet.ipBegin) {
            	findContainingInstitutionSet(institutionId, min, midIndex);
            }else if(compareval > 0){
            	findContainingInstitutionSet(institutionId, midIndex + 1, max);
            }
            else{
            	return accountSet.institutionId;
            }
            
        } catch (Exception e) {
           System.out.println("findContainingInstitutionSet : " + institutionId + " : " + min + " : " + max + " : " + midIndex);
        }
        return "-";
    }*/
    
}
