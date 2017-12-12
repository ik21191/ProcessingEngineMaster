/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mps.data.feed.ip;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.bson.Document;

import com.mps.utils.MyLogger;

/**
 *
 * @author kapil.verma
 */
public class MapperIPAccount {
    private static DataIPAccount[] ipData = null;
    private static BigInteger[] maxIndex = null;
    private static double iTracker = 0.0;
    
    /**
     * @param ipDataList , Array List of IpData Class
     * @throws Exception
     */
    public static void initialize(final ArrayList<DataIPAccount> ipDataList) throws Exception{
        
        try {
            MyLogger.log("MapperIPAccount : Start");
            if(ipDataList == null){
                throw new Exception("MapperIPAccount : NUll IpData ArrayList");
            }
            ipData = ipDataList.toArray(new DataIPAccount[ipDataList.size()]);

            iTracker = 3.0;
            //sorting the array of IP Sets
            MyLogger.log("MapperIPAccount : Sorting Array : Start");
            Arrays.sort(ipData, new ComparatorIPAccount());
            MyLogger.log("MapperIPAccount : Sorting Array : End");
            
            MyLogger.log("MapperIPAccount : Initializing Max Index : Start");
            maxIndex = new BigInteger[ipData.length];
            MyLogger.log("MapperIPAccount : Initializing Max Index : End");
            
            iTracker = 4.0;
            //initilaizing the maxEnd of array as per min and max index
            MyLogger.log("MapperIPAccount : Fill Max Index Initially : Start");
            getMaxIndex(0, ipData.length);
            MyLogger.log("MapperIPAccount : Fill Max Index Initially : END");
            MyLogger.log("MapperIPAccount : End");
        } catch (Exception e) {
        	MyLogger.log("MapperIPAccount : Exception : " + e);
            throw new Exception("MapperIPAccount : Exception : " + e);
        }
        
    }
    
    //method to get BigInteger Value of IPv4 and IPv6
    private static BigInteger ipToBigInteger(String address) throws Exception{
        InetAddress ipAddress = null;
        byte[] bytes = null;
        try {
            ipAddress = InetAddress.getByName(address);
            bytes = ipAddress.getAddress();
            return new BigInteger(1, bytes);
        } catch (Exception e) {
        	MyLogger.log("MapperIPAccount : ipToBigInteger : " + address + " : " + e.toString());
            throw new Exception("MapperIPAccount : ipToBigInteger : " + address + " : " + e.toString());
        }
    }
    
    //method to set max Index for searching x in range for recurssive method
    private static BigInteger getMaxIndex(int min, int max) throws Exception {
        
        int m = 0;
        BigInteger bigVal = new BigInteger("-2");
        BigInteger max1 = new BigInteger("-2");
        BigInteger max2 = new BigInteger("-2");
        try{
            //check for valid ranges
            if (min >= max) {            
                return new BigInteger("-2");
            }
            //
            m = (min + max) >>> 1;
            maxIndex[m] = getMaxIndex(min, m);

            bigVal = new BigInteger("-9999999999999");
            max1 = maxIndex[m].max(ipData[m].ipEnd);
            max2 = getMaxIndex(m + 1, max);
            bigVal = max1.max(max2);

            //bigval = Math.max(Math.max(maxEnd[m], intervals[m].end), initializeMaxEnd(m + 1, max));            
            //myLogger.log("getMaxIndex : " + min + " : " + max + " : " + m + " : " + max1.toString() + " : " + max2.toString() + " : " + bigval.toString());
        }catch(Exception e){
        	MyLogger.log("MapperIPAccount : getMaxIndex : " + min + " : " + max + " : " + m + " : " + max1.toString() + " : " + max2.toString() + " : " + bigVal.toString() + " : " + e);
            throw new Exception("MapperIPAccount : getMaxIndex : " + min + " : " + max + " : " + m + " : " + max1.toString() + " : " + max2.toString() + " : " + bigVal.toString() + " : " + e);
        }
        return bigVal;
    }
    
    //method to fin the x in a range in a collection
    private static void findContainingSet(BigInteger bigIP, int min, int max, ConcurrentHashMap<String, DataIPAccount> resultMap) throws Exception {
        int midIndex = 0;
        int compareval = 0;
        
        try {
            if (min >= max) {
                return;
            }
            midIndex = (min + max) >>> 1;
            DataIPAccount ipSet = ipData[midIndex];

            //Comapring X with BEGIN of IP range
            compareval = bigIP.compareTo(ipSet.ipBegin);
            if(compareval < 0){
            //if (x < ipSet.ipBegin) {
                findContainingSet(bigIP, min, midIndex, resultMap);
            }else{
                //Comapring X with END of IP range
                compareval = bigIP.compareTo(ipSet.ipEnd);
                if (compareval <= 0) {                    
                    resultMap.put(ipSet.data, ipSet);
                }
                
                //Comapring the random value from in between with IP X
                compareval = maxIndex[midIndex].compareTo(bigIP);
                if (compareval >= 0) {
                    findContainingSet(bigIP, min, midIndex, resultMap);
                }else{
                    //do nothing
                }
                findContainingSet(bigIP, midIndex + 1, max, resultMap);
            }
        } catch (Exception e) {
        	MyLogger.log("MapperIPAccount : findContainingSet : " + bigIP + " : " + min + " : " + max + " : " + midIndex);
           throw new Exception("MapperIPAccount : findContainingSet : " + bigIP + " : " + min + " : " + max + " : " + midIndex + " : " + e);
        }
    }
    
    /**
     * @param ip , IP in String variable for which lookup has to be done in IpData class collection
     * @return Array List of IpData Class
     */
    public static ArrayList<DataIPAccount> getIPAccountData(String ip){
        ConcurrentHashMap<String, DataIPAccount> resultMap = new ConcurrentHashMap<>();
        ArrayList<DataIPAccount> IpDataList = new ArrayList<>();
        BigInteger bIp;
        try {
        	if(ip == null){
        		return null;
        	}
        	if(ip.trim().equalsIgnoreCase("")){
        		return null;
        	}
        	
        	bIp = ipToBigInteger(ip);
            findContainingSet(bIp, 0, ipData.length, resultMap);
            
            //getting list from IP MAP resultMap
            for(Entry entry : resultMap.entrySet()){
            	IpDataList.add((DataIPAccount)entry.getValue());
            }
            
            return IpDataList;
        } catch (Exception e) {
        	MyLogger.log("MapperIPAccount : getIPAccountData : " + ip + " : " + e.toString());
            return null;
        }
    }
    
   
    
}
