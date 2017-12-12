package com.mps.backup;


import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;

import com.mps.data.feed.account.AccountData;
import com.mps.utils.MyLogger;

public class PageViewFilter {
  private int id;
  private int type;
  private String reg_exp;
  private Pattern reg_exp_pattern;
  private String webmart_code;
  private String type_flag;
  private BigInteger low_ip;
  private BigInteger high_ip;
  private int in_out_flag;
  private static Map<Integer, PageViewFilter[]> PageViewFilterMap;	
  private static Map<Integer, PageViewFilter[]> PageViewFilterIPMap;
  
  public BigInteger getLow_ip() {
	return low_ip;
  }
	
	public void setLow_ip(BigInteger low_ip) {
		this.low_ip = low_ip;
	}
	
	public BigInteger getHigh_ip() {
		return high_ip;
	}
	
	public void setHigh_ip(BigInteger high_ip) {
		this.high_ip = high_ip;
	}
	
	public int getIn_out_flag() {
		return in_out_flag;
	}
	
	public void setIn_out_flag(int in_out_flag) {
		this.in_out_flag = in_out_flag;
	}	
	
    public static Map<Integer, PageViewFilter[]> getPageViewFilterIPMap() {
		return PageViewFilterIPMap;
	}

	public static void setPageViewFilterIPMap(Map<Integer, PageViewFilter[]> pageViewFilterIPMap) {
		PageViewFilterIPMap = pageViewFilterIPMap;
	}

	public Pattern getReg_exp_pattern() {
	   return reg_exp_pattern;
    }
		
    public void setReg_exp_pattern(Pattern reg_exp_pattern) {
		this.reg_exp_pattern = reg_exp_pattern;
    }

	public static Map<Integer, PageViewFilter[]> getPageViewFilterMap() {
		return PageViewFilterMap;
	}
	
	public static void setPageViewFilterMap(Map<Integer, PageViewFilter[]> pageViewFilterMap) {
		PageViewFilterMap = pageViewFilterMap;
	}

	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public int getType() {
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}
	
	public String getReg_exp() {
		return reg_exp;
	}
	
	public void setReg_exp(String reg_exp) {
		this.reg_exp = reg_exp;
	}
	
	public String getWebmart_code() {
		return webmart_code;
	}
	
	public void setWebmart_code(String webmart_code) {
		this.webmart_code = webmart_code;
	}
	
	public String getType_flag() {
		return type_flag;
	}
	
	public void setType_flag(String type_flag) {
		this.type_flag = type_flag;
	}
	  
	public static void getPageViewTypeMap(Set<Document> pgViewSet) throws Exception{
		Document doc = null;
		int size, docNo;
		double iTracker = 0.0;
		List<PageViewFilter> listtype1 = new ArrayList<PageViewFilter>();
		List<PageViewFilter> listtype2 = new ArrayList<PageViewFilter>();
		List<PageViewFilter> listtype3 = new ArrayList<PageViewFilter>();
		List<PageViewFilter> listtype5 = new ArrayList<PageViewFilter>();
		
		Map<Integer, PageViewFilter[]> pgViewMap = null;
		PageViewFilter pvf = null;
	    String pageView = "";
		try{
			MyLogger.log("PageViewType Flag : getPageViewTypeMap() : Start");
			pgViewMap = new HashMap<Integer, PageViewFilter[]>();
			size = pgViewSet.size();
			for(docNo = 0; docNo < size; docNo++){
	    		doc = null;
	    		iTracker = 5.0;
	    		doc = (Document) pgViewSet.toArray()[docNo];
	    		iTracker = 6.0;	
	    		pvf = new PageViewFilter();
	    		pvf.setId(doc.getInteger("id"));
	    		pvf.setType(doc.getInteger("type"));
	    		pvf.setReg_exp(doc.getString("reg_exp"));
	    		if(doc.getInteger("type") == 5){
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
	    			pvf.setReg_exp_pattern(Pattern.compile(pageView, Pattern.CASE_INSENSITIVE));
	    		}
	    		pvf.setWebmart_code(doc.getString("webmart_code"));
	    		pvf.setType_flag(doc.getString("type_flag"));
	    		if(doc.getInteger("type") == 1){
	    			listtype1.add(pvf);
	    		}
	    		else if(doc.getInteger("type") == 2){
	    			listtype2.add(pvf);
	    		}
				else if(doc.getInteger("type") == 3){
					pvf.setReg_exp(pvf.getReg_exp().toUpperCase());
					listtype3.add(pvf); 
				}
				else if(doc.getInteger("type") == 5){					
					listtype5.add(pvf);
				}    			
	    		iTracker = 7.0;	    		
	    	}
			 Comparator<PageViewFilter> comp = new Comparator<PageViewFilter>()
	            {
	                public int compare(PageViewFilter u1, PageViewFilter u2)
	                {
	                    return u1.getReg_exp().compareTo(u2.getReg_exp());
	                }
	            };
	        Collections.sort(listtype3, comp);
			pgViewMap.put(1, listtype1.toArray(new PageViewFilter[listtype1.size()]));
			pgViewMap.put(2, listtype2.toArray(new PageViewFilter[listtype1.size()]));
			pgViewMap.put(3, listtype3.toArray(new PageViewFilter[listtype1.size()]));
			pgViewMap.put(5, listtype5.toArray(new PageViewFilter[listtype1.size()]));
			iTracker = 8.0;
			PageViewFilter.setPageViewFilterMap(pgViewMap);
			iTracker = 9.0;
			MyLogger.log("PageViewType Flag : getPageViewTypeMap() : Page View Type 1(Resource type) : Size=" + pgViewMap.get(1).length + " : Done");
			MyLogger.log("PageViewType Flag : getPageViewTypeMap() : Page View Type 2(Valid Status Code) : Size=" + pgViewMap.get(2).length + " : Done");
			MyLogger.log("PageViewType Flag : getPageViewTypeMap() : Page View Type 3(Robot Pattern) : Size=" + pgViewMap.get(3).length + " : Done");
			MyLogger.log("PageViewType Flag : getPageViewTypeMap() : Page View Type 5(Excluded Url Pattern) : Size=" + pgViewMap.get(5).length + " : Done");
		}
		catch(Exception ex){
			throw new Exception("PageViewFilter : getPageViewTypeMap() : iTracker : " + iTracker + " : " + doc.toJson() + " : " + ex.toString());
		}
		
	}
	
	public static void getPageViewIPMap(Set<Document> pgViewSet) throws Exception{
		Document doc = null;
		int size, docNo;
		double iTracker = 0.0;
		Map<Integer, PageViewFilter[]> pgViewMap = null;
		List<PageViewFilter> list = null;
		PageViewFilter pvf = null;
		try{
			MyLogger.log("PageViewType : getPageViewIPMap() : Start");
			pgViewMap = new HashMap<Integer, PageViewFilter[]>();
			list = new ArrayList<PageViewFilter>();
			size = pgViewSet.size();
			for(docNo = 0; docNo < size; docNo++){
	    		doc = null;
	    		iTracker = 5.0;
	    		doc = (Document) pgViewSet.toArray()[docNo];
	    		iTracker = 6.0;	
	    		pvf = new PageViewFilter();
	    		pvf.setId(doc.getInteger("id"));
	    		pvf.setType(doc.getInteger("type"));
	    		pvf.setReg_exp(doc.getString("reg_exp"));
	    		pvf.setLow_ip(new BigInteger(doc.get("low_ip").toString()));
	    		pvf.setHigh_ip(new BigInteger(doc.get("high_ip").toString()));
	    		pvf.setIn_out_flag(doc.getInteger("in_out_flag"));
	    		pvf.setWebmart_code(doc.getString("webmart_code"));
	    		pvf.setType_flag(doc.getString("type_flag"));	    		
	    		list.add(pvf);    			
	    		iTracker = 7.0;	    		
	    	}
			pgViewMap.put(4, list.toArray(new PageViewFilter[list.size()]));
			iTracker = 8.0;	 
			PageViewFilter.setPageViewFilterIPMap(pgViewMap);
			iTracker = 9.0;	 
			MyLogger.log("PageViewIP Flag : getPageViewIPMap() : Page View Flag IP Type 4(Excluded IP's) : Size=" + pgViewMap.get(4).length + " : Done");
		}
		catch(Exception ex){
			throw new Exception("PageViewFilter : getPageViewIPMap() : iTracker : " + iTracker + " : " + doc.toJson() + " : " + ex.toString());
		}
		
	}
	
	public static int getImagecount(String resourceType){
		int i;
		try {
			PageViewFilter[] t1List = PageViewFilterMap.get(1);
			for(PageViewFilter ptf : t1List){
				if(ptf.getReg_exp().trim().equalsIgnoreCase(resourceType))
					return ptf.getId();
			}
			return 0;
        }catch (Exception ex) {
        	return 0;
        }        	
	}
	
	public static int getStatusCodeCount(String statusCode){
		int i;
		try {
			PageViewFilter[] t1List = PageViewFilterMap.get(2);
			for(PageViewFilter ptf : t1List){
				if(ptf.getReg_exp().trim().equalsIgnoreCase(statusCode))
					return ptf.getId();
			}
			return 0;
        }catch (Exception ex) {
        	return 0;
        }        	
	}
	
	public static String getIPCount(BigInteger longIpAddress){
		int i;
		try {
			PageViewFilter[] t1List = PageViewFilterIPMap.get(4);
			for(PageViewFilter ptf : t1List){
					if((longIpAddress.compareTo(ptf.getLow_ip()) == 0 || longIpAddress.compareTo(ptf.getLow_ip()) == 1) && (longIpAddress.compareTo(ptf.getHigh_ip()) == 0 || longIpAddress.compareTo(ptf.getHigh_ip()) == -1)){
					if(ptf.getId() == 0 || ptf.getIn_out_flag() != 0){
						if(ptf.getId() != 0)
							return ptf.getType_flag();
						else
							return "NA";						
					}
					else
						return "Exclude";
				}
			}
        } catch (RuntimeException e) {
        	return "NA";
        }
        	return "NA";
	}
	
	public static String getRobotCount(String userAgent){
		try {
			PageViewFilter[] t1List = PageViewFilterMap.get(3);
			for(PageViewFilter ptf : t1List){
				if(userAgent.trim().contains(ptf.getReg_exp())){
					return ptf.getType_flag();
				}
			}
        } catch (RuntimeException e) {
        	return "NA";
        }
        	return "NA";
	}
	
	public static String getOtherUrlCount(String url){
		Matcher matcher = null;
		try {
			PageViewFilter[] t1List = PageViewFilterMap.get(5);
			for(PageViewFilter ptf : t1List){
				matcher = ptf.getReg_exp_pattern().matcher(url);
	            if( matcher.matches() == true){
	            	return ptf.getType_flag();
	            }
			}
        } catch (RuntimeException e) {
        	return "NA";
        }
        	return "NA";
	}
	
	
	//*********************************Extra code********************************
	/*
	  private static String[] roboPattern;
	  private static String[] roboFlag;
	  private static Pattern[] otherUrlPattern;
	  private static String[] otherUrlFlag;*/
	  /*public static Pattern[] getOtherUrlPattern() {
		return otherUrlPattern;
	}

	public static void setOtherUrlPattern(Pattern[] otherUrlPattern) {
		PageViewFilter.otherUrlPattern = otherUrlPattern;
	}

	public static String[] getOtherUrlFlag() {
		return otherUrlFlag;
	}

	public static void setOtherUrlFlag(String[] otherUrlFlag) {
		PageViewFilter.otherUrlFlag = otherUrlFlag;
	}
	public static String[] getRoboPattern() {
		return roboPattern;
	}

	public static void setRoboPattern(String[] roboPattern) {
		PageViewFilter.roboPattern = roboPattern;
	}

	public static String[] getRoboFlag() {
		return roboFlag;
	}

	public static void setRoboFlag(String[] roboFlag) {
		PageViewFilter.roboFlag = roboFlag;
	}
	*/
	/*
	 /*PageViewFilter[] pgView = pgViewMap.get(3);
			roboPattern = new String[pgView.length];
			roboFlag = new String[pgView.length];
			for(int i=0;i<listtype3.size();i++){
				roboPattern[i] = listtype3.get(i).getReg_exp();
				roboFlag[i] = listtype3.get(i).getType_flag();
			}
			//PageViewFilter.setRoboPattern(roboPattern);
			//PageViewFilter.setRoboFlag(roboFlag);
			
			PageViewFilter[] pgViewOther = pgViewMap.get(5);
			otherUrlPattern = new Pattern[pgViewOther.length];
			otherUrlFlag = new String[pgViewOther.length];
			for(int i=0;i<listtype5.size();i++){
				otherUrlPattern[i] = listtype5.get(i).getReg_exp_pattern();
				otherUrlFlag[i] = listtype5.get(i).getType_flag();
			}	
			//PageViewFilter.setOtherUrlFlag(otherUrlFlag);
			//PageViewFilter.setOtherUrlPattern(otherUrlPattern);
	
	 */

}
