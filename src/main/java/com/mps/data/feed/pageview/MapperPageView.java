package com.mps.data.feed.pageview;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.math.BigInteger;
import org.bson.Document;
import com.mps.utils.MyException;
import com.mps.utils.MyLogger;

public class MapperPageView {
	private  static Map<Integer, DataPageView[]> PageViewDataMap;	
	private  static Map<Integer, DataPageView[]> PageViewDataIPMap;
	private  static Map<String, List<DataPageView>> dataPageViewMap;
	private  static Map<String, String> pageViewTaskMap;
	private  static Map<String, List<DataPageView>> dataPageViewIPMap;
	
	public static void initializePageView(Set<Document> pgViewSet) throws Exception{
		Document doc = null;
		int size, docNo;
		double iTracker = 0.0;
		List<DataPageView> listtype1 = new ArrayList<>();
		List<DataPageView> listtype2 = new ArrayList<>();
		List<DataPageView> listtype3 = new ArrayList<>();
		List<DataPageView> listtype5 = new ArrayList<>();
		
		Map<Integer, DataPageView[]> pgViewMap = null;
		DataPageView pvf = null;
	    String pageView = "";
		try{
			MyLogger.log("PageViewType Flag : initializePageView() : Start");
			pgViewMap = new HashMap<>();
			size = pgViewSet.size();
			for(docNo = 0; docNo < size; docNo++){
	    		doc = null;
	    		iTracker = 5.0;
	    		doc = (Document) pgViewSet.toArray()[docNo];
	    		iTracker = 6.0;	
	    		pvf = new DataPageView();
	    		pvf.setId(doc.getInteger("id"));
	    		pvf.setType(doc.getInteger("type"));
	    		pvf.setReg_exp(doc.getString("reg_exp"));
	    		//if(doc.getInteger("type") == 5){
	    		if(doc.getInteger("type") == 5){
	    			if(doc.get("reg_exp").toString().trim().startsWith(".*") && doc.get("reg_exp").toString().trim().endsWith(".*")){
		    			pageView = doc.getString("reg_exp").trim();
		    		}
		    		else if(doc.get("reg_exp").toString().trim().startsWith(".*")){
		    			pageView = doc.getString("reg_exp").trim() + ".*";
		    		}
		    		else if(doc.get("reg_exp").toString().trim().endsWith(".*")){
		    			pageView = ".*" + doc.getString("reg_exp").trim();
		    		}
		    		else{
		    			pageView = ".*" + doc.getString("reg_exp").trim() + ".*";
		    		}
	    			pvf.setReg_exp_pattern(Pattern.compile(pageView, Pattern.CASE_INSENSITIVE));
	    		}
	    		//pvf.setTargetField(doc.getString("target_field").trim());
	    		//pvf.setOperation(doc.getString("operation").trim());
	    		//pvf.setTask(doc.getString("task").trim());
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
	            
	        Comparator<DataPageView> comparator = new ComparatorPageView();
	        Collections.sort(listtype3, comparator);
	        Collections.sort(listtype1, comparator);
	        
			pgViewMap.put(1, listtype1.toArray(new DataPageView[listtype1.size()]));
			pgViewMap.put(2, listtype2.toArray(new DataPageView[listtype2.size()]));
			pgViewMap.put(3, listtype3.toArray(new DataPageView[listtype3.size()]));
			pgViewMap.put(5, listtype5.toArray(new DataPageView[listtype5.size()]));
			
			iTracker = 8.0;
			PageViewDataMap = pgViewMap;
			
			iTracker = 9.0;
			for (Entry<Integer, DataPageView[]> map : PageViewDataMap.entrySet()) {
				int key = map.getKey();
				List<DataPageView> list = dataPageViewMap.get(key);
				if(list != null){
					MyLogger.log("MapperPageView : initializeDataPageView() : Page View Type " + key + " : Size=" + list.size() + " : Done");
				}else{
					MyLogger.log("MapperPageView : initializeDataPageView() : Page View Type " + key + " : NULL/No Record Found : Done");
				}
			}
			
			/*
			iTracker = 10.0;
			MyLogger.log("MapperPageView : initializePageView() : Page View Type 1(Resource type) : Size=" + pgViewMap.get(1).length + " : Done");
			MyLogger.log("MapperPageView : initializePageView() : Page View Type 2(Valid Status Code) : Size=" + pgViewMap.get(2).length + " : Done");
			MyLogger.log("MapperPageView : initializePageView() : Page View Type 3(Robot Pattern) : Size=" + pgViewMap.get(3).length + " : Done");
			MyLogger.log("MapperPageView : initializePageView() : Page View Type 5(Excluded Url Pattern) : Size=" + pgViewMap.get(5).length + " : Done");
			*/
		}
		catch(Exception ex){
			throw new MyException("MapperPageView : initializePageView() : iTracker : " + iTracker + " : " + doc.toJson() + " : " + ex.toString());
		}
	}
	
	public static void initializeDataPageView(Set<Document> pgViewSet) throws Exception{
		Document doc = null;
		int size, docNo;
		double iTracker = 0.0;
		Map<String, List<DataPageView>> dataPgViewMapTemp = null;
		Map<String, String> pgViewTaskMap = null;
		DataPageView dpv = null;
	    String pageView = "";
	    List<DataPageView> li = null;
		try{
			MyLogger.log("PageViewType Flag : initializeDataPageView() : Start");
			dataPgViewMapTemp = new HashMap<>();
			pgViewTaskMap = new HashMap<>();
			size = pgViewSet.size();
			for(docNo = 0; docNo < size; docNo++){
	    		doc = null;
	    		iTracker = 5.0;
	    		doc = (Document) pgViewSet.toArray()[docNo];
	    		iTracker = 6.0;	
	    		dpv = new DataPageView();
	    		dpv.setId(doc.getInteger("id"));
	    		dpv.setType(doc.getInteger("type"));
	    		dpv.setTargetField(doc.getString("targetField"));
	    		dpv.setReg_exp(doc.getString("reg_exp"));
	    		//if(doc.getInteger("type") == 5){
	    		if(doc.getString("task").trim().equalsIgnoreCase("Regex")){
	    			if(doc.get("reg_exp").toString().trim().startsWith(".*") && doc.get("reg_exp").toString().trim().endsWith(".*")){
		    			pageView = doc.getString("reg_exp").trim();
		    		}
		    		else if(doc.get("reg_exp").toString().trim().startsWith(".*")){
		    			pageView = doc.getString("reg_exp").trim() + ".*";
		    		}
		    		else if(doc.get("reg_exp").toString().trim().endsWith(".*")){
		    			pageView = ".*" + doc.getString("reg_exp").trim();
		    		}
		    		else{
		    			pageView = ".*" + doc.getString("reg_exp").trim() + ".*";
		    		}
	    			dpv.setReg_exp_pattern(Pattern.compile(pageView, Pattern.CASE_INSENSITIVE));
	    		}
	    		
	    		if(doc.getString("task").trim().equalsIgnoreCase("Contains")){
	    			dpv.setReg_exp(dpv.getReg_exp().toUpperCase());
	    		}
	    			
	    		dpv.setTargetField(doc.getString("target_field").trim());
	    		dpv.setOperation(doc.getString("operation").trim());
	    		dpv.setTask(doc.getString("task").trim());
	    		dpv.setWebmart_code(doc.getString("webmart_code"));
	    		dpv.setType_flag(doc.getString("type_flag"));
	    		if(dataPgViewMapTemp.containsKey(doc.getString("target_field").trim())){
	    			li = dataPgViewMapTemp.get(doc.getString("target_field").trim());
	    			li.add(dpv);
	    			dataPgViewMapTemp.put(doc.getString("target_field").trim(), li);
	    		}
	    		else{
	    			li = new ArrayList<DataPageView>();
	    			li.add(dpv);
	    			dataPgViewMapTemp.put(doc.getString("target_field").trim(), li);
	    		}
	    		pgViewTaskMap.put(doc.getString("target_field").trim(), doc.getString("task").trim());
	    		/*
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
				}    */			
	    		iTracker = 7.0;	    		
	    	}
	            
	        Comparator<DataPageView> comparator = new ComparatorPageView();
	        String key = "";
	        List<DataPageView> sortList = null;
	        for (Map.Entry<String, String> map : pgViewTaskMap.entrySet()) {
	            if (map.getValue().toString().equals("Search")) {
	                key = map.getKey();
	                sortList = dataPgViewMapTemp.get(key);
	                Collections.sort(sortList, comparator);
	                dataPgViewMapTemp.put(key, sortList);
	            }
	        }
	        /*List<DataPageView> sortList3 = dataPgViewMapTemp.get("Regex");
	        Collections.sort(sortList3, comparator);
	        List<DataPageView> sortList5 = dataPgViewMapTemp.get("Contains");
	        Collections.sort(sortList5, comparator);
	        dataPgViewMapTemp.put(doc.getString("Regex").trim(), sortList3);
	        dataPgViewMapTemp.put(doc.getString("Contains").trim(), sortList5);*/
			
			iTracker = 8.0;
			dataPageViewMap = dataPgViewMapTemp;
			pageViewTaskMap = pgViewTaskMap;
			iTracker = 9.0;
			for (Entry<String, List<DataPageView>> map : dataPageViewMap.entrySet()) {
				key = map.getKey();
				List<DataPageView> list = dataPageViewMap.get(key);
				if(list != null){
					MyLogger.log("MapperPageView : initializeDataPageView() : Page View Type " + key + " : Size=" + list.size() + " : Done");
				}else{
					MyLogger.log("MapperPageView : initializeDataPageView() : Page View Type " + key + " : NULL/No Record Found : Done");
				}
			}
			
			/*
			MyLogger.log("MapperPageView : initializeDataPageView() : Page View Type 1(Resource type) : Size=" + dataPageViewMap.get("resource_type").size() + " : Done");
			MyLogger.log("PageViewType Flag : initializeDataPageView() : Page View Type 2(Valid Status Code) : Size=" + dataPageViewMap.get("status_code").size() + " : Done");
			MyLogger.log("PageViewType Flag : initializeDataPageView() : Page View Type 3(Robot Pattern) : Size=" + dataPageViewMap.get("user_agent").size() + " : Done");
			MyLogger.log("PageViewType Flag : initializeDataPageView() : Page View Type 5(Excluded Url Pattern) : Size=" + dataPageViewMap.get("url").size() + " : Done");
			*/
			
		}
		catch(Exception ex){
			throw new MyException("PageViewMapper : initializeDataPageView() : iTracker : " + iTracker + " : " + doc.toJson() + " : " + ex.toString());
		}
	}
	
	public static void initializePageViewIP(Set<Document> pgViewSet) throws Exception{
		Document doc = null;
		int size, docNo;
		double iTracker = 0.0;
		Map<Integer, DataPageView[]> pgViewMap = null;
		List<DataPageView> list = null;
		DataPageView pvf = null;
		try{
			MyLogger.log("PageViewType : initializePageViewIP() : Start");
			pgViewMap = new HashMap<>();
			list = new ArrayList<>();
			size = pgViewSet.size();
			for(docNo = 0; docNo < size; docNo++){
	    		doc = null;
	    		iTracker = 5.0;
	    		doc = (Document) pgViewSet.toArray()[docNo];
	    		iTracker = 6.0;	
	    		pvf = new DataPageView();
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
			pgViewMap.put(4, list.toArray(new DataPageView[list.size()]));
			
			iTracker = 8.0;	 
			PageViewDataIPMap = pgViewMap;
			
			iTracker = 9.0;	 
			MyLogger.log("PageViewIP Flag : initializePageViewIP() : Page View Flag IP Type 4(Excluded IP's) : Size=" + pgViewMap.get(4).length + " : Done");
		}
		catch(Exception ex){
			throw new MyException("PageViewMapper : initializePageViewIP() : iTracker : " + iTracker + " : " + doc.toJson() + " : " + ex.toString());
		}
	}
	
	public static int getImagecount(String resourceType){
		try {
			DataPageView[] t1List = PageViewDataMap.get(1);
			List<DataPageView> resourceTypeList = Arrays.asList(t1List); 
			 int index = Collections.binarySearch(resourceTypeList, new DataPageView(resourceType, null), new ComparatorPageView());
			 if(index >= 0){
				 return resourceTypeList.get(index).getId();
				}
			
			/*for(DataPageView ptf : t1List){
				if(ptf.getReg_exp().trim().equalsIgnoreCase(resourceType))
					return ptf.getId();
			}*/
			return 0;
        }catch (Exception ex) {
        	return 0;
        }        	
	}
	
	public static int getStatusCodeCount(String statusCode){
		try {
			DataPageView[] t1List = PageViewDataMap.get(2);
			for(DataPageView ptf : t1List){
				if(ptf.getReg_exp().trim().equalsIgnoreCase(statusCode))
					return ptf.getId();
			}
			return 0;
        }catch (Exception ex) {
        	return 0;
        }        	
	}
	
	public static String getIPCount(BigInteger longIpAddress){
		try {
			DataPageView[] t1List = PageViewDataIPMap.get(4);
			for(DataPageView ptf : t1List){
				if((longIpAddress.compareTo(ptf.getLow_ip()) == 0 || longIpAddress.compareTo(ptf.getLow_ip()) == 1) && (longIpAddress.compareTo(ptf.getHigh_ip()) == 0 || longIpAddress.compareTo(ptf.getHigh_ip()) == -1)){
					if(ptf.getId() == 0 || ptf.getIn_out_flag() != 0){
						if(ptf.getId() != 0){
							return ptf.getType_flag();
						}else{
							return "NA";
						}						
					}else{
						return "Exclude";
					}
				}
			}
        }catch (RuntimeException e) {
        	return "NA";
        }
    	return "NA";
	}
	
	
	public static String getRobotCount(String userAgent){
		try {
			DataPageView[] t1List = PageViewDataMap.get(3);
			for(DataPageView ptf : t1List){
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
			DataPageView[] t1List = PageViewDataMap.get(5);
			for(DataPageView ptf : t1List){
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
	
	public static String commomPageView(String pageView, String field){
	try{
		String pgViewOutput = "0";
		String pgViewTask = "";
		for(String s : pageViewTaskMap.keySet()){
			if(s.equalsIgnoreCase(field)){
				pgViewTask = pageViewTaskMap.get(s);
				break;
			}
		}
		if(pgViewTask.equalsIgnoreCase("Regex")){
			pgViewOutput = regexPageView(pageView,field);
		}
		else if(pgViewTask.equalsIgnoreCase("Contains")){
			pgViewOutput = containsPageView(pageView,field);
		}
		else if(pgViewTask.equalsIgnoreCase("Search")){
			pgViewOutput = searchPageView(pageView,field);
		}
		return pgViewOutput;
		}
		catch (Exception ex) {
        	return "0";
        } 			
	}
	
	public static String searchPageView(String statusCode, String field){
		try {
			List<DataPageView> t1List = dataPageViewMap.get(field);
			/*for(DataPageView ptf : t1List){
				if(ptf.getReg_exp().trim().equalsIgnoreCase(statusCode))
					return ptf.getId()+"";
			}*/
			 int index = Collections.binarySearch(t1List, new DataPageView(statusCode, null), new ComparatorPageView());
			 if(index >= 0){
				 return t1List.get(index).getId()+"";
				}
			return "0";
        }catch (Exception ex) {
        	return "0";
        }        	
	}
	
	public static String containsPageView(String userAgent, String field){
		try {
			List<DataPageView> t1List = dataPageViewMap.get(field);
			for(DataPageView ptf : t1List){
				if(userAgent.trim().contains(ptf.getReg_exp())){
					return ptf.getType_flag();
				}
			}
        } catch (RuntimeException e) {
        	return "NA";
        }
        	return "NA";
	}
	
	
	public static String regexPageView(String url, String field){
		Matcher matcher = null;
		try {
			List<DataPageView> t1List = dataPageViewMap.get(field);
			for(DataPageView ptf : t1List){
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
	
	
	public static String getReturnCheck(String url, String field){
		return "INCLUDE~#~valuepagetype0statuscode";
	}
	
	
}
