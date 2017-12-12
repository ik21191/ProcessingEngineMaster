package com.mps.log.filter.client;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mps.data.feed.account.MapperAccountInstChildParent;
import com.mps.data.feed.ip.MapperIPAccount;
import com.mps.data.feed.ip.DataIPAccount;
import com.mps.data.feed.pageview.MapperPageView;
import com.mps.data.pagetype.MapperPageType;
import com.mps.log.filter.FilterDocStats;
import com.mps.log.filter.WebLogFilter;
import com.mps.utils.MyLogger;

public class FilteredDocumentIEEE implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final String client = "IEEE";
	
	public static Document getFilteredDoc(final Document doc) throws Exception {		
		long startTime = 0;
		String statusCode = "";
		String statusCodeCount = "0";
		//int statusCodeCount = 0;
		int pageType = 0;
		//int imageCount = 0;
		String imageCount = "0";
		List<String> pgtype = new ArrayList<String>();
		String journalPattern = "";

		String institutionId = "";
		String institutionDetails = "";
		String institutionCodes = "";
		String ipAddress = "";
		
		String userID = "";
		String pageView = "counter";
		String userAgent = "";
		String ipData = "";
		String ipFlag = "NA";
		String robotFlag = "NA";
		String otherUrlFlag = "NA";
		String journalId = "";
		String proceedingId = "";
		String issueId = "";
		String articleId = "";
		String searchTerm = "";
		String sessionId = "";
		
		
		double iTracker = 0.0;
		try {
			iTracker = 1.0;
			FilterDocStats.updateRecordsProcessed();
			
			// ################# For getting Status count : Start
			iTracker = 2.0;
			startTime = System.currentTimeMillis();
			statusCode = doc.getString("status_code");
			if (statusCode != null) {
				statusCode = statusCode.trim();
				statusCodeCount = MapperPageView.commomPageView(statusCode, "status_code");
				//statusCodeCount = MapperPageView.getStatusCodeCount(statusCode);
				//MapperPageView.commomPageView(statusCode, "status_code");
			}
			iTracker = 3.0;
			//validating StatusCodeCount
			//if (statusCodeCount.equalsIgnoreCase("0")){
			if (statusCodeCount.equalsIgnoreCase("0")){
				doc.clear();
				doc.append("skipRecord", "true");
				FilterDocStats.updateInvalidStatusCode();
				FilterDocStats.updateSkipRecords();
				FilterDocStats.updateStatusCodeDuration(System.currentTimeMillis() - startTime);
				return doc;
			}
			FilterDocStats.updateStatusCodeDuration(System.currentTimeMillis() - startTime);

			
			// ################# For inserting page_type :: Start
			iTracker = 4.0;
			startTime = System.currentTimeMillis();
			String urlString = doc.getString("url");
			if(urlString != null){
				urlString = urlString.trim();
				if(!urlString.trim().equalsIgnoreCase("") && !urlString.trim().equalsIgnoreCase("-")){
					pgtype = MapperPageType.getPageTypeFromPattern(urlString);
					if(pgtype != null){
						pageType = Integer.parseInt(pgtype.get(0));
						journalPattern = pgtype.get(1);
						if (pageType <= 0) {
							FilterDocStats.updatePatternNotFound();
						}
					}
					else{
						FilterDocStats.updatePatternNotFound();
					}
				}else{
					FilterDocStats.updateBlankUrl();
				}
			}else{
				FilterDocStats.updateBlankUrl();
			}
			FilterDocStats.updatePgtype(System.currentTimeMillis() - startTime);
			

			//################# For getting image count :: Start
			iTracker = 2.0;
			startTime = System.currentTimeMillis();
			String resourceType = doc.getString("resource_type");
			if (resourceType != null){
				resourceType = resourceType.trim();
				if (!resourceType.equalsIgnoreCase("") && !resourceType.equalsIgnoreCase("-")) {
					//imageCount = MapperPageView.getImagecount(doc.getString("resource_type"));
					imageCount = MapperPageView.commomPageView(resourceType,"resource_type");
				}else{
					//invalid resource Type
				}
			}else{
				//invalid resource Type
			}
			FilterDocStats.updateResourceTypeDuration(System.currentTimeMillis() - startTime);
			iTracker = 3.0;

			
			//#####################For getting Page View Flag IP's : Start
			startTime = System.currentTimeMillis();
			String longIPAddress = doc.getString("long_ip_address");
			if (longIPAddress != null){
				longIPAddress = longIPAddress.trim();
				if (!longIPAddress.equalsIgnoreCase("") && !longIPAddress.equalsIgnoreCase("-")) {
					BigInteger ip = new BigInteger(longIPAddress);
					ipFlag = MapperPageView.getIPCount(ip);
					
				}
			}
			FilterDocStats.updateMemipPatternDuration(System.currentTimeMillis() - startTime);
			
			
			// ################# For updating page_view_flag : Start
			iTracker = 4.0;
			startTime = System.currentTimeMillis();
			if ((pageType != 0 || imageCount.equalsIgnoreCase("0")) && !ipFlag.trim().equalsIgnoreCase("Exclude")){
				
				if (!ipFlag.trim().equalsIgnoreCase("NA")) {
					pageView = ipFlag;
				}
				
				userAgent = doc.getString("user_agent");
				if (userAgent != null) {
					userAgent = userAgent.trim();
					//robotFlag = MapperPageView.getRobotCount(userAgent.toUpperCase());
					robotFlag = MapperPageView.commomPageView(userAgent.toUpperCase(),"user_agent");
					FilterDocStats.updateRobotDuration(System.currentTimeMillis() - startTime);

					//
					if(!robotFlag.trim().equalsIgnoreCase("NA")) {
						pageView = robotFlag;
					}else{
						startTime = System.currentTimeMillis();
						if (urlString != null) {
							//otherUrlFlag = MapperPageView.getOtherUrlCount(urlString);
							otherUrlFlag = MapperPageView.commomPageView(urlString,"url");
							
						}
						if (!otherUrlFlag.trim().equalsIgnoreCase("NA")) {
							pageView = otherUrlFlag;
						}
					}
					FilterDocStats.updateOtherUrlDuration(System.currentTimeMillis() - startTime);
				}
				
				
				//Account STEP 1: getting Institution codes based on User ID, if Institution ID is NULL
				iTracker = 5.0;
				startTime = System.currentTimeMillis();
				institutionId = doc.getString("institution_id");
				if (institutionId != null){institutionId = institutionId.trim();}
				userID = doc.getString("user_id");
				if (userID != null){userID = userID.trim();}
				iTracker = 6.0;
				if ((institutionId.equalsIgnoreCase("-") || institutionId.equalsIgnoreCase("") || institutionId == null)
						&& (!userID.equalsIgnoreCase("-") && !userID.equalsIgnoreCase("") && userID != null)) {
					
					institutionCodes = MapperAccountInstChildParent.getAccountData(userID);
					if(institutionCodes == "-2" || institutionCodes == "-3"){
						institutionCodes = "-";
					}
				}else{
					//what to do
					institutionCodes = institutionId;
				}
				FilterDocStats.updateAccountStep1(System.currentTimeMillis() - startTime);
				
				
				
				//Account STEP2 : get accounts from IP Account Mapper
				iTracker = 7.0;
				startTime = System.currentTimeMillis();
				if (institutionCodes.equalsIgnoreCase("-") || institutionCodes.equalsIgnoreCase("") || institutionCodes == null) {
					ipAddress = doc.getString("ip_address");
					ArrayList<DataIPAccount> ipList = MapperIPAccount.getIPAccountData(ipAddress);
					if (ipList == null){
						FilterDocStats.updateBlankIPAddress();
					}else {
						for (int i = 0; i < ipList.size(); i++){
							ipData = ipList.toString();
							if (i > 0) {
								institutionCodes = institutionCodes + "," + ipList.get(i).data;
							} else {
								institutionCodes = ipList.get(i).data;
							}
						}
					}
					
					// check for institutionCode
					if (institutionCodes == "-2") {
						FilterDocStats.updateIpNotFound();
						institutionCodes = "-";
					} else if (institutionCodes == "-3") {
						institutionCodes = "-";
						FilterDocStats.updateIpError();
					}
				}else{
					//what to do
				}
				FilterDocStats.updateIpDeltaTime(System.currentTimeMillis() - startTime);
				

				//ACCOUNT STEP3 : getting Account/Institution Details from Institution Codes
				iTracker = 8.0;
				startTime = System.currentTimeMillis();
				if (!institutionCodes.equalsIgnoreCase("-") && !institutionCodes.equalsIgnoreCase("") && institutionCodes != null){
					institutionDetails = MapperAccountInstChildParent.getAccountGroupData(institutionCodes);
					FilterDocStats.updateAccountStep3(System.currentTimeMillis() - startTime);
				}
				
				iTracker = 9.0;
				if (institutionDetails.equalsIgnoreCase("-") || institutionDetails.equalsIgnoreCase("") || institutionDetails == null || institutionDetails.equalsIgnoreCase("-2") || institutionDetails.equalsIgnoreCase("-3")) {
					startTime = System.currentTimeMillis();
					institutionDetails = institutionCodes;
					if (institutionDetails == "-2") {
						institutionDetails = "-";
					} else if (institutionDetails == "-3") {
						institutionDetails = "-";
					}
					FilterDocStats.updateAccountStep4(System.currentTimeMillis() - startTime);
				}
				
				if(pageType != 0 && pageView.equalsIgnoreCase("counter")){
					startTime = System.currentTimeMillis();
					if(journalPattern != null){
						String temp = "";
						doc.append("journal_pattern", journalPattern);
						if(journalPattern.indexOf("{{journal_id}}") != -1 && doc.getString("journal_id")!=null && doc.getString("journal_id").equalsIgnoreCase("-")){
							temp = journalPattern.replace("{{article_id}}", ".*").replace("{{issue_id}}", ".*").replace("{{search_term}}", ".*").replace("{{session_id}}", ".*").replace("{{proceeding_id}}", ".*");
							journalId = MapperPageType.fetchIDFromUrl(temp.trim(), urlString.trim(), "{{journal_id}}", 1);
							doc.put("journal_id", journalId);
						}
						if(journalPattern.indexOf("{{proceeding_id}}") != -1 ){
							temp = journalPattern.replace("{{article_id}}", ".*").replace("{{issue_id}}", ".*").replace("{{search_term}}", ".*").replace("{{session_id}}", ".*").replace("{{journal_id}}", ".*");
							proceedingId = MapperPageType.fetchIDFromUrl(temp.trim(), urlString.trim(), "{{proceeding_id}}", 4);
							doc.append("proceeding_id", proceedingId);
						}
						if(journalPattern.indexOf("{{article_id}}") != -1 && doc.getString("article_id")!=null && doc.getString("article_id").equalsIgnoreCase("-")){
							temp = journalPattern.replace("{{journal_id}}", ".*").replace("{{issue_id}}", ".*").replace("{{search_term}}", ".*").replace("{{session_id}}", ".*").replace("{{proceeding_id}}", ".*");						
							articleId = MapperPageType.fetchIDFromUrl(temp.trim(), urlString.trim(), "{{article_id}}", 1);
							doc.put("article_id", articleId);
						}
						if(journalPattern.indexOf("{{issue_id}}") != -1){
							temp = journalPattern.replace("{{article_id}}", ".*").replace("{{journal_id}}", ".*").replace("{{search_term}}", ".*").replace("{{session_id}}", ".*").replace("{{proceeding_id}}", ".*");
							issueId = MapperPageType.fetchIDFromUrl(temp.trim(), urlString.trim(), "{{issue_id}}", 1);
							doc.append("issue_id", issueId);
						}
						if(journalPattern.indexOf("{{search_term}}") != -1 && doc.getString("search_term")!=null && doc.getString("search_term").equalsIgnoreCase("-")){
							temp = journalPattern.replace("{{article_id}}", ".*").replace("{{issue_id}}", ".*").replace("{{journal_id}}", ".*").replace("{{session_id}}", ".*").replace("{{proceeding_id}}", ".*");
							searchTerm = MapperPageType.fetchIDFromUrl(temp.trim(), urlString.trim(), "{{search_term}}", 1);
							doc.put("search_term", searchTerm);
						}
						if(journalPattern.indexOf("{{session_id}}") != -1 && doc.getString("session_id")!=null && doc.getString("session_id").equalsIgnoreCase("-")){
							temp = journalPattern.replace("{{article_id}}", ".*").replace("{{issue_id}}", ".*").replace("{{search_term}}", ".*").replace("{{journal_id}}", ".*").replace("{{proceeding_id}}", ".*");
							sessionId = MapperPageType.fetchIDFromUrl(temp.trim(), urlString.trim(), "{{session_id}}", 1);
							doc.put("session_id", sessionId);
						}
					}
					FilterDocStats.updateIdsSpilt(System.currentTimeMillis() - startTime);
				}

				// Removed columns from log filter table
				iTracker = 10.0;
				doc.remove("gmt_offset");
				doc.remove("requested_method");
				doc.remove("http_version");
				iTracker = 5.0;
				
				//Updated values in doc
				
				
				
				
				
				// New columns added in log filter tables
				doc.append("page_type", pageType)
				.append("page_view_flag", pageView)
				.append("institution_details", institutionDetails)
				.append("institution_codes", institutionCodes)
				.append("ipData", ipData);
				
				
				FilterDocStats.updateRecordSuccess();

				// logging the performance on rotation of record count
				iTracker = 11.0;
				if (FilterDocStats.getRecordSuccess() % WebLogFilter.performanceCouter == 0) {
					/*
					MyLogger.log("Performace : Records =" + FilterDocStats.getRecordSuccess() + " : " 
						+ "Status Code ="+ (FilterDocStats.getStatusCodeDuration() / 1000.0) + " sec : " 
						+ "PageType ="+ (FilterDocStats.getPgtype() / 1000.0) + " sec : " 
						+ "Resource type ="+ (FilterDocStats.getResourceTypeDuration() / 1000.0) + " sec : "
						+ "Exclude IP ="+ (FilterDocStats.getMemipPatternDuration() / 1000.0) + " sec : "
						+ "Robot ="+ (FilterDocStats.getRobotDuration() / 1000.0) + " sec : " 
						+ "Other Url="+ (FilterDocStats.getOtherUrlDuration() / 1000.0) + " sec : "
						+ "Acc Step 1 ="+ (FilterDocStats.getAccountStep1() / 1000.0) + " sec : " 
						+ "Acc Step 2(IP) =" + (FilterDocStats.getIpDeltaTime() / 1000.0) + " sec : " 
						+ "Acc Step 3 ="+ (FilterDocStats.getAccountStep3() / 1000.0) + " sec : " 
						+ "Acc Step 4 ="+ (FilterDocStats.getAccountStep4() / 1000.0) + " sec : "
					    + "JournalIDs Step ="+ (FilterDocStats.getIdsSpilt() / 1000.0) + " sec : ");
				    */
					MyLogger.log(FilterDocStats.getStats());
					FilterDocStats.reset();
				}
				return doc;
			} else {
				if(ipFlag.trim().equalsIgnoreCase("Exclude"))
					FilterDocStats.updateSkipRecordsPageViewIP();
				if(pageType != 0 || imageCount.equalsIgnoreCase("0"))
					FilterDocStats.updateSkipRecordsResourceType();
				doc.clear();
				doc.append("skipRecord", "true");
				FilterDocStats.updateSkipRecords();
				return doc;
			}

		}catch (Exception ex) {
			MyLogger.log(client +" : Filter Document Exception : getFilteredDoc() : iTracker : " + iTracker + " : " + doc.toJson()
					+ " : " + ex.toString());
			doc.clear();
			doc.append("skipRecord", "true");
			FilterDocStats.updateErrorRecords();
			MyLogger.log(client +" : Filter Document Exception : getFilteredDoc() : iTracker : " + iTracker + " : " + doc.toJson()
					+ " : " + ex.toString());
			return doc;
		}
	}

	////////////// ************************* Extra Code
	////////////// *************************************
	/*
	 * if(doc.getString("status_code") != null && statusCode){
	 * if(!doc.getString("status_code").trim().equalsIgnoreCase("200") &&
	 * !doc.getString("status_code").trim().equalsIgnoreCase("304")){
	 * doc.clear(); doc.append("skipRecord", "true");
	 * FilterDocStats.updateInvalidStatusCode(); return doc; } } public static
	 * long ipToLong(InetAddress ip) { byte[] octets = ip.getAddress(); long
	 * result = 0; for (byte octet : octets) { result <<= 8; result |= octet &
	 * 0xff; } return result; }
	 */
	// boolean processIPold = false; //variable to block processing of Megha
	// code to test the new IP account details aCode
	// if(institutionIdOnly.equalsIgnoreCase("-") ||
	// institutionIdOnly.equalsIgnoreCase("") || institutionIdOnly == null ){
	/*
	 * if(doc.getString("ip_address") != null && processIPold){
	 * 
	 * if(doc.getString("ip_address").trim().contains(".") &&
	 * !doc.getString("ip_address").trim().contains(":")){ JavaRDD<Document>
	 * rddDoc = (JavaRDD<Document>) doc.get("rdd"); institutionIdOnly =
	 * rddDoc.filter(doc->doc.get("")); //for ipv4 change//institutionIdOnly =
	 * IPDetails.getInstitutionFromIPv4(Long.parseLong(doc.get("long_ip_address"
	 * ).toString().trim())); doc.append("ip", "ipv4"); startTime =
	 * System.currentTimeMillis(); //institutionIdOnly =
	 * IPDetails.getInstitutionFromIPv4(ipToLong(InetAddress.getByName(doc.get(
	 * "ip_address").toString().trim()))); if(doc.getString("long_ip_address")
	 * != null){ institutionIdOnly =
	 * IPDetails.getInstitutionFromIPv4(Long.parseLong(doc.get("long_ip_address"
	 * ).toString().trim())); } duration = System.currentTimeMillis() -
	 * startTime; if(institutionIdOnly == "-2"){ FilterDocStats.updateIpv4NF();
	 * institutionIdOnly="-"; } else if(institutionIdOnly == "-3"){
	 * institutionIdOnly="-"; FilterDocStats.updateIpv4Error(); }
	 * FilterDocStats.updateIpv4(duration);
	 * 
	 * }else if(doc.get("ip_address").toString().trim().contains(":")){
	 * doc.append("ip", "ipv6"); startTime = System.currentTimeMillis();
	 * institutionIdOnly =
	 * IPDetails.getInstitutionFromIPv6(doc.get("ip_address").toString().trim())
	 * ; duration = System.currentTimeMillis() - startTime; if(institutionIdOnly
	 * == "-2"){ FilterDocStats.updateIpv6NF(); institutionIdOnly="-"; } else
	 * if(institutionIdOnly == "-3"){ institutionIdOnly="-";
	 * FilterDocStats.updateIpv6Error(); } FilterDocStats.updateIpv6(duration);
	 * } }
	 */
	/// ipv4 & ipv6 manipulation

}
