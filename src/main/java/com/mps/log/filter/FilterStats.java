package com.mps.log.filter;

public class FilterStats {
	private static long blankUrl = 0;
	private static long recordSuccess = 0;
	private static long invalidPageType = 0;
	private static long errorRecords = 0;
	private static long skipRecords = 0;
	private static long patternNotFound = 0;
	private static long invalidOthers = 0;
	private static long invalidStatusCode = 0;
	private static long recordsProcessed = 0;
	private static long blankIPAddress = 0;
	private static long invalidResourceType = 0;
	private static long statusCodeDuration = 0;
	private static long memipPatternDuration = 0;
	private static long resourceTypeDuration = 0;
	private static long robotDuration = 0;
	private static long otherUrlDuration = 0;
	private static long accountStep1 = 0;
	private static long accountStep3 = 0;
	private static long accountStep4 = 0;
	private static long pgtype = 0;
	private static long ipv4 = 0;
	private static long ipv6 = 0;
	private static long ipv4Error = 0;
	private static long ipv4NF = 0;
	private static long ipv6Error = 0;
	private static long ipv6NF = 0;
	private static long ipDeltaTime = 0;
	private static long ipNotFound = 0;
	private static long ipInvalid = 0;
	private static long ipError = 0;
	private static long skipRecordsPageViewIP = 0;
	private static long idsSpilt = 0;


	


	private static long skipRecordsResourceType = 0;

	//private constructor to stop instance of static class
	private FilterStats(){}
	
	//method to reset statistics
	public static void reset(){
		ipDeltaTime = 0;
		pgtype = 0;
		accountStep1 = 0;
		accountStep3 = 0;
		accountStep4 = 0;
		statusCodeDuration = 0;
		memipPatternDuration = 0;
		resourceTypeDuration = 0;
		robotDuration = 0;
		otherUrlDuration = 0;
		idsSpilt = 0;
	}

	
	public static long getAccountStep1() {
		return accountStep1;
	}
	public static long getAccountStep3() {
		return accountStep3;
	}
	public static long getAccountStep4() {
		return accountStep4;
	}
	public static long getBlankIPAddress() {
		return blankIPAddress;
	}
	public static long getBlankUrl() {
		return blankUrl;
	}
	public static long getErrorRecords() {
		return errorRecords;
	}
	public static long getInvalidOthers() {
		return invalidOthers;
	}
	public static long getInvalidPageType() {
		return invalidPageType;
	}
	public static long getInvalidStatusCode() {
		return invalidStatusCode;
	}
	//added by KSV on 2017-10-04 for IP stats
	public static long getIpDeltaTime() {
		return ipDeltaTime;
	}
	public static long getIpError() {
		return ipError;
	}
	public static long getIpInvalid() {
		return ipInvalid;
	}
	public static long getIpNotFound() {
		return ipNotFound;
	}
	public static long getIpv4() {
		return ipv4;
	}
	public static long getIpv4Error() {
		return ipv4Error;
	}
	public static long getIpv4NF() {
		return ipv4NF;
	}
	public static long getIpv6() {
		return ipv6;
	}

	public static long getIpv6Error() {
		return ipv6Error;
	}
	public static long getIpv6NF() {
		return ipv6NF;
	}
	public static String getLogStatus() {
		return "FilterDocStats [{ Records Processed :: "+recordsProcessed+" }, { Records Success :: "+recordSuccess+"/"+recordsProcessed+" }, { Blank Url :: "+blankUrl+" }, { Pattern Not Found :: "+patternNotFound+"/"+recordSuccess+" }, { Invalid Status Code :: "+invalidStatusCode+"/"+recordsProcessed+" },"
				+ " { IPv4 Not found :: " + ipv4NF +" }, { Error in IPv4 :: " + ipv4Error +" }, { IPv6 Not found :: " + ipv6NF +" }, { Error in IPv6 :: " + ipv6Error +" }, { Error Records :: " + errorRecords +" }]";
	}
	public static long getMemipPatternDuration() {
		return memipPatternDuration;
	}

	public static long getOtherUrlDuration() {
		return otherUrlDuration;
	}

	public static long getPatternNotFound() {
		return patternNotFound;
	}

	public static long getPgtype() {
		return pgtype;
	}

	public static long getRecordsProcessed() {
		return recordsProcessed;
	}

	public static long getRecordSuccess() {
		return recordSuccess;
	}

	public static long getResourceTypeDuration() {
		return resourceTypeDuration;
	}
	
	public static long getRobotDuration() {
		return robotDuration;
	}

	public static long getSkipRecords() {
		return skipRecords;
	}
	
	public static long getInvalidResourceType() {
		return invalidResourceType;
	}

	public static long getStatusCodeDuration() {
		return statusCodeDuration;
	}
	public static long getIdsSpilt() {
		return idsSpilt;
	}

	public static void updateIdsSpilt(long IdsSpilt) {
		FilterStats.idsSpilt += IdsSpilt;
	}
	public static void updateAccountStep1(long AccountStep1) {
		FilterStats.accountStep1 += AccountStep1;
	}

	public static void updateAccountStep3(long AccountStep3) {
		FilterStats.accountStep3 += AccountStep3;
	}
	
	public static void updateAccountStep4(long AccountStep4) {
		FilterStats.accountStep4 += AccountStep4;
	}
	
	public static void updateBlankIPAddress() {
		FilterStats.blankIPAddress ++;
	}
	
	public static void updateBlankUrl() {
		FilterStats.blankUrl++;
	}
	
	public static void updateErrorRecords() {
		FilterStats.errorRecords ++;
	}
	
	public static void updateInvalidOthers() {
		FilterStats.invalidOthers++;
	}
	
	public static void updateInvalidPageType() {
		FilterStats.invalidPageType++;
	}
	
	public static void updateInvalidStatusCode() {
		FilterStats.invalidStatusCode++;
	}
	
	public static void updateIpDeltaTime(long ipDeltaTime) {
		FilterStats.ipDeltaTime += ipDeltaTime;
	}
	
	public static void updateIpError() {
		FilterStats.ipError++;
	}
	
	public static void updateIpInvalid() {
		FilterStats.ipInvalid++;
	}

	public static void updateIpNotFound() {
		FilterStats.ipNotFound++;
	}

	public static void updateIpv4(long ipv4) {
		FilterStats.ipv4 += ipv4;
	}

	public static void updateIpv4Error() {
		FilterStats.ipv4Error ++;
	}

	public static void updateIpv4NF() {
		FilterStats.ipv4NF ++;
	}

	public static void updateIpv6(long ipv6) {
		FilterStats.ipv6 += ipv6;
	}

	public static void updateIpv6Error() {
		FilterStats.ipv6Error++;
	}

	public static void updateIpv6NF() {
		FilterStats.ipv6NF ++;
	}

	public static void updateMemipPatternDuration(long memipPatternDuration) {
		FilterStats.memipPatternDuration += memipPatternDuration;
	}

	public static void updateOtherUrlDuration(long otherUrlDuration) {
		FilterStats.otherUrlDuration += otherUrlDuration;
	}

	public static void updatePatternNotFound() {
		FilterStats.patternNotFound++;
	}

	public static void updatePgtype(long pgtype) {
		FilterStats.pgtype += pgtype;
	}

	public static void updateRecordsProcessed() {
		FilterStats.recordsProcessed++;
	}

	public static void updateRecordSuccess() {
		FilterStats.recordSuccess++;
	}

	public static void updateResourceTypeDuration(long resourceTypeDuration) {
		FilterStats.resourceTypeDuration += resourceTypeDuration;
	}

	public static void updateRobotDuration(long robotDuration) {
		FilterStats.robotDuration += robotDuration;
	}

	public static void updateSkipRecords() {
		FilterStats.skipRecords++;
	}

	public static void updateStatusCodeDuration(long statusCodeDuration) {
		FilterStats.statusCodeDuration += statusCodeDuration;
	}
	
	public static void updateInvalidResourceType(long invalidResourceType) {
		FilterStats.invalidResourceType += invalidResourceType;
	}
	

	public static void updateSkipRecordsPageViewIP() {
		FilterStats.skipRecordsPageViewIP++;
	}

	public static long getSkipRecordsPageViewIP() {
		return skipRecordsPageViewIP;
	}

	public static long getSkipRecordsResourceType() {
		return skipRecordsResourceType;
	}

	public static void updateSkipRecordsResourceType() {
		FilterStats.skipRecordsResourceType++;
	}


	public static String toStats() {
		StringBuilder builder = new StringBuilder();
		builder.append("FilterDocStats : ")
		.append("recordsProcessed=").append(recordsProcessed).append(" : ")
		.append("recordSuccess=").append(recordSuccess).append(" : ")
		.append("skipRecords=").append(skipRecords).append(" : ")
		.append("blankUrl=").append(blankUrl).append(" : ")
		.append("invalidPageType=").append(invalidPageType).append(" : ")
		.append("errorRecords=").append(errorRecords).append(" : ")
		.append("patternNotFound=").append(patternNotFound).append(" : ")
		.append("invalidOthers=").append(invalidOthers).append(" : ")
		.append("invalidStatusCode=").append(invalidStatusCode).append(" : ")
		.append("pgtype=").append(pgtype).append(" : ")
		.append("blankIPAddress=").append(blankIPAddress).append(" : ")
		.append("statusCodeDuration=").append(statusCodeDuration).append(" : ")
		.append("memipPatternDuration=").append(memipPatternDuration).append(" : ")
		.append("resourceTypeDuration=").append(resourceTypeDuration).append(" : ")
		.append("robotDuration=").append(robotDuration).append(" : ")
		.append("otherUrlDuration=").append(otherUrlDuration).append(" : ")
		.append("skipRecordsPageViewIP=").append(skipRecordsPageViewIP).append(" : ")
		.append("skipRecordsResourceType=").append(skipRecordsResourceType).append(" : ")
		//.append("ipv4Error=").append(ipv4Error).append(" : ")
		//.append("ipv4NF=").append(ipv4NF).append(" : ")
		//.append("ipv6Error=").append(ipv6Error).append(" : ")
		.append("idsSpilt=").append(idsSpilt).append(" : ")
		.append("ipDeltaTime=").append(ipDeltaTime).append(" : ")
		.append("ipNotFound=").append(ipNotFound).append(" : ")
		.append("ipInvalid=").append(ipInvalid).append(" : ")
		.append("ipError=").append(ipError).append(" : ")
		.append("");
		return builder.toString();
	}
	
	public static String getStats(){
		StringBuilder sbStats = new StringBuilder();	
		sbStats.append("Performance : Records=")
			.append(FilterStats.getRecordSuccess()).append(" sec : ")
			.append("Status Code=").append((FilterStats.getStatusCodeDuration() / 1000.0)).append(" sec : ") 
			.append("PageType=").append((FilterStats.getPgtype() / 1000.0)).append(" sec : ")
			.append("Resource type=").append((FilterStats.getResourceTypeDuration() / 1000.0)).append(" sec : ")
			.append("Exclude IP=").append((FilterStats.getMemipPatternDuration() / 1000.0)).append(" sec : ")
			.append("Robot=").append((FilterStats.getRobotDuration() / 1000.0)).append(" sec : ")
			.append("Other Url=").append((FilterStats.getOtherUrlDuration() / 1000.0)).append(" sec : ")
			.append("Acc Step 1=").append((FilterStats.getAccountStep1() / 1000.0)).append(" sec : ")
			.append("Acc Step 2(IP)=").append((FilterStats.getIpDeltaTime() / 1000.0)).append(" sec : ")
			.append("Acc Step 3=").append((FilterStats.getAccountStep3() / 1000.0)).append(" sec : ") 
			.append("Acc Step 4=").append((FilterStats.getAccountStep4() / 1000.0)).append(" sec : ")
		    .append("JournalIDs Step=").append((FilterStats.getIdsSpilt() / 1000.0))
			.append(" sec : ");
		return sbStats.toString();
	}
	
	
	public static void refreshTableStats() {
		recordsProcessed = 0;
		recordSuccess = 0;
		skipRecords = 0;
		blankUrl = 0;
		invalidPageType = 0;
		errorRecords=0;
		patternNotFound=0;
		invalidPageType=0;
		errorRecords=0;
		patternNotFound=0;
		invalidOthers=0;
		invalidStatusCode=0;
		pgtype=0;
		blankIPAddress=0;
		statusCodeDuration=0;
		memipPatternDuration=0;
		resourceTypeDuration=0;
		robotDuration=0;
		otherUrlDuration =0;
		skipRecordsPageViewIP=0;
		skipRecordsResourceType=0;
		idsSpilt=0;
		ipDeltaTime=0;
		ipNotFound=0;
		ipInvalid=0;
		ipError=0;
	}
	
}
