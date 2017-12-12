package com.mps.logs.counter;
import java.io.Serializable;
import java.util.Comparator;
import org.bson.Document;
import com.mps.utils.Constants;

public class DocumentComparator implements Comparator<Document>, Serializable {
	
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Document doc1, Document doc2) {
		String longIpAddress1 = (String)doc1.getOrDefault(Constants.LONG_IP_ADDRESS, "");
		String longIpAddress2 = (String)doc2.getOrDefault(Constants.LONG_IP_ADDRESS, "");
		int ipComarare = longIpAddress1.compareTo(longIpAddress2);
		//check if ip is same
		if(ipComarare == 0) {
			//if ip is same then check for url
			String url1 = (String)doc1.getOrDefault(Constants.URL, "");
			String url2 = (String)doc2.getOrDefault(Constants.URL, "");
			int urlCompare = url1.compareTo(url2);
			if(urlCompare == 0) {
				//If url is same then compare requestDateTime
				String requestDateTime1 = (String)doc1.getOrDefault(Constants.REQUEST_DATE_TIME, "");
				String requestDateTime2 = (String)doc2.getOrDefault(Constants.REQUEST_DATE_TIME, "");
				return requestDateTime1.compareTo(requestDateTime2);
			} else {
				return urlCompare;
			}
		} else {
			return ipComarare;
		}
	}
}