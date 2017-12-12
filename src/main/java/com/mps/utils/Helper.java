package com.mps.utils;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Helper implements Serializable {
	
	private static final long serialVersionUID = 1L;

	public static String getCurrentDateTime() throws ParseException {
		return new SimpleDateFormat("yyyy-M-dd hh:mm:ss").format(new Date());  
	}
}
