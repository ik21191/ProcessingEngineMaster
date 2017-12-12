package com.mps.utils;

public class MyLogger {
	private static Logger logger = new Logger();
	
	//disabling class object initialization
	private MyLogger(){}
	
    //simple log
    public static boolean log(String message){
    	return logger.writeLog(message);
    }
    //ERROR level log
    public static boolean error(String message){
        return logger.error(message);
    }
    //EXCEPTION level log
    public static boolean exception(String message) {
        return logger.exception(message);
    }
    
    //############ GETTER & SETTERS ######################
    public static String getLogFolderPath() {
        return logger.getLogFolderPath();
    }

    public static void setLogFolderPath(String logFolderPath) {
    	logger.setLogFolderPath(logFolderPath);
    }

    public static String getUser() {
        return logger.getUser();
    }

    public static void setUser(String user) {
    	logger.setUser(user);
    }

    
    
}
