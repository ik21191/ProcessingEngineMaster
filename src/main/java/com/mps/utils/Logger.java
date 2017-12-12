package com.mps.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {
	
	private final SimpleDateFormat formatterDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final SimpleDateFormat formatterDate = new SimpleDateFormat("yyyyMMdd");
    private static final String LOG = "";
    private static final String ERROR = "ERROR";    
    private static final String EXCEPTION = "EXCEPTION";
    
    private boolean enablePrintln = true;
    private String user = "";
    private String logFolderPath = "";
    
    private String logType = "";
    private String rootPath = "";
    private long lastLongTS = 0L;
    
    //
    public Logger(){
    	initialize();
    }
    
    //
    public Logger(String user){
    	this.user = user;
    	initialize();
    }
    
    //method to initialize 
    private void initialize(){

        try {
            if(this.user == null) {
            	this.user = "";
            } else {
            	this.user = this.user.trim();
            }
            
            rootPath = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()).getParent();
            rootPath = URLDecoder.decode(rootPath, "UTF-8");
            //getting root class path
            this.logFolderPath = rootPath + File.separator + "logs";

        } catch (Exception e) {
        	//what to do
        }
    }

    //simple log
    public boolean log(String message) {
        logType = LOG;
        return writeLog(message);
    }
    
    //ERROR level log
    public boolean error(String message) {
        logType = ERROR;
        return writeLog(message);
    }
    
    //EXCEPTION level log
    public boolean exception(String message) {
        logType = EXCEPTION;
        return writeLog(message);
    }

    //method to write log
    public boolean writeLog(String message) {
    	String logFilepath = null;
    	String logFileName = null;
        try{
        	
    		if(this.logFolderPath == null || this.logFolderPath.trim().equalsIgnoreCase("")){
                this.logFolderPath = this.rootPath + File.separator + "logs";
        	}
        	
        	//check for log User
            if (this.user.trim().equals("")) {
                logFileName = this.formatterDate.format(new Date()) + ".log";
            } else {
                logFileName = this.user + "_" + this.formatterDate.format(new Date()) + ".log";
            }
            
            logFilepath = this.logFolderPath + File.separator + logFileName;
            return writeLog(logType + " : " + message, logFilepath, false);

        } catch (Exception e){
            return false;
        }
    }

    //method to write log
    public boolean writeLog(String message, String logFilePath, boolean deleteExistingLogFile) {
    	BufferedWriter bufferedWriter = null;
        FileWriter fileWriter = null;
        long currentLongTS = new Date().getTime();
        
        try{
            //check for existing file
            File f = new File(logFilePath.trim());
            if (f.exists() && deleteExistingLogFile) {
                try{
                    f.delete();
                }catch (Exception e){
                	//what to do
                }
            }

            //making directory if not exists
            File dirPath = new File(f.getParent());
            if (!dirPath.exists()){
            	dirPath.mkdirs();
            }

            //setting last long Time Stamp
            String datePrefix = this.formatterDateTime.format(new Date(currentLongTS));
            double deltaTS = (currentLongTS - lastLongTS)/1000.0;
            if(this.lastLongTS == 0){deltaTS = 0.0;}
            
            //adding time stamp on message text
            if (this.enablePrintln) {
                System.out.println(datePrefix + " : " + deltaTS + " sec : " + user + " : " + message);
            }
            //writing to file
            fileWriter = new FileWriter(logFilePath, true);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(datePrefix + " : " + deltaTS + " sec : " + user + " : " + message);
            bufferedWriter.newLine();

            return true;
        } catch (Exception exp) {
            System.out.println(this.formatterDateTime.format(new Date().toString()) + " : EXCEPTION writeLog : " + message + " : " + logFilePath + " : " + deleteExistingLogFile + " : " + exp.toString());
            exp.printStackTrace();
            return false;
        } finally {
            try {
            	this.lastLongTS = currentLongTS;
            	if(bufferedWriter != null)bufferedWriter.close();
            	if(fileWriter != null) fileWriter.close();
            } catch (Exception e) {}
            this.logType = "";
        }
    }
    
    
    ///########## GETTER & SETTER ##############
	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the logFolderPath
	 */
	public String getLogFolderPath() {
		return logFolderPath;
	}

	/**
	 * @param logFolderPath the logFolderPath to set
	 */
	public void setLogFolderPath(String logFolderPath) {
		this.logFolderPath = logFolderPath;
	}

}
