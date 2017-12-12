package com.mps.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mps.utils.MyException;
import com.mps.utils.MyLogger;

public class MySqlConnection{

	//parameterised costructor  - parametrs will be passed by user class
	public Connection getConnection(String driver, String url, String user ,String password) throws Exception {
        Connection sqlConnection = null;
        try {
            //check for null;
            if(driver == null){throw new SQLException("NULL driver");}else{driver =  driver.trim();}
            if(url == null){throw new SQLException("NULL url");}else{url =  url.trim();}
            if(user == null){throw new SQLException("NULL user");}else{user =  user.trim();}
            if(password == null){throw new SQLException("NULL password");}else{}
            
            //validating DB_Driver
            if (driver.equalsIgnoreCase("")) throw new Exception("BLANK driver : " + driver);
            //validating DB_URL
            if (url.equalsIgnoreCase("")) throw new Exception("BLANK url : " + url);
            //validating DB_USER            
            if (user.equalsIgnoreCase("")) throw new Exception("BLANK user : " + user);
            //validating Password			

            Class.forName(driver.trim());
            DriverManager.setLoginTimeout(30);  //sec
            sqlConnection = DriverManager.getConnection(url.trim(), user.trim(), password);
            
            return sqlConnection;
        }catch (Exception exp){
        	throw new MyException(" : " + exp.toString());
        }	
	}

	public static Connection getMariaDBConnection(String driver, String url, String user ,String password) throws Exception {
        Connection sqlConnection = null;
        try {
        	
        	sqlConnection = DriverManager.getConnection("jdbc:mariadb://localhost:3306/report_master?user=root&password=root");
        	
        	if(true){
        		return sqlConnection;
        	}
        	
            //check for null;
            if(driver == null){throw new SQLException("NULL driver");}else{driver =  driver.trim();}
            if(url == null){throw new SQLException("NULL url");}else{url =  url.trim();}
            if(user == null){throw new SQLException("NULL user");}else{user =  user.trim();}
            if(password == null){throw new SQLException("NULL password");}else{}
            
            //validating DB_Driver
            if (driver.equalsIgnoreCase("")) throw new Exception("BLANK driver : " + driver);
            //validating DB_URL
            if (url.equalsIgnoreCase("")) throw new Exception("BLANK url : " + url);
            //validating DB_USER            
            if (user.equalsIgnoreCase("")) throw new Exception("BLANK user : " + user);
            //validating Password			

            Class.forName(driver.trim());
            DriverManager.setLoginTimeout(30);  //sec
            sqlConnection = DriverManager.getConnection(url.trim(), user.trim(), password);
            
            return sqlConnection;
        }catch (Exception exp){
        	throw new MyException(" : " + exp.toString());
        }	
	}

	
	//method to check table if not exists make copy of template table
    public static boolean checkAndCreateTable(final String tableToCheck , final String templateTable, Connection sqlConnection) throws Exception{
        boolean bRetVal = false;
        Statement smt = null;
        ResultSet rs = null;
        String checkQuery = "";
        String createQuery = "";
        try {
        	checkQuery = "select 1 from " + tableToCheck.trim();
            createQuery = "CREATE TABLE '" + tableToCheck.trim() + "' LIKE '" + templateTable.trim() + "'";
            
            smt =  sqlConnection.createStatement();
            rs =  smt.executeQuery(checkQuery);
            if(rs != null){
                bRetVal = smt.execute(createQuery);
            }else{
                bRetVal = true;
            }
        } catch (Exception e) {
        	MyLogger.exception("checkAndCreateTable : " + checkQuery + " : " + createQuery + " : " + e.toString());
        	bRetVal = smt.execute(createQuery);
        }
        return bRetVal;
    }
	
}
