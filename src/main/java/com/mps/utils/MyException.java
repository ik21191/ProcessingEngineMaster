package com.mps.utils;

public class MyException extends Exception{
	public MyException(String message){
		super(message);
		MyLogger.exception(message);
	}
}
