package com.mps.start;

import com.mps.utils.MyLogger;

public class AppLauncher {

	//
	public static void main(String[] args) throws Exception{
		
		try {
			new AppLauncher().run(args);
		} catch (Exception e) {
			throw e;
		}
	}
	
	
	//
	private void run(String[] args) throws Exception{
		String processor = "";
		try {
			MyLogger.log("Launching Application....");
			
			if(args == null && args.length <= 0){
				throw new Exception("Invalid/NULL Argument for Processor : " + args.toString());
			}else{
				processor = args[0].trim().toUpperCase();
			}
			
			MyLogger.log("Processor Invoked : " + processor);
			//
			if(processor.equalsIgnoreCase("L2")) {
				new StartFilterProcessor();
			}else if(processor.equalsIgnoreCase("L3")){
				MyLogger.log("AppLauncher : Service started : " + processor);
				new StartCounterProcessor();
			}else{
				throw new Exception("Invalid Argument for Processor : " + processor);
			}

		} catch (Exception e) {
			throw e;
		}
		
	}
	
	
}
