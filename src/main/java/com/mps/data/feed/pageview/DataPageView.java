package com.mps.data.feed.pageview;

import java.math.BigInteger;
import java.util.regex.Pattern;

public class DataPageView {
	  private int id;
	  private int type;
	  private String reg_exp;
	  private Pattern reg_exp_pattern;
	  private String webmart_code;
	  private String type_flag;
	  private BigInteger low_ip;
	  private BigInteger high_ip;
	  private int in_out_flag;
	  private String targetField;
	  private String operation;
	  private String task;
	  
	public String getTargetField() {
		return targetField;
	}
	public void setTargetField(String targetField) {
		this.targetField = targetField;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getTask() {
		return task;
	}
	public void setTask(String task) {
		this.task = task;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public String getReg_exp() {
		return reg_exp;
	}
	public void setReg_exp(String reg_exp) {
		this.reg_exp = reg_exp;
	}
	public Pattern getReg_exp_pattern() {
		return reg_exp_pattern;
	}
	public void setReg_exp_pattern(Pattern reg_exp_pattern) {
		this.reg_exp_pattern = reg_exp_pattern;
	}
	public String getWebmart_code() {
		return webmart_code;
	}
	public void setWebmart_code(String webmart_code) {
		this.webmart_code = webmart_code;
	}
	public String getType_flag() {
		return type_flag;
	}
	public void setType_flag(String type_flag) {
		this.type_flag = type_flag;
	}
	public BigInteger getLow_ip() {
		return low_ip;
	}
	public void setLow_ip(BigInteger low_ip) {
		this.low_ip = low_ip;
	}
	public BigInteger getHigh_ip() {
		return high_ip;
	}
	public void setHigh_ip(BigInteger high_ip) {
		this.high_ip = high_ip;
	}
	public int getIn_out_flag() {
		return in_out_flag;
	}
	public void setIn_out_flag(int in_out_flag) {
		this.in_out_flag = in_out_flag;
	}
	public DataPageView(String reg_exp, String webmart_code){
		this.reg_exp = reg_exp;
		this.webmart_code = webmart_code;
	}
	public DataPageView(){
		
	}
	 
	  
}
