package com.mps.backup;

import java.io.Serializable;

public class PageTypePatterns implements Serializable{
	private static final long serialVersionUID = 1L;
    private String page_type_pattern;
    private int page_type_id;
	public String getPage_type_pattern() {
		return page_type_pattern;
	}
	public void setPage_type_pattern(String page_type_pattern) {
		this.page_type_pattern = page_type_pattern;
	}
	public int getPage_type_id() {
		return page_type_id;
	}
	public void setPage_type_id(int page_type_id) {
		this.page_type_id = page_type_id;
	}
}
