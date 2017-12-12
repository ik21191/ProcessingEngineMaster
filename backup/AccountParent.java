package com.mps.backup;

public class AccountParent {
	private String institution_id;
	private String parent_id;
	public String getInstitution_id() {
		return institution_id;
	}
	public void setInstitution_id(String institution_id) {
		this.institution_id = institution_id;
	}
	public String getParent_id() {
		return parent_id;
	}
	public void setParent_id(String parent_id) {
		this.parent_id = parent_id;
	}
	 public AccountParent(String institution_id, String parent_id)
	    {
	        this.institution_id = institution_id;
	        this.parent_id = parent_id;
	    }
 
}
