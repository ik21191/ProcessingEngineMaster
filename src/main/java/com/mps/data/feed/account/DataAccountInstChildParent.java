package com.mps.data.feed.account;

public class DataAccountInstChildParent {
	public final String institutionId;
	public final String parentId;
	
	public DataAccountInstChildParent(String institutionId, String parentId){
		this.institutionId = institutionId;
		this.parentId = parentId;
	}
	
	@Override
    public String toString() {
        return "AccountData{" + "institutionId=" + institutionId + ", parentId=" + parentId + "}";
    }
}
