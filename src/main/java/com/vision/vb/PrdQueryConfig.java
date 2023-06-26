package com.vision.vb;

public class PrdQueryConfig {
	private static final long serialVersionUID = 1L;
	private String dataRefId="";
	private String queryProc="";
	private String dataRefType="";
	private String sortField = "";
	private String dbConnectionName = "";
	
	public String getDataRefId() {
		return dataRefId;
	}
	public void setDataRefId(String dataRefId) {
		this.dataRefId = dataRefId;
	}
	public String getQueryProc() {
		return queryProc;
	}
	public void setQueryProc(String queryProc) {
		this.queryProc = queryProc;
	}
	public String getDataRefType() {
		return dataRefType;
	}
	public void setDataRefType(String dataRefType) {
		this.dataRefType = dataRefType;
	}
	public String getSortField() {
		return sortField;
	}
	public void setSortField(String sortField) {
		this.sortField = sortField;
	}
	public String getDbConnectionName() {
		return dbConnectionName;
	}
	public void setDbConnectionName(String dbConnectionName) {
		this.dbConnectionName = dbConnectionName;
	}
}
