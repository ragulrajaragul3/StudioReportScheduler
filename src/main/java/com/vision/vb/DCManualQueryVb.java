package com.vision.vb;

import java.util.List;

public class DCManualQueryVb extends CommonVb{

	private String queryId = "";
	private int databaseTypeAt = 1082;
	private String databaseType = "M_QUERY";
	private String databaseConnectivityDetails = "";
	private int lookupDataLoadingAt = 1088;
	private String lookupDataLoading = "";
	private String sqlQuery = "";
	private String stgQuery1 = "";
	private String stgQuery2 = "";
	private String stgQuery3 = "";
	private String postQuery = "";
	private int vcqStatusNt = 1;
	private int vcqStatus = -1;
	private String queryDescription = "";
	private String queryValidFlag = "F";
	private String queryColumnXML = "";
	private String hashVariableScript = "";
	private Integer queryType;
	public List<SmartSearchVb> smartSearchVb=null;

	public Integer getQueryType() {
		return queryType;
	}
	public void setQueryType(Integer queryType) {
		this.queryType = queryType;
	}
	public String getQueryColumnXML() {
		return queryColumnXML;
	}
	public void setQueryColumnXML(String queryColumnXML) {
		this.queryColumnXML = queryColumnXML;
	}
	public String getQueryValidFlag() {
		return queryValidFlag;
	}
	public void setQueryValidFlag(String queryValidFlag) {
		this.queryValidFlag = queryValidFlag;
	}
	public String getQueryId() {
		return queryId;
	}
	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}
	public int getDatabaseTypeAt() {
		return databaseTypeAt;
	}
	public void setDatabaseTypeAt(int databaseTypeAt) {
		this.databaseTypeAt = databaseTypeAt;
	}
	public String getDatabaseType() {
		return databaseType;
	}
	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}
	public String getDatabaseConnectivityDetails() {
		return databaseConnectivityDetails;
	}
	public void setDatabaseConnectivityDetails(String databaseConnectivityDetails) {
		this.databaseConnectivityDetails = databaseConnectivityDetails;
	}
	public int getLookupDataLoadingAt() {
		return lookupDataLoadingAt;
	}
	public void setLookupDataLoadingAt(int lookupDataLoadingAt) {
		this.lookupDataLoadingAt = lookupDataLoadingAt;
	}
	public String getLookupDataLoading() {
		return lookupDataLoading;
	}
	public void setLookupDataLoading(String lookupDataLoading) {
		this.lookupDataLoading = lookupDataLoading;
	}
	public String getSqlQuery() {
		return sqlQuery;
	}
	public void setSqlQuery(String sqlQuery) {
//		this.sqlQuery = URLDecoder.decode(sqlQuery);
		this.sqlQuery = sqlQuery;
	}
	public String getStgQuery1() {
		return stgQuery1;
	}
	public void setStgQuery1(String stgQuery1) {
//		this.stgQuery1 = URLDecoder.decode(stgQuery1);
		this.stgQuery1 = stgQuery1;
	}
	public String getStgQuery2() {
		return stgQuery2;
	}
	public void setStgQuery2(String stgQuery2) {
//		this.stgQuery2 = URLDecoder.decode(stgQuery2);
		this.stgQuery2 =stgQuery2;
	}
	public String getStgQuery3() {
		return stgQuery3;
	}
	public void setStgQuery3(String stgQuery3) {
//		this.stgQuery3 = URLDecoder.decode(stgQuery3);
		this.stgQuery3 = stgQuery3;
	}
	public String getPostQuery() {
		return postQuery;
	}
	public void setPostQuery(String postQuery) {
//		this.postQuery = URLDecoder.decode(postQuery);
		this.postQuery = postQuery;
	}
	public int getVcqStatusNt() {
		return vcqStatusNt;
	}
	public void setVcqStatusNt(int vcqStatusNt) {
		this.vcqStatusNt = vcqStatusNt;
	}
	public int getVcqStatus() {
		return vcqStatus;
	}
	public void setVcqStatus(int vcqStatus) {
		this.vcqStatus = vcqStatus;
	}
	public String getQueryDescription() {
		return queryDescription;
	}
	public void setQueryDescription(String queryDescription) {
		this.queryDescription = queryDescription;
	}
	public String getHashVariableScript() {
		return hashVariableScript;
	}
	public void setHashVariableScript(String hashVariableScript) {
		this.hashVariableScript = hashVariableScript;
	}
	public List<SmartSearchVb> getSmartSearchVb() {
		return smartSearchVb;
	}
	public void setSmartSearchVb(List<SmartSearchVb> smartSearchVb) {
		this.smartSearchVb = smartSearchVb;
	}
}