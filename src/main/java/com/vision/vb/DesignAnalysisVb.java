package com.vision.vb;

import java.util.List;

public class DesignAnalysisVb extends CommonVb {

	private static final long serialVersionUID = -5022264255321929212L;

	/* Vcqd_queries and Access */
	private String catalogId;
	private String vcqdQueryId;
	private String vcqdQueryDesc;
	private String vcqdQuery;
	private String queryType;
	private String vcqdQueryXml;
	private int vcqdStatus;
	private String vcqdStatusDesc;
	private int vcqdStatusNt;
	private String hashVariableScript;
	public List<SmartSearchVb> smartSearchOpt=null;

	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
	public String getVcqdQueryId() {
		return vcqdQueryId;
	}
	public void setVcqdQueryId(String vcqdQueryId) {
		this.vcqdQueryId = vcqdQueryId;
	}
	public String getVcqdQueryDesc() {
		return vcqdQueryDesc;
	}
	public void setVcqdQueryDesc(String vcqdQueryDesc) {
		this.vcqdQueryDesc = vcqdQueryDesc;
	}
	public String getVcqdQuery() {
		return vcqdQuery;
	}
	public void setVcqdQuery(String vcqdQuery) {
		this.vcqdQuery = vcqdQuery;
	}
	public String getQueryType() {
		return queryType;
	}
	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}
	public String getVcqdQueryXml() {
		return vcqdQueryXml;
	}
	public void setVcqdQueryXml(String vcqdQueryXml) {
		this.vcqdQueryXml = vcqdQueryXml;
	}
	public int getVcqdStatus() {
		return vcqdStatus;
	}
	public void setVcqdStatus(int vcqdStatus) {
		this.vcqdStatus = vcqdStatus;
	}
	public String getVcqdStatusDesc() {
		return vcqdStatusDesc;
	}
	public void setVcqdStatusDesc(String vcqdStatusDesc) {
		this.vcqdStatusDesc = vcqdStatusDesc;
	}
	public int getVcqdStatusNt() {
		return vcqdStatusNt;
	}
	public void setVcqdStatusNt(int vcqdStatusNt) {
		this.vcqdStatusNt = vcqdStatusNt;
	}
	public String getHashVariableScript() {
		return hashVariableScript;
	}
	public void setHashVariableScript(String hashVariableScript) {
		this.hashVariableScript = hashVariableScript;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}
}
