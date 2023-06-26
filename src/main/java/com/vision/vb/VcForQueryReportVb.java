package com.vision.vb;

import java.util.List;

public class VcForQueryReportVb extends CommonVb {
	
	private String catalogId;
	private String userId;
	private String reportId;
	private String reportName;
	private String reportDescription;
	private String queryString;
	private String queryType = "D";
	private int vrdStatusNt = 0;
	private int vrdStatus = -1;
	private String vcqdQueryXml = "";
	private Integer baseTableId = null;
	private String hashVariableScript = "";
	private List<VcForQueryReportFieldsVb> reportFields = null;
	private String tableName="";
	
	private String select = "";
	private String where = "";
	private String groupBy = "";
	private String orderBy = "";
	
	private String distinctFlag = "";

	
	public String getQueryType() {
		return queryType;
	}
	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getReportName() {
		return reportName;
	}
	public void setReportName(String reportName) {
		this.reportName = reportName;
	}
	public String getQueryString() {
		return queryString;
	}
	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}
	public String getHashVariableScript() {
		return hashVariableScript;
	}
	public void setHashVariableScript(String hashVariableScript) {
		this.hashVariableScript = hashVariableScript;
	}
	public List<VcForQueryReportFieldsVb> getReportFields() {
		return reportFields;
	}
	public void setReportFields(List<VcForQueryReportFieldsVb> reportFields) {
		this.reportFields = reportFields;
	}
	public int getVrdStatusNt() {
		return vrdStatusNt;
	}
	public void setVrdStatusNt(int vrdStatusNt) {
		this.vrdStatusNt = vrdStatusNt;
	}
	public int getVrdStatus() {
		return vrdStatus;
	}
	public void setVrdStatus(int vrdStatus) {
		this.vrdStatus = vrdStatus;
	}
	
	public String getVcqdQueryXml() {
		return vcqdQueryXml;
	}
	public void setVcqdQueryXml(String vcqdQueryXml) {
		this.vcqdQueryXml = vcqdQueryXml;
	}
	public Integer getBaseTableId() {
		return baseTableId;
	}
	public void setBaseTableId(Integer baseTableId) {
		this.baseTableId = baseTableId;
	}
	public String getReportDescription() {
		return reportDescription;
	}
	public void setReportDescription(String reportDescription) {
		this.reportDescription = reportDescription;
	}
	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
	public String getSelect() {
		return select;
	}
	public void setSelect(String select) {
		this.select = select;
	}
	public String getWhere() {
		return where;
	}
	public void setWhere(String where) {
		this.where = where;
	}
	public String getGroupBy() {
		return groupBy;
	}
	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}
	public String getOrderBy() {
		return orderBy;
	}
	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}
	public String getReportId() {
		return reportId;
	}
	public void setReportId(String reportId) {
		this.reportId = reportId;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getDistinctFlag() {
		return distinctFlag;
	}
	public void setDistinctFlag(String distinctFlag) {
		this.distinctFlag = distinctFlag;
	}

}