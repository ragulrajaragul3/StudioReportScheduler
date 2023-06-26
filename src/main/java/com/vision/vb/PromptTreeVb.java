package com.vision.vb;

import java.io.Serializable;
import java.util.List;

public class PromptTreeVb implements Serializable{

	
	private static final long serialVersionUID = 1L;
	private String reportId = "";
	private String sessionId = "";
	private String field1 = "";
	private String field2 = "";
	private String field3 = "";
	private String field4 = "";
	private String field5 = "";
	private String field6 = "";
	private String status = "";
	private String errorMessage = "";
	private String filterString = "";
	private boolean addedToTree = false;
	private String promptId = "";
	private String tableName = "";
	private List<PromptTreeVb> children;
	private long totalRows = 0l;
	private int maxCatalogDisplayCount = 100;
	private String columnHeaderTable = "";
	
	private String cascade1 = ""; 
	private String cascade2 = "";
	private String cascade3 = "";
	private String cascade4 = "";
	private String cascade5 = "";
	private String cascade6 = "";
	
	public String getCascade1() {
		return cascade1;
	}
	public void setCascade1(String cascade1) {
		this.cascade1 = cascade1;
	}
	public String getCascade2() {
		return cascade2;
	}
	public void setCascade2(String cascade2) {
		this.cascade2 = cascade2;
	}
	public String getCascade3() {
		return cascade3;
	}
	public void setCascade3(String cascade3) {
		this.cascade3 = cascade3;
	}
	public String getCascade4() {
		return cascade4;
	}
	public void setCascade4(String cascade4) {
		this.cascade4 = cascade4;
	}
	public String getCascade5() {
		return cascade5;
	}
	public void setCascade5(String cascade5) {
		this.cascade5 = cascade5;
	}
	public String getCascade6() {
		return cascade6;
	}
	public void setCascade6(String cascade6) {
		this.cascade6 = cascade6;
	}
	public long getTotalRows() {
		return totalRows;
	}
	public void setTotalRows(long totalRows) {
		this.totalRows = totalRows;
	}
	public String getReportId() {
		return reportId;
	}
	public void setReportId(String reportId) {
		this.reportId = reportId;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getField1() {
		return field1;
	}
	public void setField1(String field1) {
		this.field1 = field1;
	}
	public String getField2() {
		return field2;
	}
	public void setField2(String field2) {
		this.field2 = field2;
	}
	public String getField3() {
		return field3;
	}
	public void setField3(String field3) {
		this.field3 = field3;
	}
	public String getField4() {
		return field4;
	}
	public void setField4(String field4) {
		this.field4 = field4;
	}
	public String getField5() {
		return field5;
	}
	public void setField5(String field5) {
		this.field5 = field5;
	}
	public String getField6() {
		return field6;
	}
	public void setField6(String field6) {
		this.field6 = field6;
	}
	public List<PromptTreeVb> getChildren() {
		return children;
	}
	public void setChildren(List<PromptTreeVb> children) {
		this.children = children;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	public String getFilterString() {
		return filterString;
	}
	public void setFilterString(String filterString) {
		this.filterString = filterString;
	}
	public boolean isAddedToTree() {
		return addedToTree;
	}
	public void setAddedToTree(boolean addedToTree) {
		this.addedToTree = addedToTree;
	}
	public String getPromptId() {
		return promptId;
	}
	public void setPromptId(String promptId) {
		this.promptId = promptId;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public int getMaxCatalogDisplayCount() {
		return maxCatalogDisplayCount;
	}
	public void setMaxCatalogDisplayCount(int maxCatalogDisplayCount) {
		this.maxCatalogDisplayCount = maxCatalogDisplayCount;
	}
	public String getColumnHeaderTable() {
		return columnHeaderTable;
	}
	public void setColumnHeaderTable(String columnHeaderTable) {
		this.columnHeaderTable = columnHeaderTable;
	}
	
}
