package com.vision.vb;

import java.io.Serializable;

public class TemplateStgVb implements Serializable{

	private static final long serialVersionUID = 1L;
	private String reportId = "";
	private String sessionId = ""; 
	private String templateId =  "";
	private String tabId =  "";
	private String rowId =  "";
	private String colId =  "";
	private String cellData =  "";
	private String colType =  "";
	private String sortField = "";
	private String createNew = "";
	private String tagId = "";
	private String contextRef = ""; 
	private String unitRef = "";
	private String contextPrefix = "";
	private String templateName =  "";
	
	public String getReportId() {
		return reportId;
	}
	public void setReportId(String reportId) {
		this.reportId = reportId;
	}
	
	public String getTemplateId() {
		return templateId;
	}
	public void setTemplateId(String templateId) {
		this.templateId = templateId;
	}
	public String getTabId() {
		return tabId;
	}
	public void setTabId(String tabId) {
		this.tabId = tabId;
	}
	public String getRowId() {
		return rowId;
	}
	public void setRowId(String rowId) {
		this.rowId = rowId;
	}
	public String getColId() {
		return colId;
	}
	public void setColId(String colId) {
		this.colId = colId;
	}
	public String getCellData() {
		return cellData;
	}
	public void setCellData(String cellData) {
		this.cellData = cellData;
	}
	public String getColType() {
		return colType;
	}
	public void setColType(String colType) {
		this.colType = colType;
	}
	public String getSortField() {
		return sortField;
	}
	public void setSortField(String sortField) {
		this.sortField = sortField;
	}
	public String getCreateNew() {
		return createNew;
	}
	public void setCreateNew(String createNew) {
		this.createNew = createNew;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getTagId() {
		return tagId;
	}
	public void setTagId(String tagId) {
		this.tagId = tagId;
	}
	public String getContextRef() {
		return contextRef;
	}
	public void setContextRef(String contextRef) {
		this.contextRef = contextRef;
	}
	public String getUnitRef() {
		return unitRef;
	}
	public void setUnitRef(String unitRef) {
		this.unitRef = unitRef;
	}
	public String getContextPrefix() {
		return contextPrefix;
	}
	public void setContextPrefix(String contextPrefix) {
		this.contextPrefix = contextPrefix;
	}
	public String getTemplateName() {
		return templateName;
	}
	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}	
	
}
