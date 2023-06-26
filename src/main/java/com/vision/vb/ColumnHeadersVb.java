package com.vision.vb;

import java.io.Serializable;

public class ColumnHeadersVb implements Serializable {

	private static final long serialVersionUID = 1L;
	private String reportId = "";
	private String sessionId = "";
	private int labelRowNum;
	private int labelColNum;
	private String caption = "";
	private String colType = "";
	//private Long columnWidth = 10l;
	private String columnWidth = "";
	private int colSpanNum;
	private int rowSpanNum;
	private String dbColumnName = "";
	private int numericColumnNo = 0;
	private int rowspan = 0;
	private int colspan = 0;
	
	private String subReportId = "";
	private Boolean drillDownLabel = false;
	private String columnXml = "";
	private String scaling = "";
	private String decimalCnt= "";
	//private Boolean groupingFlag = false;
	private String sumFlag = "Y";
	private String groupingFlag = "N";
	private String colorDiff = "N";
	
	
	public int getColSpanNum() {
		return colSpanNum;
	}
	public void setColSpanNum(int colSpanNum) {
		this.colSpanNum = colSpanNum;
	}
	public int getRowSpanNum() {
		return rowSpanNum;
	}
	public void setRowSpanNum(int rowSpanNum) {
		this.rowSpanNum = rowSpanNum;
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
	public int getLabelRowNum() {
		return labelRowNum;
	}
	public void setLabelRowNum(int labelRowNum) {
		this.labelRowNum = labelRowNum;
	}
	public int getLabelColNum() {
		return labelColNum;
	}
	public void setLabelColNum(int labelColNum) {
		this.labelColNum = labelColNum;
	}
	public String getCaption() {
		return caption;
	}
	public void setCaption(String caption) {
		this.caption = caption;
	}
	public String getColType() {
		return colType;
	}
	public void setColType(String colType) {
		this.colType = colType;
	} 
	public String getDbColumnName() {
		return dbColumnName;
	}
	public void setDbColumnName(String dbColumnName) {
		this.dbColumnName = dbColumnName;
	}
	public int getNumericColumnNo() {
		return numericColumnNo;
	}
	public void setNumericColumnNo(int numericColumnNo) {
		this.numericColumnNo = numericColumnNo;
	}
	public int getRowspan() {
		return rowspan;
	}
	public void setRowspan(int rowspan) {
		this.rowspan = rowspan;
	}
	public int getColspan() {
		return colspan;
	}
	public void setColspan(int colspan) {
		this.colspan = colspan;
	}
	public String getSubReportId() {
		return subReportId;
	}
	public void setSubReportId(String subReportId) {
		this.subReportId = subReportId;
	}
	public Boolean getDrillDownLabel() {
		return drillDownLabel;
	}
	public void setDrillDownLabel(Boolean drillDownLabel) {
		this.drillDownLabel = drillDownLabel;
	}
	public String getColumnXml() {
		return columnXml;
	}
	public void setColumnXml(String columnXml) {
		this.columnXml = columnXml;
	}
	public String getScaling() {
		return scaling;
	}
	public void setScaling(String scaling) {
		this.scaling = scaling;
	}
	public String getDecimalCnt() {
		return decimalCnt;
	}
	public void setDecimalCnt(String decimalCnt) {
		this.decimalCnt = decimalCnt;
	}
	public String getColumnWidth() {
		return columnWidth;
	}
	public void setColumnWidth(String columnWidth) {
		this.columnWidth = columnWidth;
	}
	public String getSumFlag() {
		return sumFlag;
	}
	public void setSumFlag(String sumFlag) {
		this.sumFlag = sumFlag;
	}
	public String getGroupingFlag() {
		return groupingFlag;
	}
	public void setGroupingFlag(String groupingFlag) {
		this.groupingFlag = groupingFlag;
	}
	public String getColorDiff() {
		return colorDiff;
	}
	public void setColorDiff(String colorDiff) {
		this.colorDiff = colorDiff;
	}
}
