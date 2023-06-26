package com.vision.vb;

import java.util.ArrayList;
import java.util.List;

public class ExportVb extends CommonVb{
	
	private String dashboardId = "";
	private String dashboardName = "";
	private String tabId = "";
	private String tabName = "";
	private String exportFileName = "";
	private Boolean gridAvail = false;
	private String promptValue1 ="";
	private String promptValue2 ="";
	private String promptValue3 ="";
	private String promptValue4 ="";
	private String promptValue5 ="";
	private String promptValue6 ="";
	private String promptValue7 ="";
	private String promptValue8 ="";
	private String promptValue9 ="";
	private String promptValue10 ="";
	List gridDataSet = null;
	List<ColumnHeadersVb> columnHeaderslst = null;
	private List<ExportVb> exportTabDetailsLst = new ArrayList<>(); 
	private String applicationTheme = "";
	private String promptLabel = "";
	public String getDashboardId() {
		return dashboardId;
	}
	public void setDashboardId(String dashboardId) {
		this.dashboardId = dashboardId;
	}
	public String getDashboardName() {
		return dashboardName;
	}
	public void setDashboardName(String dashboardName) {
		this.dashboardName = dashboardName;
	}
	public String getTabId() {
		return tabId;
	}
	public void setTabId(String tabId) {
		this.tabId = tabId;
	}
	public String getTabName() {
		return tabName;
	}
	public void setTabName(String tabName) {
		this.tabName = tabName;
	}
	public String getExportFileName() {
		return exportFileName;
	}
	public void setExportFileName(String exportFileName) {
		this.exportFileName = exportFileName;
	}
	public Boolean getGridAvail() {
		return gridAvail;
	}
	public void setGridAvail(Boolean gridAvail) {
		this.gridAvail = gridAvail;
	}
	public String getPromptValue1() {
		return promptValue1;
	}
	public void setPromptValue1(String promptValue1) {
		this.promptValue1 = promptValue1;
	}
	public String getPromptValue2() {
		return promptValue2;
	}
	public void setPromptValue2(String promptValue2) {
		this.promptValue2 = promptValue2;
	}
	public String getPromptValue3() {
		return promptValue3;
	}
	public void setPromptValue3(String promptValue3) {
		this.promptValue3 = promptValue3;
	}
	public String getPromptValue4() {
		return promptValue4;
	}
	public void setPromptValue4(String promptValue4) {
		this.promptValue4 = promptValue4;
	}
	public String getPromptValue5() {
		return promptValue5;
	}
	public void setPromptValue5(String promptValue5) {
		this.promptValue5 = promptValue5;
	}
	public String getPromptValue6() {
		return promptValue6;
	}
	public void setPromptValue6(String promptValue6) {
		this.promptValue6 = promptValue6;
	}
	public String getPromptValue7() {
		return promptValue7;
	}
	public void setPromptValue7(String promptValue7) {
		this.promptValue7 = promptValue7;
	}
	public String getPromptValue8() {
		return promptValue8;
	}
	public void setPromptValue8(String promptValue8) {
		this.promptValue8 = promptValue8;
	}
	public String getPromptValue9() {
		return promptValue9;
	}
	public void setPromptValue9(String promptValue9) {
		this.promptValue9 = promptValue9;
	}
	public String getPromptValue10() {
		return promptValue10;
	}
	public void setPromptValue10(String promptValue10) {
		this.promptValue10 = promptValue10;
	}
	public List getGridDataSet() {
		return gridDataSet;
	}
	public void setGridDataSet(List gridDataSet) {
		this.gridDataSet = gridDataSet;
	}
	public List<ColumnHeadersVb> getColumnHeaderslst() {
		return columnHeaderslst;
	}
	public void setColumnHeaderslst(List<ColumnHeadersVb> columnHeaderslst) {
		this.columnHeaderslst = columnHeaderslst;
	}
	public List<ExportVb> getExportTabDetailsLst() {
		return exportTabDetailsLst;
	}
	public void setExportTabDetailsLst(List<ExportVb> exportTabDetailsLst) {
		this.exportTabDetailsLst = exportTabDetailsLst;
	}
	public String getApplicationTheme() {
		return applicationTheme;
	}
	public void setApplicationTheme(String applicationTheme) {
		this.applicationTheme = applicationTheme;
	}
	public String getPromptLabel() {
		return promptLabel;
	}
	public void setPromptLabel(String promptLabel) {
		this.promptLabel = promptLabel;
	}

}
