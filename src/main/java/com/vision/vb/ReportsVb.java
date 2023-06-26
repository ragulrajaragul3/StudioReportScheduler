package com.vision.vb;

import java.util.ArrayList;
import java.util.List;

public class ReportsVb extends CommonVb {
	private static final long serialVersionUID = 1L;
	
	private String applicationId = "";
	private String applicationIdDesc = "";
	private String reportCategory = "";
	private String categoryDesc = "";
	private String subCategory = "";
	private String subCategoryDesc = "";
	private String displayType = "";
	private String reportId = "";
	private String reportTitle = "";
	private String filterFlag = "";
	private String filterRefCode = "";
	private String applyUserRestrct = "";
	
	private String subReportId = "";
	private String dataRefId = "";
	private String fetchFlag = "";
	private String ddFlag = "";
	private String currentLevel = "";
	private String nextLevel = "";
	private String chartData = "";
	
	List gridDataSet = null;
	List<ColumnHeadersVb> columnHeaderslst = null;
	
	private String promptValue1 = "";
	private String promptValue2 = ""; 
	private String promptValue3 =  "";
	private String promptValue4 =  "";
	private String promptValue5 =  "";
	private String promptValue6 =  "";
	private String promptValue7 =  "";
	private String promptValue8 =  "";
	private String promptValue9 = "";
	private String promptValue10 = "";
	
	List<ReportsVb> reportslst = null;
	private List<ReportsVb> children;
	
	private int maxHeaderRow = 0;
	private int maxColumn = 0;
	private String formatType = "";
	
	private String drillDownKey1 = "";
	private String drillDownKey2 = "";
	private String drillDownKey3 = "";
	private String drillDownKey4 = "";
	private String drillDownKey5 = "";
	private String drillDownKey6 = "";
	private String drillDownKey7 = "";
	private String drillDownKey8 = "";
	private String drillDownKey9 = "";
	private String drillDownKey10 = "";
	private String drillDownKey0 = "";
	
	private String groupingFlag = "";
	private String reportOrientation = "";
	private String parentSubReportID = "";
	private int objectTypeAT = 7004;
	private String objectType = "";
	private int reportTypeAT = 7003;
	private String reportType = "";
	private String templateId = "";
	private String chartType = "";
	private int chartTypeAT = 7005;
	private int intReportSeq = 0;
	private String pdfGroupColumn = "";
	private String screenGroupColumn = "";
	
	private String finalExeQuery = "";
	private String sortField = "";
	private String colHeaderXml = ""; 
	private String promptLabel = "";
	private String drillDownLabel = "";
	
	private int pdfWidth = 842;
	private int pdfHeight = 595;
	
	List Total = null;
	private String screenSortColumn = "";
	private PromptTreeVb promptTree = null; 
	private String scalingFactor = "0";
	private String sortFlag = "N";
	private String searchFlag = "N";
	List<SmartSearchVb> smartSearchOpt = null;
	private String columnsToHide = "";
	private int pdfGrwthPercent = 0;
	private String grandTotalCaption = "";
	private String dbConnection= "";
	private String reportInfo = "";
	private String savedWidgetId = "";
	private String reportDesignXml= "";
	private String reportPeriod= "";
	private String tileData = "";
	private String widgetTheme = "";
	private List<ReportFilterVb> reportFilters = null;
	private List<ReportUserDefVb> reportUserDeflst = null;
	
	private String dataFilter1 = "";
	private String dataFilter2 = ""; 
	private String dataFilter3 =  "";
	private String dataFilter4 =  "";
	private String dataFilter5 =  "";
	private String dataFilter6 =  "";
	private String dataFilter7 =  "";
	private String dataFilter8 =  "";
	private String dataFilter9 = "";
	private String dataFilter10 = "";
	private String filterPosition = "";
	private int widgetPagination = 0;
	
	private String applyGrouping = "";
	private String showDimensions = "";
	private String showMeasures = "";
	private String applicationTheme = "";
	private String runType = "";
	private List chartList  = new ArrayList<>();
	private String csvDelimiter = "";
	private String ddKeyPromptLabel = "";
	private int freezeColumn = 0;
	public String getReportId() {
		return reportId;
	}
	public void setReportId(String reportId) {
		this.reportId = reportId;
	}
	public String getReportTitle() {
		return reportTitle;
	}
	public void setReportTitle(String reportTitle) {
		this.reportTitle = reportTitle;
	}
	public String getApplyUserRestrct() {
		return applyUserRestrct;
	}
	public void setApplyUserRestrct(String applyUserRestrct) {
		this.applyUserRestrct = applyUserRestrct;
	}
	public String getFilterFlag() {
		return filterFlag;
	}
	public void setFilterFlag(String filterFlag) {
		this.filterFlag = filterFlag;
	}
	public String getFilterRefCode() {
		return filterRefCode;
	}
	public void setFilterRefCode(String filterRefCode) {
		this.filterRefCode = filterRefCode;
	}
	public String getReportCategory() {
		return reportCategory;
	}
	public void setReportCategory(String reportCategory) {
		this.reportCategory = reportCategory;
	}
	public String getCategoryDesc() {
		return categoryDesc;
	}
	public void setCategoryDesc(String categoryDesc) {
		this.categoryDesc = categoryDesc;
	}
	public List<ColumnHeadersVb> getColumnHeaderslst() {
		return columnHeaderslst;
	}
	public void setColumnHeaderslst(List<ColumnHeadersVb> columnHeaderslst) {
		this.columnHeaderslst = columnHeaderslst;
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
	public int getMaxHeaderRow() {
		return maxHeaderRow;
	}
	public void setMaxHeaderRow(int maxHeaderRow) {
		this.maxHeaderRow = maxHeaderRow;
	}
	public int getMaxColumn() {
		return maxColumn;
	}
	public void setMaxColumn(int maxColumn) {
		this.maxColumn = maxColumn;
	}
	public String getSubReportId() {
		return subReportId;
	}
	public void setSubReportId(String subReportId) {
		this.subReportId = subReportId;
	}
	public String getDataRefId() {
		return dataRefId;
	}
	public void setDataRefId(String dataRefId) {
		this.dataRefId = dataRefId;
	}
	
	public List getGridDataSet() {
		return gridDataSet;
	}
	public void setGridDataSet(List gridDataSet) {
		this.gridDataSet = gridDataSet;
	}
	public String getDdFlag() {
		return ddFlag;
	}
	public void setDdFlag(String ddFlag) {
		this.ddFlag = ddFlag;
	}
	public String getFetchFlag() {
		return fetchFlag;
	}
	public void setFetchFlag(String fetchFlag) {
		this.fetchFlag = fetchFlag;
	}
	public List<ReportsVb> getReportslst() {
		return reportslst;
	}
	public void setReportslst(List<ReportsVb> reportslst) {
		this.reportslst = reportslst;
	}
	public String getDrillDownKey1() {
		return drillDownKey1;
	}
	public void setDrillDownKey1(String drillDownKey1) {
		this.drillDownKey1 = drillDownKey1;
	}
	public String getDrillDownKey2() {
		return drillDownKey2;
	}
	public void setDrillDownKey2(String drillDownKey2) {
		this.drillDownKey2 = drillDownKey2;
	}
	public String getDrillDownKey3() {
		return drillDownKey3;
	}
	public void setDrillDownKey3(String drillDownKey3) {
		this.drillDownKey3 = drillDownKey3;
	}
	public String getDrillDownKey4() {
		return drillDownKey4;
	}
	public void setDrillDownKey4(String drillDownKey4) {
		this.drillDownKey4 = drillDownKey4;
	}
	public String getDrillDownKey5() {
		return drillDownKey5;
	}
	public void setDrillDownKey5(String drillDownKey5) {
		this.drillDownKey5 = drillDownKey5;
	}
	public String getDrillDownKey6() {
		return drillDownKey6;
	}
	public void setDrillDownKey6(String drillDownKey6) {
		this.drillDownKey6 = drillDownKey6;
	}
	public String getDrillDownKey7() {
		return drillDownKey7;
	}
	public void setDrillDownKey7(String drillDownKey7) {
		this.drillDownKey7 = drillDownKey7;
	}
	public String getDrillDownKey8() {
		return drillDownKey8;
	}
	public void setDrillDownKey8(String drillDownKey8) {
		this.drillDownKey8 = drillDownKey8;
	}
	public String getDrillDownKey9() {
		return drillDownKey9;
	}
	public void setDrillDownKey9(String drillDownKey9) {
		this.drillDownKey9 = drillDownKey9;
	}
	public String getDrillDownKey10() {
		return drillDownKey10;
	}
	public void setDrillDownKey10(String drillDownKey10) {
		this.drillDownKey10 = drillDownKey10;
	}
	public String getGroupingFlag() {
		return groupingFlag;
	}
	public void setGroupingFlag(String groupingFlag) {
		this.groupingFlag = groupingFlag;
	}
	public String getReportOrientation() {
		return reportOrientation;
	}
	public void setReportOrientation(String reportOrientation) {
		this.reportOrientation = reportOrientation;
	}
	public String getParentSubReportID() {
		return parentSubReportID;
	}
	public void setParentSubReportID(String parentSubReportID) {
		this.parentSubReportID = parentSubReportID;
	}
	public int getObjectTypeAT() {
		return objectTypeAT;
	}
	public void setObjectTypeAT(int objectTypeAT) {
		this.objectTypeAT = objectTypeAT;
	}
	public String getObjectType() {
		return objectType;
	}
	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}
	public int getReportTypeAT() {
		return reportTypeAT;
	}
	public void setReportTypeAT(int reportTypeAT) {
		this.reportTypeAT = reportTypeAT;
	}
	public String getReportType() {
		return reportType;
	}
	public void setReportType(String reportType) {
		this.reportType = reportType;
	}
	public String getTemplateId() {
		return templateId;
	}
	public void setTemplateId(String templateId) {
		this.templateId = templateId;
	}
	public String getChartType() {
		return chartType;
	}
	public void setChartType(String chartType) {
		this.chartType = chartType;
	}
	public String getChartData() {
		return chartData;
	}
	public void setChartData(String chartData) {
		this.chartData = chartData;
	}
	public int getChartTypeAT() {
		return chartTypeAT;
	}
	public void setChartTypeAT(int chartTypeAT) {
		this.chartTypeAT = chartTypeAT;
	}
	public int getIntReportSeq() {
		return intReportSeq;
	}
	public void setIntReportSeq(int intReportSeq) {
		this.intReportSeq = intReportSeq;
	}
	public String getPdfGroupColumn() {
		return pdfGroupColumn;
	}
	public void setPdfGroupColumn(String pdfGroupColumn) {
		this.pdfGroupColumn = pdfGroupColumn;
	}
	public String getCurrentLevel() {
		return currentLevel;
	}
	public void setCurrentLevel(String currentLevel) {
		this.currentLevel = currentLevel;
	}
	public String getNextLevel() {
		return nextLevel;
	}
	public void setNextLevel(String nextLevel) {
		this.nextLevel = nextLevel;
	}
	public String getFormatType() {
		return formatType;
	}
	public void setFormatType(String formatType) {
		this.formatType = formatType;
	}
	public String getFinalExeQuery() {
		return finalExeQuery;
	}
	public void setFinalExeQuery(String finalExeQuery) {
		this.finalExeQuery = finalExeQuery;
	}
	public String getSortField() {
		return sortField;
	}
	public void setSortField(String sortField) {
		this.sortField = sortField;
	}
	public String getColHeaderXml() {
		return colHeaderXml;
	}
	public void setColHeaderXml(String colHeaderXml) {
		this.colHeaderXml = colHeaderXml;
	}
	public String getPromptLabel() {
		return promptLabel;
	}
	public void setPromptLabel(String promptLabel) {
		this.promptLabel = promptLabel;
	}
	public String getDrillDownLabel() {
		return drillDownLabel;
	}
	public void setDrillDownLabel(String drillDownLabel) {
		this.drillDownLabel = drillDownLabel;
	}
	public int getPdfWidth() {
		return pdfWidth;
	}
	public void setPdfWidth(int pdfWidth) {
		this.pdfWidth = pdfWidth;
	}
	public int getPdfHeight() {
		return pdfHeight;
	}
	public void setPdfHeight(int pdfHeight) {
		this.pdfHeight = pdfHeight;
	}
	public List getTotal() {
		return Total;
	}
	public void setTotal(List total) {
		Total = total;
	}
	public String getScreenGroupColumn() {
		return screenGroupColumn;
	}
	public void setScreenGroupColumn(String screenGroupColumn) {
		this.screenGroupColumn = screenGroupColumn;
	}
	public String getScreenSortColumn() {
		return screenSortColumn;
	}
	public void setScreenSortColumn(String screenSortColumn) {
		this.screenSortColumn = screenSortColumn;
	}
	public PromptTreeVb getPromptTree() {
		return promptTree;
	}
	public void setPromptTree(PromptTreeVb promptTree) {
		this.promptTree = promptTree;
	}
	public String getScalingFactor() {
		return scalingFactor;
	}
	public void setScalingFactor(String scalingFactor) {
		this.scalingFactor = scalingFactor;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}
	public String getSortFlag() {
		return sortFlag;
	}
	public void setSortFlag(String sortFlag) {
		this.sortFlag = sortFlag;
	}
	public String getSearchFlag() {
		return searchFlag;
	}
	public void setSearchFlag(String searchFlag) {
		this.searchFlag = searchFlag;
	}
	public int getPdfGrwthPercent() {
		return pdfGrwthPercent;
	}
	public void setPdfGrwthPercent(int pdfGrwthPercent) {
		this.pdfGrwthPercent = pdfGrwthPercent;
	}
	public String getColumnsToHide() {
		return columnsToHide;
	}
	public void setColumnsToHide(String columnsToHide) {
		this.columnsToHide = columnsToHide;
	}
	public String getDrillDownKey0() {
		return drillDownKey0;
	}
	public void setDrillDownKey0(String drillDownKey0) {
		this.drillDownKey0 = drillDownKey0;
	}
	public String getApplicationId() {
		return applicationId;
	}
	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}
	public String getApplicationIdDesc() {
		return applicationIdDesc;
	}
	public void setApplicationIdDesc(String applicationIdDesc) {
		this.applicationIdDesc = applicationIdDesc;
	}
	public String getGrandTotalCaption() {
		return grandTotalCaption;
	}
	public void setGrandTotalCaption(String grandTotalCaption) {
		this.grandTotalCaption = grandTotalCaption;
	}
	public String getDbConnection() {
		return dbConnection;
	}
	public void setDbConnection(String dbConnection) {
		this.dbConnection = dbConnection;
	}
	public String getReportInfo() {
		return reportInfo;
	}
	public void setReportInfo(String reportInfo) {
		this.reportInfo = reportInfo;
	}
	public String getSavedWidgetId() {
		return savedWidgetId;
	}
	public void setSavedWidgetId(String savedWidgetId) {
		this.savedWidgetId = savedWidgetId;
	}
	public String getReportDesignXml() {
		return reportDesignXml;
	}
	public void setReportDesignXml(String reportDesignXml) {
		this.reportDesignXml = reportDesignXml;
	}
	public String getReportPeriod() {
		return reportPeriod;
	}
	public void setReportPeriod(String reportPeriod) {
		this.reportPeriod = reportPeriod;
	}
	public String getTileData() {
		return tileData;
	}
	public void setTileData(String tileData) {
		this.tileData = tileData;
	}
	public String getWidgetTheme() {
		return widgetTheme;
	}
	public void setWidgetTheme(String widgetTheme) {
		this.widgetTheme = widgetTheme;
	}
	public List<ReportFilterVb> getReportFilters() {
		return reportFilters;
	}
	public void setReportFilters(List<ReportFilterVb> reportFilters) {
		this.reportFilters = reportFilters;
	}
	public List<ReportUserDefVb> getReportUserDeflst() {
		return reportUserDeflst;
	}
	public void setReportUserDeflst(List<ReportUserDefVb> reportUserDeflst) {
		this.reportUserDeflst = reportUserDeflst;
	}
	public String getDataFilter1() {
		return dataFilter1;
	}
	public void setDataFilter1(String dataFilter1) {
		this.dataFilter1 = dataFilter1;
	}
	public String getDataFilter2() {
		return dataFilter2;
	}
	public void setDataFilter2(String dataFilter2) {
		this.dataFilter2 = dataFilter2;
	}
	public String getDataFilter3() {
		return dataFilter3;
	}
	public void setDataFilter3(String dataFilter3) {
		this.dataFilter3 = dataFilter3;
	}
	public String getDataFilter4() {
		return dataFilter4;
	}
	public void setDataFilter4(String dataFilter4) {
		this.dataFilter4 = dataFilter4;
	}
	public String getDataFilter5() {
		return dataFilter5;
	}
	public void setDataFilter5(String dataFilter5) {
		this.dataFilter5 = dataFilter5;
	}
	public String getDataFilter6() {
		return dataFilter6;
	}
	public void setDataFilter6(String dataFilter6) {
		this.dataFilter6 = dataFilter6;
	}
	public String getDataFilter7() {
		return dataFilter7;
	}
	public void setDataFilter7(String dataFilter7) {
		this.dataFilter7 = dataFilter7;
	}
	public String getDataFilter8() {
		return dataFilter8;
	}
	public void setDataFilter8(String dataFilter8) {
		this.dataFilter8 = dataFilter8;
	}
	public String getDataFilter9() {
		return dataFilter9;
	}
	public void setDataFilter9(String dataFilter9) {
		this.dataFilter9 = dataFilter9;
	}
	public String getDataFilter10() {
		return dataFilter10;
	}
	public void setDataFilter10(String dataFilter10) {
		this.dataFilter10 = dataFilter10;
	}
	public String getFilterPosition() {
		return filterPosition;
	}
	public void setFilterPosition(String filterPosition) {
		this.filterPosition = filterPosition;
	}
	public int getWidgetPagination() {
		return widgetPagination;
	}
	public void setWidgetPagination(int widgetPagination) {
		this.widgetPagination = widgetPagination;
	}
	public String getShowDimensions() {
		return showDimensions;
	}
	public void setShowDimensions(String showDimensions) {
		this.showDimensions = showDimensions;
	}
	public String getShowMeasures() {
		return showMeasures;
	}
	public void setShowMeasures(String showMeasures) {
		this.showMeasures = showMeasures;
	}
	public String getApplyGrouping() {
		return applyGrouping;
	}
	public void setApplyGrouping(String applyGrouping) {
		this.applyGrouping = applyGrouping;
	}
	public String getApplicationTheme() {
		return applicationTheme;
	}
	public void setApplicationTheme(String applicationTheme) {
		this.applicationTheme = applicationTheme;
	}
	public String getRunType() {
		return runType;
	}
	public void setRunType(String runType) {
		this.runType = runType;
	}
	public List getChartList() {
		return chartList;
	}
	public void setChartList(List chartList) {
		this.chartList = chartList;
	}
	public String getCsvDelimiter() {
		return csvDelimiter;
	}
	public void setCsvDelimiter(String csvDelimiter) {
		this.csvDelimiter = csvDelimiter;
	}
	public String getSubCategory() {
		return subCategory;
	}
	public void setSubCategory(String subCategory) {
		this.subCategory = subCategory;
	}
	public String getSubCategoryDesc() {
		return subCategoryDesc;
	}
	public void setSubCategoryDesc(String subCategoryDesc) {
		this.subCategoryDesc = subCategoryDesc;
	}
	public List<ReportsVb> getChildren() {
		return children;
	}
	public void setChildren(List<ReportsVb> children) {
		this.children = children;
	}
	public String getDisplayType() {
		return displayType;
	}
	public void setDisplayType(String displayType) {
		this.displayType = displayType;
	}
	public String getDdKeyPromptLabel() {
		return ddKeyPromptLabel;
	}
	public void setDdKeyPromptLabel(String ddKeyPromptLabel) {
		this.ddKeyPromptLabel = ddKeyPromptLabel;
	}
	public int getFreezeColumn() {
		return freezeColumn;
	}
	public void setFreezeColumn(int freezeColumn) {
		this.freezeColumn = freezeColumn;
	}
}
