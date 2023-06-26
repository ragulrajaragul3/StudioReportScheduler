package com.vision.vb;

public class VcForQueryReportFieldsVb extends CommonVb {

	private static final long serialVersionUID = 1L;
	private String catalogId;
	private String userId;
	private String reportId;
	private String tabelId;
	private String tableName;
	private String tabelAliasName;
	private String rowId;
	private String colId;
	private String colOrder;
	private String colName;
	private String colType;
	private String colDisplayType;
	private String joinCondition;
	private String alias;
	private String operator;
	private String displayFlag = "Y";
	private String sortType;
	private int sortOrder;
	private String aggFunction;
	private String conditionOperator;
	private String value1;
	private String value2;
	private String groupBy = "N";
	private int vrfStatusNt = 0;
	private int vrfStatus = -1;
	private int formatTypeNt = 40;
	private int formatType = -1;
	private Boolean promptSelected = false;
	private Boolean createNew = false;
	private String createNewRow = "N";
	// private String vcqdQueryXml = "";
	private String sheetName = "";
	private String rowStyle = "";
	private String excelFileName = "";

	private String vcColId = "";
	private String tableSourceType = "";
	private String queryId = "";
	private String subQueryColumns = "";
	private String colExpressionType = "";
	private String magEnableFlag = "";
	private String magType = "";
	private String magSelectionType = "";
	private String magDefault = "";
	private String magQueryId = "";
	private String magDisplayColumn = "";
	private String magUseColumn = "";

	public String colAttributeType = "";
	public String formatTypeDesc = "";
	public String experssionText = "";
	public String maskingFlag = "";
	public String maskingScript = "";
	public String colLength = "";
	private String scalingFlag="";
	private long scalingFormat;
	private String numberFormat="N";
	private String decimalFlag="";
	private int decimalCount;
	private String dynamicStartFlag;
	private String dynamicEndFlag;
	private String dynamicStartDate;
	private String dynamicEndDate;
	private Integer dynamicStartOperator;
	private Integer dynamicEndOperator;
	private String dynamicDateFormat;
	private String javaFormatDesc;
	
	private String dateFormattingSyntax = "";
	private String dateConversionSyntax = "";
	
	private String includeGroupCol = "";
	
	private String summaryCriteria = "";
	private String summaryValue1 = "";
	private String summaryValue2 = "";
	private String summaryDynamicStartFlag;
	private String summaryDynamicEndFlag;
	private String summaryDynamicStartDate;
	private String summaryDynamicEndDate;
	private Integer summaryDynamicStartOperator;
	private Integer summaryDynamicEndOperator;
	

	public String getExcelFileName() {
		return excelFileName;
	}

	public void setExcelFileName(String excelFileName) {
		this.excelFileName = excelFileName;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public String getRowStyle() {
		return rowStyle;
	}

	public void setRowStyle(String rowStyle) {
		this.rowStyle = rowStyle;
	}

	public String getCreateNewRow() {
		return createNewRow;
	}

	public void setCreateNewRow(String createNewRow) {
		this.createNewRow = createNewRow;
	}

	public Boolean getCreateNew() {
		return createNew;
	}

	public void setCreateNew(Boolean createNew) {
		this.createNew = createNew;
	}

	public String getCatalogId() {
		return catalogId;
	}

	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getReportId() {
		return reportId;
	}

	public void setReportId(String reportId) {
		this.reportId = reportId;
	}

	public String getTabelId() {
		return tabelId;
	}

	public void setTabelId(String tabelId) {
		this.tabelId = tabelId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getTabelAliasName() {
		return tabelAliasName;
	}

	public void setTabelAliasName(String tabelAliasName) {
		this.tabelAliasName = tabelAliasName;
	}

	public String getColId() {
		return colId;
	}

	public void setColId(String colId) {
		this.colId = colId;
	}

	public String getColOrder() {
		return colOrder;
	}

	public void setColOrder(String colOrder) {
		this.colOrder = colOrder;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getColType() {
		return colType;
	}

	public void setColType(String colType) {
		this.colType = colType;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getDisplayFlag() {
		return displayFlag;
	}

	public void setDisplayFlag(String displayFlag) {
		this.displayFlag = displayFlag;
	}

	public String getSortType() {
		return sortType;
	}

	public void setSortType(String sortType) {
		this.sortType = sortType;
	}

	public int getSortOrder() {
		return sortOrder;
	}

	public void setSortOrder(int sortOrder) {
		this.sortOrder = sortOrder;
	}

	public String getAggFunction() {
		return aggFunction;
	}

	public void setAggFunction(String aggFunction) {
		this.aggFunction = aggFunction;
	}

	public String getConditionOperator() {
		return conditionOperator;
	}

	public void setConditionOperator(String conditionOperator) {
		this.conditionOperator = conditionOperator;
	}

	public String getValue1() {
		return value1;
	}

	public void setValue1(String value1) {
		this.value1 = value1;
	}

	public String getValue2() {
		return value2;
	}

	public void setValue2(String value2) {
		this.value2 = value2;
	}

	public String getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	public int getVrfStatusNt() {
		return vrfStatusNt;
	}

	public void setVrfStatusNt(int vrfStatusNt) {
		this.vrfStatusNt = vrfStatusNt;
	}

	public int getVrfStatus() {
		return vrfStatus;
	}

	public void setVrfStatus(int vrfStatus) {
		this.vrfStatus = vrfStatus;
	}

	public String getJoinCondition() {
		return joinCondition;
	}

	public void setJoinCondition(String joinCondition) {
		this.joinCondition = joinCondition;
	}

	public int getFormatTypeNt() {
		return formatTypeNt;
	}

	public void setFormatTypeNt(int formatTypeNt) {
		this.formatTypeNt = formatTypeNt;
	}

	public int getFormatType() {
		return formatType;
	}

	public void setFormatType(int formatType) {
		this.formatType = formatType;
	}

	public String getColDisplayType() {
		return colDisplayType;
	}

	public void setColDisplayType(String colDisplayType) {
		this.colDisplayType = colDisplayType;
	}

	public String getRowId() {
		return rowId;
	}

	public void setRowId(String rowId) {
		this.rowId = rowId;
	}

	public Boolean getPromptSelected() {
		return promptSelected;
	}

	public void setPromptSelected(Boolean promptSelected) {
		this.promptSelected = promptSelected;
	}

	public String getVcColId() {
		return vcColId;
	}

	public void setVcColId(String vcColId) {
		this.vcColId = vcColId;
	}

	public String getTableSourceType() {
		return tableSourceType;
	}

	public void setTableSourceType(String tableSourceType) {
		this.tableSourceType = tableSourceType;
	}

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public String getSubQueryColumns() {
		return subQueryColumns;
	}

	public void setSubQueryColumns(String subQueryColumns) {
		this.subQueryColumns = subQueryColumns;
	}

	public String getColExpressionType() {
		return colExpressionType;
	}

	public void setColExpressionType(String colExpressionType) {
		this.colExpressionType = colExpressionType;
	}

	public String getMagEnableFlag() {
		return magEnableFlag;
	}

	public void setMagEnableFlag(String magEnableFlag) {
		this.magEnableFlag = magEnableFlag;
	}

	public String getMagType() {
		return magType;
	}

	public void setMagType(String magType) {
		this.magType = magType;
	}

	public String getMagSelectionType() {
		return magSelectionType;
	}

	public void setMagSelectionType(String magSelectionType) {
		this.magSelectionType = magSelectionType;
	}

	public String getMagDefault() {
		return magDefault;
	}

	public void setMagDefault(String magDefault) {
		this.magDefault = magDefault;
	}

	public String getMagQueryId() {
		return magQueryId;
	}

	public void setMagQueryId(String magQueryId) {
		this.magQueryId = magQueryId;
	}

	public String getMagDisplayColumn() {
		return magDisplayColumn;
	}

	public void setMagDisplayColumn(String magDisplayColumn) {
		this.magDisplayColumn = magDisplayColumn;
	}

	public String getMagUseColumn() {
		return magUseColumn;
	}

	public void setMagUseColumn(String magUseColumn) {
		this.magUseColumn = magUseColumn;
	}

	/*
	 * public String getVcqdQueryXml() { return vcqdQueryXml; } public void
	 * setVcqdQueryXml(String vcqdQueryXml) { this.vcqdQueryXml = vcqdQueryXml; }
	 */
	public String getColAttributeType() {
		return colAttributeType;
	}

	public void setColAttributeType(String colAttributeType) {
		this.colAttributeType = colAttributeType;
	}

	public String getFormatTypeDesc() {
		return formatTypeDesc;
	}

	public void setFormatTypeDesc(String formatTypeDesc) {
		this.formatTypeDesc = formatTypeDesc;
	}

	public String getExperssionText() {
		return experssionText;
	}

	public void setExperssionText(String experssionText) {
		this.experssionText = experssionText;
	}

	public String getMaskingFlag() {
		return maskingFlag;
	}

	public void setMaskingFlag(String maskingFlag) {
		this.maskingFlag = maskingFlag;
	}

	public String getMaskingScript() {
		return maskingScript;
	}

	public void setMaskingScript(String maskingScript) {
		this.maskingScript = maskingScript;
	}

	public String getColLength() {
		return colLength;
	}

	public void setColLength(String colLength) {
		this.colLength = colLength;
	}

	public String getScalingFlag() {
		return scalingFlag;
	}

	public void setScalingFlag(String scalingFlag) {
		this.scalingFlag = scalingFlag;
	}

	public long getScalingFormat() {
		return scalingFormat;
	}

	public void setScalingFormat(long scalingFormat) {
		this.scalingFormat = scalingFormat;
	}

	public String getNumberFormat() {
		return numberFormat;
	}

	public void setNumberFormat(String numberFormat) {
		this.numberFormat = numberFormat;
	}

	public String getDecimalFlag() {
		return decimalFlag;
	}

	public void setDecimalFlag(String decimalFlag) {
		this.decimalFlag = decimalFlag;
	}

	public int getDecimalCount() {
		return decimalCount;
	}

	public void setDecimalCount(int decimalCount) {
		this.decimalCount = decimalCount;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public String getDynamicStartFlag() {
		return dynamicStartFlag;
	}

	public void setDynamicStartFlag(String dynamicStartFlag) {
		this.dynamicStartFlag = dynamicStartFlag;
	}

	public String getDynamicEndFlag() {
		return dynamicEndFlag;
	}

	public void setDynamicEndFlag(String dynamicEndFlag) {
		this.dynamicEndFlag = dynamicEndFlag;
	}

	public String getDynamicStartDate() {
		return dynamicStartDate;
	}

	public void setDynamicStartDate(String dynamicStartDate) {
		this.dynamicStartDate = dynamicStartDate;
	}

	public String getDynamicEndDate() {
		return dynamicEndDate;
	}

	public void setDynamicEndDate(String dynamicEndDate) {
		this.dynamicEndDate = dynamicEndDate;
	}

	public Integer getDynamicStartOperator() {
		return dynamicStartOperator;
	}

	public void setDynamicStartOperator(Integer dynamicStartOperator) {
		this.dynamicStartOperator = dynamicStartOperator;
	}

	public Integer getDynamicEndOperator() {
		return dynamicEndOperator;
	}

	public void setDynamicEndOperator(Integer dynamicEndOperator) {
		this.dynamicEndOperator = dynamicEndOperator;
	}

	public String getDynamicDateFormat() {
		return dynamicDateFormat;
	}

	public void setDynamicDateFormat(String dynamicDateFormat) {
		this.dynamicDateFormat = dynamicDateFormat;
	}

	public String getJavaFormatDesc() {
		return javaFormatDesc;
	}

	public void setJavaFormatDesc(String javaFormatDesc) {
		this.javaFormatDesc = javaFormatDesc;
	}

	public String getDateFormattingSyntax() {
		return dateFormattingSyntax;
	}

	public void setDateFormattingSyntax(String dateFormattingSyntax) {
		this.dateFormattingSyntax = dateFormattingSyntax;
	}

	public String getDateConversionSyntax() {
		return dateConversionSyntax;
	}

	public void setDateConversionSyntax(String dateConversionSyntax) {
		this.dateConversionSyntax = dateConversionSyntax;
	}

	public String getIncludeGroupCol() {
		return includeGroupCol;
	}

	public void setIncludeGroupCol(String includeGroupCol) {
		this.includeGroupCol = includeGroupCol;
	}

	public String getSummaryCriteria() {
		return summaryCriteria;
	}

	public void setSummaryCriteria(String summaryCriteria) {
		this.summaryCriteria = summaryCriteria;
	}

	public String getSummaryValue1() {
		return summaryValue1;
	}

	public void setSummaryValue1(String summaryValue1) {
		this.summaryValue1 = summaryValue1;
	}

	public String getSummaryValue2() {
		return summaryValue2;
	}

	public void setSummaryValue2(String summaryValue2) {
		this.summaryValue2 = summaryValue2;
	}

	public String getSummaryDynamicStartFlag() {
		return summaryDynamicStartFlag;
	}

	public void setSummaryDynamicStartFlag(String summaryDynamicStartFlag) {
		this.summaryDynamicStartFlag = summaryDynamicStartFlag;
	}

	public String getSummaryDynamicEndFlag() {
		return summaryDynamicEndFlag;
	}

	public void setSummaryDynamicEndFlag(String summaryDynamicEndFlag) {
		this.summaryDynamicEndFlag = summaryDynamicEndFlag;
	}

	public String getSummaryDynamicStartDate() {
		return summaryDynamicStartDate;
	}

	public void setSummaryDynamicStartDate(String summaryDynamicStartDate) {
		this.summaryDynamicStartDate = summaryDynamicStartDate;
	}

	public String getSummaryDynamicEndDate() {
		return summaryDynamicEndDate;
	}

	public void setSummaryDynamicEndDate(String summaryDynamicEndDate) {
		this.summaryDynamicEndDate = summaryDynamicEndDate;
	}

	public Integer getSummaryDynamicStartOperator() {
		return summaryDynamicStartOperator;
	}

	public void setSummaryDynamicStartOperator(Integer summaryDynamicStartOperator) {
		this.summaryDynamicStartOperator = summaryDynamicStartOperator;
	}

	public Integer getSummaryDynamicEndOperator() {
		return summaryDynamicEndOperator;
	}

	public void setSummaryDynamicEndOperator(Integer summaryDynamicEndOperator) {
		this.summaryDynamicEndOperator = summaryDynamicEndOperator;
	}

}

