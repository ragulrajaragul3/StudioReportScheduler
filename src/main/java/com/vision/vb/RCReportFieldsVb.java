package com.vision.vb;

public class RCReportFieldsVb extends CommonVb {

		private String catalogId ;
		private String userId ;
		private String reportId ;
		private String tabelId ;
		private String tabelName ;
		private String rowId ;
		private String colId ;
		private String colOrder ;
		private String colName ;
		private String colType ;
		private String colDisplayType ;
		private String joinCondition;
		private String alias ;
		private String operator ;
		private String displayFlag  = "Y";
		private String sortType ;
		private String sortOrder ;
		private String aggFunction ;
		private String conditionOperator ;
		private String value1 ;
		private String value2 ;
		private String groupBy  = "N";
		private int vrfStatusNt = 0;
		private int vrfStatus = -1;
		private int formatTypeNt = 40;
		private int formatType = -1;
		private Boolean promptSelected = false;
		private Boolean createNew = false;
		private String createNewRow = "N";
		private String sheetName = "";
		private String rowStyle = "";
		private String excelFileName = "";
		
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
		public String getTabelName() {
			return tabelName;
		}
		public void setTabelName(String tabelName) {
			this.tabelName = tabelName;
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
		public String getSortOrder() {
			return sortOrder;
		}
		public void setSortOrder(String sortOrder) {
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

}
