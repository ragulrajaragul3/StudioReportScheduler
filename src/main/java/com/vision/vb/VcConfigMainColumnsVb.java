package com.vision.vb;

import java.util.List;

public class VcConfigMainColumnsVb {
	
	private String catalogId = "";
	private String tableId = "";
	private String colId = "";
	private String colName = "";
	private String aliasName = "";
	private String sortColumn = "";
	private int colDisplayTypeAt = 2000;
	private String colDisplayType = "";
	private int colAttributeTypeAt = 2002;
	private String colAttributeType = "";
	private int colExperssionTypeAt = 2003;
	private String colExperssionType = "";
	private int formatTypeNt = 2012;
	private String formatType = "";
	private String formatTypeDesc = "";
	private String magEnableFlag = "";
	private int magTypeNt = 2002;
	private int magType = -1;
	private int magSelectionTypeAt = 2007;
	private String magSelectionType = "";
	private int vccStatusNt = 1;
	private int vccStatus = 0;
	private String magDefault = "";
	private String magQueryId = "";
	private String magDisplayColumn = "";
	private String magUseColumn = "";
	private String folderIds = "";
	private String experssionText = "";
	private int colTypeAt = 2001;
	private String colType = "";
	private String colLength="";
	private String javaFormatDesc;
	
	/* Masking Script Sample
	<maskings>
	    <masking>
	        <usergroup>ADMIN</usergroup>
	        <userprofile>MANAGEMENT</userprofile>
	        <pattern>___XXX___</pattern>
	    </masking>
	</maskings>
	*/

	private String maskingScript = "";
	private List<MaskingVb> maskingScriptParsed = null;
	private String maskingFlag = "";
	
	private String dateFormattingSyntax = "";
	private String dateConversionSyntax = "";
	
	private String includeGroupCol = "";
	
	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
	public String getTableId() {
		return tableId;
	}
	public void setTableId(String tableId) {
		this.tableId = tableId;
	}
	public String getColId() {
		return colId;
	}
	public void setColId(String colId) {
		this.colId = colId;
	}
	public String getColName() {
		return colName;
	}
	public void setColName(String colName) {
		this.colName = colName;
	}
	public String getAliasName() {
		return aliasName;
	}
	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}
	public String getSortColumn() {
		return sortColumn;
	}
	public void setSortColumn(String sortColumn) {
		this.sortColumn = sortColumn;
	}
	public int getColDisplayTypeAt() {
		return colDisplayTypeAt;
	}
	public void setColDisplayTypeAt(int colDisplayTypeAt) {
		this.colDisplayTypeAt = colDisplayTypeAt;
	}
	public String getColDisplayType() {
		return colDisplayType;
	}
	public void setColDisplayType(String colDisplayType) {
		this.colDisplayType = colDisplayType;
	}
	public int getColAttributeTypeAt() {
		return colAttributeTypeAt;
	}
	public void setColAttributeTypeAt(int colAttributeTypeAt) {
		this.colAttributeTypeAt = colAttributeTypeAt;
	}
	public String getColAttributeType() {
		return colAttributeType;
	}
	public void setColAttributeType(String colAttributeType) {
		this.colAttributeType = colAttributeType;
	}
	public int getColExperssionTypeAt() {
		return colExperssionTypeAt;
	}
	public void setColExperssionTypeAt(int colExperssionTypeAt) {
		this.colExperssionTypeAt = colExperssionTypeAt;
	}
	public String getColExperssionType() {
		return colExperssionType;
	}
	public void setColExperssionType(String colExperssionType) {
		this.colExperssionType = colExperssionType;
	}
	public int getFormatTypeNt() {
		return formatTypeNt;
	}
	public void setFormatTypeNt(int formatTypeNt) {
		this.formatTypeNt = formatTypeNt;
	}
	public String getFormatType() {
		return formatType;
	}
	public void setFormatType(String formatType) {
		this.formatType = formatType;
	}
	public String getFormatTypeDesc() {
		return formatTypeDesc;
	}
	public void setFormatTypeDesc(String formatTypeDesc) {
		this.formatTypeDesc = formatTypeDesc;
	}
	public String getMagEnableFlag() {
		return magEnableFlag;
	}
	public void setMagEnableFlag(String magEnableFlag) {
		this.magEnableFlag = magEnableFlag;
	}
	public int getMagTypeNt() {
		return magTypeNt;
	}
	public void setMagTypeNt(int magTypeNt) {
		this.magTypeNt = magTypeNt;
	}
	public int getMagType() {
		return magType;
	}
	public void setMagType(int magType) {
		this.magType = magType;
	}
	public int getMagSelectionTypeAt() {
		return magSelectionTypeAt;
	}
	public void setMagSelectionTypeAt(int magSelectionTypeAt) {
		this.magSelectionTypeAt = magSelectionTypeAt;
	}
	public String getMagSelectionType() {
		return magSelectionType;
	}
	public void setMagSelectionType(String magSelectionType) {
		this.magSelectionType = magSelectionType;
	}
	public int getVccStatusNt() {
		return vccStatusNt;
	}
	public void setVccStatusNt(int vccStatusNt) {
		this.vccStatusNt = vccStatusNt;
	}
	public int getVccStatus() {
		return vccStatus;
	}
	public void setVccStatus(int vccStatus) {
		this.vccStatus = vccStatus;
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
	public String getFolderIds() {
		return folderIds;
	}
	public void setFolderIds(String folderIds) {
		this.folderIds = folderIds;
	}
	public String getExperssionText() {
		return experssionText;
	}
	public void setExperssionText(String experssionText) {
		this.experssionText = experssionText;
	}
	public int getColTypeAt() {
		return colTypeAt;
	}
	public void setColTypeAt(int colTypeAt) {
		this.colTypeAt = colTypeAt;
	}
	public String getColType() {
		return colType;
	}
	public void setColType(String colType) {
		this.colType = colType;
	}
	public String getMaskingScript() {
		return maskingScript;
	}
	public void setMaskingScript(String maskingScript) {
		this.maskingScript = maskingScript;
	}
	public List<MaskingVb> getMaskingScriptParsed() {
		return maskingScriptParsed;
	}
	public void setMaskingScriptParsed(List<MaskingVb> maskingScriptParsed) {
		this.maskingScriptParsed = maskingScriptParsed;
	}
	public String getMaskingFlag() {
		return maskingFlag;
	}
	public void setMaskingFlag(String maskingFlag) {
		this.maskingFlag = maskingFlag;
	}
	public String getColLength() {
		return colLength;
	}
	public void setColLength(String colLength) {
		this.colLength = colLength;
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
}