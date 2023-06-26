package com.vision.vb;

import java.util.List;

public class DSConnectorVb extends CommonVb{

	private static final long serialVersionUID = -7814611304253747452L;
	
	private String macroVar = ""; 
	private String macroVarScript = ""; 
	private int sortOrder = 1;
	private int scriptTypeAt=1083;
	private String scriptType;
	private int vcrStatusNt = 1;
	private int vcrStatus = 0;
	private int vcrTypeNt; 
	private int vcrType= 2;	
	private String fileName = "";
	private String delimiter = "";
	private String extension = "";
	private String vcrStatusDesc = "";

	private String macroVarType = ""; 
	private String tagValue = "";
	private String tagName = "";
	private String displayName = "";
	private String maskedFlag = "";
	private String encryption = "";
	private String description = "";
	private Object[] dynamicScript;
	private String validFlag = "";

	/*Level Of Display*/
	private int dataConnectorStatusNt = 1;
	private int dataConnectorStatus = 0;
	private int	userGroupAt = 1;
	private String userGroup = "";
	private int	userProfileAt = 2;
	private String userProfile = "";
	private int	visionId =  0;
	private String tableName = "";
	public List<SmartSearchVb> smartSearchVb=null;
	private Integer tableSize=0;
	private String mandatoryFlag = "N";
	private int tagTypeNt = 2011;
	private int tagType = 3;
	private char headerCheck;
	private String tableQuery;
	private String tableExcludeQuery;
	private String tableColumnQuery;

	public Integer getTableSize() {
		return tableSize;
	}
	public void setTableSize(Integer tableSize) {
		this.tableSize = tableSize;
	}
	public int getVisionId() {
		return visionId;
	}
	public void setVisionId(int visionId) {
		this.visionId = visionId;
	}
	public int getDataConnectorStatusNt() {
		return dataConnectorStatusNt;
	}
	public void setDataConnectorStatusNt(int dataConnectorStatusNt) {
		this.dataConnectorStatusNt = dataConnectorStatusNt;
	}
	public int getDataConnectorStatus() {
		return dataConnectorStatus;
	}
	public void setDataConnectorStatus(int dataConnectorStatus) {
		this.dataConnectorStatus = dataConnectorStatus;
	}
	public int getUserGroupAt() {
		return userGroupAt;
	}
	public void setUserGroupAt(int userGroupAt) {
		this.userGroupAt = userGroupAt;
	}
	public String getUserGroup() {
		return userGroup;
	}
	public void setUserGroup(String userGroup) {
		this.userGroup = userGroup;
	}
	public int getUserProfileAt() {
		return userProfileAt;
	}
	public void setUserProfileAt(int userProfileAt) {
		this.userProfileAt = userProfileAt;
	}
	public String getUserProfile() {
		return userProfile;
	}
	public void setUserProfile(String userProfile) {
		this.userProfile = userProfile;
	}
	public String getMaskedFlag() {
		return maskedFlag;
	}
	public void setMaskedFlag(String maskedFlag) {
		this.maskedFlag = maskedFlag;
	}
	public String getValidFlag() {
		return validFlag;
	}
	public void setValidFlag(String validFlag) {
		this.validFlag = validFlag;
	}
	public Object[] getDynamicScript() {
		return dynamicScript;
	}
	public void setDynamicScript(Object[] dynamicScript) {
		this.dynamicScript = dynamicScript;
	}
	public String getMacroVar() {
		return macroVar;
	}
	public void setMacroVar(String macroVar) {
		this.macroVar = macroVar;
	}
	public int getVcrTypeNt() {
		return vcrTypeNt;
	}
	public void setVcrTypeNt(int vcrTypeNt) {
		this.vcrTypeNt = vcrTypeNt;
	}
	public int getVcrType() {
		return vcrType;
	}
	public void setVcrType(int vcrType) {
		this.vcrType = vcrType;
	}
	public int getScriptTypeAt() {
		return scriptTypeAt;
	}
	public void setScriptTypeAt(int scriptTypeAt) {
		this.scriptTypeAt = scriptTypeAt;
	}
	public String getScriptType() {
		return scriptType;
	}
	public void setScriptType(String scriptType) {
		this.scriptType = scriptType;
	}
	public String getMacroVarScript() {
		return macroVarScript;
	}
	public void setMacroVarScript(String macroVarScript) {
		this.macroVarScript = macroVarScript;
	}
	public int getSortOrder() {
		return sortOrder;
	}
	public void setSortOrder(int sortOrder) {
		this.sortOrder = sortOrder;
	}
	public int getVcrStatusNt() {
		return vcrStatusNt;
	}
	public void setVcrStatusNt(int vcrStatusNt) {
		this.vcrStatusNt = vcrStatusNt;
	}
	public int getVcrStatus() {
		return vcrStatus;
	}
	public void setVcrStatus(int vcrStatus) {
		this.vcrStatus = vcrStatus;
	}
	public String getMacroVarType() {
		return macroVarType;
	}
	public void setMacroVarType(String macroVarType) {
		this.macroVarType = macroVarType;
	}
	public String getTagName() {
		return tagName;
	}
	public void setTagName(String tagName) {
		this.tagName = tagName;
	}
	public String getTagValue() {
		return tagValue;
	}
	public void setTagValue(String tagValue) {
		this.tagValue = tagValue;
	}
	public String getDisplayName() {
		return displayName;
	}
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	public String getEncryption() {
		return encryption;
	}
	public void setEncryption(String encryption) {
		this.encryption = encryption;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getDelimiter() {
		return delimiter;
	}
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getVcrStatusDesc() {
		return vcrStatusDesc;
	}
	public void setVcrStatusDesc(String vcrStatusDesc) {
		this.vcrStatusDesc = vcrStatusDesc;
	}
	public String getExtension() {
		return extension;
	}
	public void setExtension(String extension) {
		this.extension = extension;
	}
	public List<SmartSearchVb> getSmartSearchVb() {
		return smartSearchVb;
	}
	public void setSmartSearchVb(List<SmartSearchVb> smartSearchVb) {
		this.smartSearchVb = smartSearchVb;
	}
	public String getMandatoryFlag() {
		return mandatoryFlag;
	}
	public void setMandatoryFlag(String mandatoryFlag) {
		this.mandatoryFlag = mandatoryFlag;
	}
	public int getTagTypeNt() {
		return tagTypeNt;
	}
	public void setTagTypeNt(int tagTypeNt) {
		this.tagTypeNt = tagTypeNt;
	}
	public int getTagType() {
		return tagType;
	}
	public void setTagType(int tagType) {
		this.tagType = tagType;
	}
	public char getHeaderCheck() {
		return headerCheck;
	}
	public void setHeaderCheck(char headerCheck) {
		this.headerCheck = headerCheck;
	}
	public String getTableQuery() {
		return tableQuery;
	}
	public void setTableQuery(String tableQuery) {
		this.tableQuery = tableQuery;
	}
	public String getTableExcludeQuery() {
		return tableExcludeQuery;
	}
	public void setTableExcludeQuery(String tableExcludeQuery) {
		this.tableExcludeQuery = tableExcludeQuery;
	}
	public String getTableColumnQuery() {
		return tableColumnQuery;
	}
	public void setTableColumnQuery(String tableColumnQuery) {
		this.tableColumnQuery = tableColumnQuery;
	}
	
}