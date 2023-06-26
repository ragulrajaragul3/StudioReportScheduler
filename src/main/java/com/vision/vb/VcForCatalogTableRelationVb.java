package com.vision.vb;

import java.util.List;

public class VcForCatalogTableRelationVb  extends CommonVb {
	
	private static final long serialVersionUID = 1L;
	private String catalogId;
	private String relScriptString = "";
	private String fromTableId = "";
	private String fromTableName = "";
	private String fromTableAliasName = "";
	private String toTableId = "";
	private String toTableName = "";
	private String toTableAliasName = "";
	private String joinTypeNt = "";
	private String relJoinType = "";
	private String joinString = "";
	private String filterCondition = "";
	private int vcrStatusNt = 1;
	private int vcrStatus = 0;
	private String ansiString="";
	private String standardString="";
	
	
	private List<VcForCatalogRelationScriptVb> relationScriptsMetadata  = null;
	

	public String getAnsiString() {
		return ansiString;
	}
	public void setAnsiString(String ansiString) {
		this.ansiString = ansiString;
	}
	public String getStandardString() {
		return standardString;
	}
	public void setStandardString(String standardString) {
		this.standardString = standardString;
	}
	
	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
	public String getRelScriptString() {
		return relScriptString;
	}
	public void setRelScriptString(String relScriptString) {
		this.relScriptString = relScriptString;
	}
	public String getFromTableId() {
		return fromTableId;
	}
	public void setFromTableId(String fromTableId) {
		this.fromTableId = fromTableId;
	}
	public String getFromTableName() {
		return fromTableName;
	}
	public void setFromTableName(String fromTableName) {
		this.fromTableName = fromTableName;
	}
	public String getFromTableAliasName() {
		return fromTableAliasName;
	}
	public void setFromTableAliasName(String fromTableAliasName) {
		this.fromTableAliasName = fromTableAliasName;
	}
	public String getToTableId() {
		return toTableId;
	}
	public void setToTableId(String toTableId) {
		this.toTableId = toTableId;
	}
	public String getToTableName() {
		return toTableName;
	}
	public void setToTableName(String toTableName) {
		this.toTableName = toTableName;
	}
	public String getToTableAliasName() {
		return toTableAliasName;
	}
	public void setToTableAliasName(String toTableAliasName) {
		this.toTableAliasName = toTableAliasName;
	}
	public String getJoinTypeNt() {
		return joinTypeNt;
	}
	public void setJoinTypeNt(String joinTypeNt) {
		this.joinTypeNt = joinTypeNt;
	}
	public String getRelJoinType() {
		return relJoinType;
	}
	public void setRelJoinType(String relJoinType) {
		this.relJoinType = relJoinType;
	}
	public String getJoinString() {
		return joinString;
	}
	public void setJoinString(String joinString) {
		this.joinString = joinString;
	}
	public String getFilterCondition() {
		return filterCondition;
	}
	public void setFilterCondition(String filterCondition) {
		this.filterCondition = filterCondition;
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
	public List<VcForCatalogRelationScriptVb> getRelationScriptsMetadata() {
		return relationScriptsMetadata;
	}
	public void setRelationScriptsMetadata(List<VcForCatalogRelationScriptVb> relationScriptsMetadata) {
		this.relationScriptsMetadata = relationScriptsMetadata;
	}

	
}