package com.vision.vb;

import java.util.List;

public class UserRestrictionVb {
	private String macrovarName = "";
	private String tagName = "";
	private String displayName = "";
	private String macrovarDesc = "";
	private String tagValue = "";
	private String restrictionSql = "";
	private String restrictedValue = "";
	private List<UserRestrictionVb> children = null;
	
	
	public UserRestrictionVb(){}
	public UserRestrictionVb(String macrovarName, String tagName, String displayName, String macrovarDesc){
		this.macrovarName = macrovarName;
		this.tagName = tagName;
		this.displayName = displayName;
		this.macrovarDesc = macrovarDesc;
	}
	
	public String getMacrovarName() {
		return macrovarName;
	}
	public void setMacrovarName(String macrovarName) {
		this.macrovarName = macrovarName;
	}
	public String getTagName() {
		return tagName;
	}
	public void setTagName(String tagName) {
		this.tagName = tagName;
	}
	public String getDisplayName() {
		return displayName;
	}
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	public String getMacrovarDesc() {
		return macrovarDesc;
	}
	public void setMacrovarDesc(String macrovarDesc) {
		this.macrovarDesc = macrovarDesc;
	}
	public String getTagValue() {
		return tagValue;
	}
	public void setTagValue(String tagValue) {
		this.tagValue = tagValue;
	}
	public String getRestrictionSql() {
		return restrictionSql;
	}
	public void setRestrictionSql(String restrictionSql) {
		this.restrictionSql = restrictionSql;
	}
	public String getRestrictedValue() {
		return restrictedValue;
	}
	public void setRestrictedValue(String restrictedValue) {
		this.restrictedValue = restrictedValue;
	}
	public List<UserRestrictionVb> getChildren() {
		return children;
	}
	public void setChildren(List<UserRestrictionVb> children) {
		this.children = children;
	}
}