package com.vision.vb;

import java.io.Serializable;
import java.util.LinkedHashMap;

public class ReportFilterVb implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String filterRefCode = "";
	private String filterRefXml = "";
	private String userRestrictionXml = "";
	private String userRestrictFlag = "N";
	
	private int filterSeq = 0;
	private String filterLabel = "";
	private String filterType = "";
	//private String filterDefaultValue = "";
	private String filterSourceId = "";
	private String specificTab = "";
	private String dependencyFlag = "";
	private String dependentPrompt = "";
	private String multiWidth = "";
	private String multiSelect = "";
	
	private String filterDateFormat = "";
	private String filterString= "";
	
	LinkedHashMap<String,String> filterSourceVal = new LinkedHashMap<String,String>();
	LinkedHashMap<String,String> filterDefaultValue = new LinkedHashMap<String,String>();
	
	private String filter1Val = "";
	private String filter2Val = "";
	private String filter3Val = "";
	private String filter4Val = "";
	private String filter5Val = "";
	private String filter6Val = "";
	private String filter7Val = ""; 
	private String filter8Val = "";
	private String filter9Val = "";
	private String filter10Val = "";
	
	private String filterRow = "";
	private String filterDateRestrict= "";
	private String defaultValueId = "";
	private Boolean globalFilter = false;
	LinkedHashMap<String, String> filterValueMap= new LinkedHashMap<String, String>();
	
	public String getFilterRefCode() {
		return filterRefCode;
	}
	public void setFilterRefCode(String filterRefCode) {
		this.filterRefCode = filterRefCode;
	}
	public String getFilterRefXml() {
		return filterRefXml;
	}
	public void setFilterRefXml(String filterRefXml) {
		this.filterRefXml = filterRefXml;
	}
	public String getFilterLabel() {
		return filterLabel;
	}
	public void setFilterLabel(String filterLabel) {
		this.filterLabel = filterLabel;
	}
	public String getFilterType() {
		return filterType;
	}
	public void setFilterType(String filterType) {
		this.filterType = filterType;
	}
	public LinkedHashMap<String, String> getFilterSourceVal() {
		return filterSourceVal;
	}
	public void setFilterSourceVal(LinkedHashMap<String, String> filterSourceVal) {
		this.filterSourceVal = filterSourceVal;
	}
	public String getFilterDateFormat() {
		return filterDateFormat;
	}
	public void setFilterDateFormat(String filterDateFormat) {
		this.filterDateFormat = filterDateFormat;
	}
	public String getFilterSourceId() {
		return filterSourceId;
	}
	public void setFilterSourceId(String filterSourceId) {
		this.filterSourceId = filterSourceId;
	}
	public String getSpecificTab() {
		return specificTab;
	}
	public void setSpecificTab(String specificTab) {
		this.specificTab = specificTab;
	}
	public String getMultiSelect() {
		return multiSelect;
	}
	public void setMultiSelect(String multiSelect) {
		this.multiSelect = multiSelect;
	}
	public String getUserRestrictionXml() {
		return userRestrictionXml;
	}
	public void setUserRestrictionXml(String userRestrictionXml) {
		this.userRestrictionXml = userRestrictionXml;
	}
	public String getUserRestrictFlag() {
		return userRestrictFlag;
	}
	public void setUserRestrictFlag(String userRestrictFlag) {
		this.userRestrictFlag = userRestrictFlag;
	}
	public int getFilterSeq() {
		return filterSeq;
	}
	public void setFilterSeq(int filterSeq) {
		this.filterSeq = filterSeq;
	}
	public String getDependencyFlag() {
		return dependencyFlag;
	}
	public void setDependencyFlag(String dependencyFlag) {
		this.dependencyFlag = dependencyFlag;
	}
	public String getDependentPrompt() {
		return dependentPrompt;
	}
	public void setDependentPrompt(String dependentPrompt) {
		this.dependentPrompt = dependentPrompt;
	}
	public String getMultiWidth() {
		return multiWidth;
	}
	public void setMultiWidth(String multiWidth) {
		this.multiWidth = multiWidth;
	}
	public String getFilter1Val() {
		return filter1Val;
	}
	public void setFilter1Val(String filter1Val) {
		this.filter1Val = filter1Val;
	}
	public String getFilter2Val() {
		return filter2Val;
	}
	public void setFilter2Val(String filter2Val) {
		this.filter2Val = filter2Val;
	}
	public String getFilter3Val() {
		return filter3Val;
	}
	public void setFilter3Val(String filter3Val) {
		this.filter3Val = filter3Val;
	}
	public String getFilter4Val() {
		return filter4Val;
	}
	public void setFilter4Val(String filter4Val) {
		this.filter4Val = filter4Val;
	}
	public String getFilter5Val() {
		return filter5Val;
	}
	public void setFilter5Val(String filter5Val) {
		this.filter5Val = filter5Val;
	}
	public String getFilter6Val() {
		return filter6Val;
	}
	public void setFilter6Val(String filter6Val) {
		this.filter6Val = filter6Val;
	}
	public String getFilter7Val() {
		return filter7Val;
	}
	public void setFilter7Val(String filter7Val) {
		this.filter7Val = filter7Val;
	}
	public String getFilter8Val() {
		return filter8Val;
	}
	public void setFilter8Val(String filter8Val) {
		this.filter8Val = filter8Val;
	}
	public String getFilter9Val() {
		return filter9Val;
	}
	public void setFilter9Val(String filter9Val) {
		this.filter9Val = filter9Val;
	}
	public String getFilter10Val() {
		return filter10Val;
	}
	public void setFilter10Val(String filter10Val) {
		this.filter10Val = filter10Val;
	}
	public String getFilterRow() {
		return filterRow;
	}
	public void setFilterRow(String filterRow) {
		this.filterRow = filterRow;
	}
	public String getFilterDateRestrict() {
		return filterDateRestrict;
	}
	public void setFilterDateRestrict(String filterDateRestrict) {
		this.filterDateRestrict = filterDateRestrict;
	}
	public String getFilterString() {
		return filterString;
	}
	public void setFilterString(String filterString) {
		this.filterString = filterString;
	}
	public LinkedHashMap<String, String> getFilterDefaultValue() {
		return filterDefaultValue;
	}
	public void setFilterDefaultValue(LinkedHashMap<String, String> filterDefaultValue) {
		this.filterDefaultValue = filterDefaultValue;
	}
	/*public String getFilterDefaultValue() {
		return filterDefaultValue;
	}
	public void setFilterDefaultValue(String filterDefaultValue) {
		this.filterDefaultValue = filterDefaultValue;
	}*/
	public String getDefaultValueId() {
		return defaultValueId;
	}
	public void setDefaultValueId(String defaultValueId) {
		this.defaultValueId = defaultValueId;
	}
	public LinkedHashMap<String, String> getFilterValueMap() {
		return filterValueMap;
	}
	public void setFilterValueMap(LinkedHashMap<String, String> filterValueMap) {
		this.filterValueMap = filterValueMap;
	}
	public Boolean getGlobalFilter() {
		return globalFilter;
	}
	public void setGlobalFilter(Boolean globalFilter) {
		this.globalFilter = globalFilter;
	}
}

