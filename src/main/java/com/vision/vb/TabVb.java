package com.vision.vb;

import java.util.List;


public class TabVb extends CommonVb {


	private static final long serialVersionUID = 1L;
	private int tab = 0;
	private String tabDescription = "";
	private int tabStatus = 0;
	private int tabStatusNt = 0;
	private List<AlphaSubTabVb> alphaSubTabs = null;
	private List<NumSubTabVb> numSubTabs = null;
	private String requestType = "numTab"; 
	private int pendingRecordsCount = 0;
	private long totRecordsCount = 0;
	private String readOnly = "";
	
	public boolean compare(TabVb vObject){
		if(this.tab != vObject.tab ||
				!this.tabDescription.equals(vObject.tabDescription)){
			return false;
		}
		return true;
	}
	public int getTab() {
		return tab;
	}
	public void setTab(int tab) {
		this.tab = tab;
	}
	public String getTabDescription() {
		return tabDescription;
	}
	public void setTabDescription(String tabDescription) {
		this.tabDescription = tabDescription;
	}
	public int getTabStatus() {
		return tabStatus;
	}
	public void setTabStatus(int tabStatus) {
		this.tabStatus = tabStatus;
	}
	public int getTabStatusNt() {
		return tabStatusNt;
	}
	public void setTabStatusNt(int tabStatusNt) {
		this.tabStatusNt = tabStatusNt;
	}
	public List<AlphaSubTabVb> getAlphaSubTabs() {
		return alphaSubTabs;
	}
	public void setAlphaSubTabs(List<AlphaSubTabVb> alphaSubTabs) {
		this.alphaSubTabs = alphaSubTabs;
	}
	public List<NumSubTabVb> getNumSubTabs() {
		return numSubTabs;
	}
	public void setNumSubTabs(List<NumSubTabVb> numSubTabs) {
		this.numSubTabs = numSubTabs;
	}
	public String getRequestType() {
		return requestType;
	}
	public void setRequestType(String requestType) {
		this.requestType = requestType;
	}
	public int getPendingRecordsCount() {
		return pendingRecordsCount;
	}
	public void setPendingRecordsCount(int pendingRecordsCount) {
		this.pendingRecordsCount = pendingRecordsCount;
	}
	public long getTotRecordsCount() {
		return totRecordsCount;
	}
	public void setTotRecordsCount(long totRecordsCount) {
		this.totRecordsCount = totRecordsCount;
	}
	public String getReadOnly() {
		return readOnly;
	}
	public void setReadOnly(String readOnly) {
		this.readOnly = readOnly;
	}
}
