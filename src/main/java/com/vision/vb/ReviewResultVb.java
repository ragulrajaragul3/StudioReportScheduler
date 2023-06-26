package com.vision.vb;

public class ReviewResultVb {
	private String columnName = "";
	private String pendingColumnData = "";
	private String approvedColumnData = "";
	private boolean dataDiffer = false;
	
	public ReviewResultVb(String columnName, String pendingColumnData, String approvedColumnData) {
		super();
		this.columnName = columnName;
		this.pendingColumnData = pendingColumnData;
		this.approvedColumnData = approvedColumnData;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getPendingColumnData() {
		return pendingColumnData;
	}
	public void setPendingColumnData(String pendingColumnData) {
		this.pendingColumnData = pendingColumnData;
	}
	public String getApprovedColumnData() {
		return approvedColumnData;
	}
	public void setApprovedColumnData(String approvedColumnData) {
		this.approvedColumnData = approvedColumnData;
	}
	public boolean isDataDiffer(){
		return dataDiffer;
	}
	public void setDataDiffer(boolean dataDiffer){
		this.dataDiffer = dataDiffer;
	}

}
