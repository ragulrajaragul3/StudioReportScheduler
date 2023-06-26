package com.vision.vb;


public class AuditTrailDataVb extends CommonVb {

	private static final long serialVersionUID = -7945729916841965939L;
	// Field declarations start here 
	private String referenceNo = ""; //REFERENCE_NO - Key Field
	private String subReferenceNo = ""; //SUB_REFERENCE_NO - Key Field
	private String tableName = ""; //TABLE_NAME
	private String childTableName = ""; //CHILD_TABLE_NAME
	private int tableSequence =  0;	//TABLE_SEQUENCE
	private String auditMode = "";	//AUDIT_MODE
	private String auditDataOld = ""; //AUDIT_DATA_OLD
	private String auditDataNew = ""; //AUDIT_DATA_NEW
	private String tablecolumns = "";//Non db field
	private String fromDate = "";
	private String toDate = "";
	private String serialNo = "";
	protected int maxRecords = 50;
	private boolean oldLogic = false;
	public String getReferenceNo() {
		return referenceNo;
	}
	public void setReferenceNo(String referenceNo) {
		this.referenceNo = referenceNo;
	}
	public String getSubReferenceNo() {
		return subReferenceNo;
	}
	public void setSubReferenceNo(String subReferenceNo) {
		this.subReferenceNo = subReferenceNo;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getChildTableName() {
		return childTableName;
	}
	public void setChildTableName(String childTableName) {
		this.childTableName = childTableName;
	}
	public int getTableSequence() {
		return tableSequence;
	}
	public void setTableSequence(int tableSequence) {
		this.tableSequence = tableSequence;
	}
	public String getAuditMode() {
		return auditMode;
	}
	public void setAuditMode(String auditMode) {
		this.auditMode = auditMode;
	}
	public String getAuditDataOld() {
		return auditDataOld;
	}
	public void setAuditDataOld(String auditDataOld) {
		this.auditDataOld = auditDataOld;
	}
	public String getAuditDataNew() {
		return auditDataNew;
	}
	public void setAuditDataNew(String auditDataNew) {
		this.auditDataNew = auditDataNew;
	}
	public String getTablecolumns() {
		return tablecolumns;
	}
	public void setTablecolumns(String tablecolumns) {
		this.tablecolumns = tablecolumns;
	}
	public String getFromDate() {
		return fromDate;
	}
	public void setFromDate(String fromDate) {
		this.fromDate = fromDate;
	}
	public String getToDate() {
		return toDate;
	}
	public void setToDate(String toDate) {
		this.toDate = toDate;
	}
	@Override
	public int getMaxRecords() {
		return maxRecords;
	}
	@Override
	public void setMaxRecords(int maxRecords) {
		this.maxRecords = maxRecords;
	}
	public String getSerialNo() {
		return serialNo;
	}
	public void setSerialNo(String serialNo) {
		this.serialNo = serialNo;
	}
	public boolean isOldLogic() {
		return oldLogic;
	}
	public void setOldLogic(boolean oldLogic) {
		this.oldLogic = oldLogic;
	}
}
