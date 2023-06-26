package com.vision.vb;

import java.util.List;

public class VcMainDataSourceMetaDataVb {
	
	private String tableName = "";
	private String columnName = "";
	private String columnType = "";
	private String databaseConnectivityDetails = "";
	private String jsonFormationFor = "";
	private List<VcMainDataSourceMetaDataVb> children;
	
	public VcMainDataSourceMetaDataVb() {}
	
	public VcMainDataSourceMetaDataVb(String tableName, String columnName, List<VcMainDataSourceMetaDataVb> children) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.setChildren(children);
	}
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getColumnType() {
		return columnType;
	}
	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}
	public String getDatabaseConnectivityDetails() {
		return databaseConnectivityDetails;
	}
	public void setDatabaseConnectivityDetails(String databaseConnectivityDetails) {
		this.databaseConnectivityDetails = databaseConnectivityDetails;
	}
	public String getJsonFormationFor() {
		return jsonFormationFor;
	}
	public void setJsonFormationFor(String jsonFormationFor) {
		this.jsonFormationFor = jsonFormationFor;
	}

	public List<VcMainDataSourceMetaDataVb> getChildren() {
		return children;
	}

	public void setChildren(List<VcMainDataSourceMetaDataVb> children) {
		this.children = children;
	}

}