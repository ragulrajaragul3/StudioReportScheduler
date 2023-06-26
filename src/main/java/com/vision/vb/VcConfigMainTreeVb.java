package com.vision.vb;

import java.util.List;

public class VcConfigMainTreeVb {
	
	private String catalogId = "";
	private String tableId = "";
	private String tableName = "";
	private String aliasName = "";
	private String baseTableFlag = "";
	private String queryId = "";
	private int databaseTypeAt = 1082;
	private String databaseType = "";
	private String databaseConnectivityDetails = "";
	private String sortTree = "";
	private int vctStatusNt = 1;
	private int vctStatus = -1;
	private String tableSourceType = "";
	
	/* Access Control Script Sample
	<categories>
	    <auth>
	        <category>COUNTRY-LE_BOOK</category>
	        <COUNTRY>COUNTRY</COUNTRY>
	        <LE_BOOK>LE_BOOK</LE_BOOK>
	    </auth>
	    <auth>
	        <category>COUNTRY</category>
	        <COUNTRY>COUNTRY</COUNTRY>
	    </auth>
	</categories>
	*/
	
	private String accessControlScript = "";
	private List<VcConfigMainColumnsVb> children = null;
	private List<UserRestrictionVb> accessControlScriptParsed = null;
	private String accessControlFlag = "";
	private String variableScript = "";
	
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
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getAliasName() {
		return aliasName;
	}
	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}
	public String getBaseTableFlag() {
		return baseTableFlag;
	}
	public void setBaseTableFlag(String baseTableFlag) {
		this.baseTableFlag = baseTableFlag;
	}
	public String getQueryId() {
		return queryId;
	}
	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}
	public int getDatabaseTypeAt() {
		return databaseTypeAt;
	}
	public void setDatabaseTypeAt(int databaseTypeAt) {
		this.databaseTypeAt = databaseTypeAt;
	}
	public String getDatabaseType() {
		return databaseType;
	}
	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}
	public String getDatabaseConnectivityDetails() {
		return databaseConnectivityDetails;
	}
	public void setDatabaseConnectivityDetails(String databaseConnectivityDetails) {
		this.databaseConnectivityDetails = databaseConnectivityDetails;
	}
	public String getSortTree() {
		return sortTree;
	}
	public void setSortTree(String sortTree) {
		this.sortTree = sortTree;
	}
	public int getVctStatusNt() {
		return vctStatusNt;
	}
	public void setVctStatusNt(int vctStatusNt) {
		this.vctStatusNt = vctStatusNt;
	}
	public int getVctStatus() {
		return vctStatus;
	}
	public void setVctStatus(int vctStatus) {
		this.vctStatus = vctStatus;
	}
	public String getTableSourceType() {
		return tableSourceType;
	}
	public void setTableSourceType(String tableSourceType) {
		this.tableSourceType = tableSourceType;
	}
	public String getAccessControlScript() {
		return accessControlScript;
	}
	public void setAccessControlScript(String accessControlScript) {
		this.accessControlScript = accessControlScript;
	}
	public List<VcConfigMainColumnsVb> getChildren() {
		return children;
	}
	public void setChildren(List<VcConfigMainColumnsVb> children) {
		this.children = children;
	}
	public List<UserRestrictionVb> getAccessControlScriptParsed() {
		return accessControlScriptParsed;
	}
	public void setAccessControlScriptParsed(List<UserRestrictionVb> accessControlScriptParsed) {
		this.accessControlScriptParsed = accessControlScriptParsed;
	}
	public String getAccessControlFlag() {
		return accessControlFlag;
	}
	public void setAccessControlFlag(String accessControlFlag) {
		this.accessControlFlag = accessControlFlag;
	}
	public String getVariableScript() {
		return variableScript;
	}
	public void setVariableScript(String variableScript) {
		this.variableScript = variableScript;
	}
}
