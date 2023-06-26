package com.vision.vb;

import java.util.List;

public class VcConfigMainCRUDVb extends CommonVb {

	private String catalogId;
	private String catalogDesc = "";
	private int joinClauseNt = 2001;
	private int joinClause = -1;
	private String baseTableJoinFlag = "";
	private int vcStatusNt = 2000;
	private int vcStatus = -1;
	private String vcStatusDesc = "";

	private List<VcConfigMainTreeVb> addModifyMetadata = null;

	private List<VcConfigMainRelationVb> relationAddModifyMetadata = null;
	
	private List<TableDeleteVb> tableDeleteMetadata = null;
	
	private List<ColumnDeleteVb> columnDeleteMetadata = null;
	
	private List<RelationDeleteVb> relationDeleteMetadata = null;

	public String getCatalogId() {
		return catalogId;
	}

	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}

	public String getCatalogDesc() {
		return catalogDesc;
	}

	public void setCatalogDesc(String catalogDesc) {
		this.catalogDesc = catalogDesc;
	}

	public int getJoinClauseNt() {
		return joinClauseNt;
	}

	public void setJoinClauseNt(int joinClauseNt) {
		this.joinClauseNt = joinClauseNt;
	}

	public int getJoinClause() {
		return joinClause;
	}

	public void setJoinClause(int joinClause) {
		this.joinClause = joinClause;
	}

	public String getBaseTableJoinFlag() {
		return baseTableJoinFlag;
	}

	public void setBaseTableJoinFlag(String baseTableJoinFlag) {
		this.baseTableJoinFlag = baseTableJoinFlag;
	}

	public int getVcStatusNt() {
		return vcStatusNt;
	}

	public void setVcStatusNt(int vcStatusNt) {
		this.vcStatusNt = vcStatusNt;
	}

	public int getVcStatus() {
		return vcStatus;
	}

	public void setVcStatus(int vcStatus) {
		this.vcStatus = vcStatus;
	}

	public String getVcStatusDesc() {
		return vcStatusDesc;
	}

	public void setVcStatusDesc(String vcStatusDesc) {
		this.vcStatusDesc = vcStatusDesc;
	}

	public List<VcConfigMainTreeVb> getAddModifyMetadata() {
		return addModifyMetadata;
	}

	public void setAddModifyMetadata(List<VcConfigMainTreeVb> addModifyMetadata) {
		this.addModifyMetadata = addModifyMetadata;
	}

	public List<VcConfigMainRelationVb> getRelationAddModifyMetadata() {
		return relationAddModifyMetadata;
	}

	public void setRelationAddModifyMetadata(List<VcConfigMainRelationVb> relationAddModifyMetadata) {
		this.relationAddModifyMetadata = relationAddModifyMetadata;
	}

	public List<TableDeleteVb> getTableDeleteMetadata() {
		return tableDeleteMetadata;
	}

	public void setTableDeleteMetadata(List<TableDeleteVb> tableDeleteMetadata) {
		this.tableDeleteMetadata = tableDeleteMetadata;
	}

	public List<ColumnDeleteVb> getColumnDeleteMetadata() {
		return columnDeleteMetadata;
	}

	public void setColumnDeleteMetadata(List<ColumnDeleteVb> columnDeleteMetadata) {
		this.columnDeleteMetadata = columnDeleteMetadata;
	}

	public List<RelationDeleteVb> getRelationDeleteMetadata() {
		return relationDeleteMetadata;
	}

	public void setRelationDeleteMetadata(List<RelationDeleteVb> relationDeleteMetadata) {
		this.relationDeleteMetadata = relationDeleteMetadata;
	}

}