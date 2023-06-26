package com.vision.vb;

import java.util.List;

public class VcConfigMainVb extends CommonVb{

	private static final long serialVersionUID = -5022264255321929212L;

	/*Vision_Catalog_SelfBI*/
	private String catalogId;
	private String catalogDesc = "";
	private int joinClauseNt = 2001;
	private int joinClause = -1;
	private String baseTableJoinFlag = "";
	private int vcStatusNt = 2000;
	private int vcStatus = -1;
	private String vcStatusDesc = "";
	
	public List<SmartSearchVb> smartSearchVb=null;
	
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
	public List<SmartSearchVb> getSmartSearchVb() {
		return smartSearchVb;
	}
	public void setSmartSearchVb(List<SmartSearchVb> smartSearchVb) {
		this.smartSearchVb = smartSearchVb;
	}
	
}