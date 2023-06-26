package com.vision.vb;

import java.util.List;

public class VcConfigMainRelationVb {
	
	private String catalogId = "";
	private String fromTableId = "";
	private String toTableId = "";
	private String fromTableName = "";
	private String toTableName = "";
	private int joinTypeNt = 41;
	private int joinType = -1;
	private String filterCondition = "";
	private String customJoinString = "";
	private String relationScript = "";
	
	/* Sample XML */
	/*<relation>
	    <columnmapping>
	        <column>
	            <fcolumn>Country</fcolumn>
	            <tcolumn>Country</tcolumn>
	        </column>
	        <column>
	            <fcolumn>Le_Book</fcolumn>
	            <tcolumn>Le_Book</tcolumn>
	        </column>
	        <column>
	            <fcolumn>Year_Month</fcolumn>
	            <tcolumn>Year_Month</tcolumn>
	        </column>
	        <column>
	            <fcolumn>Sequence_FD</fcolumn>
	            <tcolumn>Sequence_FD</tcolumn>
	        </column>
	    </columnmapping>
	    <customjoin></customjoin>
	    <ansii_joinstring>(Fin_Dly_Headers.Country = Fin_Dly_Balances.Country AND Fin_Dly_Headers.Le_Book = Fin_Dly_Balances.Le_Book AND Fin_Dly_Headers.Year_Month = Fin_Dly_Balances.Year_Month AND Fin_Dly_Headers.Sequence_FD = Fin_Dly_Balances.Sequence_FD)</ansii_joinstring>
	    <std_joinstring>Fin_Dly_Headers.Country = Fin_Dly_Balances.Country AND Fin_Dly_Headers.Le_Book = Fin_Dly_Balances.Le_Book AND Fin_Dly_Headers.Year_Month = Fin_Dly_Balances.Year_Month AND Fin_Dly_Headers.Sequence_FD = Fin_Dly_Balances.Sequence_FD </std_joinstring>
	</relation>*/
	
	private List<RelationMapVb> relationScriptParsed = null;
	private int vcrStatusNt = 1;
	private int vcrStatus = 0;
	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
	public String getFromTableId() {
		return fromTableId;
	}
	public void setFromTableId(String fromTableId) {
		this.fromTableId = fromTableId;
	}
	public String getToTableId() {
		return toTableId;
	}
	public void setToTableId(String toTableId) {
		this.toTableId = toTableId;
	}
	public String getFromTableName() {
		return fromTableName;
	}
	public void setFromTableName(String fromTableName) {
		this.fromTableName = fromTableName;
	}
	public String getToTableName() {
		return toTableName;
	}
	public void setToTableName(String toTableName) {
		this.toTableName = toTableName;
	}
	public int getJoinTypeNt() {
		return joinTypeNt;
	}
	public void setJoinTypeNt(int joinTypeNt) {
		this.joinTypeNt = joinTypeNt;
	}
	public int getJoinType() {
		return joinType;
	}
	public void setJoinType(int joinType) {
		this.joinType = joinType;
	}
	public String getFilterCondition() {
		return filterCondition;
	}
	public void setFilterCondition(String filterCondition) {
		this.filterCondition = filterCondition;
	}
	public String getCustomJoinString() {
		return customJoinString;
	}
	public void setCustomJoinString(String customJoinString) {
		this.customJoinString = customJoinString;
	}
	public String getRelationScript() {
		return relationScript;
	}
	public void setRelationScript(String relationScript) {
		this.relationScript = relationScript;
	}
	public List<RelationMapVb> getRelationScriptParsed() {
		return relationScriptParsed;
	}
	public void setRelationScriptParsed(List<RelationMapVb> relationScriptParsed) {
		this.relationScriptParsed = relationScriptParsed;
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
}