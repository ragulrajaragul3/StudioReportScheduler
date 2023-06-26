package com.vision.vb;

import java.util.List;

public class VcConfigMainAliasVb {
	
	private Integer id;
	private Integer sort;
	private String name = "";
	private String aliasName = "";
	private String databaseType = "";
	private String databaseConnectivityDetails = "";
	private List<VcConfigMainAliasVb> children = null;
	
	public VcConfigMainAliasVb() {}
	
	public VcConfigMainAliasVb(Integer id, Integer sort, String name, String aliasName, String databaseType,
			String databaseConnectivityDetails, List<VcConfigMainAliasVb> children) {
		super();
		this.id = id;
		this.sort = sort;
		this.name = name;
		this.aliasName = aliasName;
		this.databaseType = databaseType;
		this.databaseConnectivityDetails = databaseConnectivityDetails;
		this.children = children;
	}
	
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public Integer getSort() {
		return sort;
	}
	public void setSort(Integer sort) {
		this.sort = sort;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAliasName() {
		return aliasName;
	}
	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
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
	public List<VcConfigMainAliasVb> getChildren() {
		return children;
	}
	public void setChildren(List<VcConfigMainAliasVb> children) {
		this.children = children;
	}

}
