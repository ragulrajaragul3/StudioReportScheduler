package com.vision.vb;

public class NumSubTabVb extends CommonVb{

	private static final long serialVersionUID = -7814611304253747452L;
	
	private int numTab = 0;
	private int numSubTab = -1;
	private String description = ""; 
	private int numSubTabStatusNt = 0;
	private int numSubTabStatus = -1;
	
	//List<AlphaSubTabVb> children = null;
	public int getNumTab() {
		return numTab;
	}
	public void setNumTab(int numTab) {
		this.numTab = numTab;
	}
	public int getNumSubTab() {
		return numSubTab;
	}
	public void setNumSubTab(int numSubTab) {
		this.numSubTab = numSubTab;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public int getNumSubTabStatusNt() {
		return numSubTabStatusNt;
	}
	public void setNumSubTabStatusNt(int numSubTabStatusNt) {
		this.numSubTabStatusNt = numSubTabStatusNt;
	}
	public int getNumSubTabStatus() {
		return numSubTabStatus;
	}
	public void setNumSubTabStatus(int numSubTabStatus) {
		this.numSubTabStatus = numSubTabStatus;
	}
	/*public List<AlphaSubTabVb> getChildren() {
		return children;
	}
	public void setChildren(List<AlphaSubTabVb> children) {
		this.children = children;
	}*/
	
}
