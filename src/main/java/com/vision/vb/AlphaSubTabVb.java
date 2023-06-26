package com.vision.vb;

import java.util.List;

public class AlphaSubTabVb extends CommonVb{
	private static final long serialVersionUID = -3560810828175889720L;
	private int alphaTab = 0;
	private String alphaSubTab = "";
	private String description = ""; 
	private int alphaSubTabStatusNt = 0;
	private int alphaSubTabStatus = -1;
	
	List<AlphaSubTabVb> children = null;
	
	public int getAlphaTab() {
		return alphaTab;
	}
	public void setAlphaTab(int alphaTab) {
		this.alphaTab = alphaTab;
	}
	public String getAlphaSubTab() {
		return alphaSubTab;
	}
	public void setAlphaSubTab(String alphaSubTab) {
		this.alphaSubTab = alphaSubTab;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public int getAlphaSubTabStatusNt() {
		return alphaSubTabStatusNt;
	}
	public void setAlphaSubTabStatusNt(int alphaSubTabStatusNt) {
		this.alphaSubTabStatusNt = alphaSubTabStatusNt;
	}
	public int getAlphaSubTabStatus() {
		return alphaSubTabStatus;
	}
	public void setAlphaSubTabStatus(int alphaSubTabStatus) {
		this.alphaSubTabStatus = alphaSubTabStatus;
	}
	public List<AlphaSubTabVb> getChildren() {
		return children;
	}
	public void setChildren(List<AlphaSubTabVb> children) {
		this.children = children;
	}
}
