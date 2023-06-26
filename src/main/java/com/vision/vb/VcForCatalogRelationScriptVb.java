package com.vision.vb;

public class VcForCatalogRelationScriptVb {
	
	private String fColumn="";
	private String tColumn="";
	
	public String getfColumn() {
		return fColumn;
	}
	public void setfColumn(String fColumn) {
		this.fColumn = fColumn;
	}
	public String gettColumn() {
		return tColumn;
	}
	public void settColumn(String tColumn) {
		this.tColumn = tColumn;
	}

	public VcForCatalogRelationScriptVb(String fColumn, String tColumn) {
		super();
		this.fColumn = fColumn;
		this.tColumn = tColumn;
	}
	
}
