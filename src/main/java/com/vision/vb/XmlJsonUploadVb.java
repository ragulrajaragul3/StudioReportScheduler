package com.vision.vb;

import java.util.HashMap;
import java.util.Map;

public class XmlJsonUploadVb {

	private String data = "";
	private String style = "";
	private Map<String, Object> property = new HashMap<String, Object>();
	private String rowSpan = "";
	private String colSpan = "";

	public XmlJsonUploadVb() {
	}

	public XmlJsonUploadVb(String data) {
		super();
		this.data = data;
	}
	
	public XmlJsonUploadVb(String data, Map<String, Object> property) {
		super();
		this.data = data;
		this.property = property;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public String getStyle() {
		return style;
	}

	public void setStyle(String style) {
		this.style = style;
	}

	public Map<String, Object> getProperty() {
		return property;
	}

	public void setProperty(Map<String, Object> property) {
		this.property = property;
	}

	public String getRowSpan() {
		return rowSpan;
	}

	public void setRowSpan(String rowSpan) {
		this.rowSpan = rowSpan;
	}

	public String getColSpan() {
		return colSpan;
	}

	public void setColSpan(String colSpan) {
		this.colSpan = colSpan;
	}

}