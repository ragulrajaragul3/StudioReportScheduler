package com.vision.vb;

public class WidgetFilterVb {

	private String columnName = "";
	private String condition = "";
	private String displayName = "";
	private String type = "";
	private String value1 = "";
	private String value2 = "";
	private boolean globalFilterFlag = false;
	private String dateFormattingSyntax = "";
	private String dateConversionSyntax = "";

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue1() {
		return value1;
	}

	public void setValue1(String value1) {
		this.value1 = value1;
	}

	public String getValue2() {
		return value2;
	}

	public void setValue2(String value2) {
		this.value2 = value2;
	}

	public boolean isGlobalFilterFlag() {
		return globalFilterFlag;
	}

	public void setGlobalFilterFlag(boolean globalFilterFlag) {
		this.globalFilterFlag = globalFilterFlag;
	}

	public String getDateFormattingSyntax() {
		return dateFormattingSyntax;
	}

	public void setDateFormattingSyntax(String dateFormattingSyntax) {
		this.dateFormattingSyntax = dateFormattingSyntax;
	}

	public String getDateConversionSyntax() {
		return dateConversionSyntax;
	}

	public void setDateConversionSyntax(String dateConversionSyntax) {
		this.dateConversionSyntax = dateConversionSyntax;
	}

}
