/**
 * 
 */
package com.vision.vb;

public class VisionVariablesVb extends CommonVb{

	private static final long serialVersionUID = 2606607394848409278L;
	private String variable = "";//VARIABLE - Key Field
	private String value = "";//VALUE
	private int	variableStatusNt =  0;//VARIABLE_STATUS_NT
	private int	variableStatus =  -1;//VARIABLE_STATUS
	private String readOnly = "";
	
	
	public String getVariable() {
		return variable;
	}
	public void setVariable(String variable) {
		this.variable = variable;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public int getVariableStatusNt() {
		return variableStatusNt;
	}
	public void setVariableStatusNt(int variableStatusNt) {
		this.variableStatusNt = variableStatusNt;
	}
	public int getVariableStatus() {
		return variableStatus;
	}
	public void setVariableStatus(int variableStatus) {
		this.variableStatus = variableStatus;
	}
	public String getReadOnly() {
		return readOnly;
	}
	public void setReadOnly(String readOnly) {
		this.readOnly = readOnly;
	}
}
