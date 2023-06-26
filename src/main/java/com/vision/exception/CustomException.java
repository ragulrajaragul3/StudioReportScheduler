package com.vision.exception;

public class CustomException extends Exception{
	
	private static final long serialVersionUID = 1L;
	Integer errorCode = null;
	String errorMsg = "Unknown Error";
	private Object otherInfo = null;
	private Object request = null;
	private Object response = null;
	
	public CustomException(Integer errorCode, String errorMsg){
		this.errorCode=errorCode;
		this.errorMsg=errorMsg;
	}
	
	public Integer getErrorCode() {
		return errorCode;
	}
	public void setErrorCode(Integer errorCode) {
		this.errorCode = errorCode;
	}
	public String getErrorMsg() {
		return errorMsg;
	}
	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}
	public Object getOtherInfo() {
		return otherInfo;
	}
	public void setOtherInfo(Object otherInfo) {
		this.otherInfo = otherInfo;
	}
	public Object getRequest() {
		return request;
	}
	public void setRequest(Object request) {
		this.request = request;
	}
	public Object getResponse() {
		return response;
	}
	public void setResponse(Object response) {
		this.response = response;
	}
}