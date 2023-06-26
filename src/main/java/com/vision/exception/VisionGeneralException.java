package com.vision.exception;

public class VisionGeneralException extends RuntimeException{
	
	private int applicationStatusCode;
	private String applicationStatusMessage;

	public VisionGeneralException(int applicationStatusCode, String applicationStatusMessage) {
		super(applicationStatusMessage);
		this.applicationStatusCode = applicationStatusCode;
		this.applicationStatusMessage = applicationStatusMessage;
	}

	public String getApplicationStatusMessage() {
		return applicationStatusMessage;
	}

	public int getApplicationStatusCode() {
		return applicationStatusCode;
	}
	
	
}
