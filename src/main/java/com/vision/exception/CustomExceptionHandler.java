package com.vision.exception;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.vision.util.Constants;

@RestControllerAdvice
public class CustomExceptionHandler extends ResponseEntityExceptionHandler{
	@Autowired
	ExceptionCode exceptionCode;
	
	@ExceptionHandler(Exception.class)
	public final ResponseEntity<JSONExceptionCode> genericExceptionHandler(Exception ex){
		exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		exceptionCode.setErrorMsg(ex.getMessage());
		return new ResponseEntity<JSONExceptionCode>(new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),null), HttpStatus.EXPECTATION_FAILED);
	}
	
	@ExceptionHandler(RuntimeCustomException.class)
	public final ResponseEntity<JSONExceptionCode> runtimeCustomExceptionHandler(RuntimeCustomException ex){
		exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		exceptionCode.setErrorMsg(ex.getMessage());
		return new ResponseEntity<JSONExceptionCode>(new JSONExceptionCode(exceptionCode.getErrorCode(), exceptionCode.getErrorMsg(),null), HttpStatus.EXPECTATION_FAILED);
	}

}