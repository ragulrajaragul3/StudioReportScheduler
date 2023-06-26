package com.vision.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

public class Constants {
	public static final int STATUS_ZERO, ERRONEOUS_OPERATION = STATUS_ZERO = 0;
	public static final int SUCCESSFUL_OPERATION,STATUS_UPDATE = SUCCESSFUL_OPERATION = 1;
	public static final int WE_HAVE_WARNING_DESCRIPTION = 36;
	public static final int INVALID_STATUS_FLAG_IN_DATABASE, STATUS_INSERT = INVALID_STATUS_FLAG_IN_DATABASE = 2;
	public static final int DUPLICATE_KEY_INSERTION,STATUS_DELETE = DUPLICATE_KEY_INSERTION = 3;
	public static final int STATUS_PENDING, RECORD_PENDING_FOR_DELETION = STATUS_PENDING = 4;
	public static final int ATTEMPT_TO_MODIFY_UNEXISTING_RECORD = 5;
	public static final int ATTEMPT_TO_DELETE_UNEXISTING_RECORD = 6;
	public static final int TRYING_TO_DELETE_APPROVAL_PENDING_RECORD = 7;
	public static final int NO_SUCH_PENDING_RECORD = 8;
	public static final int PENDING_FOR_ADD_ALREADY, PASSIVATE = PENDING_FOR_ADD_ALREADY = 9;
	public static final int RECORD_ALREADY_PRESENT_BUT_INACTIVE = 10;
	public static final int CANNOT_DELETE_AN_INACTIVE_RECORD = 11;
	public static final int NO_RECORDS_TO_APPROVE = 12;
	public static final int NO_RECORDS_TO_REJECT = 13;
	public static final int MAKER_CANNOT_APPROVE = 14;
	public static final int WE_HAVE_ERROR_DESCRIPTION = 20;
	public static final int PAGE_DISPLAY_SIZE, VALIDATION_NO_ERRORS = PAGE_DISPLAY_SIZE = 25;
	public static final int VALIDATION_ERRORS_FOUND = 26;
	public static final int AUDIT_TRAIL_ERROR = 30;
	public static final int BUILD_IS_RUNNING = 38;
	public static final int BUILD_IS_RUNNING_APPROVE = 39;
	public static final int FILE_UPLOAD_REMOTE_ERROR = 47;
	public static final int NO_RECORDS_FOUND = 41;
	public static final int FIN_ADJ_APPROVE_STATUS = 40;
	public static final int FIN_ADJ_INACTIVE_STATUS = 50;
	public static final int FIN_ADJ_DELETE_STATUS = 60;
	public static final int FINANCIAL_FETCH_SIZE = 500;
	public static final int YEAR_NOT_MAINTAIN = 49;
	public static final int LONG_MAX_LENGTH=19;
	public static final String ADD = "Add";
	public static final String MODIFY = "Modify";
	public static final String DELETE = "Delete";
	public static final String APPROVE = "Approve";
	public static final String REJECT = "Reject";
	public static final String COPY = "Copy";
	public static final String MOVE = "Move";
	public static final String QUERY = "Query";
	public static final String SAVE = "Save";
	public static final String Verification = "verification";
	public static final String Resubmission = "resubmission";
	public static final String Rejection = "rejection";
	public static final int DB_CONNECTION_ERROR = 50;
	public static final String EMPTY = "";
	public static final String CREATE = "Created";
	public static final String CLEAR = "Clear";
	public static final int BLANK_SUCCESSFUL_EXIT = 4;
	public static final int BLANK_ERRONEOUS_EXIT = 3;
	public static final int ERROR_OPERATION = 1;
	public static final int SUCCESS_OPERATION = 0;
	public static final String VISION_SERVER_ENVIRONMENT = System.getenv("VISION_SERVER_ENVIRONMENT");
	
	public static final String VISION_APP_TIME_ZONE = StringUtils.isNotBlank(System.getenv("VISION_APP_TIME_ZONE")) ? System.getenv("VISION_APP_TIME_ZONE") : "Asia/Dubai" ;
	//public static final String VISION_APP_TIME_ZONE = "Asia/Dubai";

	public static final String PASSWORD_ENCRYPT_FLAG = StringUtils.isNotBlank(
			System.getenv("VISION_PASSWORD_ENCRYPT_FLAG")) ? System.getenv("VISION_PASSWORD_ENCRYPT_FLAG") : "N";


	public static final String JDBC_URL_MAIN = System.getenv("VISION_APP_CONNECT_STRING");
	public static final String DATABASE_NAME = System.getenv("VISION_DATABASE_NAME");
	public static final String DATABASE_TYPE = System.getenv("VISION_DATABASE_TYPE");
	public static final String USER_NAME = System.getenv("VISION_USER_NAME");
	public static final String PASSWORD = ("Y".equals(PASSWORD_ENCRYPT_FLAG))
			? EncryptDecryptFunctions.passwordDecrypt(System.getenv("VISION_PASSWORD"))
			: System.getenv("VISION_PASSWORD");
	
	
	public static final String SERVER_OS_TYPE = StringUtils.isNotBlank(
			System.getenv("VISION_SERVER_OS_TYPE")) ? System.getenv("VISION_SERVER_OS_TYPE") : "WINDOWS";
			
	//System.getenv("VISION_SERVER_OS_TYPE");
	
	public static final String TABLE_PREFIX = StringUtils.isNotBlank(System.getenv("VISION_TABLE_PREFIX"))
			? System.getenv("VISION_TABLE_PREFIX")
			: "DBO.";
	public static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	
	public static HashMap<String, Object> macroHashVariables = new HashMap<String, Object>();
	
	
	/*public static final String JDBC_URL_MAIN = "jdbc:sqlserver://10.16.1.38;instance=VISIONBISQL2019;port=52866;DatabaseName=VISION_RA";
	public static final String DATABASE_TYPE = "MSSQL";
	public static final String PASSWORD = "Vision@123";
	public static final String USER_NAME = "vision";
	public static final String DATABASE_NAME = "VISION_RA";*/
	
}