package com.vision.wb;

import org.springframework.stereotype.Component;

@Component
public class DataVariables {

	public static String dbHost  =  "";
	public static String dbPort  =  "";
	public static String schema  =  "";
	public static String dbPassword  =  "";
	public static String serviceName  =  "";
	
	public static String ftpHostName  =  "";
	public static String ftpPortNo  =  "";
	public static String ftpUserName  =  "";
	public static String ftpPassWord  =  "";
	public static String ftpPath  =  "";
	
	public static String uploadLogFilePath  =  "";
	public static String uploadLogFileName  =  "";
	
	public static int retVal  =  0;
	public static int ERROR_OPERATION  =  1;
	public static int SUCCESS_OPERATION  =  0;
	public static int READINESS_ERRONEOUS_EXIT  =  2;
	public static String mailhost = "";
	public static int mailPort = -1;
	public static String mailUsername = "";
	public static String mailPassword = "";
	public static String folderPath  =  "";
	public static String assetFolderUrl  =  "";
	public static String vmFileUrl  =  "";
	public static String processId ="";
	public static Boolean debugMode =false;
	public static String versionNo ="";
	public static String readinessCheck ="";
	public static String rsMailTemplatePath = "";
	public static String ipAddress = "";
 	public static String dbServiceName = "";
	public static String serverIp = "";
	public static String serverHostName = "";
	public static String serverUser = "";
	public static String serverPassword = "";
	public static String hostName = "";
	public static String userName = "";
	public static String passWord = "";
	public static String serverPort = "";
	public static String dbOracleSid = "";
	public static String dbUserName = "";
	public static String dbPassWord = "";
	public static String dbPortNumber = "";
	public static String dbPostFix = "";
	public static String dbPreFix = "";
	public static String dataBaseType = "";
	public static String dbJdbcUrl  =  "";
	public static String dbIp = "";
	public static String dataBaseName = "";
	public static String dbIP = "";
	public static int connectionTestTimeout=3600;
	public static int extractionCount = 0;
	public static int connectivityTypeAt = 1081;
	public static String connectivityType = "MACROVAR";
	public static String connectivityDetails = "";
	public static String portNumber = "";
	public static int databaseTypeAt = 1082;
	public static String databaseType = "MACROVAR";
	public static String databaseConnectivityDetails = "";
	public static String dbInstance = "";
	public static String serverName = "";
	public static String dbSetParam1 = "";
	public static String dbSetParam2 = "";
	public static String dbSetParam3 = "";
	public static String version = "";
	public static String acquisitionReadinessScripts = "";
	public static String unixScriptDir = "/home/vision/execs/";
	public static String ftpServerIp = "";
	public static String ftpServerHostName = "";
	public static String ftpServerUser = "";
	public static String ftpServerPassword = "";
	public static String ftpServerPath = "";
	public static String ftpServerPort = "";
	public static String ftpServerType = "";
	public static String ftpType = "";
	
	static String strErrorDesc =  "";
	static String currentUser = "";
	static String fileSize = "";
	
	public static String jdbcUrl  =  "";
	public static String username  =  System.getenv("VISION_USER_NAME");
	public static String password  =  System.getenv("VISION_PASSWORD");
	public static String jdbcUrlMain  =  System.getenv("VISION_APP_CONNECT_STRING");
	public static String classPath = System.getenv("VISION_CLASS_PATH");
}