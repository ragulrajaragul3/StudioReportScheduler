package com.vision.wb;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mail.MailAuthenticationException;
import org.springframework.mail.MailPreparationException;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import com.vision.dao.AbstractDao;
import com.vision.dao.CommonDao;
import com.vision.dao.ScheduleReportDao;
import com.vision.exception.CustomException;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.ColumnHeadersVb;
import com.vision.vb.RSSProcessControlVb;
import com.vision.vb.ReportsVb;
import com.vision.vb.VcForQueryReportFieldsWrapperVb;
import com.vision.vb.VcForQueryReportVb;
import com.vision.vb.VisionUsersVb;

import freemarker.template.Configuration;
@Component
public class ScheduledReportWb extends AbstractDynaWorkerBean<RSSProcessControlVb>{
	
	private static final Logger logger = LoggerFactory.getLogger(ScheduledReportWb.class);

	public JdbcTemplate jdbcTemplate = null;

	public ScheduledReportWb(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	FileWriter logfile = null;
	FileWriter mainLogfile = null;
	BufferedWriter bufferedWriter = null;
	
	
	public Connection readinessDbConnection = null;

	private JavaMailSenderImpl javaMailSender = new JavaMailSenderImpl();
	private JavaMailSenderImpl mailSender;

	public JavaMailSenderImpl getJavaMailSender() {
		return javaMailSender;
	}

	public void setJavaMailSender(JavaMailSenderImpl javaMailSender) {
		this.javaMailSender = javaMailSender;
	}

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
	//public static String vmFileUrl  =  "";
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
	//public static String passWord = "";
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
	public static String currentUser = "";
	static String fileSize = "";
	
	public String getCurrentDateTime() {
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date);
	}
	public void logWriter(String logString,String processId,String uploadLogFilePath){
		try{
			//uploadLogFilePath = "D:\\Mail_Temp_Path\\";
			logfile = new FileWriter(uploadLogFilePath+processId,true);
			bufferedWriter = new BufferedWriter(logfile);
			bufferedWriter.newLine();
			bufferedWriter.write("Process ID : "+processId+" : " + getCurrentDateTime() + " : " + logString);
			//bufferedWriter.write(""+logString);
			bufferedWriter.close();
			logfile.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	
	public int initializeData(String processPId, String debugPMode, String versionPNo) {
		int retVal = 0;
		RSSProcessControlVb rssProcessControlVb = new RSSProcessControlVb();
		try {
			processId = processPId;
			if ("Y".equalsIgnoreCase(debugPMode))
				debugMode = true;
			versionNo = versionPNo;
			//String uploadLogFilePath = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_LOG_FILE_PATH");
			logWriter("************Scheuler Process Start**********",processId,uploadLogFilePath);
			logWriter("Process ID ["+processId+"] Debug Mode ["+debugPMode+"] Version No ["+versionPNo+"]" , processId,uploadLogFilePath);
			logWriter("************Get Process Control Details**********",processId,uploadLogFilePath);
			rssProcessControlVb = new ScheduleReportDao(jdbcTemplate).getScheduleProcessControl(processId);
			logWriter("Check Environment Maintenance Checkup", processId,uploadLogFilePath);
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Process Initialize",
					"Check Environment Maintenance Checkup");

			checkEnvironmentMaintenance();
			int readinessFlag = checkReadiness(rssProcessControlVb); 
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Readiness Check",
					"Checking Readiness!!!"+readinessFlag);
			logWriter("Check Readiness Flag : "+readinessFlag , processId,uploadLogFilePath);
			if(readinessFlag ==	ERROR_OPERATION || readinessFlag == READINESS_ERRONEOUS_EXIT){
				return readinessFlag; 
			}
			retVal = checkScheduleProcessControls();
			logWriter("************Scheuler Process End **********",processId,uploadLogFilePath);
			return retVal;
		} catch (Exception e) {
			try {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Exception",
						e.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			return ERROR_OPERATION;
		}
	}

	public int checkScheduleProcessControls() throws Exception {
		int returnStatus = SUCCESS_OPERATION;
		try {
			String emailSchedularEnable = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_EMAIL_SCHEDULER");
			emailSchedularEnable = (ValidationUtil.isValid(emailSchedularEnable)) ? emailSchedularEnable : "N";
			if (!"Y".equalsIgnoreCase(emailSchedularEnable)) {
				//logger.info("Email Scheduler not enabled in Vision Maintenance");
				return ERROR_OPERATION;
			}
			VisionUsersVb vObje = new VisionUsersVb();
			//logger.info("currentUser:" + currentUser + "--" + "fileSize:" + fileSize);
			if (!ValidationUtil.isValid(currentUser)) {
				logWriter("Error: System User is null ",processId,uploadLogFilePath);
				return ERROR_OPERATION;
			}
			vObje.setVisionId(Integer.parseInt(currentUser));
			vObje.setVerificationRequired(false);
			vObje.setRecordIndicator(0);
			vObje.setUserStatus(0);

			List<VisionUsersVb> result = new CommonDao(jdbcTemplate).getSenderVisionIdEmailId(vObje);
			VisionUsersVb systemUser = null;
			if (result != null && !result.isEmpty()) {
				systemUser = result.get(0);
			}
			if (systemUser == null) {
				logWriter("Error: System User is null ",processId,uploadLogFilePath);
				return ERROR_OPERATION;
			}

			RSSProcessControlVb vObj = new ScheduleReportDao(jdbcTemplate).getScheduleProcessControl(processId);
			if (!ValidationUtil.isValid(vObj.getReportId())) {
				logWriter("No Records to Process on the Process Id:  "+processId,processId,uploadLogFilePath);
				//logger.error("No Records to Process on the Process Id:[" + processId + "]");
				return ERROR_OPERATION;
			}
			StringBuilder displayingReportData = null;
			vObj.setFileSize(fileSize);
			returnStatus = processScheduleReport(vObj, displayingReportData);
		} catch (Exception e) {
			e.printStackTrace();
			returnStatus = ERROR_OPERATION;
		}
		new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Process End",
				"Schedule Process End!!");
		return returnStatus;
	}

	public int processScheduleReport(RSSProcessControlVb vObj, StringBuilder displayingReportData) {
		int returnStatus = SUCCESS_OPERATION;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Process Start",
					"Schedule Process start for the Report [" + vObj.getReportId() + "]");
			
			logWriter("Do Send Mail Process start for the Report [" + vObj.getReportId() + "]",processId,uploadLogFilePath);
			exceptionCode = doSendEmail(vObj, displayingReportData);
			logWriter("Do Send Mail Process End for the Report [" + vObj.getReportId() + "]",processId,uploadLogFilePath);
			
			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				logWriter("Mail Sent Successfully with Data",processId,uploadLogFilePath);
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Mail Sent With Data",
						"Mail Sent Successfully with Data");
				returnStatus = 0;
			} else if (exceptionCode.getErrorCode() == Constants.BLANK_SUCCESSFUL_EXIT) {
				logWriter("Mail Sent Successfully without Data, Blank Report Flag enabled",processId,uploadLogFilePath);
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
						"Mail Sent for Blank Report flag Enable", "Mail Sent Successfully without Data");
				//logger.info("Email Sent Successfully for the Schedule Process Id [" + processId + "]Error Code:[1]");
				returnStatus = 0;
			} else if (exceptionCode.getErrorCode() == Constants.BLANK_ERRONEOUS_EXIT) {
				logWriter("Mail Not Sent for without Data as Blank Report Flag Disabled",processId,uploadLogFilePath);
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Mail Not Sent",
						"Mail Not Sent for without Data as Blank Report Flag Disabled");
				returnStatus = 0;
			} else if (exceptionCode.getErrorCode() == Constants.ERRONEOUS_OPERATION) {
				logWriter("Report Errored and Mail not Sent",processId,uploadLogFilePath);
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Error On Report",
						"Report Errored and Mail not Sent");
				/*logger.info("Email Not Sent for the Schedule Process Id [" + processId + "]Error Code:["
						+ Constants.ERRONEOUS_OPERATION + "]");*/
				returnStatus = 1;
			}
		} catch (Exception e) {
			logWriter("Exception : "+e.getMessage(),processId,uploadLogFilePath);
			try {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Exception",
						e.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			returnStatus = ERROR_OPERATION;
		}
		return returnStatus;
	}

	public ExceptionCode doSendEmail(RSSProcessControlVb vObject, StringBuilder displayingReportData) throws Exception {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			ReportsVb reportVb = new ReportsVb();
			//reportVb.setUserId(vObject.getUserId());
			currentUser =String.valueOf(vObject.getUserId());
			reportVb.setReportId(vObject.getReportId());
			reportVb.setPromptValue1(vObject.getPromptValue1());
			reportVb.setPromptValue2(vObject.getPromptValue2());
			reportVb.setPromptValue3(vObject.getPromptValue3());
			reportVb.setPromptValue4(vObject.getPromptValue4());
			reportVb.setPromptValue5(vObject.getPromptValue5());
			reportVb.setPromptValue6(vObject.getPromptValue6());
			reportVb.setPromptValue7(vObject.getPromptValue7());
			reportVb.setPromptValue8(vObject.getPromptValue8());
			reportVb.setPromptValue9(vObject.getPromptValue9());
			reportVb.setPromptValue10(vObject.getPromptValue10());
			reportVb.setCurrentLevel(new ScheduleReportDao(jdbcTemplate).getCurrentLevel(vObject.getReportId()));
			reportVb.setNextLevel(reportVb.getCurrentLevel());
			reportVb.setMaker(vObject.getMaker());
			reportVb.setReportTitle(vObject.getReportTitle());
			//String scalingFactor  = vObject.getScallingFactor();
			reportVb.setScalingFactor(vObject.getScallingFactor());
			if("1000000000".equalsIgnoreCase(vObject.getScallingFactor())) 
				vObject.setScallingFactor("In Billion's");
			else if("1000000".equalsIgnoreCase(vObject.getScallingFactor()))
				vObject.setScallingFactor("In Million's");
			else if("1000".equalsIgnoreCase(vObject.getScallingFactor()))
				vObject.setScallingFactor("In Thousand's");
			else
				vObject.setScallingFactor("No Scaling");
			String promptLabel = "";
			StringBuffer promptLabelBuf = new StringBuffer();
			StringJoiner filterPosition = new StringJoiner("-");
			if(isValid(vObject.getPromptValue1())) {
				promptLabelBuf.append(vObject.getPromptLabel1()+" : "+vObject.getPromptValue1Desc());
				filterPosition.add("G1");
			}
			if(isValid(vObject.getPromptValue2())) {
				promptLabelBuf.append("!@#"+vObject.getPromptLabel2()+" : "+vObject.getPromptValue2Desc());
				filterPosition.add("G2");
			}	
			if(isValid(vObject.getPromptValue3())) {
				promptLabelBuf.append("!@#"+vObject.getPromptLabel3()+" : "+vObject.getPromptValue3Desc());
				filterPosition.add("G3");
			}
			if(ValidationUtil.isValid(vObject.getPromptValue4())) {
				promptLabelBuf.append("!@#"+vObject.getPromptLabel4()+" : "+vObject.getPromptValue4Desc());
				filterPosition.add("G4");
			}
			if(isValid(vObject.getPromptValue5())) {
				promptLabelBuf.append("!@#"+vObject.getPromptLabel5()+" : "+vObject.getPromptValue5Desc());
				filterPosition.add("G5");
			}
			if(isValid(vObject.getPromptValue6())) {
				promptLabelBuf.append("!@#"+vObject.getPromptLabel6()+" : "+vObject.getPromptValue6Desc());
				filterPosition.add("G6");
			}	
			if(isValid(vObject.getPromptValue7())) {
				promptLabelBuf.append("!@#"+vObject.getPromptLabel7()+" : "+vObject.getPromptValue7Desc());
				filterPosition.add("G7");	
			}	
			if(isValid(vObject.getPromptValue8())) {
				promptLabelBuf.append("!@#"+vObject.getPromptLabel8()+" : "+vObject.getPromptValue8Desc());
				filterPosition.add("G8");
			}	
			if(isValid(vObject.getPromptValue9())) {
				promptLabelBuf.append("!@#"+vObject.getPromptLabel9()+" : "+vObject.getPromptValue9Desc());
				filterPosition.add("G9");
			}	
			if(isValid(vObject.getPromptValue10())) {
				promptLabelBuf.append("!@#"+vObject.getPromptLabel10()+" : "+vObject.getPromptValue10Desc());
				filterPosition.add("G10");
			}	
			
			promptLabelBuf.append("!@#Scaling Factor : "+vObject.getScallingFactor());
			promptLabel = promptLabelBuf.toString();
			reportVb.setPromptLabel(promptLabel);
			reportVb.setFilterPosition(filterPosition.toString());
			vObject.setMakerName(new ScheduleReportDao(jdbcTemplate).getMakerName(vObject.getMaker()));
			userName = vObject.getMakerName();
			//logger.info(" Procedure Execution Started...!!");
			if("R".equalsIgnoreCase(vObject.getReportType())) { 
				logWriter("Get Report Data Process start",processId,uploadLogFilePath);
				exceptionCode = getReportData(vObject, displayingReportData, reportVb);
				logWriter("Get Report Data Process End",processId,uploadLogFilePath);
			}else {
				logWriter("Get Catalog Data Start ",processId,uploadLogFilePath);
				exceptionCode = getCatalogData(vObject);
				logWriter("Get Catalog Data End ",processId,uploadLogFilePath);
			}
			
			//logger.info(" Procedure Execution End...!!");
			if ( exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION || exceptionCode.getErrorCode() == Constants.BLANK_SUCCESSFUL_EXIT) {
				if (vObject.getAttachHtml() && exceptionCode != null && exceptionCode.getRequest() != null) {
					displayingReportData = (StringBuilder) exceptionCode.getRequest();
				}
				if (vObject.getEmailFlag()) {
					new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "PrePare Email Start",
					"PreParing Email Start");
					if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
						logWriter("prepareEmail Start ",processId,uploadLogFilePath);
						exceptionCode = prepareEmail(exceptionCode, vObject, displayingReportData);
						logWriter("prepareEmail End ",processId,uploadLogFilePath);
					}
					new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "PrePare Email End",
					"PreParing Email End");
				}
				if (vObject.getFtpFlag()) {
					exceptionCode = prepareFtp(exceptionCode, vObject);
				}
				if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					exceptionCode.setErrorSevr("S");
				} else {
					exceptionCode.setErrorSevr("F");
				}
			} else {
				exceptionCode.setErrorSevr("F");
			}
			return exceptionCode;
		} catch (MailAuthenticationException e) {
			logWriter("MailAuthenticationException : "+e.getMessage(),processId,uploadLogFilePath);
			e.printStackTrace();
			try {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Exception",e.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} catch (MailPreparationException e) {
			logWriter("MailPreparationException : "+e.getMessage(),processId,uploadLogFilePath);
			e.printStackTrace();
			try {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Exception",e.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} catch (NullPointerException e) {
			logWriter("NullPointerException : "+e.getMessage(),processId,uploadLogFilePath);
			try {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Exception",e.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode = CommonUtils.getResultObject("Email Schedule", Constants.ERRONEOUS_OPERATION, "E-Mail", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		} catch (Exception e) {
			logWriter("Exception : "+e.getMessage(),processId,uploadLogFilePath);
			e.printStackTrace();
			try {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Exception",e.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}
	}

	public ExceptionCode prepareFtp(ExceptionCode exceptionCode, RSSProcessControlVb vObject)
			throws SQLException, Exception {
		try {
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Schedule FTP",
					"Initializing the FTP Schedule !!!!");
			String ftpConnectivityDetails = new CommonDao(jdbcTemplate).getScriptValue(vObject.getFtpVarName());
			ftpServerIp = getKeyValue(ftpConnectivityDetails, "SERVER_IP");
			ftpServerHostName = getKeyValue(ftpConnectivityDetails, "SERVER_HOST_NAME");
			ftpServerPort = getKeyValue(ftpConnectivityDetails, "SERVER_PORT");
			ftpServerUser = getKeyValue(ftpConnectivityDetails, "SERVER_USER");
			ftpServerPassword = getKeyValue(ftpConnectivityDetails, "SERVER_PWD");
			ftpServerType = getKeyValue(ftpConnectivityDetails, "SERVER_TYPE");
			ftpType = getKeyValue(ftpConnectivityDetails, "FTP_TYPE");
			ftpServerPath = getKeyValue(ftpConnectivityDetails, "PATH");
			exceptionCode = schedulerFtpToUnixServer(exceptionCode, vObject);
			if (exceptionCode.getErrorCode() != SUCCESS_OPERATION) {
				exceptionCode.setErrorCode(ERROR_OPERATION);
				//logger.info("Error in FTP");
			}
		} catch (Exception e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Schedule FTP",
					"Error on FTP Schedule[" + e.getMessage() + "]");
			exceptionCode.setErrorCode(ERROR_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public ExceptionCode schedulerFtpToUnixServer(ExceptionCode exceptionCode, RSSProcessControlVb vObject) {
		try {
			FileInputStream htmlFile = null;
			FileInputStream excelFile = null;
			FileInputStream pdfFile = null;
			FileInputStream csvFile = null;
			String filePath = System.getProperty("java.io.tmpdir");
			String htmlFileName = "";
			String xlFileName = "";
			String csvFileName = "";
			String pdfFileName = "";
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			File lFileHtml = null;
			File lFileXlsx = null;
			File lFilePdf = null;
			File lFileCsv = null;

			/*if (vObject.getAttachHtml()) {
				lFileHtml = new File(filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + processId + "_"
						+ versionNo + ".html");
				htmlFile = new FileInputStream(lFileHtml);
				htmlFileName = filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + processId + "_"
						+ versionNo + ".html";
			}*/
			if (vObject.getAttachXl()) {
				lFileXlsx = new File(filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + processId + "_"
						+ versionNo + ".xlsx");
				excelFile = new FileInputStream(lFileXlsx);
				xlFileName = filePath+ValidationUtil.encode(vObject.getReportTitle()) + "_" + processId + "_"
						+ versionNo + ".xlsx";
			}
			if (vObject.getAttachPdf()) {
				lFilePdf = new File(filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + processId + "_"
						+ versionNo + ".pdf");
				pdfFile = new FileInputStream(lFilePdf);
				pdfFileName = filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + processId + "_"
						+ versionNo + ".pdf";
			}
			if (vObject.getAttachCsv()) {
				lFileCsv = new File(filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + processId + "_"
						+ versionNo + ".txt");
				csvFile = new FileInputStream(lFileCsv);
				csvFileName = filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + processId + "_"
						+ versionNo + ".txt";
			}
			if("W".equalsIgnoreCase(ftpServerType)){
				String uploadPathInWindowsServer = new CommonDao(jdbcTemplate).findVisionVariableValue("EXCEL_DATA_PATH_IN_WINDOWS_SEVER");
				if (vObject.getAttachXl()) {
					File lFile = new File(uploadPathInWindowsServer + lFileXlsx.getName());
					if (lFile.exists()) {
						lFile.delete();
					}
					lFile.createNewFile();
					FileOutputStream os = new FileOutputStream(lFile);
					byte[] array = new byte[50];
					excelFile.read(array);
					os.write(array);
					excelFile.close();
					os.close();
				}
				if (vObject.getAttachPdf()) {
					File lFile = new File(uploadPathInWindowsServer + lFilePdf.getName());
					if (lFile.exists()) {
						lFile.delete();
					}
					lFile.createNewFile();
					FileOutputStream fileOutStream = new FileOutputStream(lFile);
					byte[] array = new byte[50];
					pdfFile.read(array);
					fileOutStream.write(array);
					pdfFile.close();
					fileOutStream.close();
				}
				if (vObject.getAttachCsv()) {
					File lFile = new File(uploadPathInWindowsServer + lFileCsv.getName());
					if (lFile.exists()) {
						lFile.delete();
					}
					lFile.createNewFile();
					FileOutputStream fileOutStream = new FileOutputStream(lFile);
					byte[] array = new byte[50];
					csvFile.read(array);
					fileOutStream.write(array);
					csvFile.close();
					fileOutStream.close();
				}
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			    return exceptionCode;
			
			}else if("FTP".equalsIgnoreCase(ftpType)) {
				FTPClient ftpClient = getConnection();
            	ftpClient.connect(ftpServerIp);
            	boolean response = ftpClient.login(ftpServerUser, ftpServerPassword);
            	if(!response){
            		ftpClient.disconnect();
                	exceptionCode.setErrorCode(Constants.FILE_UPLOAD_REMOTE_ERROR);
        			exceptionCode.setErrorMsg("Unable to connect to Ftp server");
        			return exceptionCode;
            	}
        		ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            	response = ftpClient.changeWorkingDirectory(ftpServerPath);
            	if(!response){
            		ftpClient.disconnect();
                	ftpClient.disconnect();
                	exceptionCode.setErrorCode(Constants.FILE_UPLOAD_REMOTE_ERROR);
        			exceptionCode.setErrorMsg("Unable to connect to Ftp server");
        			return exceptionCode;
            	}
				if (vObject.getAttachXl()) {
					ftpClient.storeFile(lFileXlsx.getName(), excelFile);
				}
				if (vObject.getAttachPdf()) {
					ftpClient.storeFile(lFilePdf.getName(), pdfFile);
				}
				if (vObject.getAttachCsv()) {
					ftpClient.storeFile(lFileCsv.getName(), csvFile);
				}
    	        ftpClient.disconnect();
			} else {
				JSch jsch = new JSch();
				jsch.setKnownHosts("c\\:known_hosts");
				ftpServerIp = "10.16.1.41";
				ftpServerUser = "Administrator";
				ftpServerPassword = "Sunoida1234";
				ftpServerPath="ADF_Upload_Dir";
				Session session = jsch.getSession(ftpServerUser, ftpServerIp);
				{
					UserInfo ui = new MyUserInfo();
					session.setUserInfo(ui);
					session.setPassword(ftpServerPassword);
				}
				session.setConfig("StrictHostKeyChecking", "no");
				session.connect();
				Channel channel = session.openChannel("sftp");
				channel.connect();
				ChannelSftp sftpChannel = (ChannelSftp) channel;
				sftpChannel.cd(ftpServerPath);
				/*if (vObject.getAttachHtml()) {
					sftpChannel.put(htmlFile, "/vision/Test_schedule.html");
				}*/
				if (vObject.getAttachXl()) {
					sftpChannel.put(excelFile, lFileXlsx.getName());
				}
				if (vObject.getAttachPdf()) {
					sftpChannel.put(pdfFile, lFilePdf.getName());
				}
				if (vObject.getAttachCsv()) {
					sftpChannel.put(csvFile, lFileCsv.getName());
				}
				sftpChannel.exit();
				channel = session.openChannel("shell");
				OutputStream inputstream_for_the_channel = channel.getOutputStream();
				PrintStream commander = new PrintStream(inputstream_for_the_channel, true);
				channel.connect();
				commander.println("exit");
				commander.close();
				do {
					Thread.sleep(1000);
				} while (!channel.isEOF());
				session.disconnect();
			}
			exceptionCode.setErrorMsg("Report Scheduled on FTP Successfully!!!");
			exceptionCode.setErrorSevr("S");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Schedule FTP",
					"Report Scheduled on FTP Successfully !!!!");
		} catch (FileNotFoundException e) {
			//System.out.println("File Not found :" + e.getMessage());
			exceptionCode.setErrorMsg("Exception on Scheduling report FTP File Not found[" + e.getMessage() + "]");
			exceptionCode.setErrorSevr("F");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		} catch (IOException e) {
			//System.out.println("IOExpception :" + e.getMessage());
			exceptionCode.setErrorMsg("IOExpception[" + e.getMessage() + "]");
			exceptionCode.setErrorSevr("F");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		} catch (Exception e) {
			//System.out.println("Exception :" + e.getMessage());
			exceptionCode.setErrorMsg("Exception[" + e.getMessage() + "]");
			exceptionCode.setErrorSevr("F");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		return exceptionCode;
	}

	public class MyUserInfo implements UserInfo {
		public String getPassword() {
			return serverPassword;
		}

		public boolean promptYesNo(String str) {
			return false;
		}

		public String getPassphrase() {
			return null;
		}

		public boolean promptPassphrase(String message) {
			return true;
		}

		public boolean promptPassword(String message) {
			return false;
		}

		public void showMessage(String message) {
			return;
		}
	}

	private ExceptionCode prepareEmail(ExceptionCode exceptionCode, RSSProcessControlVb vObject,
			StringBuilder displayingReportData) throws MessagingException, Exception {
		new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Email Preparation","Email Preparation start");
		//logger.info("Prepare Email Starts...!!");
		mailSender = new JavaMailSenderImpl();
		MimeMessage message = mailSender.createMimeMessage();
		MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");
		mailSender.setHost(mailhost);
		mailSender.setPort(mailPort);
		mailSender.setPassword(mailPassword);
		mailSender.setUsername(mailUsername);
		Properties properties = new Properties();
		
		String smtpSslEnable = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_EMAIL_SSL_ENABLE");
		String smtpStartlsReq = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_EMAIL_STARTLS_REQ");
		String smtpStartlsEnable = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_EMAIL_STARTLS");
		String smtpAuth = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_EMAIL_AUTH");
		String smtpSslTrust = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_EMAIL_SSL_TRUST");
		
		if(!ValidationUtil.isValid(smtpSslEnable))smtpSslEnable="false";
		if(!ValidationUtil.isValid(smtpStartlsReq))smtpStartlsReq="false";
		if(!ValidationUtil.isValid(smtpStartlsEnable))smtpStartlsEnable="false";
		if(!ValidationUtil.isValid(smtpAuth))smtpAuth="false";
		if(!ValidationUtil.isValid(smtpSslTrust))smtpSslTrust="false";
		
		properties.setProperty("mail.smtp.host", mailhost);
		properties.setProperty("mail.smtp.port", "" + mailPort);
		properties.setProperty("mail.smtp.auth", smtpAuth);
		if("true".equals(smtpStartlsEnable))
			properties.setProperty("mail.smtp.starttls.enable", smtpStartlsEnable);
		if("true".equals(smtpStartlsReq))
			properties.setProperty("mail.smtp.starttls.required", smtpStartlsReq);
	    if("true".equals(smtpSslEnable))
	    	properties.setProperty("mail.smtp.ssl.enable",smtpSslEnable);
	    if("true".equals(smtpSslTrust))
	    	properties.setProperty("mail.smtp.ssl.trust", mailhost);
		
		mailSender.setJavaMailProperties(properties);
		try {
			String msgBody = "";
			ReportsVb vObj = null;
			String[] fileName = new String[25];
			String[] extension = new String[25];
			String[] contentType = new String[25];
			Map<String, Object> map = new HashMap<String, Object>();
			String schedulerEmailFrom = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_EMAIL_FROM");
			if (!ValidationUtil.isValid(schedulerEmailFrom)) {
				//logger.error("No valid maintenance for PRD_SCHEDULER_EMAIL_FROM variable");
			}
			vObject.setSchedulerEmailFrom(schedulerEmailFrom);

			String supportEmail = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_EMAIL_SUPPORT");
			if (!ValidationUtil.isValid(supportEmail)) {
				//logger.error("No valid maintenance for PRD_SCHEDULER_EMAIL_SUPPORT variable");
			}
			vObject.setSchedulerSupportEmail(supportEmail);

			vObj = (ReportsVb) exceptionCode.getOtherInfo();
			vObject.setFormatType("");
			if (vObject.getAttachXl()) {
				vObject.setFormatType(vObject.getFormatType() + "E-");
			}
			if (vObject.getAttachPdf()) {
				vObject.setFormatType(vObject.getFormatType() + "P-");
			}
			if (vObject.getAttachCsv()) {
				vObject.setFormatType(vObject.getFormatType() + "C-");
			}

			String[] formatType = vObject.getFormatType().split("-");
			int fomratCount = formatType.length;
			String filePath = System.getProperty("java.io.tmpdir");
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			for (int i = 0; i < fomratCount; i++) {
				if ("P".equalsIgnoreCase(formatType[i])) {
					extension[i] = ".pdf";
				} else if ("E".equalsIgnoreCase(formatType[i])) {
					extension[i] = ".xlsx";
				} else {
					extension[i] = ".txt";
				}
				File lFile = null;

				lFile = new File(filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_" + processId + "_"+ versionNo + extension[i]);

				long fileSizeInKB = 0;
				long fileSizeInMB = 0;
				if (lFile.exists()) {
					long fileSizeInBytes = lFile.length();
					// Convert the bytes to Kilobytes (1 KB = 1024 Bytes)
					fileSizeInKB = fileSizeInBytes / 1024;
					// Convert the KB to MegaBytes (1 MB = 1024 KBytes)
					fileSizeInMB = fileSizeInKB / 1024;
				}
				if (fileSizeInMB > Long.parseLong(vObject.getFileSize())) {
					File renameTo = new File (filePath +ValidationUtil.encode(vObject.getReportTitle()+"_"+ processId + "_" + versionNo+  extension[i]));
					/*if (renameTo.exists()) {
						renameTo.delete();
					}
					renameTo.createNewFile();*/
					String zipFileName = filePath + vObject.getReportTitle() + ".zip";
					FileOutputStream fos = new FileOutputStream(zipFileName);
					ZipOutputStream zos = new ZipOutputStream(fos);
					ZipEntry ze = new ZipEntry(renameTo.getName());
					zos.putNextEntry(ze);
					FileInputStream fis = new FileInputStream(renameTo);
					byte[] buffer = new byte[1024];
					int len;
					while ((len = fis.read(buffer)) > 0) {
						zos.write(buffer, 0, len);
					}
					zos.closeEntry();
					zos.close();
					fis.close();
					fos.close();
					extension[i] = ".zip";
					contentType[i] = "application/zip";
					fileName[i] = zipFileName;
				} else {
					if ("P".equalsIgnoreCase(formatType[i])) {
						extension[i] = ".pdf";
						contentType[i] = "application/pdf";
					} else if ("E".equalsIgnoreCase(formatType[i])) {
						extension[i] = ".xlsx";
						contentType[i] = "application/excel";
					} else if ("C".equalsIgnoreCase(formatType[i])) {
						extension[i] = ".txt";
						contentType[i] = "application/text";
					} 
					fileName[i] = filePath + ValidationUtil.encode(vObject.getReportTitle()) + "_"
							+ ScheduledReportWb.processId + "_" + ScheduledReportWb.versionNo + extension[i];
				}
			}
			String emailHeader = "";
			emailHeader = vObject.getEmailHeader();
			StringBuilder htmlData = new StringBuilder();
			Boolean dataFlag = true;
			Boolean htmlFlag = false;
			if(ValidationUtil.isValid(vObject.getEmailHeader())) {
				htmlData.append(vObject.getEmailHeader());
			}
			//htmlData.append("<br/>Hello,<br/>");
			if(vObject.getAttachHtml()) {
				if (ValidationUtil.isValid(displayingReportData)) {
					htmlFlag = true;
					if(!ValidationUtil.isValid(emailHeader))
						htmlData.append("<br/>Hello,<br/><br/>Please find below : " + vObject.getReportTitle() + "<br/>");
					htmlData.append("<br/>");
					String dataHeader = "<div style='BORDER-TOP: #000000 1px solid; BORDER-RIGHT: #000000 1px solid; BORDER-BOTTOM: #000000 1px solid;BORDER-LEFT: #000000 1px solid;border:1px solid #000000;border-bottom:0px;'><table class='displayHeader' id='gridtableIDHeader' style='color:#000 !important;' width='100%'>"
							+ " <tbody><tr style='height: 40px;'>" + "	<td style='width:5%;height: 30px;'> "
							+ "   <img id='img1' style='float:left;height:40px;width:100%' src=\"cid:Product_Logo\"></td>"
							+ " <td style='width:30%'>" + " <div class='CenterDiv'>"
							+ " <p  class='reportTitle' style='font-family: \"calibri\";font-size:16px;font-weight:bold;text-align: center;padding-left:10px;color:#000;'>"
							+ vObject.getReportTitle() + "</p>" + " </div>" + " </td>"
							+ " <td style='width:5%;height: 30px;text-align:right;'><img id='img1' style='float:right; height:40px;width:100%'  src=\"cid:Bank_Logo\"></td>"
							+ "  </tr>" + " </tbody>" + " </table></div>";
					htmlData.append(dataHeader);
					htmlData.append("<div style='BORDER-TOP: #000000 1px solid; BORDER-RIGHT: #000000 1px solid; BORDER-BOTTOM: #000000 1px solid;BORDER-LEFT: #000000 1px solid;border:1px solid #000000;padding:10px;'>"
									+ displayingReportData.toString() + "</div>");

				}else {
					htmlData.append("<br/>Hello,<br/><br/>No Data Found for the Report : "+vObject.getReportTitle()+",<br/>");
				}
			}
			if(ValidationUtil.isValid(vObject.getEmailFooter())){
				htmlData.append(vObject.getEmailFooter());
			}else {
				htmlData.append("<br/>" + "<br/>" + "This is an automated E-mail generated by Vision.<br/>"
					+ "<br/>" + "For any further queries, please E-mail to " + supportEmail + "<br/>" + "<br/>"
					+ "-----------------------------------------------------------<br/>"
					+ "This is a system generated mail, Please do not reply.<br/>"
					+ "-----------------------------------------------------------");
			}
			
			
			Configuration config = new Configuration();
		    BodyPart  messageBodyPart = new MimeBodyPart();
		    config.setDirectoryForTemplateLoading(new File(assetFolderUrl));
		    
			Multipart multipart = new MimeMultipart( "alternative" );	
		    MimeBodyPart textPart = new MimeBodyPart();
		    //textPart.setText( htmlData.toString(), "utf-8" );
		    textPart.setContent( htmlData.toString(), "text/html; charset=utf-8" );
		    multipart.addBodyPart( textPart );
		   message.setContent(multipart);
		    if(vObject.getAttachHtml()) {
		    	if (ValidationUtil.isValid(displayingReportData)) {
					BodyPart productLogo = new MimeBodyPart();
					BodyPart BankLogo = new MimeBodyPart();
					MimeBodyPart htmlPart = new MimeBodyPart(); // Email Body
					htmlPart.setContent( htmlData.toString(), "text/html; charset=utf-8" );
					multipart.addBodyPart( htmlPart );
					InputStream imageStream = new BufferedInputStream(
							new FileInputStream(assetFolderUrl + "/Product_Logo.png"));
					DataSource fds = new ByteArrayDataSource(IOUtils.toByteArray(imageStream), "image/png");
					productLogo.setDataHandler(new DataHandler(fds));
					productLogo.setHeader("Content-ID", "<Product_Logo>");
					multipart.addBodyPart(productLogo);
					message.setContent(multipart);
	
					InputStream imageStream1 = new BufferedInputStream(
							new FileInputStream(assetFolderUrl + "/Bank_Logo.png"));
					DataSource fds1 = new ByteArrayDataSource(IOUtils.toByteArray(imageStream1), "image/png");
					BankLogo.setDataHandler(new DataHandler(fds1));
					BankLogo.setHeader("Content-ID", "<Bank_Logo>");
					multipart.addBodyPart(BankLogo);
					message.setContent(multipart);
		    	}
		    }

		    
			helper.setFrom(vObject.getSchedulerEmailFrom());
			if (ValidationUtil.isValid(vObject.getEmailTo())) {
				String[] ids = vObject.getEmailTo().split(";");
				helper.setTo(ids);
			}
			if (ValidationUtil.isValid(vObject.getEmailCc())) {
				String[] ids = vObject.getEmailCc().split(";");
				helper.setCc(ids);
			}
			if (ValidationUtil.isValid(vObject.getEmailBcc())) {
				String[] ids = vObject.getEmailBcc().split(";");
				helper.setBcc(ids);
			}
			helper.setText(msgBody, true);
			helper.setSentDate(new Date());
			if (ValidationUtil.isValid(vObject.getEmailSubject())) {
				String emailSubject = new ScheduleReportDao(jdbcTemplate).replaceSubjectValues(vObject.getEmailSubject(),
						vObject);
				helper.setSubject(emailSubject);
			} else {
				helper.setSubject(vObject.getReportTitle());
			}
			for (int i = 0; i < fomratCount; i++) {
				if (!"H".equalsIgnoreCase(formatType[i])) {
					File lfile = new File(fileName[i]);
					if (lfile.exists()) {
						vObject.setSwfFileName(fileName[i]);
						boolean isNotEmpty = false;
						if (formatType[i].equalsIgnoreCase("E")) {
							try {

								XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(fileName[i]));
								for (int ctr = 0; ctr < workbook.getNumberOfSheets(); ctr++) {
									isNotEmpty = isSheetEmpty(workbook.getSheetAt(ctr));
									if (isNotEmpty == true) {
										break;
									}
								}
							} catch (Exception e) {
								SXSSFWorkbook workbooks = new SXSSFWorkbook(new XSSFWorkbook(new FileInputStream(fileName[i])));
								for (int ctr = 0; ctr < workbooks.getNumberOfSheets(); ctr++) {
									isNotEmpty = isSheetEmpty(workbooks.getSheetAt(ctr));
									if (isNotEmpty == true) {
										break;
									}
								}
							}
						}

						if ("E".equalsIgnoreCase(formatType[i])) {
							if (!isNotEmpty) {
								new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,"Excel File Check","Excel File Generated as Blank.It will not attach to the Email");
								exceptionCode = CommonUtils.getResultObject("Email Schedule",Constants.ERRONEOUS_OPERATION, "E-Mail", "");
								exceptionCode.setOtherInfo(vObject);
								exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
								vObject.setErrorDescription("Report is generating Blank Excel File.Kindly Re-schedule!!!");
								//logger.error("Blank Report Generated Excel file not attached");
								dataFlag = Boolean.valueOf(false);
							} else {
								messageBodyPart = new MimeBodyPart();
								FileSystemResource fips = new FileSystemResource(fileName[i]);
								DataSource source = new FileDataSource(fileName[i]);
								messageBodyPart.setDataHandler(new DataHandler(source));
								messageBodyPart.setFileName(ValidationUtil.isValid(vObject.getAttachFileName())
										? (String.valueOf(vObject.getAttachFileName()) + extension[i])
										: (String.valueOf(vObject.getReportTitle()) + extension[i]));
								multipart.addBodyPart((BodyPart) messageBodyPart);
								message.setContent(multipart);
							}
						} else {
							messageBodyPart = new MimeBodyPart();
							FileSystemResource fips = new FileSystemResource(fileName[i]);
							DataSource source = new FileDataSource(fileName[i]);
							messageBodyPart.setDataHandler(new DataHandler(source));
							messageBodyPart.setFileName(ValidationUtil.isValid(vObject.getAttachFileName())
									? (String.valueOf(vObject.getAttachFileName()) + extension[i])
									: (String.valueOf(vObject.getReportTitle()) + extension[i]));
							multipart.addBodyPart((BodyPart) messageBodyPart);
							message.setContent(multipart);
						}
					} else {
						//logger.error("File Not Exists" + fileName[i]);
						logWriter("Report Errored or No Records Found ",processId,uploadLogFilePath);
						exceptionCode.setOtherInfo(vObject);
						exceptionCode.setErrorMsg("Report Errored or No Records Found.Kindly check Error Audit table!!");
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						//return exceptionCode;
					}

				} 
			}
			if (dataFlag == true || htmlFlag == true) {
				if (vObject.getEmailFlag()) {
					mailSender.send(message);
					logWriter("Mail Prepared and Sent Successfully ",processId,uploadLogFilePath);
					new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Mail Sent",
					"Mail Prepared and Sent Successfully");
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				}
				if (vObject.getFtpFlag()) {
				}
			} else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
			//logger.info("Mail Prepared and Sent Successfully");
			
			/*
			 * exceptionCode = CommonUtils.getResultObject("Email Schedule",
			 * Constants.SUCCESSFUL_OPERATION, "E-Mail", "");
			 * exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			 */
		} catch (MailAuthenticationException e) {
			logWriter("MailAuthenticationException : "+e.getMessage(),processId,uploadLogFilePath);
			//logger.error("Mail Authentication Exception" + e.getMessage());
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
					"Mail Sending Error", e.getMessage());
			e.printStackTrace();
			exceptionCode = CommonUtils.getResultObject("Email Schedule", Constants.ERRONEOUS_OPERATION, "E-Mail", "");
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			return exceptionCode;
		} catch (Exception e) {
			logWriter("Exception : "+e.getMessage(),processId,uploadLogFilePath);
			//logger.error("Exception on Prepare Email" + e.getMessage());
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
					"Mail Sending Error", e.getMessage());
			exceptionCode = CommonUtils.getResultObject("Email Schedule", Constants.ERRONEOUS_OPERATION,
					"Mail Sending Error", e.getMessage());
			vObject.setErrorDescription(e.getMessage());
			exceptionCode.setOtherInfo(vObject);
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			return exceptionCode;
		}
		return exceptionCode;
	}


	public ExceptionCode getReportData(RSSProcessControlVb rssvObj, StringBuilder displayingReportData,
			ReportsVb reportVb) throws Exception {
		ExceptionCode exceptionCode = new ExceptionCode();
		ExceptionCode exceptionCodeData = new ExceptionCode();
		ReportsVb vObj = null;
		List<ColumnHeadersVb> columnHeaders = new ArrayList<ColumnHeadersVb>();
		List<HashMap<String, String>> dataLst = null;
		List<HashMap<String, String>> totalLst = null;
		try {
			// reportVb.setMaker(rssvObj.getUserId());
			reportVb.setMaker(reportVb.getMaker());
			// reportVb.setRunType("R");
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Get Report Details Start",
					"Get Report Details End");
			logWriter("Get Report Details Start",processId,uploadLogFilePath);
			exceptionCode = new ReportsWb(jdbcTemplate).getReportDetails(reportVb);
			logWriter("Get Report Details End",processId,uploadLogFilePath);
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Get Report Details End",
					"Get Report Details End");

			if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				vObj = (ReportsVb) exceptionCode.getResponse();
				vObj.setReportTitle(reportVb.getReportTitle());
				vObj.setMaker(reportVb.getMaker());
				rssvObj.setReportType(vObj.getReportType());
				vObj.setMakerName(new ScheduleReportDao(jdbcTemplate).getMakerName(vObj.getMaker()));
				vObj.setScalingFactor(rssvObj.getScallingFactor());
				if (ValidationUtil.isValid(exceptionCode.getResponse())) {
					logWriter("Get getResult Data Start",processId,uploadLogFilePath);
					new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Get Result Data Start",
					"Get Result Data Start");
					logWriter("Result Data Start",processId,uploadLogFilePath);
					exceptionCodeData = new ReportsWb(jdbcTemplate).getResultData(vObj);
					logWriter("Result Data End",processId,uploadLogFilePath);
					new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Get Result Data End",
					"Get Result Data End");
				}
				ReportsVb resultVb = new ReportsVb();
				if (exceptionCodeData.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					if (ValidationUtil.isValid(exceptionCode.getResponse())) {
						resultVb = (ReportsVb) exceptionCode.getResponse();
						dataLst = resultVb.getGridDataSet();
						totalLst = resultVb.getTotal();
						if (rssvObj.getAttachHtml()) {
						ExceptionCode exceptionCodeHtml = null;
						logWriter("HTML Export Start",processId,uploadLogFilePath);
						new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "HTML Export Start","HTML Export Start");
						exceptionCodeHtml = new ReportsWb(jdbcTemplate).exportToHtml(vObj, vObj.getColumnHeaderslst(), dataLst,totalLst);
						new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "HTML Export End","HTML Export End");
						logWriter("HTML Export End",processId,uploadLogFilePath);
						if (exceptionCodeHtml.getErrorCode() == Constants.SUCCESSFUL_OPERATION
								&& exceptionCodeHtml.getRequest() != null) {
							displayingReportData = (StringBuilder) exceptionCodeHtml.getRequest();
							exceptionCode.setRequest(displayingReportData);
						}
					}
					}		
				}else if(exceptionCodeData.getErrorCode() == Constants.ERRONEOUS_OPERATION){
					return exceptionCodeData;
				}
				if(exceptionCodeData.getErrorCode() == Constants.NO_RECORDS_FOUND && "Y".equalsIgnoreCase(rssvObj.getBlankReportFlag())) {
					//columnHeaders = vObj.getColumnHeaderslst();
					if (rssvObj.getAttachPdf()) {
						new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "PDF Export Start",
								"Exporting into PDF Start");
						logWriter("PDF Export Start",processId,uploadLogFilePath);
						new ReportsWb(jdbcTemplate).exportToPdf(vObj, vObj.getColumnHeaderslst(), dataLst, totalLst, processId,versionNo);
						logWriter("PDF Export End",processId,uploadLogFilePath);
						new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "PDF Export End",
								"Exporting into PDF End");
					}
					if (rssvObj.getAttachXl()) {
						new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Excel Export Start",
								"Exporting into Excel Start");
						logWriter("Excel Export Start",processId,uploadLogFilePath);
						new ReportsWb(jdbcTemplate).exportToXls(vObj, vObj.getColumnHeaderslst(), dataLst, totalLst, processId,versionNo);
						logWriter("Excel Export Start",processId,uploadLogFilePath);
						new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Excel Export End",
								"Exporting into Excel End");
					}
					if ((rssvObj.getAttachCsv())) {
						//exceptionCode = exceptionCodeData;
						new ReportsWb(jdbcTemplate).exportReportToCsv(vObj, vObj.getColumnHeaderslst(), dataLst, totalLst,processId, versionNo);
					}

				}else if(exceptionCodeData.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					if (rssvObj.getAttachPdf()) {
						logWriter("PDF Export Start",processId,uploadLogFilePath);
						new ReportsWb(jdbcTemplate).exportToPdf(vObj, vObj.getColumnHeaderslst(), dataLst, totalLst, processId,versionNo);
						logWriter("PDF Export End",processId,uploadLogFilePath);
					}
					if (rssvObj.getAttachXl()) {
						logWriter("Excel Export Start",processId,uploadLogFilePath);
						new ReportsWb(jdbcTemplate).exportToXls(vObj, vObj.getColumnHeaderslst(), dataLst, totalLst, processId,versionNo);
						logWriter("Excel Export End",processId,uploadLogFilePath);
					}
					if ((rssvObj.getAttachCsv())) {
						//exceptionCode = exceptionCodeData;
						new ReportsWb(jdbcTemplate).exportReportToCsv(vObj, vObj.getColumnHeaderslst(), dataLst, totalLst,processId, versionNo);
					}
				}else {
					logWriter("Data not available for this Report, Process Id",processId,uploadLogFilePath);
					new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Extract Report Suite Data",
							"Data not available for this Report, Process Id");
					exceptionCode.setErrorCode(Constants.BLANK_ERRONEOUS_EXIT);
					return exceptionCode;
				}
			} else {
				exceptionCode.setOtherInfo(vObj);
				return exceptionCode;
			}
		} catch (Exception rex) {
			logWriter("Exception in getReportData : "+rex.getMessage(),processId,uploadLogFilePath);
			rex.printStackTrace();
			try {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Exception",rex.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			return exceptionCode;
		}
		exceptionCode.setOtherInfo(vObj);
		return exceptionCode;
	}

	public static boolean isSheetEmpty(Sheet sheet) {
		Iterator rows = sheet.rowIterator();
		while (rows.hasNext()) {
			XSSFRow row = (XSSFRow) rows.next();
			Iterator cells = row.cellIterator();
			while (cells.hasNext()) {
				XSSFCell cell = (XSSFCell) cells.next();
				if (!cell.getStringCellValue().isEmpty()) {
					return true;
				}
			}
		}
		return false;
	}

	public void checkEnvironmentMaintenance() throws CustomException, SQLException {
		mailhost = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_EMAIL_HOST");
		mailPort = Integer.parseInt(new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_EMAIL_PORT"));
		mailUsername = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_EMAIL_FROM");
		mailPassword=new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_EMAIL_PASSWORD");
		assetFolderUrl = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_IMAGE_PATH");
	//	rsMailTemplatePath = new CommonDao(jdbcTemplate).findVisionVariableValue("RS_MAIL_TEMPLATES_PATH");
//		uploadLogFileName = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_LOG_FILE_NAME");

		if (!ValidationUtil.isValid(assetFolderUrl)) {
			throw new CustomException(ERROR_OPERATION, "No valid maintenance for Images Path");
		}
		if (!ValidationUtil.isValid(mailhost)) {
			mailhost = "smtp.gmail.com";
		}
		if (-1 == mailPort) {
			mailPort = 587;
		}
		uploadLogFilePath = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_LOG_FILE_PATH");

		if (!ValidationUtil.isValid(uploadLogFilePath)) {
			throw new CustomException(ERROR_OPERATION,"No valid maintenance for PRD_SCHEDULER_LOG_FILE_PATH variable in Vision_Variables table");
		}
		currentUser = new CommonDao(jdbcTemplate).findVisionVariableValue("SYSTEM_USER_ID");
		fileSize = new CommonDao(jdbcTemplate).findVisionVariableValue("PRD_SCHEDULER_MAX_FILE_EXPORT_SIZE");
		if (!ValidationUtil.isValid(fileSize))
			fileSize = "10";
		/*assetFolderUrl = "D:\\Mail_Scheduler_Images\\";
		uploadLogFilePath = "D:\\Mail_Temp_Path\\";*/
	}

	public int checkReadiness(RSSProcessControlVb rssProcessControlVb) throws SQLException, Exception {
		int readinessFlag = SUCCESS_OPERATION;
		String processId = rssProcessControlVb.getProcessId();
		try {
			String readinessScriptType = rssProcessControlVb.getReadinessScriptType();
			String readinessScript = rssProcessControlVb.getReadinessScript();
			if (!"NONE".equalsIgnoreCase(readinessScriptType)) {
				int retVal = doDBConnection(rssProcessControlVb);
				if (retVal == ERROR_OPERATION) {
					return ERROR_OPERATION;
				}
				String scriptType = "";
				if ("MACROVAR".equalsIgnoreCase(readinessScriptType)) {
					List<String> hashVariableList = new CommonDao(jdbcTemplate).getDynamicHashVariable(readinessScript);
					if (hashVariableList != null && !hashVariableList.isEmpty()) {
						readinessScript = hashVariableList.get(0);
						scriptType = hashVariableList.get(1);
					}
				} else {
					scriptType = readinessScriptType;
				}
				if ("PLSQL".equalsIgnoreCase(scriptType)) {
					String readinessStatus = callReadinessProc(readinessScript);
					String[] array = readinessStatus.split("@");
					String status = array[0];
					String ErrorMessage = array[1];
					if (!"0".equalsIgnoreCase(status)) {
						return READINESS_ERRONEOUS_EXIT;
					}
					readinessFlag = SUCCESS_OPERATION;
				} else if ("PLSQLFUNC".equalsIgnoreCase(scriptType)) {
					String readinessStatus = callReadinessProcFunction(readinessScript);
					String[] array = readinessStatus.split("@");
					String status = array[0];
					String ErrorMessage = array[1];
					if (!"0".equalsIgnoreCase(status)) {
						return READINESS_ERRONEOUS_EXIT;
					}
					readinessFlag = SUCCESS_OPERATION;
				} else if ("PLSQLB".equalsIgnoreCase(scriptType)) {
					new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
							"Readiness Script", "PLSQL Block is not allowed for Readiness script !");
					return READINESS_ERRONEOUS_EXIT;
				} else if ("SQL".equalsIgnoreCase(scriptType)) {
					String query = readinessScript;
					readinessFlag = getReportProcessReadiness(query);
					if (readinessFlag == SUCCESS_OPERATION)
						new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
								"Readiness Process : Execute Successfully ", "Execute Successfully");
				} else if ("UNIXSCRIPT".equalsIgnoreCase(scriptType)) {
					String parseAcquisitionReadinessScripts = acquisitionReadinessScripts;
					// String parseAcquisitionReadinessScripts=
					// replaceMacroValuesInFilePattern(acquisitionReadinessScripts);
					new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
							"Readiness Process : Checking Readiness",
							"Readiness Process : Checking Readiness [" + parseAcquisitionReadinessScripts + "]  ");
					int uploadReturnValue = execUnxComAndGetResult(serverUser, serverPassword, serverIp, serverPort,
							parseAcquisitionReadinessScripts, unixScriptDir);
					if (uploadReturnValue != 0) {
						new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
								"Readiness  Script Execute Failed",
								"Readiness Script Execute Failed [" + parseAcquisitionReadinessScripts
										+ "] : Return Value[" + uploadReturnValue + "]");
						return ERROR_OPERATION;
					}
					new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
							"Readiness Process : Readiness Sucessfully", "Readiness Process : Readiness Sucessfully");
					readinessFlag = uploadReturnValue;
				}
			}
		} catch (Exception e) {
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
					"Readiness Check Error", e.getMessage());
			readinessFlag = READINESS_ERRONEOUS_EXIT;
		} finally {
			try {
				readinessConnectionClose();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return readinessFlag;
	}

	public void readinessConnectionClose() throws SQLException, Exception {
		try {
			if (readinessDbConnection != null) {
				readinessDbConnection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Readiness DB Connection Close",
					"Error on Closing Readiness DB Connection[" + e.getMessage() + "]");
		}
	}

	private int execUnxComAndGetResult(String user, String password, String host, String ftpPort, String command,
			String scriptDir) {
		int returnVal = -1;
		int port = 22;
		if (isValid(serverPort)) {
			port = Integer.parseInt(serverPort);
		}
		try {
			JSch jsch = new JSch();
			Session session = jsch.getSession(user, host, port);
			session.setPassword(password);
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("shell");
			OutputStream inputstream_for_the_channel = channel.getOutputStream();
			PrintStream commander = new PrintStream(inputstream_for_the_channel, true);
			channel.connect();
			InputStream in = channel.getInputStream();
			commander.println("cd " + scriptDir);
			StringBuilder cmd = new StringBuilder();
			cmd.append(command);
			commander.println(cmd);
			commander.println("exit");
			commander.close();
			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
				}
				if (channel.isClosed()) {
					returnVal = channel.getExitStatus();
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					return returnVal;
				}
			}
			channel.disconnect();
			session.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			return returnVal;
		}
		return returnVal;
	}

	private String getKeyValue(String source, String key) {
		try {
			Matcher regexMatcher = Pattern.compile("\\{" + key + ":#(.*?)\\$@!(.*?)\\#}").matcher(source); // Changed to overcome partially similar pattern - DD
			String findValue = regexMatcher.find() ? regexMatcher.group(2) : "";
			return isValid(findValue) ? findValue : "";
		} catch (Exception e) {
			return null;
		}
	}

	private int doDBConnection(RSSProcessControlVb rssProcessControlVb) throws SQLException, Exception {
		int retVal = 0;
		//String username  =  System.getenv("VISION_USER_NAME");
		//String password  =  System.getenv("VISION_PASSWORD");
		//String jdbcUrlMain  =  System.getenv("VISION_APP_CONNECT_STRING");
		String classPath = System.getenv("VISION_CLASS_PATH");
		if ("MACROVAR".equalsIgnoreCase(connectivityType)) {
			connectivityDetails = new CommonDao(jdbcTemplate).getScriptValue(rssProcessControlVb.getConnectivityDetails());
		}
		if ("MACROVAR".equalsIgnoreCase(databaseType)) {
			databaseConnectivityDetails = new CommonDao(jdbcTemplate).getScriptValue(rssProcessControlVb.getDatabaseConnectivityDetails());
		}

		serverIp = getKeyValue(connectivityDetails, "SERVER_IP");
		serverHostName = getKeyValue(connectivityDetails, "SERVER_HOSTNAME");
		serverUser = getKeyValue(connectivityDetails, "SERVER_USER");
		serverPassword = getKeyValue(connectivityDetails, "SERVER_PWD");
		serverPort = getKeyValue(connectivityDetails, "SERVER_PORT");

		dbServiceName = getKeyValue(databaseConnectivityDetails, "SERVICE_NAME");
		dbServiceName = getKeyValue(databaseConnectivityDetails, "SERVICENAME");
		dbOracleSid = getKeyValue(databaseConnectivityDetails, "SID");
		dbUserName = getKeyValue(databaseConnectivityDetails, "USER");
		dbPassWord = getKeyValue(databaseConnectivityDetails, "PWD");
		dbPortNumber = getKeyValue(databaseConnectivityDetails, "DB_PORT");
		dbPostFix = getKeyValue(databaseConnectivityDetails, "SUFFIX");
		dbPreFix = getKeyValue(databaseConnectivityDetails, "PREFIX");
		dataBaseName = getKeyValue(databaseConnectivityDetails, "DB_NAME");
		dataBaseType = getKeyValue(databaseConnectivityDetails, "DATABASE_TYPE");
		dbInstance = getKeyValue(databaseConnectivityDetails, "DB_INSTANCE");
		dbIp = getKeyValue(databaseConnectivityDetails, "DB_IP");
		serverName = getKeyValue(databaseConnectivityDetails, "SERVER_NAME");
		dbSetParam1 = getKeyValue(databaseConnectivityDetails, "DB_SET_PARAM1");
		dbSetParam2 = getKeyValue(databaseConnectivityDetails, "DB_SET_PARAM2");
		dbSetParam3 = getKeyValue(databaseConnectivityDetails, "DB_SET_PARAM3");
		version = getKeyValue(databaseConnectivityDetails, "JAR_VERSION");

		String hostName = dbServiceName;
		if (!isValid(hostName)) {
			hostName = dbOracleSid;
		}
		/*if (!isValid(serverIp)) {
			dbIP = serverIp;
		}
		if (isValid(dbIp))
			dbIP = dbIp;
		else
			dbIP = serverIp;*/

		retVal = new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId, "Connecting to source database",
				"Connecting to source [" + dataBaseType + "] database");
		// getExtractionTimeout();
		if ("ORACLE".equalsIgnoreCase(dataBaseType)) {
			dbJdbcUrl = "jdbc:oracle:thin:@" + dbIp + ":" + dbPortNumber + ":" + hostName;
			retVal = getDbConnection(dbJdbcUrl, dbUserName, dbPassWord, "ORACLE", version, classPath);
		} else if ("MSSQL".equalsIgnoreCase(dataBaseType)) {
			if (isValid(dbInstance) && isValid(hostName)) {
				dbJdbcUrl = "jdbc:sqlserver://" + dbIp + ":" + dbPortNumber + ";instanceName=" + dbInstance
						+ ";databaseName=" + hostName;
			} else if (isValid(dbInstance) && !isValid(hostName)) {
				dbJdbcUrl = "jdbc:sqlserver://" + dbIp + ":" + dbPortNumber + ";instanceName=" + dbInstance;
			} else if (!isValid(dbInstance) && isValid(hostName)) {
				dbJdbcUrl = "jdbc:sqlserver://" + dbIp + ":" + dbPortNumber + ";databaseName=" + hostName;
			} else {
				dbJdbcUrl = "jdbc:sqlserver://" + dbIp + ":" + dbPortNumber + ";databaseName=" + hostName;
			}
			retVal = getDbConnection(dbJdbcUrl, dbUserName, dbPassWord, "MSSQL", version,classPath);
		} else if ("MYSQL".equalsIgnoreCase(dataBaseType)) {

			if (isValid(dbInstance) && isValid(dataBaseName)) {
				dbJdbcUrl = "jdbc:mysql://" + dbIp + ":" + dbPortNumber + "//instanceName=" + dbInstance
						+ "//databaseName=" + dataBaseName;
			} else if (isValid(dbInstance) && !isValid(dataBaseName)) {
				dbJdbcUrl = "jdbc:mysql://" + dbIp + ":" + dbPortNumber + "//instanceName=" + dbInstance;
			} else if (!isValid(dbInstance) && isValid(dataBaseName)) {
				dbJdbcUrl = "jdbc:mysql://" + dbIp + ":" + dbPortNumber + "/" + dataBaseName;
			} else {
				dbJdbcUrl = "jdbc:mysql://" + dbIp + ":" + dbPortNumber + "/" + dataBaseName;
			}

			retVal = getDbConnection(dbJdbcUrl, dbUserName, dbPassWord, "MYSQL", version,classPath);
		} else if ("POSTGRESQL".equalsIgnoreCase(dataBaseType)) {
			dbJdbcUrl = "jdbc:postgresql://" + dbIP + ":" + dbPortNumber + "/" + dataBaseName;
			retVal = getDbConnection(dbJdbcUrl, dbUserName, dbPassWord, "POSTGRESQL", version,classPath);
		} else if ("SYBASE".equalsIgnoreCase(dataBaseType)) {
			dbJdbcUrl = "jdbc:sybase:Tds:" + dbIP + ":" + dbPortNumber + "?ServiceName=" + hostName;
			retVal = getDbConnection(dbJdbcUrl, dbUserName, dbPassWord, "SYBASE", version,classPath);
		} else if ("INFORMIX".equalsIgnoreCase(dataBaseType)) {
			dbJdbcUrl = "jdbc:informix-sqli://" + dbIP + ":" + dbPortNumber + "/" + dataBaseName + ":informixserver="
					+ serverName;
			retVal = getDbConnection(dbJdbcUrl, dbUserName, dbPassWord, "INFORMIX", version,classPath);
		} else if ("DB2".equalsIgnoreCase(dataBaseType)) {
			dbJdbcUrl = "jdbc:db2://" + dbIP + ":" + dbPortNumber + "/" + dataBaseName;
			retVal = getDbConnection(dbJdbcUrl, dbUserName, dbPassWord, "DB2", version,classPath);
		}
		if (retVal == SUCCESS_OPERATION) {
			retVal = new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Successfully Connected to source database",
					"Successfully Connected to source [" + dataBaseType + "] database");
		} else {
			retVal = new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Failed while connecting to source Data Base", "Data Base Connection Failure  User Name : [" + dbUserName + "] Password : [****]");
			return ERROR_OPERATION;
		}
		return SUCCESS_OPERATION;
	}

	public String callReadinessProc(String acquisitionReadinessScripts) throws Exception {
		CallableStatement cs = null;
		String procedure = "";
		String status = "0";
		String errorMsg = "";
		try {
			int outPrams = 2;
			procedure = acquisitionReadinessScripts;
			procedure = procedure.replaceAll("#OUTPUT_STATUS#", "?");
			procedure = procedure.replaceAll("#OUTPUT_ERRORMSG#", "?");
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
					"Readiness Process : Checking Readiness",
					"Readiness Procedure : Checking Readiness [" + procedure + "]");
			cs = readinessDbConnection.prepareCall("{call " + procedure + "}");
			for (int idx = 1; idx <= outPrams; idx++) {
				cs.registerOutParameter(idx, java.sql.Types.VARCHAR);
			}
			ResultSet rs = cs.executeQuery();
			for (int idx = 1; idx <= outPrams; idx++) {
				if (idx == 1)
					status = cs.getString(1);
				if (idx == 2)
					errorMsg = cs.getString(2);
			}
			rs.close();
			// System.out.println("Readiness Script Return : "+status);
			if (!"0".equalsIgnoreCase(status)) {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
						"Readiness Process : Readiness Failed",
						"Readiness Process : Execute PLSQL Failed : [" + procedure + "]  Error : " + errorMsg.trim());
			} else {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
						"Readiness Process : Readiness Successfully",
						"Readiness Process : Execute PLSQL Failed : [" + procedure + "]  Error : " + errorMsg.trim());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
					"Execute Procedure Failed ", ex.getMessage().trim());
			return "-1" + "@" + ex.getMessage();
		}
		return status + "@" + errorMsg;
	}

	public String callReadinessProcFunction(String acquisitionReadinessScripts) throws Exception {
		CallableStatement cs = null;
		String procedure = "";
		String status = "0";
		String errorMsg = "";
		try {
			procedure = acquisitionReadinessScripts;
			int outPrams = 1;
			procedure = procedure.replaceAll("#OUTPUT_STATUS#", "?");
			// procedure = procedure.replaceAll("#OUTPUT_ERRORMSG#", "?");
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
					"Readiness Process : Checking Readiness",
					"Readiness Procedure : Checking Readiness [" + procedure + "]");
			// System.out.println("ReadinessProc Procedure : "+procedure);
			cs = readinessDbConnection.prepareCall("{?=call " + procedure + "}");
			for (int idx = 1; idx <= outPrams; idx++) {
				cs.registerOutParameter(idx, java.sql.Types.VARCHAR);
			}
			ResultSet rs = cs.executeQuery();
			for (int idx = 1; idx <= outPrams; idx++) {
				if (idx == 1)
					status = cs.getString(1);
			}
			rs.close();
			// System.out.println("Readiness Script Return : "+status);
			if (!"0".equalsIgnoreCase(status)) {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
						"Readiness Process : Readiness Failed", "Readiness Procedure : Execute PLSQLFUNC Failed : ["
								+ procedure + "]  Error : " + errorMsg.trim());
			} else {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
						"Readiness Process : Readiness Successfully",
						"Readiness Procedure : Execute PLSQLFUNC Failed : [" + procedure + "]  Error : "
								+ errorMsg.trim());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId,
					"Readiness Process : Execute Procedure Failed ", ex.getMessage().trim());
			return "-1" + "@" + ex.getMessage();
		}
		return status + "@" + errorMsg;
	}

	private boolean isValid(String pInput) {
		return !((pInput == null) || (pInput.trim().length() == 0) || ("".equals(pInput)));
	}

	public int getReportProcessReadiness(String readinessQuery) throws Exception {
		String currentExeQuery = "";
		String readinessValue = "-1";
		int retVal = SUCCESS_OPERATION;
		try {
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Readiness Process : Checking Readiness",
					"Readiness SQL : Checking Readiness [" + readinessQuery + "]  ");
//			System.out.println("Readiness Query : "+readinessQuery);
//			PreparedStatement preStatement = connection.prepareStatement(readinessQuery);
			Statement stmtReadiness = readinessDbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			if (isValid(dbSetParam1)) {
				currentExeQuery = dbSetParam1;
				stmtReadiness.executeUpdate(currentExeQuery);
			}
			if (isValid(dbSetParam2)) {
				currentExeQuery = dbSetParam2;
				stmtReadiness.executeUpdate(currentExeQuery);
			}
			if (isValid(dbSetParam3)) {
				currentExeQuery = dbSetParam3;
				stmtReadiness.executeUpdate(currentExeQuery);
			}
			currentExeQuery = readinessQuery;
			ResultSet rs = stmtReadiness.executeQuery(currentExeQuery);
			while (rs.next()) {
				readinessValue = rs.getString(1);
			}
			if (!isValid(readinessValue) || "-1".equalsIgnoreCase(readinessValue)) {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
						"Readiness Process : Readiness Failed", "Readiness Check failed : There is no data found");
				retVal = READINESS_ERRONEOUS_EXIT;
			}
			rs.close();
			stmtReadiness.close();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Readiness Process : Readiness Failed", "Readiness Script Execution failed :" + e.getMessage()
							+ "Executeing Query : [" + currentExeQuery + "]");
			retVal = READINESS_ERRONEOUS_EXIT;
		} catch (SQLException e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Readiness Process : Readiness Failed", "Readiness Script Execution failed :" + e.getMessage()
							+ "Executeing Query : [" + currentExeQuery + "]");
			retVal = READINESS_ERRONEOUS_EXIT;
		}
		return retVal;
	}

	private int getDbConnection(String jdbcUrl, String username, String password, String type, String version,String classPath)
			throws SQLException, Exception {
		try {
			if ("ORACLE".equalsIgnoreCase(type)) {
				URL ojdbcJar;
				//System.out.println("***ClassPath:" + classPath);
				if ("1".equalsIgnoreCase(version)) {
					ojdbcJar = new URL("file:" + classPath + "ojdbc7.jar");
				} else {
					ojdbcJar = new URL("file:" + classPath + "ojdbc8.jar");
				}
				String classname = "oracle.jdbc.driver.OracleDriver";
				URLClassLoader ucl = new URLClassLoader(new URL[] { ojdbcJar });
				Driver d = (Driver) Class.forName(classname, true, ucl).newInstance();
				DriverManager.registerDriver(new DriverShim(d));
			} else if ("MSSQL".equalsIgnoreCase(type))
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			else if ("MYSQL".equalsIgnoreCase(type))
				Class.forName("com.mysql.jdbc.Driver");
			else if ("POSTGRESQL".equalsIgnoreCase(type))
				Class.forName("org.postgresql.Driver");
			else if ("SYBASE".equalsIgnoreCase(type))
				Class.forName("com.sybase.jdbc4.jdbc.SybDataSource");
			else if ("INFORMIX".equalsIgnoreCase(type))
				Class.forName("com.informix.jdbc.IfxDriver");
			else if ("DB2".equalsIgnoreCase(type))
				Class.forName("com.ibm.db2.jcc.DB2Driver");

			readinessDbConnection = DriverManager.getConnection(jdbcUrl, username, password);
			return SUCCESS_OPERATION;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Error while connecting to the database", "Errorconnecting to the database - " + e.getMessage());
			return ERROR_OPERATION;
		} catch (SQLException e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Error while connecting to the database", "Errorconnecting to the database - " + e.getMessage());
			return ERROR_OPERATION;
		} catch (MalformedURLException e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Error while connecting to the database", "Errorconnecting to the database - " + e.getMessage());
			return ERROR_OPERATION;
		} catch (InstantiationException e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Error while connecting to the database", "Errorconnecting to the database - " + e.getMessage());
			return ERROR_OPERATION;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Error while connecting to the database", "Errorconnecting to the database - " + e.getMessage());
			return ERROR_OPERATION;
		} catch (Exception e) {
			e.printStackTrace();
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(processId,
					"Error while connecting to the database", "Errorconnecting to the database - " + e.getMessage());
			return ERROR_OPERATION;
		}
	}

	class DriverShim implements Driver {
		private Driver driver;

		DriverShim(Driver d) {
			this.driver = d;
		}

		public boolean acceptsURL(String u) throws SQLException {
			//System.out.println(this.driver.acceptsURL(u));
			return this.driver.acceptsURL(u);
		}

		public Connection connect(String u, Properties p) throws SQLException {
			return this.driver.connect(u, p);
		}

		public int getMajorVersion() {
			return this.driver.getMajorVersion();
		}

		public int getMinorVersion() {
			return this.driver.getMinorVersion();
		}

		public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
			return this.driver.getPropertyInfo(u, p);
		}

		public boolean jdbcCompliant() {
			return this.driver.jdbcCompliant();
		}

		@Override
		public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return null;
		}
	}

	private FTPClient getConnection() throws IOException {
		FTPClient ftpClient = new FTPClient();
		FTPClientConfig conf = new FTPClientConfig("U");
		ftpClient.configure(conf);
		return ftpClient;
	}
	public ExceptionCode getCatalogData(RSSProcessControlVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			VcForQueryReportVb vcForQueryReportVb = new VcForQueryReportVb();
			vcForQueryReportVb.setCatalogId(vObject.getCatalogId());
			vcForQueryReportVb.setReportId(vObject.getReportId());
			String reportTitle = vObject.getReportTitle();
			exceptionCode = new DesignAnalysisWb(jdbcTemplate).getReportDetailFromReportDefs(vcForQueryReportVb);
			VcForQueryReportFieldsWrapperVb wrapperVb = (VcForQueryReportFieldsWrapperVb) exceptionCode.getResponse();
			if(exceptionCode.getErrorCode()==Constants.SUCCESSFUL_OPERATION) {
				/*wrapperVb.setHashArray(vObjectMain.getHashArray());
				wrapperVb.setHashValueArray(vObjectMain.getHashValueArray());*/
				wrapperVb.getMainModel().setStartIndex(1);
				wrapperVb.getMainModel().setLastIndex(1000);
			}
			
			//wrapperVb.getMainModel().setTableName(vObjectMain.getMainModel().getTableName());
			wrapperVb.getMainModel().setReportName(reportTitle);
			//System.out.println("DD1: controller Start");
			HttpServletResponse response = null;
			exceptionCode = new DesignAnalysisExportWb(jdbcTemplate).generateExcel(wrapperVb);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				response = new DesignAnalysisExportWb(jdbcTemplate).setFileResponse(exceptionCode, vObject.getReportId(), response);
			} else {
				response.setStatus(Constants.ERRONEOUS_OPERATION, "File Not Found");
				//System.out.println("DD100: controller error End2");
			}
		}catch(Exception e) {
			
		}
		return exceptionCode;
	}

	@Override
	protected AbstractDao<RSSProcessControlVb> getScreenDao() {
		return new ScheduleReportDao(jdbcTemplate);
	}

	@Override
	protected void setAtNtValues(RSSProcessControlVb vObject) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void setVerifReqDeleteType(RSSProcessControlVb vObject) {
		// TODO Auto-generated method stub
		
	}
}