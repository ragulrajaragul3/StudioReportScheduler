package com.vision.vb;

public class RSSProcessControlVb extends CommonVb{

	private String processId = "";
	private String versionNo = "";
	private long userId = 0;
	private String scheduleSequenceNo = "1";
	private String reportId = "";
	private Boolean attachXl = false;
	private Boolean attachPdf = false;
	private Boolean attachHtml = false;
	private Boolean attachCsv = false;
	private Boolean emailFlag = false;
	private Boolean ftpFlag = false;
	private String ftpVarName ="";
	private String excelExportType="";
	private String burstId = "";
	private String burstFlag = "";
	private int burstSequenceNo = 0;
	private String blankReportFlag = "N";
	private String emailSubject = "";
	private String emailBody = "";
	private String emailHeader = "";
	private String emailFooter = "";
	private String emailTo = "";
	private String emailCc = "";
	private String emailBcc = "";
	private String emailFcc = "";
	private String promptValue1 = "";
	private String promptValue2 = "";
	private String promptValue3 = "";
	private String promptValue4 = "";
	private String promptValue5 = "";
	private String promptValue6 = "";
	private String promptValue7 = "";
	private String promptValue8 = "";
	private String promptValue9 = "";
	private String promptValue10 = "";
	private String promptValue1Desc = "";
	private String promptValue2Desc = "";
	private String promptValue3Desc = "";
	private String promptValue4Desc = "";
	private String promptValue5Desc = "";
	private String promptValue6Desc = "";
	private String promptValue7Desc = "";
	private String promptValue8Desc = "";
	private String promptValue9Desc = "";
	private String promptValue10Desc = "";
    private String attachFileName = "";
	private String scallingFactor = "";
	private String readinessScript ="";
	private String readinessScriptType ="";
	private String databaseConnectivityDetails = "";
	private String connectivityDetails = "";
	private String reportTitle = "";
	private String promptHashVar ="";
	
	private String fileSize = "";
	private int reportTypeAt =713;
    private String reportType="";
	private String formatType = "E";
	private String subHeaderList = "";
	private String schedulerEmailFrom="";
	private String schedulerSupportEmail="";
	private String swfFileName = "";
	private String errorDescription =""; 
	private String sessionId = "";
	private String scheduleStartDate = "";
	private String scheduleEndDate = "";
	private String scheduleType = "";
	//private String nextLevel = "";
	private String promptLabel1 = "";
	private String promptLabel2 = "";
	private String promptLabel3 = "";
	private String promptLabel4 = "";
	private String promptLabel5 = "";
	private String promptLabel6 = "";
	private String promptLabel7 = "";
	private String promptLabel8 = "";
	private String promptLabel9 = "";
	private String promptLabel10 = "";
	private String currentLevel = "";
	private String catalogId = "";
	
	public String getScheduleType() {
		return scheduleType;
	}
	public void setScheduleType(String scheduleType) {
		this.scheduleType = scheduleType;
	}
	public String getScheduleStartDate() {
		return scheduleStartDate;
	}
	public void setScheduleStartDate(String scheduleStartDate) {
		this.scheduleStartDate = scheduleStartDate;
	}
	public String getScheduleEndDate() {
		return scheduleEndDate;
	}
	public void setScheduleEndDate(String scheduleEndDate) {
		this.scheduleEndDate = scheduleEndDate;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getErrorDescription() {
		return errorDescription;
	}
	public void setErrorDescription(String errorDescription) {
		this.errorDescription = errorDescription;
	}
	public String getSwfFileName() {
		return swfFileName;
	}
	public void setSwfFileName(String swfFileName) {
		this.swfFileName = swfFileName;
	}
	public String getSchedulerEmailFrom() {
		return schedulerEmailFrom;
	}
	public void setSchedulerEmailFrom(String schedulerEmailFrom) {
		this.schedulerEmailFrom = schedulerEmailFrom;
	}
	public String getSchedulerSupportEmail() {
		return schedulerSupportEmail;
	}
	public void setSchedulerSupportEmail(String schedulerSupportEmail) {
		this.schedulerSupportEmail = schedulerSupportEmail;
	}
	public String getSubHeaderList() {
		return subHeaderList;
	}
	public void setSubHeaderList(String subHeaderList) {
		this.subHeaderList = subHeaderList;
	}
	public String getFormatType() {
		return formatType;
	}
	public void setFormatType(String formatType) {
		this.formatType = formatType;
	}
	
	public int getReportTypeAt() {
		return reportTypeAt;
	}
	public void setReportTypeAt(int reportTypeAt) {
		this.reportTypeAt = reportTypeAt;
	}
	public String getReportType() {
		return reportType;
	}
	public void setReportType(String reportType) {
		this.reportType = reportType;
	}
	public String getFileSize() {
		return fileSize;
	}
	public void setFileSize(String fileSize) {
		this.fileSize = fileSize;
	}
	public String getProcessId() {
		return processId;
	}
	public void setProcessId(String processId) {
		this.processId = processId;
	}
	public String getVersionNo() {
		return versionNo;
	}
	public void setVersionNo(String versionNo) {
		this.versionNo = versionNo;
	}
	public long getUserId() {
		return userId;
	}
	public void setUserId(long userId) {
		this.userId = userId;
	}
	public String getScheduleSequenceNo() {
		return scheduleSequenceNo;
	}
	public void setScheduleSequenceNo(String scheduleSequenceNo) {
		this.scheduleSequenceNo = scheduleSequenceNo;
	}
	public String getReportId() {
		return reportId;
	}
	public void setReportId(String reportId) {
		this.reportId = reportId;
	}
	public Boolean getAttachXl() {
		return attachXl;
	}
	public void setAttachXl(Boolean attachXl) {
		this.attachXl = attachXl;
	}
	public Boolean getAttachPdf() {
		return attachPdf;
	}
	public void setAttachPdf(Boolean attachPdf) {
		this.attachPdf = attachPdf;
	}
	public Boolean getAttachHtml() {
		return attachHtml;
	}
	public void setAttachHtml(Boolean attachHtml) {
		this.attachHtml = attachHtml;
	}
	public Boolean getAttachCsv() {
		return attachCsv;
	}
	public void setAttachCsv(Boolean attachCsv) {
		this.attachCsv = attachCsv;
	}
	public Boolean getEmailFlag() {
		return emailFlag;
	}
	public void setEmailFlag(Boolean emailFlag) {
		this.emailFlag = emailFlag;
	}
	public Boolean getFtpFlag() {
		return ftpFlag;
	}
	public void setFtpFlag(Boolean ftpFlag) {
		this.ftpFlag = ftpFlag;
	}
	public String getFtpVarName() {
		return ftpVarName;
	}
	public void setFtpVarName(String ftpVarName) {
		this.ftpVarName = ftpVarName;
	}
	public String getExcelExportType() {
		return excelExportType;
	}
	public void setExcelExportType(String excelExportType) {
		this.excelExportType = excelExportType;
	}
	public String getBurstId() {
		return burstId;
	}
	public void setBurstId(String burstId) {
		this.burstId = burstId;
	}
	public String getBurstFlag() {
		return burstFlag;
	}
	public void setBurstFlag(String burstFlag) {
		this.burstFlag = burstFlag;
	}
	public int getBurstSequenceNo() {
		return burstSequenceNo;
	}
	public void setBurstSequenceNo(int burstSequenceNo) {
		this.burstSequenceNo = burstSequenceNo;
	}
	public String getBlankReportFlag() {
		return blankReportFlag;
	}
	public void setBlankReportFlag(String blankReportFlag) {
		this.blankReportFlag = blankReportFlag;
	}
	public String getEmailSubject() {
		return emailSubject;
	}
	public void setEmailSubject(String emailSubject) {
		this.emailSubject = emailSubject;
	}
	public String getEmailBody() {
		return emailBody;
	}
	public void setEmailBody(String emailBody) {
		this.emailBody = emailBody;
	}
	public String getEmailHeader() {
		return emailHeader;
	}
	public void setEmailHeader(String emailHeader) {
		this.emailHeader = emailHeader;
	}
	public String getEmailFooter() {
		return emailFooter;
	}
	public void setEmailFooter(String emailFooter) {
		this.emailFooter = emailFooter;
	}
	public String getEmailTo() {
		return emailTo;
	}
	public void setEmailTo(String emailTo) {
		this.emailTo = emailTo;
	}
	public String getEmailCc() {
		return emailCc;
	}
	public void setEmailCc(String emailCc) {
		this.emailCc = emailCc;
	}
	public String getEmailBcc() {
		return emailBcc;
	}
	public void setEmailBcc(String emailBcc) {
		this.emailBcc = emailBcc;
	}
	public String getEmailFcc() {
		return emailFcc;
	}
	public void setEmailFcc(String emailFcc) {
		this.emailFcc = emailFcc;
	}
	public String getPromptValue1() {
		return promptValue1;
	}
	public void setPromptValue1(String promptValue1) {
		this.promptValue1 = promptValue1;
	}
	public String getPromptValue2() {
		return promptValue2;
	}
	public void setPromptValue2(String promptValue2) {
		this.promptValue2 = promptValue2;
	}
	public String getPromptValue3() {
		return promptValue3;
	}
	public void setPromptValue3(String promptValue3) {
		this.promptValue3 = promptValue3;
	}
	public String getPromptValue4() {
		return promptValue4;
	}
	public void setPromptValue4(String promptValue4) {
		this.promptValue4 = promptValue4;
	}
	public String getPromptValue5() {
		return promptValue5;
	}
	public void setPromptValue5(String promptValue5) {
		this.promptValue5 = promptValue5;
	}
	public String getPromptValue6() {
		return promptValue6;
	}
	public void setPromptValue6(String promptValue6) {
		this.promptValue6 = promptValue6;
	}
	public String getPromptValue1Desc() {
		return promptValue1Desc;
	}
	public void setPromptValue1Desc(String promptValue1Desc) {
		this.promptValue1Desc = promptValue1Desc;
	}
	public String getPromptValue2Desc() {
		return promptValue2Desc;
	}
	public void setPromptValue2Desc(String promptValue2Desc) {
		this.promptValue2Desc = promptValue2Desc;
	}
	public String getPromptValue3Desc() {
		return promptValue3Desc;
	}
	public void setPromptValue3Desc(String promptValue3Desc) {
		this.promptValue3Desc = promptValue3Desc;
	}
	public String getPromptValue4Desc() {
		return promptValue4Desc;
	}
	public void setPromptValue4Desc(String promptValue4Desc) {
		this.promptValue4Desc = promptValue4Desc;
	}
	public String getPromptValue5Desc() {
		return promptValue5Desc;
	}
	public void setPromptValue5Desc(String promptValue5Desc) {
		this.promptValue5Desc = promptValue5Desc;
	}
	public String getPromptValue6Desc() {
		return promptValue6Desc;
	}
	public void setPromptValue6Desc(String promptValue6Desc) {
		this.promptValue6Desc = promptValue6Desc;
	}
	public String getAttachFileName() {
		return attachFileName;
	}
	public void setAttachFileName(String attachFileName) {
		this.attachFileName = attachFileName;
	}
	public String getScallingFactor() {
		return scallingFactor;
	}
	public void setScallingFactor(String scallingFactor) {
		this.scallingFactor = scallingFactor;
	}
	public String getReadinessScript() {
		return readinessScript;
	}
	public void setReadinessScript(String readinessScript) {
		this.readinessScript = readinessScript;
	}
	public String getReadinessScriptType() {
		return readinessScriptType;
	}
	public void setReadinessScriptType(String readinessScriptType) {
		this.readinessScriptType = readinessScriptType;
	}
	public String getDatabaseConnectivityDetails() {
		return databaseConnectivityDetails;
	}
	public void setDatabaseConnectivityDetails(String databaseConnectivityDetails) {
		this.databaseConnectivityDetails = databaseConnectivityDetails;
	}
	public String getConnectivityDetails() {
		return connectivityDetails;
	}
	public void setConnectivityDetails(String connectivityDetails) {
		this.connectivityDetails = connectivityDetails;
	}
	public String getReportTitle() {
		return reportTitle;
	}
	public void setReportTitle(String reportTitle) {
		this.reportTitle = reportTitle;
	}
	public String getPromptHashVar() {
		return promptHashVar;
	}
	public void setPromptHashVar(String promptHashVar) {
		this.promptHashVar = promptHashVar;
	}
	/*public String getNextLevel() {
		return nextLevel;
	}
	public void setNextLevel(String nextLevel) {
		this.nextLevel = nextLevel;
	}*/
	public String getPromptValue7() {
		return promptValue7;
	}
	public void setPromptValue7(String promptValue7) {
		this.promptValue7 = promptValue7;
	}
	public String getPromptValue8() {
		return promptValue8;
	}
	public void setPromptValue8(String promptValue8) {
		this.promptValue8 = promptValue8;
	}
	public String getPromptValue9() {
		return promptValue9;
	}
	public void setPromptValue9(String promptValue9) {
		this.promptValue9 = promptValue9;
	}
	public String getPromptValue10() {
		return promptValue10;
	}
	public void setPromptValue10(String promptValue10) {
		this.promptValue10 = promptValue10;
	}
	public String getPromptLabel1() {
		return promptLabel1;
	}
	public void setPromptLabel1(String promptLabel1) {
		this.promptLabel1 = promptLabel1;
	}
	public String getPromptLabel2() {
		return promptLabel2;
	}
	public void setPromptLabel2(String promptLabel2) {
		this.promptLabel2 = promptLabel2;
	}
	public String getPromptLabel3() {
		return promptLabel3;
	}
	public void setPromptLabel3(String promptLabel3) {
		this.promptLabel3 = promptLabel3;
	}
	public String getPromptLabel4() {
		return promptLabel4;
	}
	public void setPromptLabel4(String promptLabel4) {
		this.promptLabel4 = promptLabel4;
	}
	public String getPromptLabel5() {
		return promptLabel5;
	}
	public void setPromptLabel5(String promptLabel5) {
		this.promptLabel5 = promptLabel5;
	}
	public String getPromptLabel6() {
		return promptLabel6;
	}
	public void setPromptLabel6(String promptLabel6) {
		this.promptLabel6 = promptLabel6;
	}
	public String getPromptValue7Desc() {
		return promptValue7Desc;
	}
	public void setPromptValue7Desc(String promptValue7Desc) {
		this.promptValue7Desc = promptValue7Desc;
	}
	public String getPromptValue8Desc() {
		return promptValue8Desc;
	}
	public void setPromptValue8Desc(String promptValue8Desc) {
		this.promptValue8Desc = promptValue8Desc;
	}
	public String getPromptValue9Desc() {
		return promptValue9Desc;
	}
	public void setPromptValue9Desc(String promptValue9Desc) {
		this.promptValue9Desc = promptValue9Desc;
	}
	public String getPromptValue10Desc() {
		return promptValue10Desc;
	}
	public void setPromptValue10Desc(String promptValue10Desc) {
		this.promptValue10Desc = promptValue10Desc;
	}
	public String getPromptLabel7() {
		return promptLabel7;
	}
	public void setPromptLabel7(String promptLabel7) {
		this.promptLabel7 = promptLabel7;
	}
	public String getPromptLabel8() {
		return promptLabel8;
	}
	public void setPromptLabel8(String promptLabel8) {
		this.promptLabel8 = promptLabel8;
	}
	public String getPromptLabel9() {
		return promptLabel9;
	}
	public void setPromptLabel9(String promptLabel9) {
		this.promptLabel9 = promptLabel9;
	}
	public String getPromptLabel10() {
		return promptLabel10;
	}
	public void setPromptLabel10(String promptLabel10) {
		this.promptLabel10 = promptLabel10;
	}
	public String getCurrentLevel() {
		return currentLevel;
	}
	public void setCurrentLevel(String currentLevel) {
		this.currentLevel = currentLevel;
	}
	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
}