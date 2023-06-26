package com.vision.vb;

public class ScheduleReportVb extends CommonVb{

	private static final long serialVersionUID = 1410935578921368062L;
	private String reportId = "";
	private long userId = 0;
	private String scheduleType = "";
	private String scheduleStartDate = "";
	private String scheduleEndDate = "";
	private String formatType = "";
	private String emailTo = "";
	private String emailCc = "";
	private String scheduleStatus = "";
	private String promptValue1 = "";
	private String promptValue2 = "";
	private String promptValue3 = "";
	private String promptValue4 = "";
	private String promptValue5 = "";
	private String promptValue6 = "";
	private String promptValue1Desc = "";
	private String promptValue2Desc = "";
	private String promptValue3Desc = "";
	private String promptValue4Desc = "";
	private String promptValue5Desc = "";
	private String promptValue6Desc = "";
	private String emailStatus = "";
	private String scallingFactor = "";
	private int rsScheduleStatusNt = 0;
	private int rsScheduleStatus = -1;
	private String emailFrom = "";
	private String nextScheduleDate = "";
	private long visionId = 0; 
	private String refrenceId = "";
	private String runType = "";
	private String reportType = "";
	private String catalogQuery = "";
	private int errorCount = 0;
	private String burstFlag = "";
	private String fileSize = "";
	private String receiverUserId = "";
	private String scheduleSequenceNo = "";
	private int burstSequenceNo = 0;
	private int burstFlagCount = 0;
	private String blankReportFlag = "N";
	private String reportSubject = "";
	private String reportBody = "";
	private String oldFlag ="N";
	private String newFlag ="N";
	
	private String promptLabel1 = "";
	private String promptLabel2 = "";
	private String promptLabel3 = "";
	private String promptLabel4 = "";
	private String promptLabel5 = "";
	private String promptLabel6 = "";
	private String burstId = "";
	private String vcPromptValue = "";
	private String promptHashVar ="";
	private String ftpVarName ="";
	private int sourceScriptTypeAt = 1083;
	private String ScriptType ="";
	private String scheduleOption = "E";
	
	
	public String getReportId() {
		return reportId;
	}
	public void setReportId(String reportId) {
		this.reportId = reportId;
	}

	
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
	public String getFormatType() {
		return formatType;
	}
	public void setFormatType(String formatType) {
		this.formatType = formatType;
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
	public String getScheduleStatus() {
		return scheduleStatus;
	}
	public void setScheduleStatus(String scheduleStatus) {
		this.scheduleStatus = scheduleStatus;
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
	public String getEmailStatus() {
		return emailStatus;
	}
	public void setEmailStatus(String emailStatus) {
		this.emailStatus = emailStatus;
	}
	public String getScallingFactor() {
		return scallingFactor;
	}
	public void setScallingFactor(String scallingFactor) {
		this.scallingFactor = scallingFactor;
	}
	public int getRsScheduleStatusNt() {
		return rsScheduleStatusNt;
	}
	public void setRsScheduleStatusNt(int rsScheduleStatusNt) {
		this.rsScheduleStatusNt = rsScheduleStatusNt;
	}
	public int getRsScheduleStatus() {
		return rsScheduleStatus;
	}
	public void setRsScheduleStatus(int rsScheduleStatus) {
		this.rsScheduleStatus = rsScheduleStatus;
	}
	public String getEmailFrom() {
		return emailFrom;
	}
	public void setEmailFrom(String emailFrom) {
		this.emailFrom = emailFrom;
	}
	public String getPromptValue1Desc() {
		return promptValue1Desc;
	}
	public String getPromptValue2Desc() {
		return promptValue2Desc;
	}
	public String getPromptValue3Desc() {
		return promptValue3Desc;
	}
	public String getPromptValue4Desc() {
		return promptValue4Desc;
	}
	public String getPromptValue5Desc() {
		return promptValue5Desc;
	}
	public void setPromptValue1Desc(String promptValue1Desc) {
		this.promptValue1Desc = promptValue1Desc;
	}
	public void setPromptValue2Desc(String promptValue2Desc) {
		this.promptValue2Desc = promptValue2Desc;
	}
	public void setPromptValue3Desc(String promptValue3Desc) {
		this.promptValue3Desc = promptValue3Desc;
	}
	public void setPromptValue4Desc(String promptValue4Desc) {
		this.promptValue4Desc = promptValue4Desc;
	}
	public void setPromptValue5Desc(String promptValue5Desc) {
		this.promptValue5Desc = promptValue5Desc;
	}
	public long getUserId() {
		return userId;
	}
	public void setUserId(long userId) {
		this.userId = userId;
	}
	public String getNextScheduleDate() {
		return nextScheduleDate;
	}
	public void setNextScheduleDate(String nextScheduleDate) {
		this.nextScheduleDate = nextScheduleDate;
	}
	public long getVisionId() {
		return visionId;
	}
	public void setVisionId(long visionId) {
		this.visionId = visionId;
	}
	public String getPromptValue6() {
		return promptValue6;
	}
	public void setPromptValue6(String promptValue6) {
		this.promptValue6 = promptValue6;
	}
	public String getPromptValue6Desc() {
		return promptValue6Desc;
	}
	public void setPromptValue6Desc(String promptValue6Desc) {
		this.promptValue6Desc = promptValue6Desc;
	}
	public String getRefrenceId() {
		return refrenceId;
	}
	public void setRefrenceId(String refrenceId) {
		this.refrenceId = refrenceId;
	}
	public String getRunType() {
		return runType;
	}
	public void setRunType(String runType) {
		this.runType = runType;
	}
	public String getReportType() {
		return reportType;
	}
	public void setReportType(String reportType) {
		this.reportType = reportType;
	}
	public String getCatalogQuery() {
		return catalogQuery;
	}
	public void setCatalogQuery(String catalogQuery) {
		this.catalogQuery = catalogQuery;
	}
	public int getErrorCount() {
		return errorCount;
	}
	public void setErrorCount(int errorCount) {
		this.errorCount = errorCount;
	}
	public String getBurstFlag() {
		return burstFlag;
	}
	public void setBurstFlag(String burstFlag) {
		this.burstFlag = burstFlag;
	}
	public String getFileSize() {
		return fileSize;
	}
	public void setFileSize(String fileSize) {
		this.fileSize = fileSize;
	}
	public String getReceiverUserId() {
		return receiverUserId;
	}
	public void setReceiverUserId(String receiverUserId) {
		this.receiverUserId = receiverUserId;
	}
	public String getBlankReportFlag() {
		return blankReportFlag;
	}
	public void setBlankReportFlag(String blankReportFlag) {
		this.blankReportFlag = blankReportFlag;
	}
	public String getReportSubject() {
		return reportSubject;
	}
	public void setReportSubject(String reportSubject) {
		this.reportSubject = reportSubject;
	}
	public String getReportBody() {
		return reportBody;
	}
	public void setReportBody(String reportBody) {
		this.reportBody = reportBody;
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
	public String getScheduleSequenceNo() {
		return scheduleSequenceNo;
	}
	public void setScheduleSequenceNo(String scheduleSequenceNo) {
		this.scheduleSequenceNo = scheduleSequenceNo;
	}
	public int getBurstSequenceNo() {
		return burstSequenceNo;
	}
	public void setBurstSequenceNo(int burstSequenceNo) {
		this.burstSequenceNo = burstSequenceNo;
	}
	public String getOldFlag() {
		return oldFlag;
	}
	public void setOldFlag(String oldFlag) {
		this.oldFlag = oldFlag;
	}
	public String getNewFlag() {
		return newFlag;
	}
	public void setNewFlag(String newFlag) {
		this.newFlag = newFlag;
	}
	public int getBurstFlagCount() {
		return burstFlagCount;
	}
	public void setBurstFlagCount(int burstFlagCount) {
		this.burstFlagCount = burstFlagCount;
	}
	public String getBurstId() {
		return burstId;
	}
	public void setBurstId(String burstId) {
		this.burstId = burstId;
	}
	public String getVcPromptValue() {
		return vcPromptValue;
	}
	public void setVcPromptValue(String vcPromptValue) {
		this.vcPromptValue = vcPromptValue;
	}
	public String getPromptHashVar() {
		return promptHashVar;
	}
	public void setPromptHashVar(String promptHashVar) {
		this.promptHashVar = promptHashVar;
	}
	public String getFtpVarName() {
		return ftpVarName;
	}
	public void setFtpVarName(String ftpVarName) {
		this.ftpVarName = ftpVarName;
	}
	public int getSourceScriptTypeAt() {
		return sourceScriptTypeAt;
	}
	public void setSourceScriptTypeAt(int sourceScriptTypeAt) {
		this.sourceScriptTypeAt = sourceScriptTypeAt;
	}
	public String getScriptType() {
		return ScriptType;
	}
	public void setScriptType(String scriptType) {
		ScriptType = scriptType;
	}
	public String getScheduleOption() {
		return scheduleOption;
	}
	public void setScheduleOption(String scheduleOption) {
		this.scheduleOption = scheduleOption;
	}
}