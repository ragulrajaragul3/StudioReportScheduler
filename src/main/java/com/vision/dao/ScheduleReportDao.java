package com.vision.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.util.CommonUtils;
import com.vision.util.ValidationUtil;
import com.vision.vb.RSSProcessControlVb;
import com.vision.vb.ReportsVb;
import com.vision.wb.DataVariables;
@Component
public class ScheduleReportDao extends AbstractDao<RSSProcessControlVb>{
	
	private static final Logger logger = LoggerFactory.getLogger(ScheduleReportDao.class);
	@Autowired
	CommonDao commonDao;
	
	public JdbcTemplate jdbcTemplate = null;

	public ScheduleReportDao(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}

	private static String uploadLogFileName = System.getenv("RS_uploadLogFileName");
	DataVariables dataVariables;
	int retVal = 0;
	int ERROR_OPERATION = 1;
	int SUCCESS_OPERATION = 0;
	int DB_CONNECTION_ERROR = 2;
	String strErrorDesc = "";
	public String columnNameErr="";
	public static String schema=System.getenv("RS_schema");
	
	public RSSProcessControlVb getScheduleProcessControl(String processId) {
		RSSProcessControlVb rssProcessControlVb = new RSSProcessControlVb();
		List<RSSProcessControlVb> ScheduledreportLst = new ArrayList<RSSProcessControlVb>();
		String sql = " SELECT                                "+
				"        Rsp.RSS_PROCESS_ID,            "+
				"        RSP.Process_version_No,        "+
				"        RSP.Report_ID,                 "+
				"        rsp.Vision_Id,                 "+
				"        rsp.rs_SChedule_Seq,           "+
				"        rsp.Attach_XL_Flag,            "+
				"        rsp.Attach_Html_Flag,          "+
				"        rsp.Attach_PDF_Flag,           "+
				"        rsp.Email_File_Flag,           "+
				"        rsp.Ftp_File_Flag,             "+
				"        rsp.ftp_Connectivity_Details,  "+
				"        rsp.XL_Export_Type,            "+
				"        rsp.SCH_BLANK_REPORT_FLAG,     "+
				"        rsp.Email_Subject,             "+
				"        rsp.Email_Header,              "+
				"        rsp.Email_Footer,              "+
				"        rsp.Burst_ID,                  "+
				"        rsp.Burst_Flag,                "+
				"        rsp.Burst_ID_Seq,              "+
				"        rsp.Scalling_Factor,           "+
				"        rsp.rs_Vision_ID,              "+
				"        rsp.Readiness_scripts_Type,    "+
				"        Rsp.Readiness_scripts,         "+
				"        Rsp.Email_to,                  "+
				"        rsp.Email_CC,                  "+
				"        rsp.Email_BCC,                 "+
				"        Rsp.Email_FCC,                 "+
				"        rsp.Prompt_Value_1 Prompt_Value_1,            "+//PRD_GET_RSS_MANUAL_PROMPTVALUE
				"        Rsp.Prompt_Value_2 Prompt_Value_2,            "+
				"        Rsp.Prompt_value_3 Prompt_value_3,            "+
				"        Rsp.Prompt_value_4 Prompt_value_4,            "+
				"        Rsp.Prompt_value_5 Prompt_value_5,            "+
				"        Rsp.Prompt_Value_6 Prompt_Value_6,            "+
				"        Rsp.Prompt_Value_7 Prompt_Value_7,            "+
				"        Rsp.Prompt_Value_8 Prompt_Value_8,            "+
				"        Rsp.Prompt_Value_9 Prompt_Value_9,            "+
				"        Rsp.Prompt_Value_10 Prompt_Value_10,          "+
				"        Rsp.Prompt_Value_1_Desc Prompt_Value_1_Desc,       "+
				"        Rsp.Prompt_Value_2_Desc Prompt_Value_2_Desc,       "+
				"        Rsp.Prompt_Value_3_Desc Prompt_Value_3_Desc,       "+
				"        Rsp.Prompt_Value_4_Desc Prompt_Value_4_Desc,       "+
				"        Rsp.Prompt_Value_5_Desc Prompt_Value_5_Desc,       "+
				"        Rsp.Prompt_Value_6_Desc Prompt_Value_6_Desc,        "+
				"        Rsp.Prompt_Value_7_Desc Prompt_Value_7_Desc,        "+
				"        Rsp.Prompt_Value_8_Desc Prompt_Value_8_Desc,        "+
				"        Rsp.Prompt_Value_9_Desc Prompt_Value_9_Desc ,       "+
				"        Rsp.Prompt_Value_10_Desc Prompt_Value_10_Desc,       "+      
				"        Rsp.PROMPT_LABEL_1,        "+
				"        Rsp.PROMPT_LABEL_2,        "+
				"        Rsp.PROMPT_LABEL_3,        "+
				"        Rsp.PROMPT_LABEL_4,        "+
				"        Rsp.PROMPT_LABEL_5,        "+
				"        Rsp.PROMPT_LABEL_6,        "+
				"        Rsp.PROMPT_LABEL_7,        "+
				"        Rsp.PROMPT_LABEL_8,        "+
				"        Rsp.PROMPT_LABEL_9,        "+
				"        Rsp.PROMPT_LABEL_10,        "+
				"        Rsp.ATTACH_FILE_NAME,        "+
				"        Rsp.PROMPT_HASH_SCRIPT,Rsp.REPORT_TITLE,Rsp.DB_CONNECTIVITY_DETAILS, "+
				"        Rsp.SERVER_CONNECTIVITY_DETAILS,Rsp.READINESS_SCRIPTS_TYPE,Rsp.READINESS_SCRIPTS, "+
				"		 Rsp.CATALOG_ID,Rsp.REPORT_TYPE,  "+
				"        rsp.Attach_CSV_Flag           "+
				"  FROM PRD_RSS_PROCESS_CONTROL RSP         "+
				" WHERE rsp.RSS_PROCESS_ID = '"+processId+"'	"; 
				
		Object args[] =null;
		try{
			RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				RSSProcessControlVb rssProcessControlVb = new RSSProcessControlVb();
				rssProcessControlVb.setProcessId(rs.getString("RSS_PROCESS_ID"));
				rssProcessControlVb.setVersionNo(rs.getString("Process_version_No"));
				rssProcessControlVb.setReportId(rs.getString("Report_ID"));
				rssProcessControlVb.setUserId(rs.getLong("Vision_Id"));
				rssProcessControlVb.setScheduleSequenceNo(rs.getString("rs_SChedule_Seq"));
				if ("Y".equalsIgnoreCase(rs.getString("Attach_XL_Flag")))
					rssProcessControlVb.setAttachXl(true);
				if ("Y".equalsIgnoreCase(rs.getString("Attach_Html_Flag")))
					rssProcessControlVb.setAttachHtml(true);
				if ("Y".equalsIgnoreCase(rs.getString("Attach_PDF_Flag")))
					rssProcessControlVb.setAttachPdf(true);
				if ("Y".equalsIgnoreCase(rs.getString("Email_File_Flag")))
					rssProcessControlVb.setEmailFlag(true);
				if ("Y".equalsIgnoreCase(rs.getString("Ftp_File_Flag")))
					rssProcessControlVb.setFtpFlag(true);
				rssProcessControlVb.setFtpVarName(rs.getString("ftp_Connectivity_Details"));
				rssProcessControlVb.setExcelExportType(rs.getString("XL_Export_Type"));
				rssProcessControlVb.setBurstId(rs.getString("Burst_ID"));
				rssProcessControlVb.setBurstFlag(rs.getString("Burst_Flag"));
				rssProcessControlVb.setBurstSequenceNo(rs.getInt("Burst_ID_Seq"));
				rssProcessControlVb.setBlankReportFlag(rs.getString("SCH_BLANK_REPORT_FLAG"));
				rssProcessControlVb.setEmailSubject(rs.getString("Email_Subject"));
				rssProcessControlVb.setEmailHeader(rs.getString("Email_Header"));
				rssProcessControlVb.setEmailFooter(rs.getString("Email_Footer"));
				rssProcessControlVb.setEmailTo(rs.getString("Email_To"));
				rssProcessControlVb.setEmailCc(rs.getString("Email_CC"));
				rssProcessControlVb.setEmailBcc(rs.getString("Email_BCC"));
				rssProcessControlVb.setEmailFcc(rs.getString("Email_FCC"));
				rssProcessControlVb.setPromptValue1Desc(rs.getString("PROMPT_VALUE_1_DESC"));
				rssProcessControlVb.setPromptValue2Desc(rs.getString("PROMPT_VALUE_2_DESC"));
				rssProcessControlVb.setPromptValue3Desc(rs.getString("PROMPT_VALUE_3_DESC"));
				rssProcessControlVb.setPromptValue4Desc(rs.getString("PROMPT_VALUE_4_DESC"));
				rssProcessControlVb.setPromptValue5Desc(rs.getString("PROMPT_VALUE_5_DESC"));
				rssProcessControlVb.setPromptValue6Desc(rs.getString("PROMPT_VALUE_6_DESC"));
				rssProcessControlVb.setPromptValue7Desc(rs.getString("PROMPT_VALUE_7_DESC"));
				rssProcessControlVb.setPromptValue8Desc(rs.getString("PROMPT_VALUE_8_DESC"));
				rssProcessControlVb.setPromptValue9Desc(rs.getString("PROMPT_VALUE_9_DESC"));
				rssProcessControlVb.setPromptValue10Desc(rs.getString("PROMPT_VALUE_10_DESC"));
				rssProcessControlVb.setAttachFileName(rs.getString("ATTACH_FILE_NAME"));

				if (ValidationUtil.isValid(rs.getString("SCALLING_FACTOR") != null))
					rssProcessControlVb.setScallingFactor(rs.getString("SCALLING_FACTOR"));
				else
					rssProcessControlVb.setScallingFactor("1");
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_1"))) {
					rssProcessControlVb.setPromptValue1("'"+rs.getString("Prompt_Value_1")+"'");
					if(rs.getString("Prompt_Value_1").contains(","))
						rssProcessControlVb.setPromptValue1(rssProcessControlVb.getPromptValue1().replaceAll(",", "','"));
					// rssProcessControlVb.setPromptValue1Desc("'"+rs.getString("Prompt_Value_1"));
				}
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_2"))) {
					rssProcessControlVb.setPromptValue2("'"+rs.getString("Prompt_Value_2")+"'");
					if(rs.getString("Prompt_Value_2").contains(","))
						rssProcessControlVb.setPromptValue2(rssProcessControlVb.getPromptValue2().replaceAll(",", "','"));
					// rssProcessControlVb.setPromptValue2Desc("'"+rs.getString("Prompt_Value_2")+"'");
				}
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_3"))) {
					rssProcessControlVb.setPromptValue3("'"+rs.getString("Prompt_Value_3")+"'");
					if(rs.getString("Prompt_Value_3").contains(","))
						rssProcessControlVb.setPromptValue3(rssProcessControlVb.getPromptValue3().replaceAll(",", "','"));
					// rssProcessControlVb.setPromptValue3Desc("'"+rs.getString("Prompt_Value_3")+"'");
				}
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_4"))) {
					rssProcessControlVb.setPromptValue4("'"+rs.getString("Prompt_Value_4")+"'");
					if(rs.getString("Prompt_Value_4").contains(","))
						rssProcessControlVb.setPromptValue4(rssProcessControlVb.getPromptValue4().replaceAll(",", "','"));
					// rssProcessControlVb.setPromptValue4Desc("'"+rs.getString("Prompt_Value_4")+"'");
				}
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_5"))) {
					rssProcessControlVb.setPromptValue5("'"+rs.getString("Prompt_Value_5")+"'");
					if(rs.getString("Prompt_Value_5").contains(","))
						rssProcessControlVb.setPromptValue5(rssProcessControlVb.getPromptValue5().replaceAll(",", "','"));
					// rssProcessControlVb.setPromptValue5Desc("'"+rs.getString("Prompt_Value_5")+"'");
				}
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_6"))) {
					rssProcessControlVb.setPromptValue6("'"+rs.getString("Prompt_Value_6")+"'");
					if(rs.getString("Prompt_Value_6").contains(","))
						rssProcessControlVb.setPromptValue6(rssProcessControlVb.getPromptValue6().replaceAll(",", "','"));
					// rssProcessControlVb.setPromptValue6Desc("'"+rs.getString("Prompt_Value_6")+"'");
				}
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_7"))) {
					rssProcessControlVb.setPromptValue7("'"+rs.getString("Prompt_Value_7")+"'");
					if(rs.getString("Prompt_Value_7").contains(","))
						rssProcessControlVb.setPromptValue7(rssProcessControlVb.getPromptValue7().replaceAll(",", "','"));
				}
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_8"))) {
					rssProcessControlVb.setPromptValue8("'"+rs.getString("Prompt_Value_8")+"'");
					if(rs.getString("Prompt_Value_8").contains(","))
						rssProcessControlVb.setPromptValue8(rssProcessControlVb.getPromptValue8().replaceAll(",", "','"));
				}
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_9"))) {
					rssProcessControlVb.setPromptValue9("'"+rs.getString("Prompt_Value_9")+"'");
					if(rs.getString("Prompt_Value_9").contains(","))
						rssProcessControlVb.setPromptValue9(rssProcessControlVb.getPromptValue9().replaceAll(",", "','"));
				}
				if (ValidationUtil.isValid(rs.getString("Prompt_Value_10"))) {
					rssProcessControlVb.setPromptValue10("'"+rs.getString("Prompt_Value_10")+"'");
					if(rs.getString("Prompt_Value_10").contains(","))
						rssProcessControlVb.setPromptValue10(rssProcessControlVb.getPromptValue10().replaceAll(",", "','"));
				}
				rssProcessControlVb.setPromptLabel1(rs.getString("PROMPT_LABEL_1"));
				rssProcessControlVb.setPromptLabel2(rs.getString("PROMPT_LABEL_2"));
				rssProcessControlVb.setPromptLabel3(rs.getString("PROMPT_LABEL_3"));
				rssProcessControlVb.setPromptLabel4(rs.getString("PROMPT_LABEL_4"));
				rssProcessControlVb.setPromptLabel5(rs.getString("PROMPT_LABEL_5"));
				rssProcessControlVb.setPromptLabel6(rs.getString("PROMPT_LABEL_6"));
				rssProcessControlVb.setPromptLabel7(rs.getString("PROMPT_LABEL_7"));
				rssProcessControlVb.setPromptLabel8(rs.getString("PROMPT_LABEL_8"));
				rssProcessControlVb.setPromptLabel9(rs.getString("PROMPT_LABEL_9"));
				rssProcessControlVb.setPromptLabel10(rs.getString("PROMPT_LABEL_10"));
				rssProcessControlVb.setMaker(rs.getLong("rs_Vision_ID"));
				rssProcessControlVb.setReadinessScriptType(rs.getString("Readiness_scripts_Type"));
				rssProcessControlVb.setReadinessScript(rs.getString("Readiness_scripts"));
				rssProcessControlVb.setPromptHashVar(rs.getString("PROMPT_HASH_SCRIPT"));
				rssProcessControlVb.setReportTitle(rs.getString("REPORT_TITLE"));
				rssProcessControlVb.setDatabaseConnectivityDetails(rs.getString("DB_CONNECTIVITY_DETAILS"));
				rssProcessControlVb.setConnectivityDetails(rs.getString("SERVER_CONNECTIVITY_DETAILS"));
				rssProcessControlVb.setReadinessScriptType(rs.getString("READINESS_SCRIPTS_TYPE"));
				rssProcessControlVb.setReadinessScript(rs.getString("READINESS_SCRIPTS"));
				rssProcessControlVb.setCatalogId(rs.getString("CATALOG_ID"));
				rssProcessControlVb.setReportType(rs.getString("REPORT_TYPE"));
				if ("Y".equalsIgnoreCase(rs.getString("Attach_CSV_Flag")))
					rssProcessControlVb.setAttachCsv(true);
				return rssProcessControlVb;
			}
		};
			ScheduledreportLst = jdbcTemplate.query(sql,mapper);
			if(ScheduledreportLst != null && !ScheduledreportLst.isEmpty())
				rssProcessControlVb = ScheduledreportLst.get(0);
			return rssProcessControlVb;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return rssProcessControlVb;
	}

	public RowMapper getRssProcessControlMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				RSSProcessControlVb rssProcessControlVb = new RSSProcessControlVb();
				rssProcessControlVb.setProcessId(rs.getString("RSS_PROCESS_ID"));
				rssProcessControlVb.setVersionNo(rs.getString("Process_version_No"));
				rssProcessControlVb.setReportId(rs.getString("Report_ID"));
				rssProcessControlVb.setUserId(rs.getLong("Vision_Id"));
				rssProcessControlVb.setScheduleSequenceNo(rs.getString("rs_SChedule_Seq"));
				if ("Y".equalsIgnoreCase(rs.getString("Attach_XL_Flag")))
					rssProcessControlVb.setAttachXl(true);
				if ("Y".equalsIgnoreCase(rs.getString("Attach_Html_Flag")))
					rssProcessControlVb.setAttachHtml(true);
				if ("Y".equalsIgnoreCase(rs.getString("Attach_PDF_Flag")))
					rssProcessControlVb.setAttachPdf(true);
				if ("Y".equalsIgnoreCase(rs.getString("Email_File_Flag")))
					rssProcessControlVb.setEmailFlag(true);
				if ("Y".equalsIgnoreCase(rs.getString("Ftp_File_Flag")))
					rssProcessControlVb.setFtpFlag(true);
				rssProcessControlVb.setFtpVarName(rs.getString("ftp_Connectivity_Details"));
				rssProcessControlVb.setExcelExportType(rs.getString("XL_Export_Type"));
				rssProcessControlVb.setBurstId(rs.getString("Burst_ID"));
				rssProcessControlVb.setBurstFlag(rs.getString("Burst_Flag"));
				rssProcessControlVb.setBurstSequenceNo(rs.getInt("Burst_ID_Seq"));
				rssProcessControlVb.setBlankReportFlag(rs.getString("SCH_BLANK_REPORT_FLAG"));
				rssProcessControlVb.setEmailSubject(rs.getString("Email_Subject"));
				rssProcessControlVb.setEmailHeader(rs.getString("Email_Header"));
				rssProcessControlVb.setEmailFooter(rs.getString("Email_Footer"));
				rssProcessControlVb.setEmailTo(rs.getString("Email_To"));
				rssProcessControlVb.setEmailCc(rs.getString("Email_CC"));
				rssProcessControlVb.setEmailBcc(rs.getString("Email_BCC"));
				rssProcessControlVb.setEmailFcc(rs.getString("Email_FCC"));
				rssProcessControlVb.setPromptValue1Desc(rs.getString("PROMPT_VALUE_1_DESC"));
				rssProcessControlVb.setPromptValue2Desc(rs.getString("PROMPT_VALUE_2_DESC"));
				rssProcessControlVb.setPromptValue3Desc(rs.getString("PROMPT_VALUE_3_DESC"));
				rssProcessControlVb.setPromptValue4Desc(rs.getString("PROMPT_VALUE_4_DESC"));
				rssProcessControlVb.setPromptValue5Desc(rs.getString("PROMPT_VALUE_5_DESC"));
				rssProcessControlVb.setPromptValue6Desc(rs.getString("PROMPT_VALUE_6_DESC"));
				rssProcessControlVb.setPromptValue7Desc(rs.getString("PROMPT_VALUE_7_DESC"));
				rssProcessControlVb.setPromptValue8Desc(rs.getString("PROMPT_VALUE_8_DESC"));
				rssProcessControlVb.setPromptValue9Desc(rs.getString("PROMPT_VALUE_9_DESC"));
				rssProcessControlVb.setPromptValue10Desc(rs.getString("PROMPT_VALUE_10_DESC"));
				rssProcessControlVb.setAttachFileName(rs.getString("ATTACH_FILE_NAME"));

				if (rs.getString("SCALLING_FACTOR") != null)
					rssProcessControlVb.setScallingFactor(rs.getString("SCALLING_FACTOR"));
				else
					rssProcessControlVb.setScallingFactor("1");
				if (rs.getString("Prompt_Value_1") != null) {
					rssProcessControlVb.setPromptValue1(rs.getString("Prompt_Value_1"));
					// rssProcessControlVb.setPromptValue1Desc(rs.getString("Prompt_Value_1"));
				}
				if (rs.getString("Prompt_Value_2") != null) {
					rssProcessControlVb.setPromptValue2(rs.getString("Prompt_Value_2"));
					// rssProcessControlVb.setPromptValue2Desc(rs.getString("Prompt_Value_2"));
				}
				if (rs.getString("Prompt_Value_3") != null) {
					rssProcessControlVb.setPromptValue3(rs.getString("Prompt_Value_3"));
					// rssProcessControlVb.setPromptValue3Desc(rs.getString("Prompt_Value_3"));
				}
				if (rs.getString("Prompt_Value_4") != null) {
					rssProcessControlVb.setPromptValue4(rs.getString("Prompt_Value_4"));
					// rssProcessControlVb.setPromptValue4Desc(rs.getString("Prompt_Value_4"));
				}
				if (rs.getString("Prompt_Value_5") != null) {
					rssProcessControlVb.setPromptValue5(rs.getString("Prompt_Value_5"));
					// rssProcessControlVb.setPromptValue5Desc(rs.getString("Prompt_Value_5"));
				}
				if (rs.getString("Prompt_Value_6") != null) {
					rssProcessControlVb.setPromptValue6(rs.getString("Prompt_Value_6"));
					// rssProcessControlVb.setPromptValue6Desc(rs.getString("Prompt_Value_6"));
				}
				if (rs.getString("Prompt_Value_7") != null) {
					rssProcessControlVb.setPromptValue7(rs.getString("Prompt_Value_7"));
				}
				if (rs.getString("Prompt_Value_8") != null) {
					rssProcessControlVb.setPromptValue8(rs.getString("Prompt_Value_8"));
				}
				if (rs.getString("Prompt_Value_9") != null) {
					rssProcessControlVb.setPromptValue9(rs.getString("Prompt_Value_9"));
				}
				if (rs.getString("Prompt_Value_10") != null) {
					rssProcessControlVb.setPromptValue10(rs.getString("Prompt_Value_10"));
				}
				rssProcessControlVb.setPromptLabel1(rs.getString("PROMPT_LABEL_1"));
				rssProcessControlVb.setPromptLabel2(rs.getString("PROMPT_LABEL_2"));
				rssProcessControlVb.setPromptLabel3(rs.getString("PROMPT_LABEL_3"));
				rssProcessControlVb.setPromptLabel4(rs.getString("PROMPT_LABEL_4"));
				rssProcessControlVb.setPromptLabel5(rs.getString("PROMPT_LABEL_5"));
				rssProcessControlVb.setPromptLabel6(rs.getString("PROMPT_LABEL_6"));
				rssProcessControlVb.setPromptLabel7(rs.getString("PROMPT_LABEL_7"));
				rssProcessControlVb.setPromptLabel8(rs.getString("PROMPT_LABEL_8"));
				rssProcessControlVb.setPromptLabel9(rs.getString("PROMPT_LABEL_9"));
				rssProcessControlVb.setPromptLabel10(rs.getString("PROMPT_LABEL_10"));
				rssProcessControlVb.setMaker(rs.getLong("rs_Vision_ID"));
				rssProcessControlVb.setReadinessScriptType(rs.getString("Readiness_scripts_Type"));
				rssProcessControlVb.setReadinessScript(rs.getString("Readiness_scripts"));
				rssProcessControlVb.setPromptHashVar(rs.getString("PROMPT_HASH_SCRIPT"));
				rssProcessControlVb.setReportTitle(rs.getString("REPORT_TITLE"));
				rssProcessControlVb.setDatabaseConnectivityDetails(rs.getString("DB_CONNECTIVITY_DETAILS"));
				rssProcessControlVb.setConnectivityDetails(rs.getString("SERVER_CONNECTIVITY_DETAILS"));
				rssProcessControlVb.setReadinessScriptType(rs.getString("READINESS_SCRIPTS_TYPE"));
				rssProcessControlVb.setReadinessScript(rs.getString("READINESS_SCRIPTS"));
				rssProcessControlVb.setCatalogId(rs.getString("CATALOG_ID"));
				rssProcessControlVb.setReportType(rs.getString("REPORT_TYPE"));
				return rssProcessControlVb;
			}
		};
		return mapper;
	}
	public int insertScheduleAuditTrail(String processId,String errorDescription,String detailLog) throws Exception {
		int retVal = 0;
		try{
			int maxSeq = new CommonDao(jdbcTemplate).getAuditMaxSeq(processId)+1;
			String query = "Insert Into PRD_RSS_AUDIT_TRAIL (RSS_PROCESS_ID, AUDIT_TRAIL_SEQUENCE_ID, DATETIME_STAMP, AUDIT_DESCRIPTION,"
					+ " AUDIT_DESCRIPTION_DETAIL, DATE_LAST_MODIFIED, DATE_CREATION)"+
					" Values (?, ?, "+ new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", ?, ?, "+ new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+","+ new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+") "; 
			Object args[] = {processId,maxSeq,errorDescription,detailLog};
			retVal= jdbcTemplate.update(query,args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}
	public void setPdfWidhtAndHeight(ReportsVb reportsVb) throws Exception{
		String query = new String("SELECT PDF_WIDTH, PDF_HEIGHT FROM REPORT_SUITE WHERE REPORT_ID = ? ");
		try{
			Connection connectionMain = null;
			PreparedStatement psMain = connectionMain.prepareStatement(query);
			ResultSet rsMain = psMain.executeQuery();
			if(rsMain.next()){
				reportsVb.setPdfWidth(rsMain.getInt("PDF_WIDTH"));
				reportsVb.setPdfHeight(rsMain.getInt("PDF_HEIGHT"));
			}
		}catch(Exception ex){
			ex.printStackTrace();
			try {
				CommonUtils.writeLogToFile(uploadLogFileName,((query==null)? "query is Null":query),dataVariables.uploadLogFilePath);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public String replaceSubjectValues(String subject, RSSProcessControlVb vObject){
		if(subject.contains("#PROMPT_VALUE_1#")){
			subject = subject.replaceAll("#PROMPT_VALUE_1#",vObject.getPromptValue1Desc());
		}
		if(subject.indexOf("#PROMPT_VALUE_2#") > 0){
			subject = subject.replaceAll("#PROMPT_VALUE_2#",vObject.getPromptValue2Desc());
		}
		if(subject.indexOf("#PROMPT_VALUE_3#") > 0){
			subject = subject.replaceAll("#PROMPT_VALUE_3#",vObject.getPromptValue3Desc());
		}
		if(subject.indexOf("#PROMPT_VALUE_4#") > 0){
			subject = subject.replaceAll("#PROMPT_VALUE_4#",vObject.getPromptValue4Desc());
		}
		if(subject.indexOf("#PROMPT_VALUE_5#") > 0){
			subject = subject.replaceAll("#PROMPT_VALUE_5#",vObject.getPromptValue5Desc());
		}
		if(subject.indexOf("#PROMPT_VALUE_6#") > 0){
			subject = subject.replaceAll("#PROMPT_VALUE_6#",vObject.getPromptValue6Desc());
		}
		if(subject.indexOf("#SCHEDULE_START_DATE#") > 0){
			subject = subject.replaceAll("#SCHEDULE_START_DATE#",vObject.getScheduleStartDate());
		}
		if(subject.indexOf("#SCHEDULE_END_DATE#") > 0){
			subject = subject.replaceAll("#SCHEDULE_END_DATE#",vObject.getScheduleEndDate());
		}
		if(subject.indexOf("#SCHEDULE_TYPE#") > 0){
			String scheduleType = "Daily";
			if("H".equalsIgnoreCase(vObject.getScheduleType()))
				scheduleType = "Hourly";
			else if("D".equalsIgnoreCase(vObject.getScheduleType())) 
				scheduleType = "Daily";
			else if("W".equalsIgnoreCase(vObject.getScheduleType()))
				scheduleType = "Weekly";
			else if("M".equalsIgnoreCase(vObject.getScheduleType()))
				scheduleType = "Monthly";
			else if("A".equalsIgnoreCase(vObject.getScheduleType()))
				scheduleType = "One time";			
			
			subject = subject.replaceAll("#SCHEDULE_TYPE#",scheduleType);
		}
		return subject;
	}
	
	public String getMakerName(Long visionId) {
		try {
			String sql = "select USER_NAME FROM VISION_USERS where VISION_ID = '"+visionId+"'";
			return jdbcTemplate.queryForObject(sql, String.class);
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}
	public String getCurrentLevel(String reportId) {
		String currentLevel = "";
		try {
			String sql = "(SELECT "+ new CommonDao(jdbcTemplate).getDbFunction("NVL")+"(MIN(SUBREPORT_SEQ),0)CURRENT_LEVEL FROM PRD_REPORT_DETAILS PR WHERE PR.REPORT_ID= '"+ reportId + "') ";
			return jdbcTemplate.queryForObject(sql, String.class);
		}catch(Exception e) {
			return "0";
		}
	}

	
	}