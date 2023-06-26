package com.vision.wb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.vision.dao.CommonDao;
import com.vision.exception.VisionGeneralException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.RSScheduleTabColVb;

import CatalogScheduler.StudioReportSchedulerApplication;

public class RSSProcessorProcess {
	StudioReportSchedulerApplication reportScheduler = new StudioReportSchedulerApplication();
	
	public JdbcTemplate jdbcTemplate = null;

	public RSSProcessorProcess(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	
	FileWriter logfile = null;
	FileWriter mainLogfile = null;
	BufferedWriter bufferedWriter = null;
	String uploadLogFilePath = "";
	
	
	/*public void StudioReportSchedulerApplication.logWriter(String logString){
		try{
			logfile = new FileWriter(uploadLogFilePath,true);
			bufferedWriter = new BufferedWriter(logfile);
			bufferedWriter.newLine();
			bufferedWriter.write(getCurrentDateTime() + " : " + logString);
			//bufferedWriter.write(""+logString);
			bufferedWriter.close();
			logfile.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}*/
	public int rssProcessMain(String logPath) {
		try {
			StudioReportSchedulerApplication.logWriter("Rss Process Main Started",logPath);
			Date programStartDateTime = new Date();
			int Status = doRSSProcess(logPath);
			if(Status == Constants.SUCCESS_OPERATION)
			{
				StudioReportSchedulerApplication.logWriter("Rss Process Main Completed..."+printProgramDuration(programStartDateTime),logPath);
			} else {
				StudioReportSchedulerApplication.logWriter("Rss Process Main Aborted..."+printProgramDuration(programStartDateTime),logPath);
			}
		}catch(Exception e) {
			StudioReportSchedulerApplication.logWriter("Error in RssProcessor "+e.getMessage(),logPath);
			return Constants.ERROR_OPERATION;
		}
		return Constants.SUCCESS_OPERATION;
	}
	public int doRSSProcess(String logPath) {
		try {
			
			String  P_Vision_Var_Check = new CommonDao(jdbcTemplate).findVisionVariableValue("SPAWN_PR_RSS_PROCESSOR");
			
			if(!"Y".equalsIgnoreCase(P_Vision_Var_Check)) {
				StudioReportSchedulerApplication.logWriter("Vision Variable SPAWN_PR_RSS_PROCESSOR Flag is N !!",logPath);
				return Constants.ERROR_OPERATION;
			}

			int RS_Schedule_Service_Status = getRSScheduleServiceStatus(logPath);
			
			RS_Schedule_Service_Status = 1; // RSS Services are not on cron anymore
			
			if(RS_Schedule_Service_Status <= 0) {
				StudioReportSchedulerApplication.logWriter("Returned without processing because RS Scheduler services are down.",logPath);
				return Constants.ERROR_OPERATION;
			}
			
			doUpdateRSEmailStatus(logPath);
			
			List<RSScheduleTabColVb> RS_SCHEDULE_TABLE_DATA = getRSScheduleTabData(logPath);
			
			if(RS_SCHEDULE_TABLE_DATA == null || RS_SCHEDULE_TABLE_DATA.isEmpty()){
				StudioReportSchedulerApplication.logWriter("[0] records available to process.",logPath);
				return Constants.ERROR_OPERATION;
		    }
			
			String localRecordPrimaryKeyString;
			StringBuilder insert_query = new StringBuilder();
			StringBuilder update_query = new StringBuilder();
			String localNodeName = null;
			int tempCounter = 0;
			LocalDateTime localNextProcessTime = null;
			
			for(int ind_i = 0; ind_i<RS_SCHEDULE_TABLE_DATA.size() ; ind_i++){
				
				RSScheduleTabColVb LR_RS_SCHEDULE_TABLE_DATA = RS_SCHEDULE_TABLE_DATA.get(ind_i);
				
				/*localNodeName = getNodeRequest(); 
				 * This is not working on cron basis anymore. Its part of Vision Studio 
				 */
				localNodeName = "U2";
				
				localRecordPrimaryKeyString = 
						"["
						+LR_RS_SCHEDULE_TABLE_DATA.getREPORT_ID()
						+":"
						+LR_RS_SCHEDULE_TABLE_DATA.getVISION_ID()
						+":"
						+LR_RS_SCHEDULE_TABLE_DATA.getRS_SCHEDULE_SEQ()
						+"]";
				
				insert_query = new StringBuilder();
				
				if("N".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getBURST_FLAG())) {
					
					
					if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
						
						insert_query.append(
								         " Insert Into "+Constants.TABLE_PREFIX+"PRD_rss_process_control "
										+" (NEXT_PROCESS_TIME "
										+" ,PROCESS_VERSION_NO "
										+" ,PROCESS_START_TIME "
										+" ,PROCESS_END_TIME "
										+" ,REPORT_ID "
										+" ,VISION_ID "
										+" ,RS_SCHEDULE_SEQ "
										+" ,SCHEDULE_DATE "
										+" ,PREV_SCHEDULE_DATE "
										+" ,SCH_ITERATION_COUNT "
										+" ,RSS_PROCESS_STATUS "
										+" ,BURST_ID "
										+" ,BURST_FLAG "
										+" ,BURST_ID_SEQ "
										+" ,SCHEDULE_START_DATE "
										+" ,SCHEDULE_END_DATE "
										+" ,RSS_FREQUENCY "
										+" ,ATTACH_XL_FLAG "
										+" ,ATTACH_HTML_FLAG "
										+" ,ATTACH_PDF_FLAG "
										+" ,EMAIL_FILE_FLAG "
										+" ,FTP_FILE_FLAG "
										+" ,FTP_CONNECTIVITY_DETAILS "
										+" ,XL_EXPORT_TYPE "
										+" ,SCH_BLANK_REPORT_FLAG "
										+" ,EMAIL_SUBJECT "
										+" ,EMAIL_HEADER "
										+" ,EMAIL_FOOTER "
										+" ,SCALLING_FACTOR "
										+" ,RS_VISION_ID "
										+" ,PROMPT_VALUE_1 "
										+" ,PROMPT_VALUE_2 "
										+" ,PROMPT_VALUE_3 "
										+" ,PROMPT_VALUE_4 "
										+" ,PROMPT_VALUE_5 "
										+" ,PROMPT_VALUE_6 "
										+" ,PROMPT_VALUE_7 "
										+" ,PROMPT_VALUE_8 "
										+" ,PROMPT_VALUE_9 "
										+" ,PROMPT_VALUE_10 "
										+" ,prompt_value_1_desc "
										+" ,prompt_value_2_desc "
										+" ,prompt_value_3_desc "
										+" ,prompt_value_4_desc "
										+" ,prompt_value_5_desc "
										+" ,prompt_value_6_desc "
										+" ,prompt_value_7_desc "
										+" ,prompt_value_8_desc "
										+" ,prompt_value_9_desc "
										+" ,prompt_value_10_desc "
										+" ,prompt_label_1 "
										+" ,prompt_label_2 "
										+" ,prompt_label_3 "
										+" ,prompt_label_4 "
										+" ,prompt_label_5 "
										+" ,prompt_label_6 "
										+" ,prompt_label_7 "
										+" ,prompt_label_8 "
										+" ,prompt_label_9 "
										+" ,prompt_label_10 "
										+" ,RSS_STATUS "
										+" ,RECORD_INDICATOR "
										+" ,MAKER "
										+" ,VERIFIER "
										+" ,INTERNAL_STATUS "
										+" ,DATE_LAST_MODIFIED "
										+" ,DATE_CREATION "
										+" ,INTERVAL_X_DAYS "
										+" ,INTERVAL_X_HOURS "
										+" ,INTERVAL_X_MINS "
										+" ,DAYS_OF_WEEK "
										+" ,MONTHS_OF_YEAR "
										+" ,DAY_OF_THE_MONTH "
										+" ,DB_CONNECTIVITY_DETAILS "
										+" ,READINESS_SCRIPTS_TYPE "
										+" ,READINESS_SCRIPTS "
										+" ,SCH_ITERATION_MINS "
										+" ,SCH_MAX_ITERATION_COUNT "
										+" ,RSS_NEXT_SCH_TYPE "
										+" ,EMAIL_TO "
										+" ,EMAIL_CC "
										+" ,EMAIL_BCC "
										+" ,EMAIL_FCC "
										+" ,NODE_REQUEST "
										+" ,NODE_REQUEST_TIME "
										+" ,BURST_SCRIPTS_TYPE "
										+" ,BURST_SCRIPTS "
										+" ,ATTACH_FILE_NAME "
										+" ,PRIORITY "
										+" ,SERVER_CONNECTIVITY_DETAILS "
										+" ,TIMEZONE_COUNTRY_CODE "
										+" ,SCHEDULE_GROUP "
										+" ,SCHEDULE_SUBGROUP "
										+" ,PROMPT_HASH_SCRIPT "
										+" ,REPORT_TITLE "
										+" ,INTERVAL_X_STARTTIME "
										+" ,INTERVAL_X_ENDTIME) "
										+" Select "
										+"  T1.NEXT_SCHEDULE_DATE NEXT_PROCESS_TIME "
										+" ,0 PROCESS_VERSION_NO "
										+" ,null PROCESS_START_TIME "
										+" ,null PROCESS_END_TIME "
										+" ,T1.REPORT_ID "
										+" ,T1.VISION_ID "
										+" ,T1.RS_SCHEDULE_SEQ "
										+" ,T1.NEXT_SCHEDULE_DATE SCHEDULE_DATE "
										+" ,NVL(T1.PREV_SCHEDULE_DATE,To_Date('01-JAN-1900','DD-Mon-YYYY')) PREV_SCHEDULE_DATE "
										+" ,0 SCH_ITERATION_COUNT "
										+" ,'P' RSS_PROCESS_STATUS "
										+" ,T1.BURST_ID "
										+" ,T1.BURST_FLAG "
										+" ,0 BURST_ID_SEQ "
										+" ,T1.SCHEDULE_START_DATE "
										+" ,T1.SCHEDULE_END_DATE "
										+" ,T1.RSS_FREQUENCY "
										+" ,T1.ATTACH_XL_FLAG "
										+" ,T1.ATTACH_HTML_FLAG "
										+" ,T1.ATTACH_PDF_FLAG "
										+" ,T1.EMAIL_FILE_FLAG "
										+" ,T1.FTP_FILE_FLAG "
										+" ,T1.FTP_CONNECTIVITY_DETAILS "
										+" ,T1.XL_EXPORT_TYPE "
										+" ,T1.SCH_BLANK_REPORT_FLAG "
										+" ,Replace(Replace(Replace(Replace(T1.EMAIL_SUBJECT "
										+"       ,'#REPORT_TITLE#',RS.REPORT_TITLE) "
										+"       ,'#SCHEDULE_DATE_HH24:MI#',TO_CHAR(T1.NEXT_SCHEDULE_DATE,'HH24:MI')) "
										+"       ,'#SCHEDULE_DATE_HH:MI#',TO_CHAR(T1.NEXT_SCHEDULE_DATE,'HH:MI AM')) "
										+"       ,'#SCHEDULE_DATE_DD-MON-YYYY#',TO_CHAR(T1.NEXT_SCHEDULE_DATE,'DD-MON-YYYY')) EMAIL_SUBJECT "
										+" ,Replace(T1.EMAIL_HEADER ,'#REPORT_TITLE#',RS.REPORT_TITLE) EMAIL_HEADER "
										+" ,T1.EMAIL_FOOTER "
										+" ,T1.SCALLING_FACTOR "
										+" ,T1.VISION_ID RS_VISION_ID "
										+" ,Case When instr(T1.PROMPT_VALUE_1,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_1||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_1 End PROMPT_VALUE_1 "
										+" ,Case When instr(T1.PROMPT_VALUE_2,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_2||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_2 End PROMPT_VALUE_2 "
										+" ,Case When instr(T1.PROMPT_VALUE_3,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_3||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_3 End PROMPT_VALUE_3 "
										+" ,Case When instr(T1.PROMPT_VALUE_4,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_4||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_4 End PROMPT_VALUE_4 "
										+" ,Case When instr(T1.PROMPT_VALUE_5,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_5||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_5 End PROMPT_VALUE_5 "
										+" ,Case When instr(T1.PROMPT_VALUE_6,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_6||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_6 End PROMPT_VALUE_6 "
										+" ,Case When instr(T1.PROMPT_VALUE_7,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_7||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_7 End PROMPT_VALUE_7 "
										+" ,Case When instr(T1.PROMPT_VALUE_8,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_8||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_8 End PROMPT_VALUE_8 "
										+" ,Case When instr(T1.PROMPT_VALUE_9,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_9||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_9 End PROMPT_VALUE_9 "
										+" ,Case When instr(T1.PROMPT_VALUE_10,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_10||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_10 End PROMPT_VALUE_10 "
										+" ,T1.PROMPT_VALUE_1_DESC "
										+" ,T1.PROMPT_VALUE_2_DESC "
										+" ,T1.PROMPT_VALUE_3_DESC "
										+" ,T1.PROMPT_VALUE_4_DESC "
										+" ,T1.PROMPT_VALUE_5_DESC "
										+" ,T1.PROMPT_VALUE_6_DESC "
										+" ,T1.PROMPT_VALUE_7_DESC "
										+" ,T1.PROMPT_VALUE_8_DESC "
										+" ,T1.PROMPT_VALUE_9_DESC "
										+" ,T1.PROMPT_VALUE_10_DESC "
										+" ,T1.PROMPT_LABEL_1 "
										+" ,T1.PROMPT_LABEL_2 "
										+" ,T1.PROMPT_LABEL_3 "
										+" ,T1.PROMPT_LABEL_4 "
										+" ,T1.PROMPT_LABEL_5 "
										+" ,T1.PROMPT_LABEL_6 "
										+" ,T1.PROMPT_LABEL_7 "
										+" ,T1.PROMPT_LABEL_8 "
										+" ,T1.PROMPT_LABEL_9 "
										+" ,T1.PROMPT_LABEL_10 "
										+" ,T1.RSS_STATUS "
										+" ,T1.RECORD_INDICATOR "
										+" ,T1.MAKER "
										+" ,T1.VERIFIER "
										+" ,T1.INTERNAL_STATUS "
										+" ,sysdate DATE_LAST_MODIFIED "
										+" ,sysdate DATE_CREATION "
										+" ,T1.INTERVAL_X_DAYS "
										+" ,T1.INTERVAL_X_HOURS "
										+" ,T1.INTERVAL_X_MINS "
										+" ,T1.DAYS_OF_WEEK "
										+" ,T1.MONTHS_OF_YEAR "
										+" ,T1.DAY_OF_THE_MONTH "
										+" ,T1.DB_CONNECTIVITY_DETAILS "
										+" ,T1.READINESS_SCRIPTS_TYPE "
										+" ,T1.READINESS_SCRIPTS "
										+" ,T1.SCH_ITERATION_MINS "
										+" ,T1.SCH_MAX_ITERATION_COUNT "
										+" ,T1.RSS_NEXT_SCH_TYPE "
										+" ,FN_GET_LISAGG_RS_SCH_EMAILS(T1.REPORT_ID,T1.VISION_ID,T1.RS_SCHEDULE_SEQ,'TO')  EMAIL_TO "
										+" ,FN_GET_LISAGG_RS_SCH_EMAILS(T1.REPORT_ID,T1.VISION_ID,T1.RS_SCHEDULE_SEQ,'CC')  EMAIL_CC "
										+" ,FN_GET_LISAGG_RS_SCH_EMAILS(T1.REPORT_ID,T1.VISION_ID,T1.RS_SCHEDULE_SEQ,'BCC') EMAIL_BCC "
										+" ,FN_GET_LISAGG_RS_SCH_EMAILS(T1.REPORT_ID,T1.VISION_ID,T1.RS_SCHEDULE_SEQ,'FCC') EMAIL_FCC "
										+" ,'"+localNodeName+"' Node_Request "
										+" ,sysdate Node_Request_Time "
										+" ,T1.BURST_SCRIPTS_TYPE "
										+" ,T1.BURST_SCRIPTS "
										+" ,Replace(T1.ATTACH_FILE_NAME,'#REPORT_TITLE#',RS.REPORT_TITLE) ATTACH_FILE_NAME "
										+" ,T1.PRIORITY "
										+" ,T1.SERVER_CONNECTIVITY_DETAILS "
										+" ,T1.TIMEZONE_COUNTRY_CODE "
										+" ,T1.SCHEDULE_GROUP "
										+" ,T1.SCHEDULE_SUBGROUP "
										+" ,T1.PROMPT_HASH_SCRIPT "
										+" ,RS.REPORT_TITLE "
										+" ,T1.INTERVAL_X_STARTTIME "
										+" ,T1.INTERVAL_X_ENDTIME "
										+" From "+Constants.TABLE_PREFIX+"PRD_RS_Schedule T1, "+Constants.TABLE_PREFIX+"Prd_Report_Master RS "
										+" Where T1.Report_Id = '"+LR_RS_SCHEDULE_TABLE_DATA.getREPORT_ID()+"' "
										+"   And T1.Vision_Id = '"+LR_RS_SCHEDULE_TABLE_DATA.getVISION_ID()+"' "
										+"   And T1.RS_Schedule_Seq = '"+LR_RS_SCHEDULE_TABLE_DATA.getRS_SCHEDULE_SEQ()+"' "
										+"   And T1.Report_Id = RS.Report_Id "
								);
						
					}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
						
						insert_query.append(
								         " Insert Into "+Constants.TABLE_PREFIX+"PRD_rss_process_control "
										+" (NEXT_PROCESS_TIME "
										+" ,PROCESS_VERSION_NO "
										+" ,PROCESS_START_TIME "
										+" ,PROCESS_END_TIME "
										+" ,REPORT_ID "
										+" ,VISION_ID "
										+" ,RS_SCHEDULE_SEQ "
										+" ,SCHEDULE_DATE "
										+" ,PREV_SCHEDULE_DATE "
										+" ,SCH_ITERATION_COUNT "
										+" ,RSS_PROCESS_STATUS "
										+" ,BURST_ID "
										+" ,BURST_FLAG "
										+" ,BURST_ID_SEQ "
										+" ,SCHEDULE_START_DATE "
										+" ,SCHEDULE_END_DATE "
										+" ,RSS_FREQUENCY "
										+" ,ATTACH_XL_FLAG "
										+" ,ATTACH_HTML_FLAG "
										+" ,ATTACH_PDF_FLAG "
										+" ,EMAIL_FILE_FLAG "
										+" ,FTP_FILE_FLAG "
										+" ,FTP_CONNECTIVITY_DETAILS "
										+" ,XL_EXPORT_TYPE "
										+" ,SCH_BLANK_REPORT_FLAG "
										+" ,EMAIL_SUBJECT "
										+" ,EMAIL_HEADER "
										+" ,EMAIL_FOOTER "
										+" ,SCALLING_FACTOR "
										+" ,RS_VISION_ID "
										+" ,PROMPT_VALUE_1 "
										+" ,PROMPT_VALUE_2 "
										+" ,PROMPT_VALUE_3 "
										+" ,PROMPT_VALUE_4 "
										+" ,PROMPT_VALUE_5 "
										+" ,PROMPT_VALUE_6 "
										+" ,PROMPT_VALUE_7 "
										+" ,PROMPT_VALUE_8 "
										+" ,PROMPT_VALUE_9 "
										+" ,PROMPT_VALUE_10 "
										+" ,PROMPT_VALUE_1_DESC "
										+" ,PROMPT_VALUE_2_DESC "
										+" ,PROMPT_VALUE_3_DESC "
										+" ,PROMPT_VALUE_4_DESC "
										+" ,PROMPT_VALUE_5_DESC "
										+" ,PROMPT_VALUE_6_DESC "
										+" ,PROMPT_VALUE_7_DESC "
										+" ,PROMPT_VALUE_8_DESC "
										+" ,PROMPT_VALUE_9_DESC "
										+" ,PROMPT_VALUE_10_DESC "
										+" ,prompt_label_1 "
										+" ,prompt_label_2 "
										+" ,prompt_label_3 "
										+" ,prompt_label_4 "
										+" ,prompt_label_5 "
										+" ,prompt_label_6 "
										+" ,prompt_label_7 "
										+" ,prompt_label_8 "
										+" ,prompt_label_9 "
										+" ,prompt_label_10 "
										+" ,RSS_STATUS "
										+" ,RECORD_INDICATOR "
										+" ,MAKER "
										+" ,VERIFIER "
										+" ,INTERNAL_STATUS "
										+" ,DATE_LAST_MODIFIED "
										+" ,DATE_CREATION "
										+" ,INTERVAL_X_DAYS "
										+" ,INTERVAL_X_HOURS "
										+" ,INTERVAL_X_MINS "
										+" ,DAYS_OF_WEEK "
										+" ,MONTHS_OF_YEAR "
										+" ,DAY_OF_THE_MONTH "
										+" ,DB_CONNECTIVITY_DETAILS "
										+" ,READINESS_SCRIPTS_TYPE "
										+" ,READINESS_SCRIPTS "
										+" ,SCH_ITERATION_MINS "
										+" ,SCH_MAX_ITERATION_COUNT "
										+" ,RSS_NEXT_SCH_TYPE "
										+" ,EMAIL_TO "
										+" ,EMAIL_CC "
										+" ,EMAIL_BCC "
										+" ,EMAIL_FCC "
										+" ,NODE_REQUEST "
										+" ,NODE_REQUEST_TIME "
										+" ,BURST_SCRIPTS_TYPE "
										+" ,BURST_SCRIPTS "
										+" ,ATTACH_FILE_NAME "
										+" ,PRIORITY "
										+" ,SERVER_CONNECTIVITY_DETAILS "
										+" ,TIMEZONE_COUNTRY_CODE "
										+" ,SCHEDULE_GROUP "
										+" ,SCHEDULE_SUBGROUP "
										+" ,PROMPT_HASH_SCRIPT "
										+" ,REPORT_TITLE "
										+" ,INTERVAL_X_STARTTIME "
										+" ,INTERVAL_X_ENDTIME"
										+ ",REPORT_TYPE) "
										+" Select "
										+"  T1.NEXT_SCHEDULE_DATE NEXT_PROCESS_TIME "
										+" ,0 PROCESS_VERSION_NO "
										+" ,null PROCESS_START_TIME "
										+" ,null PROCESS_END_TIME "
										+" ,T1.REPORT_ID "
										+" ,T1.VISION_ID "
										+" ,T1.RS_SCHEDULE_SEQ "
										+" ,T1.NEXT_SCHEDULE_DATE SCHEDULE_DATE "
										+" ,ISNULL(T1.PREV_SCHEDULE_DATE,convert(DATE,'01-JAN-1900')) PREV_SCHEDULE_DATE "
										+" ,0 SCH_ITERATION_COUNT "
										+" ,'P' RSS_PROCESS_STATUS "
										+" ,T1.BURST_ID "
										+" ,T1.BURST_FLAG "
										+" ,0 BURST_ID_SEQ "
										+" ,T1.SCHEDULE_START_DATE "
										+" ,T1.SCHEDULE_END_DATE "
										+" ,T1.RSS_FREQUENCY "
										+" ,T1.ATTACH_XL_FLAG "
										+" ,T1.ATTACH_HTML_FLAG "
										+" ,T1.ATTACH_PDF_FLAG "
										+" ,T1.EMAIL_FILE_FLAG "
										+" ,T1.FTP_FILE_FLAG "
										+" ,T1.FTP_CONNECTIVITY_DETAILS "
										+" ,T1.XL_EXPORT_TYPE "
										+" ,T1.SCH_BLANK_REPORT_FLAG "
										+" ,Replace(Replace(Replace(Replace(T1.EMAIL_SUBJECT "
										+"       ,'#REPORT_TITLE#',RS.REPORT_TITLE) "
										+"       ,'#SCHEDULE_DATE_HH24:MI#',FORMAT(T1.NEXT_SCHEDULE_DATE,'HH:mm')) "
										+"       ,'#SCHEDULE_DATE_HH:MI#',FORMAT(T1.NEXT_SCHEDULE_DATE,'hh:mm tt')) "
										+"       ,'#SCHEDULE_DATE_DD-MON-YYYY#',FORMAT(T1.NEXT_SCHEDULE_DATE,'dd-MMM-yyyy')) EMAIL_SUBJECT "
										+" ,Replace(T1.EMAIL_HEADER ,'#REPORT_TITLE#',RS.REPORT_TITLE) EMAIL_HEADER "
										+" ,T1.EMAIL_FOOTER "
										+" ,T1.SCALLING_FACTOR "
										+" ,T1.VISION_ID RS_VISION_ID "
										/*+" ,Case When instr(T1.PROMPT_VALUE_1,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_1||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_1 End PROMPT_VALUE_1 "
										+" ,Case When instr(T1.PROMPT_VALUE_2,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_2||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_2 End PROMPT_VALUE_2 "
										+" ,Case When instr(T1.PROMPT_VALUE_3,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_3||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_3 End PROMPT_VALUE_3 "
										+" ,Case When instr(T1.PROMPT_VALUE_4,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_4||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_4 End PROMPT_VALUE_4 "
										+" ,Case When instr(T1.PROMPT_VALUE_5,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_5||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_5 End PROMPT_VALUE_5 "
										+" ,Case When instr(T1.PROMPT_VALUE_6,'AUTO') > 0 then FN_GET_RSS_PROMPTVALUE(T1.PROMPT_VALUE_6||' ',T1.NEXT_SCHEDULE_DATE,T1.PREV_SCHEDULE_DATE,null) else T1.PROMPT_VALUE_6 End PROMPT_VALUE_6 "*/
										+" ,T1.PROMPT_VALUE_1 PROMPT_VALUE_1 "
										+" ,T1.PROMPT_VALUE_2 PROMPT_VALUE_2 "
										+" ,T1.PROMPT_VALUE_3 PROMPT_VALUE_3 "
										+" ,T1.PROMPT_VALUE_4 PROMPT_VALUE_4 "
										+" ,T1.PROMPT_VALUE_5 PROMPT_VALUE_5 "
										+" ,T1.PROMPT_VALUE_6 PROMPT_VALUE_6 "
										+" ,T1.PROMPT_VALUE_7 PROMPT_VALUE_7 "
										+" ,T1.PROMPT_VALUE_8 PROMPT_VALUE_8 "
										+" ,T1.PROMPT_VALUE_9 PROMPT_VALUE_9 "
										+" ,T1.PROMPT_VALUE_10 PROMPT_VALUE_10 "
										+" ,T1.PROMPT_VALUE_1_DESC "
										+" ,T1.PROMPT_VALUE_2_DESC "
										+" ,T1.PROMPT_VALUE_3_DESC "
										+" ,T1.PROMPT_VALUE_4_DESC "
										+" ,T1.PROMPT_VALUE_5_DESC "
										+" ,T1.PROMPT_VALUE_6_DESC "
										+" ,T1.PROMPT_VALUE_7_DESC "
										+" ,T1.PROMPT_VALUE_8_DESC "
										+" ,T1.PROMPT_VALUE_9_DESC "
										+" ,T1.PROMPT_VALUE_10_DESC "
										+" ,T1.PROMPT_LABEL_1 "
										+" ,T1.PROMPT_LABEL_2 "
										+" ,T1.PROMPT_LABEL_3 "
										+" ,T1.PROMPT_LABEL_4 "
										+" ,T1.PROMPT_LABEL_5 "
										+" ,T1.PROMPT_LABEL_6 "
										+" ,T1.PROMPT_LABEL_7 "
										+" ,T1.PROMPT_LABEL_8 "
										+" ,T1.PROMPT_LABEL_9 "
										+" ,T1.PROMPT_LABEL_10 "
										+" ,T1.RSS_STATUS "
										+" ,T1.RECORD_INDICATOR "
										+" ,T1.MAKER "
										+" ,T1.VERIFIER "
										+" ,T1.INTERNAL_STATUS "
										+" ,GETDATE() DATE_LAST_MODIFIED "
										+" ,GETDATE() DATE_CREATION "
										+" ,T1.INTERVAL_X_DAYS "
										+" ,T1.INTERVAL_X_HOURS "
										+" ,T1.INTERVAL_X_MINS "
										+" ,T1.DAYS_OF_WEEK "
										+" ,T1.MONTHS_OF_YEAR "
										+" ,T1.DAY_OF_THE_MONTH "
										+" ,T1.DB_CONNECTIVITY_DETAILS "
										+" ,T1.READINESS_SCRIPTS_TYPE "
										+" ,T1.READINESS_SCRIPTS "
										+" ,T1.SCH_ITERATION_MINS "
										+" ,T1.SCH_MAX_ITERATION_COUNT "
										+" ,T1.RSS_NEXT_SCH_TYPE "
										+" ,"+Constants.TABLE_PREFIX+"FN_GET_LISAGG_RS_SCH_EMAILS(T1.REPORT_ID,T1.VISION_ID,T1.RS_SCHEDULE_SEQ,'TO')  EMAIL_TO "
										+" ,"+Constants.TABLE_PREFIX+"FN_GET_LISAGG_RS_SCH_EMAILS(T1.REPORT_ID,T1.VISION_ID,T1.RS_SCHEDULE_SEQ,'CC')  EMAIL_CC "
										+" ,"+Constants.TABLE_PREFIX+"FN_GET_LISAGG_RS_SCH_EMAILS(T1.REPORT_ID,T1.VISION_ID,T1.RS_SCHEDULE_SEQ,'BCC') EMAIL_BCC "
										+" ,"+Constants.TABLE_PREFIX+"FN_GET_LISAGG_RS_SCH_EMAILS(T1.REPORT_ID,T1.VISION_ID,T1.RS_SCHEDULE_SEQ,'FCC') EMAIL_FCC "
										+" ,'"+localNodeName+"' Node_Request "
										+" ,GETDATE() Node_Request_Time "
										+" ,T1.BURST_SCRIPTS_TYPE "
										+" ,T1.BURST_SCRIPTS "
										+" ,Replace(T1.ATTACH_FILE_NAME,'#REPORT_TITLE#',RS.REPORT_TITLE) ATTACH_FILE_NAME "
										+" ,T1.PRIORITY "
										+" ,T1.SERVER_CONNECTIVITY_DETAILS "
										+" ,T1.TIMEZONE_COUNTRY_CODE "
										+" ,T1.SCHEDULE_GROUP "
										+" ,T1.SCHEDULE_SUBGROUP "
										+" ,T1.PROMPT_HASH_SCRIPT "
										+" ,RS.REPORT_TITLE "
										+" ,T1.INTERVAL_X_STARTTIME "
										+" ,T1.INTERVAL_X_ENDTIME,t1.REPORT_TYPE "
										+" From "+Constants.TABLE_PREFIX+"PRD_RS_Schedule T1, "+Constants.TABLE_PREFIX+"Prd_Report_Master RS "
										+" Where T1.Report_Id = '"+LR_RS_SCHEDULE_TABLE_DATA.getREPORT_ID()+"' "
										+"   And T1.Vision_Id = '"+LR_RS_SCHEDULE_TABLE_DATA.getVISION_ID()+"' "
										+"   And T1.RS_Schedule_Seq = '"+LR_RS_SCHEDULE_TABLE_DATA.getRS_SCHEDULE_SEQ()+"' "
										+"   And T1.Report_Id = RS.Report_Id "
								);
						
					}else {
						insert_query.append(" ***Invalid Database Type*** ");
					}
					
				}else if ("Y".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getBURST_FLAG()) 
					   && !"NONE".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getBURST_SCRIPTS_TYPE())) {
					
					insert_query.append(
							""
							
							);
					
					
				}else if ("Y".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getBURST_FLAG()) 
					   && StringUtils.isNotBlank(LR_RS_SCHEDULE_TABLE_DATA.getBURST_ID())) {
					
					insert_query.append(
							""
							
							);
					
				}else {
					throw new VisionGeneralException(Constants.ERROR_OPERATION,
							"Invalid Burst Flag Logic:"+localRecordPrimaryKeyString);
				}
				
				
				
				StudioReportSchedulerApplication.logWriter(insert_query.toString(),logPath);
				
				
				executeStatment(insert_query.toString(), true, "INSERT",logPath);
				
				
				if("DLY".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getRSS_FREQUENCY())) {
					
					localNextProcessTime = LR_RS_SCHEDULE_TABLE_DATA.getNEXT_SCHEDULE_DATE_DATE();
					
					if(LR_RS_SCHEDULE_TABLE_DATA.getDAYS_OF_WEEK().indexOf("Y") == -1) {
						
						//ROLLBACK
						throw new VisionGeneralException(Constants.ERROR_OPERATION,
								"Error finding next process type using invalid DOW:"+localRecordPrimaryKeyString);
						
						
					} else {
						
						
						tempCounter = 0;
						
						for(;;) {
							
							tempCounter++;
							
							if(
									"Y".equalsIgnoreCase(
									LR_RS_SCHEDULE_TABLE_DATA.getDAYS_OF_WEEK().substring(
											 (int) Duration.between(
													 localNextProcessTime.plusDays(tempCounter).with(TemporalAdjusters.previousOrSame( DayOfWeek.MONDAY )),
													 localNextProcessTime.plusDays(tempCounter)).toDays(),
											 (int) Duration.between(
													 localNextProcessTime.plusDays(tempCounter).with(TemporalAdjusters.previousOrSame( DayOfWeek.MONDAY )),
													 localNextProcessTime.plusDays(tempCounter)).toDays() +1)
									 )
									&& 
									  (localNextProcessTime.plusDays(tempCounter).compareTo(LocalDateTime.now()) > 0
									|| "NEXT".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getRSS_NEXT_SCH_TYPE()))
									) {
								
								
								localNextProcessTime = localNextProcessTime.plusDays(tempCounter);
								break;
								
							}
						}
						
						
					}
				} else if ("MTH".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getRSS_FREQUENCY())) {
					
					localNextProcessTime = LR_RS_SCHEDULE_TABLE_DATA.getNEXT_SCHEDULE_DATE_DATE();
					
					if(LR_RS_SCHEDULE_TABLE_DATA.getMONTHS_OF_YEAR().indexOf("Y") == -1) {
						
						//ROLLBACK
						throw new VisionGeneralException(Constants.ERROR_OPERATION,
								"Error finding next process type using invalid DOW:"+localRecordPrimaryKeyString);
						
						
					} else {
						
						tempCounter = 0;
						
						for(;;) {
							tempCounter++;
							
							if (LR_RS_SCHEDULE_TABLE_DATA.getDAY_OF_THE_MONTH() == 99
									|| LR_RS_SCHEDULE_TABLE_DATA.getDAY_OF_THE_MONTH() > 
											localNextProcessTime
											.plusMonths(tempCounter)
											.with(TemporalAdjusters.lastDayOfMonth())
											.getDayOfMonth()) {
		
								localNextProcessTime = localNextProcessTime.plusMonths(tempCounter).with(TemporalAdjusters.lastDayOfMonth());
							} else {
								
								localNextProcessTime = localNextProcessTime.plusMonths(tempCounter).withDayOfMonth(LR_RS_SCHEDULE_TABLE_DATA.getDAY_OF_THE_MONTH());
								
							}
							
							if(
									"Y".equalsIgnoreCase(
									LR_RS_SCHEDULE_TABLE_DATA.getMONTHS_OF_YEAR().substring(
											localNextProcessTime.getMonthValue()-1,
											localNextProcessTime.getMonthValue())
									 )
									&& 
									  (localNextProcessTime.compareTo(LocalDateTime.now()) > 0
									|| "NEXT".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getRSS_NEXT_SCH_TYPE()))
									) {
								
								break;
								
							}
							
							
						}
					
					}
					
					
				} else if ("ADHOC".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getRSS_FREQUENCY())) {
					
					localNextProcessTime = LR_RS_SCHEDULE_TABLE_DATA.getNEXT_SCHEDULE_DATE_DATE().plusMinutes(1);
					
				} else if ("INTERVAL".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getRSS_FREQUENCY())) {
					
					if(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_ENDTIME() < LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_STARTTIME()) {
						
						//ROLLBACK
						throw new VisionGeneralException(Constants.ERROR_OPERATION,
								"Error finding next process type using invalid Interval Start,End Time:"+localRecordPrimaryKeyString);
						
						
					} else if(LR_RS_SCHEDULE_TABLE_DATA.getDAYS_OF_WEEK().indexOf("Y") == -1) {
						
						//ROLLBACK
						throw new VisionGeneralException(Constants.ERROR_OPERATION,
								"Error finding next process type using invalid DOW:"+localRecordPrimaryKeyString);
						
						
					} else {
						
						tempCounter = 1;
						
						localNextProcessTime = 
								LR_RS_SCHEDULE_TABLE_DATA.getNEXT_SCHEDULE_DATE_DATE()
								.plusDays(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_DAYS())
								.plusHours(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_HOURS())
								.plusMinutes(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_MINS());
						
						
						for(;;) {
							
							
							if(!"Y".equalsIgnoreCase(
									LR_RS_SCHEDULE_TABLE_DATA.getDAYS_OF_WEEK().substring(
											 (int) Duration.between(
													 localNextProcessTime.plusDays(tempCounter).with(TemporalAdjusters.previousOrSame( DayOfWeek.MONDAY )),
													 localNextProcessTime.plusDays(tempCounter)).toDays(),
											 (int) Duration.between(
													 localNextProcessTime.plusDays(tempCounter).with(TemporalAdjusters.previousOrSame( DayOfWeek.MONDAY )),
													 localNextProcessTime.plusDays(tempCounter)).toDays() +1)
									 )){
								
								localNextProcessTime = localNextProcessTime.plusDays(1);
								continue;
								
							}
							
							if(Integer.parseInt(CommonUtils.convertDateTimeToString(localNextProcessTime, "HHmm")) 
									< LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_STARTTIME()) {
								
								localNextProcessTime = localNextProcessTime
														.withHour(Integer.parseInt(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_STARTTIME_TXT().substring(0,2)))
														.withMinute(Integer.parseInt(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_STARTTIME_TXT().substring(3,5)))
														.withSecond(0);
								continue;
								
							}
							
							if(Integer.parseInt(CommonUtils.convertDateTimeToString(localNextProcessTime, "HHmm")) 
									> LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_ENDTIME()) {
								
								localNextProcessTime = localNextProcessTime
														.plusDays(1)
														.withHour(Integer.parseInt(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_STARTTIME_TXT().substring(0,2)))
														.withMinute(Integer.parseInt(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_STARTTIME_TXT().substring(3,5)))
														.withSecond(0);
								continue;
								
							}
							
							if(!("NEAREST".equalsIgnoreCase(LR_RS_SCHEDULE_TABLE_DATA.getRSS_NEXT_SCH_TYPE())
									&& localNextProcessTime.compareTo(LocalDateTime.now()) < 0)) {
								
								break;
								
							}
							
							tempCounter++;
							
							localNextProcessTime = 
									localNextProcessTime
									.plusDays(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_DAYS())
									.plusHours(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_HOURS())
									.plusMinutes(LR_RS_SCHEDULE_TABLE_DATA.getINTERVAL_X_MINS());
							
							if(tempCounter > 10000) {
								
								//ROLLBACK
								throw new VisionGeneralException(Constants.ERROR_OPERATION,
										"Error finding next process type.. Infinite Loop Alert!:"+localRecordPrimaryKeyString);
								
								
							}
							
						}
					}
				}
				
				
				update_query = new StringBuilder();
				
				if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
					update_query.append(
							
							 "Update "+Constants.TABLE_PREFIX+"PRD_RS_Schedule set "
							+"  NEXT_SCHEDULE_DATE = to_date('"+CommonUtils.convertDateTimeToString(localNextProcessTime, "yyyy-MM-dd HH:mm:ss")+"','YYYY-MM-DD HH24:MI:SS')"
							+" ,PREV_SCHEDULE_DATE = NEXT_SCHEDULE_DATE"
							+" ,DATE_LAST_UPDATED = sysdate "
							+" Where Report_Id = '"+LR_RS_SCHEDULE_TABLE_DATA.getREPORT_ID()+"' "
							+"   And Vision_Id = '"+LR_RS_SCHEDULE_TABLE_DATA.getVISION_ID()+"' "
							+"   And RS_Schedule_Seq = '"+LR_RS_SCHEDULE_TABLE_DATA.getRS_SCHEDULE_SEQ()+"' ");
				} else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
					update_query.append(
							
							 "Update "+Constants.TABLE_PREFIX+"PRD_RS_Schedule set "
							+"  NEXT_SCHEDULE_DATE = convert(datetime,'"+CommonUtils.convertDateTimeToString(localNextProcessTime, "yyyy-MM-dd HH:mm:ss")+"',120)"
							+" ,PREV_SCHEDULE_DATE = NEXT_SCHEDULE_DATE"
							+" ,DATE_LAST_UPDATED = GETDATE() "
							+" Where Report_Id = '"+LR_RS_SCHEDULE_TABLE_DATA.getREPORT_ID()+"' "
							+"   And Vision_Id = '"+LR_RS_SCHEDULE_TABLE_DATA.getVISION_ID()+"' "
							+"   And RS_Schedule_Seq = '"+LR_RS_SCHEDULE_TABLE_DATA.getRS_SCHEDULE_SEQ()+"' ");
				}else {
					update_query.append(" ***Invalid Database Type*** ");
				}
				

				StudioReportSchedulerApplication.logWriter(update_query.toString(),logPath);
				
				executeStatment(update_query.toString(), true, "UPDATE",logPath);
				
				
			} /* RS_SCHEDULE_TABLE_DATA Loop End */
			return Constants.SUCCESS_OPERATION;
		}catch(Exception e) {
			StudioReportSchedulerApplication.logWriter("Error in Rss Processor Jar "+e.getMessage(),logPath);
			return Constants.ERROR_OPERATION;
		}
		
	}
	
	private int getRSScheduleServiceStatus(String logFilePath) {
		
		ResultSet rs;
		
		int RS_Schedule_Service_Status;
		
		String  P_Node_Downtime = new CommonDao(jdbcTemplate).findVisionVariableValue("NODE_DOWNTIME");
		
		if(!ValidationUtil.isValid(P_Node_Downtime)) {
			StudioReportSchedulerApplication.logWriter("Vision Variable NODE_DOWNTIME is not Maintained !!",logFilePath);
			return Constants.ERROR_OPERATION;
		}
		
		StringBuilder strQuery_SB = new StringBuilder();
		strQuery_SB.append(" select count(1) COUNTER"
						+" from "+Constants.TABLE_PREFIX+"vision_rac_fetch_det t1,  "
						+"      "+Constants.TABLE_PREFIX+"vision_node_credentials t2  "
						+" where t1.node_name = t2.node_name "
						+"   and t1.cron_name = 'PRD_PROCESS_SCHEDULE_CRON' "
						+"   and t2.server_environment = '"+Constants.VISION_SERVER_ENVIRONMENT+"' "
						+"   and t1.last_ping_time >  ");
		
		
		if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
			
			strQuery_SB.append(" sysdate-("+P_Node_Downtime+"/1440) ");
			
		}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
			
			strQuery_SB.append(" DATEADD(minute, -"+P_Node_Downtime+", GETDATE()) ");
			
		}else {
			strQuery_SB.append(" ***Invalid Database Type*** ");
		}
		
		StudioReportSchedulerApplication.logWriter(strQuery_SB.toString(),logFilePath);
		
		if(!ValidationUtil.isValid(Constants.VISION_SERVER_ENVIRONMENT)) {
			StudioReportSchedulerApplication.logWriter("Env Variable VISION_SERVER_ENVIRONMENT is not Maintained !!",logFilePath);
			return Constants.ERROR_OPERATION;
		}
		
		
		
		try {
			RS_Schedule_Service_Status = jdbcTemplate.queryForObject(strQuery_SB.toString(),Integer.class);
			if(!ValidationUtil.isValid(RS_Schedule_Service_Status)){
				throw new VisionGeneralException(Constants.ERROR_OPERATION,
						"Error fetching status for Schedule Crons in VISION_RAC_FETCH_DET & VISION_NODE_CREDENTIALS  !!");
			}
		} catch (Exception e) {
			//e.printStackTrace();
			throw new VisionGeneralException(Constants.ERROR_OPERATION,
					"Error fetching status for Schedule Crons ! Query["+strQuery_SB.toString()+" error message ["+e.getMessage()+"]");
		}
		
		return RS_Schedule_Service_Status;
	}
	
	private void doUpdateRSEmailStatus(String logPath) {
		
		String  P_Verify_Staff_Email_Flag = new CommonDao(jdbcTemplate).findVisionVariableValue("RSS_VERIFY_STAFF_EMAIL_FLAG");
		
		if("Y".equalsIgnoreCase(P_Verify_Staff_Email_Flag)) {
			
			StringBuilder strQuery_SB = new StringBuilder();
			
			if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				strQuery_SB.append(
						 " update "+Constants.TABLE_PREFIX+"PRD_RS_Schedule_email t1 "
						+" set t1.RS_EMAIL_STATUS = 8,  "
						+"     t1.DATE_LAST_MODIFIED = sysdate "
						+" Where t1.RS_EMAIL_STATUS = 0 "
						+"   And Exists ( "
						+"       SELECT 1 "
						+"       FROM "+Constants.TABLE_PREFIX+"staff_id x "
						+"       WHERE x.resignation_date is not null "
						+"         AND TRUNC(x.resignation_date) <= TRUNC (SYSDATE) "
						+"         AND trim(lower(x.official_email_ids)) =  trim(lower(t1.email_id))) "
						+"   And NOT Exists ( "
						+"       SELECT 1 "
						+"       FROM "+Constants.TABLE_PREFIX+"staff_id x "
						+"       WHERE (x.resignation_date is null  "
						+"          OR  TRUNC(x.resignation_date) > TRUNC (SYSDATE)) "
						+"         AND trim(lower(x.official_email_ids)) =  trim(lower(t1.email_id))) ");
			}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				strQuery_SB.append(
						 " update "+Constants.TABLE_PREFIX+"PRD_RS_Schedule_email "
						+" set RS_EMAIL_STATUS = 8,  "
						+"     DATE_LAST_MODIFIED = GETDATE() "
						+" from "+Constants.TABLE_PREFIX+"PRD_RS_Schedule_email t1 "
						+" Where t1.RS_EMAIL_STATUS = 0 "
						+"   And Exists ( "
						+"       SELECT 1 "
						+"       FROM "+Constants.TABLE_PREFIX+"staff_id x "
						+"       WHERE x.resignation_date is not null "
						+"         AND (x.resignation_date) <=  (GETDATE()) "
						+"         AND trim(lower(x.official_email_ids)) =  trim(lower(t1.email_id))) "
						+"   And NOT Exists ( "
						+"       SELECT 1 "
						+"       FROM "+Constants.TABLE_PREFIX+"staff_id x "
						+"       WHERE (x.resignation_date is null  "
						+"          OR  (x.resignation_date) >  (GETDATE())) "
						+"         AND trim(lower(x.official_email_ids)) =  trim(lower(t1.email_id)))");
			}else {
				strQuery_SB.append(" ***Invalid Database Type*** ");
			}
			
			executeStatment(strQuery_SB.toString(), true, "UPDATE",logPath);
			
			
		
		}

		return;
	}
	
	private String getNodeRequest(String logFilePath) {
		
		String localNodeName = null;
		
		ResultSet rs;
		
		String  P_Node_Downtime = new CommonDao(jdbcTemplate).findVisionVariableValue("NODE_DOWNTIME");
		
		if(!ValidationUtil.isValid(P_Node_Downtime)) {
			StudioReportSchedulerApplication.logWriter("Vision Variable NODE_DOWNTIME is not Maintained !!",logFilePath);
			return "";
		}
		
		StringBuilder strQuery_SB = new StringBuilder();
		
		if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
			strQuery_SB.append(" select x.node_name "
					+" from ( "
					+"     select x1.node_name, "
					+"            row_number() over (order by (count(1)/nullif(max(x1.RS_SCHEDULER_MAX_PROCESSES),0)) nulls last) b "
					+"     from "+Constants.TABLE_PREFIX+"vision_node_credentials x1 inner join "+Constants.TABLE_PREFIX+"vision_rac_fetch_det x3 "
					+"      on (x1.node_name = x3.node_name  "
					+"      and x3.cron_name = 'PRD_PROCESS_SCHEDULE_CRON'  "
					+"      and x3.last_ping_time > sysdate-("+P_Node_Downtime+"/1440)) "
					+"      left outer join "+Constants.TABLE_PREFIX+"rss_process_control x2 "
					+"      on (x1.node_name = nvl(x2.node_override,x2.node_request) "
					+"      and x2.rss_process_status in ('P','R','I','M')) "
					+"     where x1.server_environment = '"+Constants.VISION_SERVER_ENVIRONMENT+"' "
					+"       and x1.node_status = 0 "
					+"     group by x1.node_name "
					+"     ) x "
					+" where x.b = 1 ");
			
		}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
			
			strQuery_SB.append(" select x.node_name "
					+" from ( "
					+"     select x1.node_name, "
					+"            row_number() over (order by (count(1)/nullif(max(x1.RS_SCHEDULER_MAX_PROCESSES),0)) ) b "/*nulls last*/
					+"     from "+Constants.TABLE_PREFIX+"vision_node_credentials x1 inner join "+Constants.TABLE_PREFIX+"vision_rac_fetch_det x3 "
					+"      on (x1.node_name = x3.node_name  "
					+"      and x3.cron_name = 'PRD_PROCESS_SCHEDULE_CRON'  "
					+"      and x3.last_ping_time >  DATEADD(minute, -"+P_Node_Downtime+", GETDATE()))  "
					+"      left outer join "+Constants.TABLE_PREFIX+"rss_process_control x2 "
					+"      on (x1.node_name = isnull(x2.node_override,x2.node_request) "
					+"      and x2.rss_process_status in ('P','R','I','M')) "
					+"     where x1.server_environment = '"+Constants.VISION_SERVER_ENVIRONMENT+"' "
					+"       and x1.node_status = 0 "
					+"     group by x1.node_name "
					+"     ) x "
					+" where x.b = 1 ");
			
		}else {
			strQuery_SB.append(" ***Invalid Database Type*** ");
		}
		
		StudioReportSchedulerApplication.logWriter(strQuery_SB.toString(),logFilePath);
		
		try {
			localNodeName = jdbcTemplate.queryForObject(strQuery_SB.toString(),String.class);
			if(!ValidationUtil.isValid(localNodeName)  ){
				throw new VisionGeneralException(Constants.ERROR_OPERATION,
						"Error fetching Node Name for new request ! Query["+strQuery_SB.toString()+"] error message [No Records Found]");
			}
		} catch (Exception e) {
			//e.printStackTrace();
			throw new VisionGeneralException(Constants.ERROR_OPERATION,
					"Error fetching Node Name for new request ! Query["+strQuery_SB.toString()+"] error message ["+e.getMessage()+"]");
		}
		return localNodeName;
	}
	
	private List<RSScheduleTabColVb> getRSScheduleTabData(String logFilePath) {
		List<RSScheduleTabColVb> LL_RS_SCHEDULE_TABLE_DATA = new ArrayList<RSScheduleTabColVb>();
		ResultSet rs;
		StringBuilder strQuery_SB = new StringBuilder();
		
		if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
			strQuery_SB.append(" select "
					+"  T1.REPORT_ID "
					+" ,T1.VISION_ID "
					+" ,T1.RS_SCHEDULE_SEQ "
					+" ,to_char(T1.NEXT_SCHEDULE_DATE, 'YYYY-MM-DD HH24:MI:SS') NEXT_SCHEDULE_DATE "
					+" ,T1.RSS_FREQUENCY "
					+" ,nvl(T1.INTERVAL_X_DAYS,0) INTERVAL_X_DAYS "
					+" ,nvl(T1.INTERVAL_X_HOURS,0) INTERVAL_X_HOURS "
					+" ,nvl(T1.INTERVAL_X_MINS,0) INTERVAL_X_MINS "
					+" ,T1.DAYS_OF_WEEK "
					+" ,T1.MONTHS_OF_YEAR "
					+" ,T1.DAY_OF_THE_MONTH "
					+" ,T1.RSS_NEXT_SCH_TYPE "
					+" ,T1.BURST_FLAG "
					+" ,T1.BURST_ID "
					+" ,T1.BURST_SCRIPTS_TYPE "
					+" ,Case When T1.BURST_SCRIPTS_TYPE = 'MACROVAR' Then  "
					+"       (Select to_clob(vv.variable_script) from "+Constants.TABLE_PREFIX+"vision_dynamic_hash_var vv  "
					+"        where vv.variable_name = to_char(T1.BURST_SCRIPTS) "
					+"          and vv.variable_type = 2) "
					+"  Else T1.BURST_SCRIPTS End BURST_SCRIPTS "
					+" ,T1.PROMPT_VALUE_1 "
					+" ,T1.PROMPT_VALUE_2 "
					+" ,T1.PROMPT_VALUE_3 "
					+" ,T1.PROMPT_VALUE_4 "
					+" ,T1.PROMPT_VALUE_5 "
					+" ,T1.PROMPT_VALUE_6 "
					+" ,T1.PROMPT_VALUE_7 "
					+" ,T1.PROMPT_VALUE_8 "
					+" ,T1.PROMPT_VALUE_9 "
					+" ,T1.PROMPT_VALUE_10 "
					+" ,T1.prompt_value_1_desc "
					+" ,T1.prompt_value_2_desc "
					+" ,T1.prompt_value_3_desc "
					+" ,T1.prompt_value_4_desc "
					+" ,T1.prompt_value_5_desc "
					+" ,T1.prompt_value_6_desc "
					+" ,T1.prompt_value_7_desc "
					+" ,T1.prompt_value_8_desc "
					+" ,T1.prompt_value_9_desc "
					+" ,T1.prompt_value_10_desc "
					+" ,T1.prompt_label_1 "
					+" ,T1.prompt_label_2 "
					+" ,T1.prompt_label_3 "
					+" ,T1.prompt_label_4 "
					+" ,T1.prompt_label_5 "
					+" ,T1.prompt_label_6 "
					+" ,T1.prompt_label_7 "
					+" ,T1.prompt_label_8 "
					+" ,T1.prompt_label_9 "
					+" ,T1.prompt_label_10 "
					+" ,NVL(TO_NUMBER(REPLACE(T1.INTERVAL_X_STARTTIME,':','')),0) INTERVAL_X_STARTTIME "
					+" ,NVL(TO_NUMBER(REPLACE(T1.INTERVAL_X_ENDTIME,':','')),2359) INTERVAL_X_ENDTIME "
					+" ,NVL(T1.INTERVAL_X_STARTTIME,'00:00') INTERVAL_X_STARTTIME_TXT "
					+" ,NVL(T1.INTERVAL_X_ENDTIME,'23:59') INTERVAL_X_ENDTIME_TXT "
					+" From "+Constants.TABLE_PREFIX+"PRD_RS_Schedule T1 "
					+" Where T1.NEXT_SCHEDULE_DATE <= (SYSDATE+(3/1440)) "
					+"   And T1.NEXT_SCHEDULE_DATE between T1.SCHEDULE_START_DATE and NVL(T1.SCHEDULE_END_DATE,T1.NEXT_SCHEDULE_DATE) "
					+"   And T1.RSS_Status = 0 And T1.Burst_Flag = 'N' "
					+"   And (Exists "
					+"        (Select 1 From "+Constants.TABLE_PREFIX+"Prd_report_Master RS Where RS.Report_Id = T1.Report_Id And RS.Status = 0) "
					+"     OR Exists "
					+"        (Select 1 From "+Constants.TABLE_PREFIX+"VC_REPORT_DEFS_SELFBI VC Where VC.Catalog_Id= T1.Catalog_Id and VC.Report_Id = T1.Report_Id And VC.VRD_STATUS = 0) ) "
					+"   And Exists "
					+"       (Select 1 From "+Constants.TABLE_PREFIX+"Vision_Users VU Where VU.Vision_Id = T1.Vision_Id And VU.User_Status = 0) "
					+"   And Exists "
					+"       (Select 1 From "+Constants.TABLE_PREFIX+"PRD_RS_Schedule_Email RSE "
					+"        Where RSE.Report_Id = T1.Report_Id "
					+"          And RSE.Vision_Id = T1.Vision_Id "
					+"          And RSE.RS_Schedule_Seq = T1.RS_Schedule_Seq "
					+"          And RSE.Email_Type = 'TO' "
					+"          And RSE.RS_Email_Status = 0) "
					+"   And Not Exists "
					+"       (Select 1  "
					+"        From "+Constants.TABLE_PREFIX+"PRD_RS_Schedule_Control RSC "
					+"        Where NVL(RSC.Report_Id,T1.Report_Id) = T1.Report_Id "
					+"          And NVL(RSC.Vision_Id,T1.Vision_Id) = T1.Vision_Id "
					+"          And NVL(RSC.Schedule_Group,T1.Schedule_Group) = T1.Schedule_Group "
					+"          And NVL(RSC.Schedule_SubGroup,T1.Schedule_SubGroup) = T1.Schedule_SubGroup "
					+"          And (RSC.RSS_Status != 0  "
					+"            OR NOT(sysdate between NVL(RSC.Effective_Start_date,sysdate) And NVL(RSC.Effective_End_Date,sysdate)))  "
					+"          And (RSC.Report_Id is not null "
					+"            OR RSC.Vision_Id is not null "
					+"            OR RSC.Schedule_Group is not null "
					+"            OR RSC.Schedule_SubGroup is not null))");
		}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
			strQuery_SB.append(" select "
					+"  T1.REPORT_ID "
					+" ,T1.VISION_ID "
					+" ,T1.RS_SCHEDULE_SEQ "
					+" ,convert(varchar, T1.NEXT_SCHEDULE_DATE, 120) NEXT_SCHEDULE_DATE " /*YYYY-MM-DD HH24:MI:SS*/
					+" ,T1.RSS_FREQUENCY "
					+" ,ISNULL(T1.INTERVAL_X_DAYS,0) INTERVAL_X_DAYS "
					+" ,ISNULL(T1.INTERVAL_X_HOURS,0) INTERVAL_X_HOURS "
					+" ,ISNULL(T1.INTERVAL_X_MINS,0) INTERVAL_X_MINS "
					+" ,T1.DAYS_OF_WEEK "
					+" ,T1.MONTHS_OF_YEAR "
					+" ,T1.DAY_OF_THE_MONTH "
					+" ,T1.RSS_NEXT_SCH_TYPE "
					+" ,T1.BURST_FLAG "
					+" ,T1.BURST_ID "
					+" ,T1.BURST_SCRIPTS_TYPE "
					/*+" ,Case When T1.BURST_SCRIPTS_TYPE = 'MACROVAR' Then  "
					+"       (Select to_clob(vv.variable_script) from "+Constants.TABLE_PREFIX+"vision_dynamic_hash_var vv  "
					+"        where vv.variable_name = to_char(T1.BURST_SCRIPTS) "
					+"          and vv.variable_type = 2) "
					+"  Else T1.BURST_SCRIPTS End BURST_SCRIPTS "*/
					+" ,T1.BURST_SCRIPTS "
					+" ,T1.PROMPT_VALUE_1 "
					+" ,T1.PROMPT_VALUE_2 "
					+" ,T1.PROMPT_VALUE_3 "
					+" ,T1.PROMPT_VALUE_4 "
					+" ,T1.PROMPT_VALUE_5 "
					+" ,T1.PROMPT_VALUE_6 "
					+" ,T1.PROMPT_VALUE_7 "
					+" ,T1.PROMPT_VALUE_8 "
					+" ,T1.PROMPT_VALUE_9 "
					+" ,T1.PROMPT_VALUE_10 "
					+" ,T1.prompt_value_1_desc "
					+" ,T1.prompt_value_2_desc "
					+" ,T1.prompt_value_3_desc "
					+" ,T1.prompt_value_4_desc "
					+" ,T1.prompt_value_5_desc "
					+" ,T1.prompt_value_6_desc "
					+" ,T1.prompt_value_7_desc "
					+" ,T1.prompt_value_8_desc "
					+" ,T1.prompt_value_9_desc "
					+" ,T1.prompt_value_10_desc "
					+" ,T1.prompt_label_1 "
					+" ,T1.prompt_label_2 "
					+" ,T1.prompt_label_3 "
					+" ,T1.prompt_label_4 "
					+" ,T1.prompt_label_5 "
					+" ,T1.prompt_label_6 "
					+" ,T1.prompt_label_7 "
					+" ,T1.prompt_label_8 "
					+" ,T1.prompt_label_9 "
					+" ,T1.prompt_label_10 "
					+" ,ISNULL(CAST(REPLACE(T1.INTERVAL_X_STARTTIME,':','') AS INT),0) INTERVAL_X_STARTTIME "
					+" ,ISNULL(CAST(REPLACE(T1.INTERVAL_X_ENDTIME,':','') AS INT),2359) INTERVAL_X_ENDTIME "
					+" ,ISNULL(T1.INTERVAL_X_STARTTIME,'00:00') INTERVAL_X_STARTTIME_TXT "
					+" ,ISNULL(T1.INTERVAL_X_ENDTIME,'23:59') INTERVAL_X_ENDTIME_TXT "
					+" From "+Constants.TABLE_PREFIX+"PRD_RS_Schedule T1 "
					+" Where T1.NEXT_SCHEDULE_DATE <= DATEADD(minute, 5, GETDATE()) "
					+"   And T1.NEXT_SCHEDULE_DATE between T1.SCHEDULE_START_DATE and ISNULL(T1.SCHEDULE_END_DATE,T1.NEXT_SCHEDULE_DATE) "
					+"   And T1.RSS_Status = 0 And T1.Burst_Flag = 'N' "
					+"   And (Exists "
					+"        (Select 1 From "+Constants.TABLE_PREFIX+"Prd_report_Master RS Where RS.Report_Id = T1.Report_Id And RS.Status = 0) "
					+"     OR Exists "
					+"        (Select 1 From "+Constants.TABLE_PREFIX+"VC_REPORT_DEFS_SELFBI VC Where VC.Catalog_Id= T1.Catalog_Id and VC.Report_Id = T1.Report_Id And VC.VRD_STATUS = 0) ) "
					+"   And Exists "
					+"       (Select 1 From "+Constants.TABLE_PREFIX+"Vision_Users VU Where VU.Vision_Id = T1.Vision_Id And VU.User_Status = 0) "
					+"   And Exists "
					+"       (Select 1 From "+Constants.TABLE_PREFIX+"PRD_RS_Schedule_Email RSE "
					+"        Where RSE.Report_Id = T1.Report_Id "
					+"          And RSE.Vision_Id = T1.Vision_Id "
					+"          And RSE.RS_Schedule_Seq = T1.RS_Schedule_Seq "
					+"          And RSE.Email_Type = 'TO' "
					+"          And RSE.RS_Email_Status = 0) "
					+"   And Not Exists "
					+"       (Select 1  "
					+"        From "+Constants.TABLE_PREFIX+"PRD_RS_Schedule_Control RSC "
					+"        Where ISNULL(RSC.Report_Id,T1.Report_Id) = T1.Report_Id "
					+"          And ISNULL(RSC.Vision_Id,T1.Vision_Id) = T1.Vision_Id "
					+"          And ISNULL(RSC.Schedule_Group,T1.Schedule_Group) = T1.Schedule_Group "
					+"          And ISNULL(RSC.Schedule_SubGroup,T1.Schedule_SubGroup) = T1.Schedule_SubGroup "
					+"          And (RSC.RSS_Status != 0  "
					+"            OR NOT(GETDATE() between ISNULL(RSC.Effective_Start_date,GETDATE()) And ISNULL(RSC.Effective_End_Date,GETDATE())))  "
					+"          And (RSC.Report_Id is not null "
					+"            OR RSC.Vision_Id is not null "
					+"            OR RSC.Schedule_Group is not null "
					+"            OR RSC.Schedule_SubGroup is not null))");
		}else {
			strQuery_SB.append(" ***Invalid Database Type*** ");
		}
		
		StudioReportSchedulerApplication.logWriter(strQuery_SB.toString(),logFilePath);

		try{
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
					ResultSetMetaData metaData = rs.getMetaData();
					Boolean recPresent = false;
					int recCount = 0;
					while(rs.next()){
						RSScheduleTabColVb rSScheduleTabColVb = new RSScheduleTabColVb();
						
						if(rs.getString("REPORT_ID") != null)                  rSScheduleTabColVb.setREPORT_ID                      (rs.getString("REPORT_ID"));
					       													   rSScheduleTabColVb.setVISION_ID                	    (rs.getInt("VISION_ID"));
						if(rs.getString("RS_SCHEDULE_SEQ") != null)            rSScheduleTabColVb.setRS_SCHEDULE_SEQ                (rs.getString("RS_SCHEDULE_SEQ"));
						if(rs.getString("NEXT_SCHEDULE_DATE") != null)         rSScheduleTabColVb.setNEXT_SCHEDULE_DATE             (rs.getString("NEXT_SCHEDULE_DATE"));
											                                   rSScheduleTabColVb.setNEXT_SCHEDULE_DATE_DATE        (CommonUtils.convertStringToDateTime(rSScheduleTabColVb.getNEXT_SCHEDULE_DATE(),"yyyy-MM-dd HH:mm:ss"));
											                                   rSScheduleTabColVb.setNEXT_SCHEDULE_DATE_MS          (CommonUtils.convertStringToDateTimeMillis(rSScheduleTabColVb.getNEXT_SCHEDULE_DATE(),"yyyy-MM-dd HH:mm:ss"));
						if(rs.getString("RSS_FREQUENCY") != null)              rSScheduleTabColVb.setRSS_FREQUENCY                  (rs.getString("RSS_FREQUENCY"));
											                                   rSScheduleTabColVb.setINTERVAL_X_DAYS                (rs.getInt("INTERVAL_X_DAYS"));
														                       rSScheduleTabColVb.setINTERVAL_X_HOURS               (rs.getInt("INTERVAL_X_HOURS"));
														                       rSScheduleTabColVb.setINTERVAL_X_MINS                (rs.getInt("INTERVAL_X_MINS"));
						if(rs.getString("DAYS_OF_WEEK") != null)               rSScheduleTabColVb.setDAYS_OF_WEEK                   (rs.getString("DAYS_OF_WEEK"));
						if(rs.getString("MONTHS_OF_YEAR") != null)             rSScheduleTabColVb.setMONTHS_OF_YEAR                 (rs.getString("MONTHS_OF_YEAR"));
											       							   rSScheduleTabColVb.setDAY_OF_THE_MONTH               (rs.getInt("DAY_OF_THE_MONTH"));
						if(rs.getString("RSS_NEXT_SCH_TYPE") != null)          rSScheduleTabColVb.setRSS_NEXT_SCH_TYPE              (rs.getString("RSS_NEXT_SCH_TYPE"));
						if(rs.getString("BURST_FLAG") != null)                 rSScheduleTabColVb.setBURST_FLAG                     (rs.getString("BURST_FLAG"));
						if(rs.getString("BURST_ID") != null)                   rSScheduleTabColVb.setBURST_ID                       (rs.getString("BURST_ID"));
						if(rs.getString("BURST_SCRIPTS_TYPE") != null)         rSScheduleTabColVb.setBURST_SCRIPTS_TYPE             (rs.getString("BURST_SCRIPTS_TYPE"));
						if(rs.getString("BURST_SCRIPTS") != null)              rSScheduleTabColVb.setBURST_SCRIPTS                  (rs.getString("BURST_SCRIPTS"));
						if(rs.getString("PROMPT_VALUE_1") != null)             rSScheduleTabColVb.setPromptValue1                   (rs.getString("PROMPT_VALUE_1"));
						if(rs.getString("PROMPT_VALUE_2") != null)             rSScheduleTabColVb.setPromptValue2                   (rs.getString("PROMPT_VALUE_2"));
						if(rs.getString("PROMPT_VALUE_3") != null)             rSScheduleTabColVb.setPromptValue3                   (rs.getString("PROMPT_VALUE_3"));
						if(rs.getString("PROMPT_VALUE_4") != null)             rSScheduleTabColVb.setPromptValue4                   (rs.getString("PROMPT_VALUE_4"));
						if(rs.getString("PROMPT_VALUE_5") != null)             rSScheduleTabColVb.setPromptValue5                   (rs.getString("PROMPT_VALUE_5"));
						if(rs.getString("PROMPT_VALUE_6") != null)             rSScheduleTabColVb.setPromptValue6                   (rs.getString("PROMPT_VALUE_6"));
						if(rs.getString("PROMPT_VALUE_7") != null)             rSScheduleTabColVb.setPromptValue7                   (rs.getString("PROMPT_VALUE_7"));
						if(rs.getString("PROMPT_VALUE_8") != null)             rSScheduleTabColVb.setPromptValue8                   (rs.getString("PROMPT_VALUE_8"));
						if(rs.getString("PROMPT_VALUE_9") != null)             rSScheduleTabColVb.setPromptValue9                   (rs.getString("PROMPT_VALUE_9"));
						if(rs.getString("PROMPT_VALUE_10") != null)            rSScheduleTabColVb.setPromptValue10                  (rs.getString("PROMPT_VALUE_10"));
											       
						if(rs.getString("prompt_value_1_desc") != null)        rSScheduleTabColVb.setPromptValueDesc1                 (rs.getString("prompt_value_1_desc"));
						if(rs.getString("prompt_value_2_desc") != null)        rSScheduleTabColVb.setPromptValueDesc2                 (rs.getString("prompt_value_2_desc"));
						if(rs.getString("prompt_value_3_desc") != null)        rSScheduleTabColVb.setPromptValueDesc3                 (rs.getString("prompt_value_3_desc"));
						if(rs.getString("prompt_value_4_desc") != null)        rSScheduleTabColVb.setPromptValueDesc4                 (rs.getString("prompt_value_4_desc"));
						if(rs.getString("prompt_value_5_desc") != null)        rSScheduleTabColVb.setPromptValueDesc5                 (rs.getString("prompt_value_5_desc"));
						if(rs.getString("prompt_value_6_desc") != null)        rSScheduleTabColVb.setPromptValueDesc6                 (rs.getString("prompt_value_6_desc"));
						if(rs.getString("prompt_value_7_desc") != null)        rSScheduleTabColVb.setPromptValueDesc7                 (rs.getString("prompt_value_7_desc"));
						if(rs.getString("prompt_value_8_desc") != null)        rSScheduleTabColVb.setPromptValueDesc8                 (rs.getString("prompt_value_8_desc"));
						if(rs.getString("prompt_value_9_desc") != null)        rSScheduleTabColVb.setPromptValueDesc9                 (rs.getString("prompt_value_9_desc"));
						if(rs.getString("prompt_value_10_desc") != null)       rSScheduleTabColVb.setPromptValueDesc10                 (rs.getString("prompt_value_10_desc"));
											       
						if(rs.getString("prompt_label_1") != null)             rSScheduleTabColVb.setPromptValueLabel1                 (rs.getString("prompt_label_1"));
						if(rs.getString("prompt_label_2") != null)             rSScheduleTabColVb.setPromptValueLabel2                 (rs.getString("prompt_label_2"));
						if(rs.getString("prompt_label_3") != null)             rSScheduleTabColVb.setPromptValueLabel3                 (rs.getString("prompt_label_3"));
						if(rs.getString("prompt_label_4") != null)             rSScheduleTabColVb.setPromptValueLabel4                 (rs.getString("prompt_label_4"));
						if(rs.getString("prompt_label_5") != null)             rSScheduleTabColVb.setPromptValueLabel5                 (rs.getString("prompt_label_5"));
						if(rs.getString("prompt_label_6") != null)             rSScheduleTabColVb.setPromptValueLabel6                 (rs.getString("prompt_label_6"));
						if(rs.getString("prompt_label_7") != null)             rSScheduleTabColVb.setPromptValueLabel7                 (rs.getString("prompt_label_7"));
						if(rs.getString("prompt_label_8") != null)             rSScheduleTabColVb.setPromptValueLabel8                 (rs.getString("prompt_label_8"));
						if(rs.getString("prompt_label_9") != null)             rSScheduleTabColVb.setPromptValueLabel9                 (rs.getString("prompt_label_9"));
						if(rs.getString("prompt_label_10") != null)            rSScheduleTabColVb.setPromptValueLabel10                 (rs.getString("prompt_label_10"));
											       
																		       rSScheduleTabColVb.setINTERVAL_X_STARTTIME           (rs.getInt("INTERVAL_X_STARTTIME"));
																		       rSScheduleTabColVb.setINTERVAL_X_ENDTIME             (rs.getInt("INTERVAL_X_ENDTIME"));
						if(rs.getString("INTERVAL_X_STARTTIME_TXT") != null)   rSScheduleTabColVb.setINTERVAL_X_STARTTIME_TXT       (rs.getString("INTERVAL_X_STARTTIME_TXT"));
						if(rs.getString("INTERVAL_X_ENDTIME_TXT") != null)     rSScheduleTabColVb.setINTERVAL_X_ENDTIME_TXT         (rs.getString("INTERVAL_X_ENDTIME_TXT"));
						
						
						LL_RS_SCHEDULE_TABLE_DATA.add(rSScheduleTabColVb);
					}
					return LL_RS_SCHEDULE_TABLE_DATA;
				}
			};
			return (List<RSScheduleTabColVb>)jdbcTemplate.query(strQuery_SB.toString(),mapper);
		}catch(Exception e){
			e.printStackTrace();
			throw new VisionGeneralException(Constants.ERROR_OPERATION,
					"Error while getting RS Schedule Table ! Query[\n"+strQuery_SB.toString()+"\n] Error Message ["+e.getMessage()+"]");
		}
	}

	public  int  executeStatment(String query, boolean printCountFlag, String logType,String logFilePath){
		int recordsCount =  0;
	
		try {
			recordsCount = jdbcTemplate.update(query);
			
			if(printCountFlag)
			{
				StudioReportSchedulerApplication.logWriter("["+recordsCount+"] records processed for ("+logType+") operation.",logFilePath);
			}
		} catch (Exception e) {
			//e.printStackTrace();
			throw new VisionGeneralException(Constants.ERROR_OPERATION,
					"Errored Query:["+query+"], Error Message ["+e.getMessage()+"]");
		}
		return recordsCount;
	}
	public String convertDateTimeToString(LocalDateTime inputDate, String inputPattern) {
		
		return (inputDate.format(DateTimeFormatter.ofPattern(inputPattern)));
	}
	public static String printProgramDuration(Date programStartDateTime) {
			
			Date programEndDateTime = new Date();
			
			long elapsedSeconds = Math.round((double)(programEndDateTime.getTime() - programStartDateTime.getTime()) / 1000);
			
			long localHours,localMinutes,localSeconds,remSeconds;
	
			localHours    = (elapsedSeconds / (60 * 60));
			remSeconds    = elapsedSeconds - (localHours * (60 * 60));
			localMinutes  = remSeconds / 60 ;
			remSeconds    = remSeconds - (localMinutes * 60);
			localSeconds  = remSeconds;
			
			String finalElapsedString = String.format("Start Time:[%s], End Time:[%s], Difference:[%02d:%02d:%02d]"
					, Constants.DATETIME_FORMAT.format(programStartDateTime), Constants.DATETIME_FORMAT.format(programEndDateTime), localHours,localMinutes,localSeconds);
			
			return finalElapsedString;
		}
}


/* TO READ */
/* 
CSD  - Schedule Date (DD-MON-YYYY)
CSDT - Schedule Date Time (DD-MON-YYYY HH24:MI:SS)
CSW  - Schedule Date (DD-MON-YYYY)
CSWT - Schedule Date Time (DD-MON-YYYY HH24:MI:SS)
CSYM - Schedule Year Month (Mon-YYYY)
CSY  - Schedule Year (YYYY)
PSD  - Previous Schedule Date (DD-MON-YYYY)
PSDT - Previous Schedule Date Time (DD-MON-YYYY HH24:MI:SS)
PSW  - Previous Schedule Date (DD-MON-YYYY)
PSWT - Previous Schedule Date Time (DD-MON-YYYY HH24:MI:SS)
PSYM - Previous Schedule Year Month (Mon-YYYY)
PSY  - Previous Schedule Year (YYYY)
Prompt Special Cases
CBD    - Current Business Day (Max) (DD-MON-YYYY)
CBW    - Current Business Week (Max) (DD-MON-YYYY)
CBYM   - Current Business Year Month (Max) (Mon-YYYY)
CBY    - Current Business Year (Max) (YYYY)
CBDLP1 - Current Business Day (With P1 prompt LV-C-LEB) (DD-MON-YYYY)
CBDLP2 - Current Business Day (With P2 prompt LV-C-LEB) (DD-MON-YYYY)
CBDLP3 - Current Business Day (With P3 prompt LV-C-LEB) (DD-MON-YYYY)
CBDLP4 - Current Business Day (With P4 prompt LV-C-LEB) (DD-MON-YYYY)
CBDLP5 - Current Business Day (With P5 prompt LV-C-LEB) (DD-MON-YYYY)
CBWLP1 - Current Business Day (With P1 prompt LV-C-LEB) (DD-MON-YYYY)
CBWLP2 - Current Business Day (With P2 prompt LV-C-LEB) (DD-MON-YYYY)
CBWLP3 - Current Business Day (With P3 prompt LV-C-LEB) (DD-MON-YYYY)
CBWLP4 - Current Business Day (With P4 prompt LV-C-LEB) (DD-MON-YYYY)
CBWLP5 - Current Business Day (With P5 prompt LV-C-LEB) (DD-MON-YYYY)
CBYMLP1 - Current Business Day (With P1 prompt LV-C-LEB) (Mon-YYYY)
CBYMLP2 - Current Business Day (With P2 prompt LV-C-LEB) (Mon-YYYY)
CBYMLP3 - Current Business Day (With P3 prompt LV-C-LEB) (Mon-YYYY)
CBYMLP4 - Current Business Day (With P4 prompt LV-C-LEB) (Mon-YYYY)
CBYMLP5 - Current Business Day (With P5 prompt LV-C-LEB) (Mon-YYYY)
CBYLP1 - Current Business Day (With P1 prompt LV-C-LEB) (YYYY)
CBYLP2 - Current Business Day (With P2 prompt LV-C-LEB) (YYYY)
CBYLP3 - Current Business Day (With P3 prompt LV-C-LEB) (YYYY)
CBYLP4 - Current Business Day (With P4 prompt LV-C-LEB) (YYYY)
CBYLP5 - Current Business Day (With P5 prompt LV-C-LEB) (YYYY)
*/