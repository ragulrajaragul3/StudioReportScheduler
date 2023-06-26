package com.vision.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.StringJoiner;

import javax.servlet.ServletContext;

import org.json.JSONObject;
import org.json.XML;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.context.ServletContextAware;

import com.sun.rowset.CachedRowSetImpl;
import com.vision.authentication.SessionContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.ChartUtils;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.Paginationhelper;
import com.vision.util.ValidationUtil;
import com.vision.vb.ColumnHeadersVb;
import com.vision.vb.PrdQueryConfig;
import com.vision.vb.PromptTreeVb;
import com.vision.vb.RCReportFieldsVb;
import com.vision.vb.ReportFilterVb;
import com.vision.vb.ReportUserDefVb;
import com.vision.vb.ReportsVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.ScheduledReportWb;

@Component
public class ReportsDao extends AbstractDao<ReportsVb> implements ServletContextAware {
	private ServletContext servletContext;
	public void setServletContext(ServletContext arg0) {
		servletContext = arg0;
	}
	
	
	public JdbcTemplate jdbcTemplate = null;

	public ReportsDao(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	@Value("${app.productName}")
	private String productName;
	@Autowired
	CommonDao commonDao;
	@Autowired
	ChartUtils chartUtils;
	@Autowired
	CommonApiDao commonApiDao;
	
	public List<ReportsVb> getReportList(String reportGroup,String applicationId) throws DataAccessException {
		VisionUsersVb visionUsersVb = SessionContextHolder.getContext();
		String sql = "";
		try
		{
				if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
					sql = " Select T2.Report_ID,T2.REPORT_TITLE,T2.FILTER_FLAG,T2.FILTER_REF_CODE,T2.APPLY_USER_RESTRCT, "
							+ " (SELECT NVL(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE REPORT_ID = T2.REPORT_ID) NEXT_LEVEL,T2.GROUPING_FLAG, "
							+ " T2.REPORT_ORDER, T2.Report_Type_AT, T2.Report_Type, T2.Template_ID,"
							+ " (SELECT VALUE FROM VISION_VARIABLES WHERE VARIABLE = 'PRD_REPORT_MAXROW') MAX_PERPAGE,T2.SCALING_FACTOR, "
							+ "	(SELECT NVL(REPORT_ORIENTATION,'P') FROM PRD_REPORT_DETAILS PRD WHERE PRD.REPORT_ID= T2.REPORT_ID AND "
		                    + " SUBREPORT_SEQ = (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS PR WHERE PR.REPORT_ID= T2.REPORT_ID) "
		                    + ") REPORT_ORIENTATION " 
							+ " FROM PRD_REPORT_ACCESS T1,PRD_REPORT_MASTER T2 "
							+ " WHERE t1.REPORT_ID  = t2.REPORT_ID AND T1.PRODUCT_NAME = T2.APPLICATION_ID AND T1.USER_GROUP||'-'||T1.USER_PROFILE = ? " + 
							" AND T2.APPLICATION_ID = '"
							+ applicationId + "' AND T2.REPORT_GROUP = ? AND T2.STATUS = 0 AND T2.report_type != 'W'";			
				}else {
					sql = " Select T2.Report_ID,T2.REPORT_TITLE,T2.FILTER_FLAG,T2.FILTER_REF_CODE,T2.APPLY_USER_RESTRCT, "
							+ " (SELECT ISNULL(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE REPORT_ID = T2.REPORT_ID) NEXT_LEVEL,T2.GROUPING_FLAG, "
							+ " T2.REPORT_ORDER, T2.Report_Type_AT, T2.Report_Type, T2.Template_ID,"
							+ " (SELECT VALUE FROM VISION_VARIABLES WHERE VARIABLE = 'PRD_REPORT_MAXROW') MAX_PERPAGE,T2.SCALING_FACTOR, "
							+ "	(SELECT ISNULL(REPORT_ORIENTATION,'P') FROM PRD_REPORT_DETAILS PRD WHERE PRD.REPORT_ID= T2.REPORT_ID AND "
		                    + " SUBREPORT_SEQ = (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS PR WHERE PR.REPORT_ID= T2.REPORT_ID) "
		                    + ") REPORT_ORIENTATION " 
							+ " FROM PRD_REPORT_ACCESS T1,PRD_REPORT_MASTER T2 "
							+ " WHERE t1.REPORT_ID  = t2.REPORT_ID AND T1.PRODUCT_NAME = T2.APPLICATION_ID AND T1.USER_GROUP+'-'+T1.USER_PROFILE = ? " + 
							" AND T2.APPLICATION_ID = '"
							+ applicationId + "' AND T2.REPORT_GROUP = ? AND T2.STATUS = 0 AND T2.report_type != 'W'";
				}
			String orderBy = " ORDER BY REPORT_ORDER";
			sql = sql + orderBy;
			Object[] lParams = new Object[2];
			lParams[0] = visionUsersVb.getUserGrpProfile();
			lParams[1] = reportGroup;
			return jdbcTemplate.query(sql, lParams, getReportListMapper());
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while getting Report List...!!");
			return null;
		}
			
	}
	protected RowMapper getReportListMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setReportId(rs.getString("REPORT_ID"));
				vObject.setReportTitle(rs.getString("REPORT_TITLE"));
				vObject.setFilterFlag(rs.getString("FILTER_FLAG"));
				vObject.setFilterRefCode(rs.getString("FILTER_REF_CODE"));
				vObject.setApplyUserRestrct(rs.getString("APPLY_USER_RESTRCT"));
				vObject.setCurrentLevel(rs.getString("NEXT_LEVEL").replaceAll(".0", ""));
				vObject.setNextLevel(rs.getString("NEXT_LEVEL").replaceAll(".0", ""));
				vObject.setGroupingFlag(rs.getString("GROUPING_FLAG"));
				vObject.setMaxRecords(rs.getInt("MAX_PERPAGE"));
				vObject.setReportTypeAT(rs.getInt("Report_Type_AT"));
				vObject.setReportType(rs.getString("Report_Type"));
				vObject.setTemplateId(rs.getString("Template_ID"));
				vObject.setScalingFactor(rs.getString("SCALING_FACTOR"));
				vObject.setReportOrientation(rs.getString("REPORT_ORIENTATION"));
				return vObject;
			}
		};
		return mapper;
	}
	public List<ReportFilterVb> getReportFilterDetail(String filterRefCode){
		setServiceDefaults();
		List<ReportFilterVb> collTemp = null;
		try
		{			
			String query = " SELECT FILTER_REF_CODE,FILTER_XML,USER_RESTRICTION_XML,USER_RESTRICT_FLAG FROM PRD_REPORT_FILTERS WHERE STATUS = 0 AND FILTER_REF_CODE = '"+filterRefCode+"' ";
			collTemp = jdbcTemplate.query(query,getReportFilterMapper());
			return collTemp;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while getting Dashboard Detail...!!");
			return null;
		}
	}
	private RowMapper getReportFilterMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportFilterVb vObject = new ReportFilterVb();
				vObject.setFilterRefCode(rs.getString("FILTER_REF_CODE"));
				vObject.setFilterRefXml(rs.getString("FILTER_XML"));
				vObject.setUserRestrictionXml(rs.getString("USER_RESTRICTION_XML"));
				vObject.setUserRestrictFlag(rs.getString("USER_RESTRICT_FLAG"));
				return vObject;
			}
		};
		return mapper;
	}
	public List<ReportsVb> findReportCategory(String productId) throws DataAccessException {
		String sql = "SELECT DISTINCT REPORT_GROUP ALPHA_SUB_TAB,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE "+
					" ALPHA_TAB = 6016 AND ALPHA_SUB_TAB = REPORT_GROUP) ALPHA_SUBTAB_DESCRIPTION "+
					" FROM PRD_REPORT_MASTER WHERE APPLICATION_ID = ? ORDER BY ALPHA_SUB_TAB " ; 
		Object[] lParams = new Object[1];
		lParams[0] = productId;
		return  jdbcTemplate.query(sql, lParams, getCategoryMapper());
	}
	private RowMapper getCategoryMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setReportCategory(rs.getString("ALPHA_SUB_TAB"));
				vObject.setCategoryDesc(rs.getString("ALPHA_SUBTAB_DESCRIPTION"));
				return vObject;
			}
		};
		return mapper;
	}
	@SuppressWarnings("unchecked")
	public ExceptionCode extractReportData(ReportsVb vObj,Connection conExt) {
		ArrayList datalst = new ArrayList();
		ExceptionCode exceptionCode = new ExceptionCode();
		int totalRows = 0;
		Statement stmt1 = null;
		String resultFetchTable = "";
		String tmpTableOrg = String.valueOf("TMP"+System.currentTimeMillis())+vObj.getSubReportId();
		String tmpTableGrp = String.valueOf("TMPG"+System.currentTimeMillis())+vObj.getSubReportId();
		try {
			String sqlQuery = vObj.getFinalExeQuery();
			//totalRows = (int) vObj.getTotalRows();
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
				    ResultSet.CONCUR_READ_ONLY);
			
			String createTabScript = "SELECT * FROM ("+sqlQuery+") T1";
			Boolean formatTypeAvail = false;
			ResultSet rsFormatCnt = null;
			if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				totalRows = stmt1.executeUpdate("CREATE TABLE "+tmpTableOrg+" AS ("+createTabScript+")");
				
				rsFormatCnt = stmt1.executeQuery("SELECT * FROM USER_TAB_COLS WHERE "
						+ "TABLE_NAME = '"+tmpTableOrg+"' AND COLUMN_NAME = 'FORMAT_TYPE' ");
				
				while(rsFormatCnt.next()) {
					formatTypeAvail = true;
				}
			}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				totalRows = stmt1.executeUpdate("Select * into "+tmpTableOrg+" FROM ("+createTabScript+") A");
				
				rsFormatCnt = stmt1.executeQuery("SELECT * FROM INFORMATION_SCHEMA.columns WHERE "
						+ "TABLE_NAME = '"+tmpTableOrg+"' AND COLUMN_NAME = 'FORMAT_TYPE' ");
				
				while(rsFormatCnt.next()) {
					formatTypeAvail = true;
				}
			}
			rsFormatCnt.close();
			
			resultFetchTable = tmpTableOrg;
			String formatTypeCond = "";
			if(formatTypeAvail && "Y".equalsIgnoreCase(vObj.getApplyGrouping()))
				formatTypeCond = "WHERE FORMAT_TYPE NOT IN ('S','FT') ";
			else
				formatTypeCond= "";
			if ("Y".equalsIgnoreCase(vObj.getApplyGrouping())) {
				String showMeasures = "";
				if(ValidationUtil.isValid(vObj.getShowMeasures())) {
					showMeasures = ","+vObj.getShowMeasures();
				}else
					showMeasures = "";
					
				String query = "SELECT " + vObj.getShowDimensions() + showMeasures + " FROM "+tmpTableOrg+ " "+formatTypeCond+ " GROUP BY " + vObj.getShowDimensions();
				if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
					totalRows = stmt1.executeUpdate("CREATE TABLE "+tmpTableGrp+" AS ("+query+")");
				}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
					totalRows = stmt1.executeUpdate("Select * into "+tmpTableGrp+" FROM ("+query+") A");
				}
				resultFetchTable = tmpTableGrp;
			}
			new ScheduledReportWb(jdbcTemplate).logWriter("Number of Records ["+totalRows+"]",ScheduledReportWb.processId,ScheduledReportWb.uploadLogFilePath);
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Total Rows",
					String.valueOf(totalRows));
			if(totalRows == 0) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				exceptionCode.setErrorMsg("No Records Found!!");
				return exceptionCode;
			}
			
			Paginationhelper<ReportsVb> paginationhelper = new Paginationhelper<ReportsVb>();
			/*Report Suite having the functionality of Pagination for every 5000 rows
			(Rows per page in parameterized in Vision_Variable PRD_REPORT_MAXROW/PRD_REPORT_MAXFETCH */
			if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				sqlQuery = paginationhelper.reportFetchPage(ValidationUtil.convertQuery(resultFetchTable, vObj.getSortField()),vObj.getCurrentPage(), vObj.getMaxRecords(),totalRows);
			}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				String sqlTempTable = "SELECT * FROM "+resultFetchTable+"";
				sqlQuery = paginationhelper.reportFetchPage(ValidationUtil.convertQuery(sqlTempTable, vObj.getSortField()),vObj.getCurrentPage(), vObj.getMaxRecords(),totalRows);
			}
			new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Pagination Query ",
					"Pagination Query ");
			ResultSet rsData = stmt1.executeQuery(sqlQuery);// Final Result Data Query Execution
			
			ResultSetMetaData metaData = rsData.getMetaData();
			int colCount = metaData.getColumnCount();
			HashMap<String,String> columns = new HashMap<String,String>();
			ArrayList<String> collst =new ArrayList<>();
			Boolean columnHeaderFetch = false;
			int rowNum = 1;
			while(rsData.next()){
				columns = new HashMap<String,String>();
				for(int cn = 1;cn <= colCount;cn++) {
					String columnName = metaData.getColumnName(cn);
					if("DD_KEY_ID".equalsIgnoreCase(columnName.toUpperCase())) {
						columns.put("DDKEYID", rsData.getString(columnName));
					}else{
						columns.put(columnName.toUpperCase(), rsData.getString(columnName));
					}
					if(!columnHeaderFetch)
						collst.add(columnName.toUpperCase());
				}
				columnHeaderFetch = true;
				if(!columnHeaderFetch) {
					StringJoiner missingColumns = new StringJoiner(",");
					vObj.getColumnHeaderslst().forEach(colHeadersDataVb -> {
						if(!collst.contains(colHeadersDataVb.getDbColumnName())) {
							missingColumns.add(colHeadersDataVb.getDbColumnName());
						}
					});
					if(ValidationUtil.isValid(missingColumns.toString())) {
						exceptionCode.setErrorMsg(missingColumns+"these Source columns are not maintained  in the Result set");
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						break;
					}
				}
				columns.put("INDEXING",""+rowNum);//This is only for front-end scroll purpose not used anywhere in report.
				datalst.add(columns);
				if(rsData.getRow() == vObj.getMaxRecords())
					break;
				
				columnHeaderFetch = true;
				rowNum++;
			}
			rsData.close();
			//HashMap<String,String> columns = (HashMap<String,String>)jdbcTemplate.query(sqlQuery, mapper);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(datalst);
			exceptionCode.setRequest(totalRows);
		}catch(Exception e) {
			new ScheduledReportWb(jdbcTemplate).logWriter("Exception in Extract Report Data :"+e.getMessage(),ScheduledReportWb.processId,ScheduledReportWb.uploadLogFilePath);
			try {
				new ScheduleReportDao(jdbcTemplate).insertScheduleAuditTrail(ScheduledReportWb.processId, "Exception in Extract Report Data",
						e.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			if(!ValidationUtil.isValid(exceptionCode.getErrorMsg())) {
				exceptionCode.setErrorMsg(e.getMessage());
			}
			return exceptionCode;
		}finally {
			try {
				if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
					stmt1.executeUpdate("DROP TABLE "+tmpTableOrg+" PURGE ");
					if("Y".equalsIgnoreCase(vObj.getApplyGrouping()))
						stmt1.executeUpdate("DROP TABLE "+tmpTableGrp+" PURGE ");
				}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
					stmt1.executeUpdate("DROP TABLE "+tmpTableOrg);
					if("Y".equalsIgnoreCase(vObj.getApplyGrouping()))
						stmt1.executeUpdate("DROP TABLE "+tmpTableGrp);
				}
				
				stmt1.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return exceptionCode;
	}
	public List<ReportsVb> getSubReportDetail(ReportsVb dObj){
		List<ReportsVb> collTemp = null;
		try
		{			
			/*String query = "SELECT REPORT_ID,SUBREPORT_SEQ,SUB_REPORT_ID,DATA_REF_ID,COUNT_FETCH_FLAG,DD_FLAG, "+
						   "REPORT_ORIENTATION,PDF_GROUP_COLUMNS,PARENT_SUBREPORT_ID,"+new CommonDao(jdbcTemplate).getDbFunction(""+new CommonDao(jdbcTemplate).getDbFunction("NVL")+"")+"(PDF_WIDTH_GRPERCENT,0) PDF_WIDTH_GRPERCENT FROM PRD_REPORT_DETAILS WHERE SUBREPORT_SEQ = ? AND REPORT_ID = ? "+
						   "AND STATUS = 0";*/
			
			String query = "SELECT REPORT_ID,SUBREPORT_SEQ,SUB_REPORT_ID,DATA_REF_ID,COUNT_FETCH_FLAG,DD_FLAG, "+
					   "REPORT_ORIENTATION,PDF_GROUP_COLUMNS,PARENT_SUBREPORT_ID,SORTING_ENABLE,SEARCH_ENABLE,"+new CommonDao(jdbcTemplate).getDbFunction("NVL")+"(PDF_WIDTH_GRPERCENT,0) PDF_WIDTH_GRPERCENT,"
					   + "(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE "+
					   " ALPHA_TAB = 7005 AND ALPHA_SUB_TAB = T1.CHART_TYPE) CHART_TYPE,OBJECT_TYPE,REPORT_HELP,REPORT_DESIGN_XML,CSV_DELIMITER,SCALING_FACTOR,FREEZE_COLUMN  "
					   + " FROM PRD_REPORT_DETAILS T1 WHERE SUBREPORT_SEQ = ? AND REPORT_ID = ? "+
					   "AND STATUS = 0";
			
			Object[] lParams = new Object[2];
			lParams[0] = dObj.getNextLevel();
			lParams[1] = dObj.getReportId();
			collTemp = jdbcTemplate.query(query,lParams,getSubReportsMapper());
			return collTemp;
		}catch(Exception ex){
			new ScheduledReportWb(jdbcTemplate).logWriter("Exception while getting sub Report Details : " +ex.getMessage(),ScheduledReportWb.processId,ScheduledReportWb.uploadLogFilePath);
			ex.printStackTrace();
			logger.error("Exception while getting sub Report Details...!!"+ex.getMessage());
			return null;
		}
	}
	private RowMapper getSubReportsMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb reportsVb = new ReportsVb();
				reportsVb.setReportId(rs.getString("REPORT_ID"));
				reportsVb.setIntReportSeq(rs.getInt("SUBREPORT_SEQ"));
				reportsVb.setCurrentLevel(rs.getString("SUBREPORT_SEQ").replaceAll(".0", ""));
				reportsVb.setSubReportId(rs.getString("SUB_REPORT_ID"));
				reportsVb.setDataRefId(rs.getString("DATA_REF_ID"));
				reportsVb.setFetchFlag(rs.getString("COUNT_FETCH_FLAG"));
				reportsVb.setDdFlag(rs.getString("DD_FLAG"));
				reportsVb.setReportOrientation(ValidationUtil.isValid(rs.getString("REPORT_ORIENTATION"))?rs.getString("REPORT_ORIENTATION"):"P");
				reportsVb.setPdfGroupColumn(rs.getString("PDF_GROUP_COLUMNS"));
				reportsVb.setParentSubReportID(rs.getString("PARENT_SUBREPORT_ID"));
				reportsVb.setSortFlag(rs.getString("SORTING_ENABLE"));
				reportsVb.setSearchFlag(rs.getString("SEARCH_ENABLE"));
				reportsVb.setPdfGrwthPercent(rs.getInt("PDF_WIDTH_GRPERCENT"));
				reportsVb.setChartType(rs.getString("CHART_TYPE"));
				reportsVb.setObjectType(rs.getString("OBJECT_TYPE"));
				reportsVb.setReportInfo(rs.getString("REPORT_HELP"));
				reportsVb.setScalingFactor(rs.getString("SCALING_FACTOR"));
				if(ValidationUtil.isValid(rs.getString("REPORT_DESIGN_XML"))) {
					JSONObject xmlJSONObj = XML.toJSONObject(rs.getString("REPORT_DESIGN_XML"));
					int PRETTY_PRINT_INDENT_FACTOR = 4;
					String resultData = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR).replaceAll("[\\n\\t ]", "");
					reportsVb.setReportDesignXml(resultData);
				}
				reportsVb.setCsvDelimiter(rs.getString("CSV_DELIMITER"));
				reportsVb.setFreezeColumn(rs.getInt("FREEZE_COLUMN"));
				return reportsVb;
			}
		};
		return mapper;
	}
	public String getNextLevel(ReportsVb dObj){
		List<ReportsVb> collTemp = null;
		String nextLevel = "";
		String query = "";
		try
		{
			query = " SELECT "+new CommonDao(jdbcTemplate).getDbFunction("NVL")+"(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE SUBREPORT_SEQ > ? "
					+ " AND REPORT_ID = ?  ";
			Object[] lParams = new Object[2];
			lParams[0] = dObj.getCurrentLevel();
			lParams[1] = dObj.getReportId();
			nextLevel = jdbcTemplate.queryForObject(query, lParams, String.class);
			return nextLevel;
		}catch(Exception ex){
			new ScheduledReportWb(jdbcTemplate).logWriter("Exception while getting Next Level Sequence : " +ex.getMessage(),ScheduledReportWb.processId,ScheduledReportWb.uploadLogFilePath);
			ex.printStackTrace();
			logger.error("Exception while getting Next Level Sequence...!!");
			return "0";
		}
	}
	public List<ColumnHeadersVb> getReportColumns(ReportsVb dObj){
		List<ColumnHeadersVb> collTemp = null;
		try
		{			
			String query = " Select REPORT_ID,SUB_REPORT_ID,COLUMN_XML from PRD_REPORT_COLUMN " + 
					" WHERE REPORT_ID = ? AND SUB_REPORT_ID = ?";
			
			Object[] lParams = new Object[2];
			lParams[0] = dObj.getReportId();
			lParams[1] = dObj.getSubReportId();
			collTemp = jdbcTemplate.query(query,lParams,getReportColumnHeadersMapper());
			return collTemp;
		}catch(Exception ex){
			new ScheduledReportWb(jdbcTemplate).logWriter("Exception in getReportColumns : " +ex.getMessage(),ScheduledReportWb.processId,ScheduledReportWb.uploadLogFilePath);
			ex.printStackTrace();
			logger.error("Exception while getting Report Columns for the sub Report Id["+dObj.getSubReportId()+"]");
			return null;
		}
	}
	private RowMapper getReportColumnHeadersMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ColumnHeadersVb columnHeadersVb = new ColumnHeadersVb();
				columnHeadersVb.setReportId(rs.getString("REPORT_ID"));
				columnHeadersVb.setSubReportId(rs.getString ("SUB_REPORT_ID"));
				columnHeadersVb.setColumnXml(rs.getString ("COLUMN_XML"));
				return columnHeadersVb;
			}
		};
		return mapper;
	}
	public List<PrdQueryConfig> getSqlQuery(String dataRefId){
		List<PrdQueryConfig> collTemp = null;
		try
		{			
			String query = "SELECT DATA_REF_ID, QUERY, DATA_REF_TYPE,DB_CONNECTION_NAME from PRD_QUERY_CONFIG "+
					       "WHERE DATA_REF_ID = ? AND STATUS = 0 ";
			
			Object[] lParams = new Object[1];
			lParams[0] = dataRefId;
			collTemp = jdbcTemplate.query(query,lParams,getSqlQueryMapper());
			return collTemp;
		}catch(Exception ex){
			new ScheduledReportWb(jdbcTemplate).logWriter("Exception in the Query for the Data Ref Id: " +ex.getMessage(),ScheduledReportWb.processId,ScheduledReportWb.uploadLogFilePath);
			ex.printStackTrace();
			logger.error("Exception while getting the Query for the Data Ref Id["+dataRefId+"]");
			return null;
		}
	}
	private RowMapper getSqlQueryMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				PrdQueryConfig prdQueryConfig = new PrdQueryConfig();
				prdQueryConfig.setDataRefId(rs.getString("DATA_REF_ID"));
				prdQueryConfig.setQueryProc(rs.getString ("QUERY"));
				prdQueryConfig.setDataRefType(rs.getString("DATA_REF_TYPE"));
				prdQueryConfig.setDbConnectionName(rs.getString("DB_CONNECTION_NAME"));
				return prdQueryConfig;
			}
		};
		return mapper;
	}
	public int insertReportsAudit( ReportsVb dObj,String auditMsg){
		VisionUsersVb visionUsersVb =  SessionContextHolder.getContext();
		String referenceId = getReferenceNumforAudit();
		String promptXml = convertToXml(dObj);
		if(!ValidationUtil.isValid(promptXml)) 
			promptXml = "";
		int retVal = 0;
		String query = "";
		try {
			query = "Insert Into PRD_AUDIT_REPORTS (REFERENCE_NO,REPORT_ID, PROMPT_XML," + 
					"RUN_DATE ,AUDIT_MESSAGE,USER_LOGIN_ID,VISION_ID,IP_ADDRESS,	MAC_ADDRESS,HOST_NAME ) "+
					"Values (?,?,?,"+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+",?,?,?,?,?,?)";
			
			Object[] args = {referenceId,dObj.getReportId(),promptXml,auditMsg,visionUsersVb.getUserLoginId(),visionUsersVb.getVisionId() ,visionUsersVb.getIpAddress(), 
					visionUsersVb.getMacAddress(),visionUsersVb.getRemoteHostName()};  
			retVal= jdbcTemplate.update(query,args);
			return retVal;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while inserting into the PRD_AUDIT_REPORTS...!!");
			return 0;
		}
	}
	public static String getReferenceNumforAudit(){
		try
		{
			String strDay   = "";
			String strMonth = "";
			String strYear  = "";
			String strHour  = "";
			String strMin   = "";
			String strSec   = "";
			String strMSec  = "";
			String strAMPM  = "";

			Calendar c = Calendar.getInstance();
			strMonth = c.get(Calendar.MONTH) + 1 + "";
			strDay   = c.get(Calendar.DATE) + "";
			strYear  = c.get(Calendar.YEAR) + "";
			strAMPM  = c.get(Calendar.AM_PM) + "";
			strMin  = c.get(Calendar.MINUTE) + "";
			strSec  = c.get(Calendar.SECOND) + "";
			strMSec  = c.get(Calendar.MILLISECOND) + "";

			if (strAMPM.equals("1"))
				strHour  = (c.get(Calendar.HOUR) + 12 )+ "";
			else
				strHour  = c.get(Calendar.HOUR) + "";

			if (strHour.length() == 1)
				strHour = "0" + strHour;

			if (strMin.length() == 1)
				strMin = "0" + strMin;

			if (strSec.length() == 1)
				strSec = "0" + strSec;

			if (strMSec.length() == 1)
				strMSec = "00" + strMSec;
			else if (strMSec.length() == 2)
				strMSec = "0" + strMSec;

			if (strMonth.length() == 1)
				strMonth = "0" + strMonth;

			if (strDay.length() == 1)
				strDay = "0" + strDay;

			return strYear + strMonth + strDay + strHour + strMin + strSec  + strMSec;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return "";
		}
	}
	public ExceptionCode callProcforReportData( ReportsVb vObject, String procedure) {
		setServiceDefaults();
		strCurrentOperation = "Query";
		strErrorDesc = "";
		Connection conExt = null;
		CallableStatement cs =  null;
		ExceptionCode exceptionCode = new ExceptionCode();
		PromptTreeVb promptTreeVb = new PromptTreeVb();
		try{
			if(!ValidationUtil.isValid(vObject.getDbConnection()) || "DEFAULT".equalsIgnoreCase(vObject.getDbConnection())) {
				//conExt = getConnection();
				conExt = jdbcTemplate.getDataSource().getConnection();
			}else {
				String dbScript = new CommonDao(jdbcTemplate).getScriptValue(vObject.getDbConnection());
				ExceptionCode exceptionCodeCon = CommonUtils.getConnection(dbScript);
				if(exceptionCodeCon != null && exceptionCodeCon.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					conExt = (Connection)exceptionCodeCon.getResponse();
				}else {
					exceptionCode = exceptionCodeCon;
					return exceptionCode;
				}
			}
			if(!ValidationUtil.isValid(procedure)){
				strErrorDesc = "Invalid Procedure in PRD_QUERY_CONFIG table for report Id["+vObject.getReportId()+"].";
				throw buildRuntimeCustomException(getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION));
			}
			//System.out.println("Date:"+new Date()+"ReportID["+vObject.getReportId()+"]SubReport["+vObject.getSubReportId()+"]Visionid["+intCurrentUserId+"]Session["+sessionId+"]");
			cs = conExt.prepareCall("{call "+procedure+"}");
	        cs.registerOutParameter(1, java.sql.Types.VARCHAR); //Status 
	        cs.registerOutParameter(2, java.sql.Types.VARCHAR); //Error Message
	        cs.registerOutParameter(3, java.sql.Types.VARCHAR); //Table Name
	        cs.registerOutParameter(4, java.sql.Types.VARCHAR); //Column Headers
	        cs.execute();
	        exceptionCode.setErrorCode(cs.getInt(1));
	        exceptionCode.setErrorMsg(cs.getString(2));
        	promptTreeVb.setTableName(cs.getString(3));
        	promptTreeVb.setColumnHeaderTable(cs.getString(4));
        	promptTreeVb.setSessionId(vObject.getDateCreation());
        	promptTreeVb.setReportId(vObject.getSubReportId());
        	exceptionCode.setResponse(promptTreeVb);
	        cs.close();
		}catch(Exception ex){
			new ScheduledReportWb(jdbcTemplate).logWriter("Exception in callProcforReportData : " +ex.getMessage(),ScheduledReportWb.processId,ScheduledReportWb.uploadLogFilePath);
			ex.printStackTrace();
			exceptionCode.setErrorMsg(ex.getMessage());
			exceptionCode.setErrorCode(Constants.PASSIVATE);
			return exceptionCode;
		}finally{
			JdbcUtils.closeStatement(cs);
			try {
				conExt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			//DataSourceUtils.releaseConnection(con, jdbcTemplate.getDataSource());
		}
		return exceptionCode;
	}
	public void deleteTempTable(String tableName){
		try
		{			
			String query = " DROP TABLE  "+tableName+" PURGE ";
			jdbcTemplate.update(query);
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while dropping the temp table...!!");
		}
	}
	private String convertToXml(ReportsVb vObject) {
		String promptsXml = "";
		promptsXml = "<Prompts>";
		 String promptvalue1 = "<Prompt1>"+vObject.getPromptValue1()+"</Prompt1>"; 
		promptsXml = promptsXml+promptvalue1;
		 String promptvalue2 = "<Prompt2>"+vObject.getPromptValue2()+"</Prompt2>"; 
		promptsXml = promptsXml+promptvalue2;
		String promptvalue3 = "<Prompt3>"+vObject.getPromptValue3()+"</Prompt3>"; 
		promptsXml = promptsXml+promptvalue3;
		String promptvalue4 = "<Prompt4>"+vObject.getPromptValue4()+"</Prompt4>"; 
		promptsXml = promptsXml+promptvalue4;
		String promptvalue5 = "<Prompt5>"+vObject.getPromptValue5()+"</Prompt5>"; 
		promptsXml = promptsXml+promptvalue5;
		String promptvalue6 = "<Prompt6>"+vObject.getPromptValue6()+"</Prompt6>"; 
		promptsXml = promptsXml+promptvalue6;
		String promptvalue7 = "<Prompt7>"+vObject.getPromptValue7()+"</Prompt7>"; 
		promptsXml = promptsXml+promptvalue7;
		String promptvalue8 = "<Prompt8>"+vObject.getPromptValue8()+"</Prompt8>"; 
		promptsXml = promptsXml+promptvalue8;
		String promptvalue9 = "<Prompt9>"+vObject.getPromptValue9()+"</Prompt9>"; 
		promptsXml = promptsXml+promptvalue9;
		String promptvalue10 = "<Prompt10>"+vObject.getPromptValue10()+"</Prompt10>"; 
		promptsXml = promptsXml+promptvalue10;
		promptsXml = promptsXml + "</Prompts>";
		return promptsXml;
	}
	public List<ReportsVb> getReportsDetail(ReportsVb dObj){
		List<ReportsVb> collTemp = null;
		try
		{			
			String query =
					" SELECT * FROM (SELECT REPORT_ID,SUBREPORT_SEQ,SUB_REPORT_ID,DATA_REF_ID,COUNT_FETCH_FLAG,DD_FLAG,"
						+ " REPORT_ORIENTATION, PARENT_SUBREPORT_ID, OBJECT_TYPE_AT, OBJECT_TYPE,"
						+ " (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE "
						+ " ALPHA_TAB = 7005 AND ALPHA_SUB_TAB = CHART_TYPE) CHART_TYPE, CHART_TYPE_AT, "
						+ " CASE WHEN SUB_REPORT_ID != PARENT_SUBREPORT_ID THEN 'Y'" 
			           // + " (SELECT DD_FLAG FROM PRD_REPORT_DETAILS S2 WHERE T1.PARENT_SUBREPORT_ID = S2.SUB_REPORT_ID AND T1.PARENT_SUBREPORT_ID = S2.REPORT_ID) " 
			            + " ELSE 'N' END ISDRILLDOWN "
						+ " FROM PRD_REPORT_DETAILS T1 WHERE REPORT_ID = ? ) A1 WHERE A1.ISDRILLDOWN = 'N' "
						+ " ORDER BY A1.SUB_REPORT_ID";
			Object[] lParams = new Object[1];
			lParams[0] = dObj.getReportId();
			//lParams[1] = dObj.getReportId();
			collTemp = jdbcTemplate.query(query,lParams,getInteractiveReportsMapper());
			return collTemp;
		}catch(Exception ex){ 
			ex.printStackTrace();
			logger.error("Exception while getting interactive report detail...!!");
			return null;
		}
	}
	private RowMapper getInteractiveReportsMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb reportsVb = new ReportsVb();
				reportsVb.setReportId(rs.getString("REPORT_ID"));
				reportsVb.setIntReportSeq(rs.getInt("SUBREPORT_SEQ"));
				reportsVb.setSubReportId(rs.getString("SUB_REPORT_ID"));
				reportsVb.setDataRefId(rs.getString("DATA_REF_ID"));
				reportsVb.setFetchFlag(rs.getString("COUNT_FETCH_FLAG"));
				reportsVb.setDdFlag(rs.getString("DD_FLAG"));
				reportsVb.setReportOrientation(rs.getString("REPORT_ORIENTATION"));
				reportsVb.setParentSubReportID(rs.getString("PARENT_SUBREPORT_ID"));
				reportsVb.setObjectTypeAT(rs.getInt("OBJECT_TYPE_AT"));
				reportsVb.setObjectType(rs.getString("OBJECT_TYPE"));
				reportsVb.setChartType(rs.getString("CHART_TYPE"));
				reportsVb.setChartTypeAT(rs.getInt("CHART_TYPE_AT"));
				reportsVb.setCurrentLevel(rs.getString("SUBREPORT_SEQ").replaceAll(".0", ""));
				reportsVb.setNextLevel(rs.getString("SUBREPORT_SEQ").replaceAll(".0", ""));
				return reportsVb;
			}
		};
		return mapper;
	}
	public ExceptionCode getChartReportData(ReportsVb vObject,  String orginalQuery,Connection conExt) throws SQLException {
		ExceptionCode exceptionCode = new ExceptionCode();
		Statement stmt = null;
		List collTemp = new ArrayList();
		ResultSet rs = null;
		try {
			stmt = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(orginalQuery);
			CachedRowSetImpl rsChild = new CachedRowSetImpl();
			rsChild = new CachedRowSetImpl();
			rsChild.populate(rs);
			exceptionCode = chartUtils.getChartXML(vObject.getChartType(), vObject.getColHeaderXml(),rs ,rsChild,vObject.getWidgetTheme());
			if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
			String chartResultXml = exceptionCode.getResponse().toString();
			if(ValidationUtil.isValid(chartResultXml)) {
				chartResultXml = replaceTagValues(chartResultXml);
			}
			exceptionCode.setResponse(chartResultXml);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (Exception e) {
			exceptionCode.setErrorMsg(e.getCause().getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			if(!ValidationUtil.isValid(exceptionCode.getErrorMsg())) {
				exceptionCode.setErrorMsg(e.getMessage());
			}
			e.printStackTrace();
			return exceptionCode;
		}
	}
	public String getIntReportNextLevel(ReportsVb dObj){
		String nextLevel = "";
		String query = "";
		try
		{	
			query = " SELECT "+new CommonDao(jdbcTemplate).getDbFunction("NVL")+"(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE SUBREPORT_SEQ > ? "
				+ " AND REPORT_ID = ?  ";
			
			Object[] lParams = new Object[2];
			lParams[0] = dObj.getCurrentLevel();
			lParams[1] = dObj.getReportId();
			//lParams[2] = dObj.getSubReportId();
			nextLevel = jdbcTemplate.queryForObject(query, lParams,String.class);
			return nextLevel;
		}catch(Exception ex){
			new ScheduledReportWb(jdbcTemplate).logWriter("Exception while getting Next Level Sequence : " +ex.getMessage(),ScheduledReportWb.processId,ScheduledReportWb.uploadLogFilePath);
			ex.printStackTrace();
			logger.error("Exception while getting Next Level Sequence...!!");
			return "0";
		}
	}
	public String getDateFormat(String promptDate,String format){
		setServiceDefaults();
		String query = "";
		if("PYM".equalsIgnoreCase(format)) {
			query = "Select TO_char(Add_Months(To_Date("+promptDate+",'RRRRMM'),-1),'RRRRMM') from Dual";
		}else if("NYM".equalsIgnoreCase(format)) {
			query = "Select TO_char(Add_Months(To_Date("+promptDate+",'RRRRMM'),+1),'RRRRMM') from Dual";
		}else if("PM".equalsIgnoreCase(format)) {
			query = "Select TO_char(Add_Months(To_Date("+promptDate+",'RRRRMM'),-1),'MM') from Dual";
		}else if("NM".equalsIgnoreCase(format)) {
			query = "Select TO_char(Add_Months(To_Date("+promptDate+",'RRRRMM'),+1),'MM') from Dual";
		}else if("CM".equalsIgnoreCase(format)) {
			query = "Select TO_char(Add_Months(To_Date("+promptDate+",'RRRRMM'),0),'MM') from Dual";
		}else if("CY".equalsIgnoreCase(format)) {
			query = "Select TO_char(Add_Months(To_Date("+promptDate+",'RRRRMM'),0),'RRRR') from Dual";
		}else if("PY".equalsIgnoreCase(format)) {
			query = "Select TO_char(Add_Months(To_Date("+promptDate+",'RRRRMM'),-12),'RRRR') from Dual";
		}
		
		try
		{
			String i = jdbcTemplate.queryForObject(query,null, String.class);
			return i;
		}catch(Exception ex){
			return "";
		}
	}
	@Override
	protected void setServiceDefaults(){
		serviceName = "ReportsDao";
		serviceDesc = "ReportsDao";;
		tableName = "PRD_REPORTS";
		childTableName = "PRD_REPORTS";
		//intCurrentUserId = SessionContextHolder.getContext().getVisionId();
		intCurrentUserId = Long.parseLong(ScheduledReportWb.currentUser);
	}
	public ExceptionCode getTreePromptData(ReportFilterVb vObject,PrdQueryConfig prdQueryConfigVb ){
		setServiceDefaults();
		ExceptionCode exceptionCode = new ExceptionCode();
		Connection conExt  = null;
		List<PromptTreeVb> tempPromptsList = new ArrayList<PromptTreeVb>();
		Statement stmt1 = null;
		ResultSet rs1 = null;
		try {
			ExceptionCode exConnection = new CommonDao(jdbcTemplate).getReqdConnection(conExt,prdQueryConfigVb.getDbConnectionName());
			if(exConnection.getErrorCode() != Constants.ERRONEOUS_OPERATION && exConnection.getResponse() != null) {
				conExt = (Connection)exConnection.getResponse();
			}else {
				exceptionCode.setErrorCode(exConnection.getErrorCode());
				exceptionCode.setErrorMsg(exConnection.getErrorMsg());
				exceptionCode.setResponse(exConnection.getResponse());
				return exceptionCode;
			}
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			String query="";
			String sessionId = String.valueOf(System.currentTimeMillis());
			CallableStatement cs = null;
			cs = conExt.prepareCall("{call " + prdQueryConfigVb.getQueryProc() + "}");
			cs.setString(1, ""+intCurrentUserId);
	        cs.setString(2, sessionId);
        	cs.setString(3, vObject.getFilterSourceId());
        	cs.registerOutParameter(4, java.sql.Types.VARCHAR);//filterString
	        cs.registerOutParameter(5, java.sql.Types.VARCHAR); //Status 
	        cs.registerOutParameter(6, java.sql.Types.VARCHAR);  //Error Message
	        cs.execute();
            PromptTreeVb promptTreeVb = new PromptTreeVb();
            promptTreeVb.setFilterString(cs.getString(4));
            promptTreeVb.setStatus(cs.getString(5));
            promptTreeVb.setErrorMessage(cs.getString(6));
 	        cs.close();
			if (promptTreeVb != null && "0".equalsIgnoreCase(promptTreeVb.getStatus())) {
				vObject.setFilterString(promptTreeVb.getFilterString());
				query = "SELECT FIELD_1, FIELD_2, FIELD_3, FIELD_4, PROMPT_ID FROM "
					+ "PROMPTS_STG WHERE VISION_ID = '"+intCurrentUserId+"' AND SESSION_ID= '"+sessionId+"' AND PROMPT_ID = '"+vObject.getFilterSourceId()+"' ";
				if (ValidationUtil.isValid(promptTreeVb.getFilterString())) {
					query = query + " " + promptTreeVb.getFilterString();
				}
				rs1 = stmt1.executeQuery(query);
				while(rs1.next()) {
					PromptTreeVb Obj =new PromptTreeVb();
					Obj.setField1(rs1.getString("FIELD_1"));
					Obj.setField2(rs1.getString("FIELD_2"));
					Obj.setField3(rs1.getString("FIELD_3"));
					Obj.setField4(rs1.getString("FIELD_4"));
					Obj.setPromptId(rs1.getString("PROMPT_ID"));
					tempPromptsList.add(Obj);
				}
				query = "DELETE FROM PROMPTS_STG WHERE VISION_ID = '"+intCurrentUserId+"' AND SESSION_ID= '"+sessionId+"' AND PROMPT_ID = '"+vObject.getFilterSourceId()+"' ";
				stmt1.executeUpdate(query);
			} 
			if(tempPromptsList==null && tempPromptsList.size() == 0 ) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				exceptionCode.setErrorMsg("No data found");
				return exceptionCode;
			}
			//return tempPromptsList;
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(tempPromptsList);
		}catch(Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}finally {
			try {
				if(stmt1 != null)
					stmt1.close();
				if(conExt != null)
					conExt.close();
				if(rs1 != null)
					rs1.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return exceptionCode;
		
	}
	private RowMapper getPromptTreeMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				PromptTreeVb promptTreeVb = new PromptTreeVb();
				promptTreeVb.setField1(rs.getString("FIELD_1"));
				promptTreeVb.setField2(rs.getString("FIELD_2"));
				promptTreeVb.setField3(rs.getString("FIELD_3"));
				promptTreeVb.setField4(rs.getString("FIELD_4"));
				promptTreeVb.setPromptId(rs.getString("PROMPT_ID"));
				return promptTreeVb;
			}
		};
		return mapper;
	}
	class TreePromptCallableStatement implements CallableStatementCreator,CallableStatementCallback  {
		private ReportFilterVb vObject = null;
		private String currentTimeAsSessionId = null;
		private String visionId = null; 
		private String procSrc = null;
		private Connection conExt =null;
		public TreePromptCallableStatement(ReportFilterVb vObject,String currentTimeAsSessionId,String visionId,String procSrc,Connection conExt){
			this.vObject = vObject;
			this.currentTimeAsSessionId = currentTimeAsSessionId;
			this.visionId = visionId;
			this.procSrc = procSrc;
			this.conExt = conExt;
		}
		public CallableStatement createCallableStatement(Connection connection) throws SQLException {
			CallableStatement cs = connection.prepareCall("{call "+procSrc+"}");
			cs.setString(1, visionId);
	        cs.setString(2, currentTimeAsSessionId);
        	cs.setString(3, vObject.getFilterSourceId());
        	cs.registerOutParameter(4, java.sql.Types.VARCHAR);//filterString
	        cs.registerOutParameter(5, java.sql.Types.VARCHAR); //Status 
	        cs.registerOutParameter(6, java.sql.Types.VARCHAR);  //Error Message
			return cs;
		}

		public Object doInCallableStatement(CallableStatement cs)	throws SQLException, DataAccessException {
            ResultSet rs = cs.executeQuery();
            PromptTreeVb promptTreeVb = new PromptTreeVb();
            promptTreeVb.setFilterString(cs.getString(4));
            promptTreeVb.setStatus(cs.getString(5));
            promptTreeVb.setErrorMessage(cs.getString(6));
 	        rs.close();
            return promptTreeVb;
		}
	}
	public List<ColumnHeadersVb> getColumnHeaderFromTable(PromptTreeVb promptTreeVb) {
		setServiceDefaults();
		int intKeyFieldsCount = 2;
		String query = new String("SELECT REPORT_ID, SESSION_ID, LABEL_ROW_NUM, LABEL_COL_NUM, CAPTION, COLUMN_WIDTH, COL_TYPE, ROW_SPAN, COL_SPAN, NUMERIC_COLUMN_NO,UPPER(DB_COLUMN) DB_COLUMN, " +
				"COLUMN_WIDTH,SUM_FLAG,DRILLDOWN_LABEL_FLAG,SCALING,DECIMAL_CNT,GROUPING_FLAG,COLOR_DIFF FROM "+promptTreeVb.getColumnHeaderTable()+" WHERE REPORT_ID = ? AND SESSION_ID=? ORDER BY LABEL_COL_NUM,LABEL_ROW_NUM");
		Object params[] = new Object[intKeyFieldsCount];
		params[0] = promptTreeVb.getReportId();
		params[1] = promptTreeVb.getSessionId();
		try{
			return jdbcTemplate.query(query, params, getColumnHeadersTableMapper());
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error(((query==null)? "query is Null":query));
			return null;
		}
	}
	private RowMapper getColumnHeadersTableMapper(){
		RowMapper mapper = new RowMapper(){
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ColumnHeadersVb columnHeadersVb = new ColumnHeadersVb();
				columnHeadersVb.setReportId(rs.getString("REPORT_ID"));
				columnHeadersVb.setSessionId(rs.getString("SESSION_ID"));
				columnHeadersVb.setLabelRowNum(rs.getInt("LABEL_ROW_NUM"));
				columnHeadersVb.setLabelColNum(rs.getInt("LABEL_COL_NUM"));
				columnHeadersVb.setCaption(rs.getString("CAPTION"));
				columnHeadersVb.setColType(rs.getString("COL_TYPE"));
				columnHeadersVb.setRowSpanNum(rs.getInt("ROW_SPAN"));
				columnHeadersVb.setRowspan(rs.getInt("ROW_SPAN"));
				columnHeadersVb.setNumericColumnNo(rs.getInt("NUMERIC_COLUMN_NO"));
				columnHeadersVb.setColSpanNum(rs.getInt("COL_SPAN"));
				columnHeadersVb.setColspan(rs.getInt("COL_SPAN"));
				columnHeadersVb.setDbColumnName(rs.getString("DB_COLUMN"));
				if(!ValidationUtil.isValid(columnHeadersVb.getDbColumnName()))
					columnHeadersVb.setDbColumnName(columnHeadersVb.getCaption().toUpperCase());	
				columnHeadersVb.setColumnWidth(rs.getString("COLUMN_WIDTH"));
				columnHeadersVb.setSumFlag(rs.getString("SUM_FLAG"));
				String drillDownLabel = rs.getString("DRILLDOWN_LABEL_FLAG");
				if (ValidationUtil.isValid(drillDownLabel) && "Y".equalsIgnoreCase(drillDownLabel)) {
					columnHeadersVb.setDrillDownLabel(true);
				} else {
					columnHeadersVb.setDrillDownLabel(false);
				}
				columnHeadersVb.setScaling(rs.getString("SCALING"));
				columnHeadersVb.setDecimalCnt(rs.getString("DECIMAL_CNT"));
				columnHeadersVb.setGroupingFlag(rs.getString("GROUPING_FLAG"));
				if(ValidationUtil.isValid(rs.getString("COLOR_DIFF")))
					columnHeadersVb.setColorDiff(rs.getString("COLOR_DIFF"));
				
				/*String groupingFlag = rs.getString("GROUPING_FLAG");
				if (ValidationUtil.isValid(groupingFlag) && "Y".equalsIgnoreCase(groupingFlag)) {
					columnHeadersVb.setGroupingFlag(true);
				} else {
					columnHeadersVb.setGroupingFlag(false);
				}*/
				return columnHeadersVb;
			}
		};
		return mapper;
	}
	public int deleteReportsStgData(PromptTreeVb vObject){
		String query = "DELETE FROM REPORTS_STG WHERE REPORT_ID = ?  And SESSION_ID = ? ";
		Object args[] = {vObject.getReportId(), vObject.getSessionId()};
		return jdbcTemplate.update(query,args);
	}
	public int deleteColumnHeadersData(PromptTreeVb vObject){
		String query = "DELETE FROM COLUMN_HEADERS_STG WHERE REPORT_ID = ?  And SESSION_ID = ? ";
		Object args[] = {vObject.getReportId(), vObject.getSessionId()};
		return jdbcTemplate.update(query,args);
	}

	public ExceptionCode getComboPromptData(ReportFilterVb vObject, PrdQueryConfig prdQueryConfigVb) {
		setServiceDefaults();
		ExceptionCode exceptionCode = new ExceptionCode();
		String query = "SELECT FIELD_1, FIELD_2, FIELD_3, FIELD_4, PROMPT_ID FROM PROMPTS_STG WHERE VISION_ID = ? AND SESSION_ID= ? AND PROMPT_ID = ? ORDER BY SORT_FIELD ";
		Connection conExt = null;
		strCurrentOperation = "Prompts";
		CallableStatement cs = null;
		Statement stmt1 = null;
		try {
			String sessionId = String.valueOf(System.currentTimeMillis());
			ExceptionCode exConnection = new CommonDao(jdbcTemplate).getReqdConnection(conExt,prdQueryConfigVb.getDbConnectionName());
			if(exConnection.getErrorCode() != Constants.ERRONEOUS_OPERATION && exConnection.getResponse() != null) {
				conExt = (Connection)exConnection.getResponse();
			}else {
				exceptionCode.setErrorCode(exConnection.getErrorCode());
				exceptionCode.setErrorMsg(exConnection.getErrorMsg());
				exceptionCode.setResponse(exConnection.getResponse());
				return exceptionCode;
			}
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			cs = conExt.prepareCall("{call " + prdQueryConfigVb.getQueryProc() + "}");
			int parameterCount = prdQueryConfigVb.getQueryProc().split("\\?").length -1;
			String filter1Val = vObject.getFilter1Val().replaceAll("'", "");
			if(parameterCount > 5) {
				cs.setString(1, String.valueOf(intCurrentUserId));
				cs.setString(2, sessionId);
				cs.setString(3, vObject.getFilterSourceId());
				cs.setString(4, filter1Val);
				cs.registerOutParameter(5, java.sql.Types.VARCHAR); // Status
				cs.registerOutParameter(6, java.sql.Types.VARCHAR); // Error Message
			}else {
				cs.setString(1, String.valueOf(intCurrentUserId));
				cs.setString(2, sessionId);
				cs.setString(3, vObject.getFilterSourceId());
				cs.registerOutParameter(4, java.sql.Types.VARCHAR); // Status
				cs.registerOutParameter(5, java.sql.Types.VARCHAR); // Error Message
			}
			cs.execute();
			PromptTreeVb promptTreeVb = new PromptTreeVb();
			if(parameterCount > 5) {
				promptTreeVb.setStatus(cs.getString(5));
				promptTreeVb.setErrorMessage(cs.getString(6));
			}else {
				promptTreeVb.setStatus(cs.getString(4));
				promptTreeVb.setErrorMessage(cs.getString(5));
			}
			cs.close();
			if (promptTreeVb != null && "0".equalsIgnoreCase(promptTreeVb.getStatus())) {
				String[] params = new String[3];
				params[0] = String.valueOf(intCurrentUserId);
				params[1] = sessionId;
				params[2] = vObject.getFilterSourceId();
				if (ValidationUtil.isValid(vObject.getFilterString())) {
					query = query + " " + vObject.getFilterString();
				}
				String resultQuery = "";
				resultQuery = query;
				for (int i = 0; i < params.length; i++) {
					resultQuery = resultQuery.replaceFirst("\\?", "'?'");
					resultQuery = resultQuery.replaceFirst("\\?", params[i]);
				}
				LinkedHashMap<String, String> comboValueMap = new LinkedHashMap<String, String>();
				exceptionCode = getReportPromptsFilterValue(prdQueryConfigVb,resultQuery);
				if(exceptionCode.getErrorCode()==Constants.SUCCESSFUL_OPERATION && exceptionCode.getResponse()!= null)
					comboValueMap = (LinkedHashMap<String, String>) exceptionCode.getResponse();
				query = query.toUpperCase();
				if (query.indexOf("ORDER BY") > 0) {
					query = query.substring(query.indexOf("FROM "), query.indexOf("ORDER BY") - 1);
				} else {
					query = query.substring(query.indexOf("FROM "), query.length());
				}
				for (int i = 0; i < params.length; i++) {
					if (query.contains("?"))
						query = query.replaceFirst("\\?","'"+ params[i]+"'");
				}
				query = "DELETE " + query;
				stmt1.executeUpdate(query);
				//int count = jdbcTemplate.update(query, params);
				
				exceptionCode.setResponse(comboValueMap);
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			} else if (promptTreeVb != null && "1".equalsIgnoreCase(promptTreeVb.getStatus())) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
			//throw new RuntimeCustomException(promptTreeVb.getErrorMessage());
		}catch (Exception ex) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ex.getMessage());
		} finally {
			JdbcUtils.closeStatement(cs);
			try {
				conExt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			//DataSourceUtils.releaseConnection(con, jdbcTemplate.getDataSource());
		}
		return exceptionCode;
	}
	public ExceptionCode getReportFilterValue(String sourceQuery){
		ExceptionCode exceptionCode =new ExceptionCode();
		LinkedHashMap<String,String> filterMapNew = new LinkedHashMap<String,String>();
		ResultSetExtractor mapper = new ResultSetExtractor() {
			public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
				ResultSetMetaData metaData = rs.getMetaData();
				LinkedHashMap<String,String> filterMap = new LinkedHashMap<String,String>();
				int colCount = metaData.getColumnCount();
				while(rs.next()){
					if(colCount == 1)
						filterMap.put("@"+rs.getString(1),rs.getString(1));
					else 
						filterMap.put("@"+rs.getString(1),rs.getString(2));
				}
				return filterMap;
			}
		};
		filterMapNew =  (LinkedHashMap<String,String>)jdbcTemplate.query(sourceQuery, mapper);
		exceptionCode.setResponse(filterMapNew);
		return exceptionCode;
	}

	public ExceptionCode getReportPromptsFilterValue(PrdQueryConfig vObject,String query) {
		ExceptionCode exceptionCode = new ExceptionCode();
		LinkedHashMap<String, String> filterMap = new LinkedHashMap<String, String>();
		Connection conExt = null;
		Statement stmt1 =null;
		try {
			ExceptionCode exConnection = new CommonDao(jdbcTemplate).getReqdConnection(conExt,vObject.getDbConnectionName());
			if(exConnection.getErrorCode() != Constants.ERRONEOUS_OPERATION && exConnection.getResponse() != null) {
				conExt = (Connection)exConnection.getResponse();
			}else {
				exceptionCode.setErrorCode(exConnection.getErrorCode());
				exceptionCode.setErrorMsg(exConnection.getErrorMsg());
				exceptionCode.setResponse(exConnection.getResponse());
				return exceptionCode;
			}
			if(ValidationUtil.isValid(query))
				vObject.setQueryProc(query);
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet rsData = stmt1.executeQuery(vObject.getQueryProc());
			ResultSetMetaData metaData = rsData.getMetaData();
			int colCount = metaData.getColumnCount();
			while (rsData.next()) {
				if (colCount == 1)
					filterMap.put("@"+rsData.getString(1), rsData.getString(1));
				else
					filterMap.put("@"+rsData.getString(1), rsData.getString(2));
			}
			if(filterMap==null && filterMap.isEmpty()) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				return exceptionCode;
			}
			exceptionCode.setResponse(filterMap);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		} finally {
			try {
				if(stmt1 != null)
					stmt1.close();
				if(conExt != null)
					conExt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return exceptionCode;
		// return (LinkedHashMap<String, String>) jdbcTemplate.query(sourceQuery,
		// mapper);
	}
	public String getReportFilterDefaultValue(String sourceQuery){
		ResultSetExtractor mapper = new ResultSetExtractor() {
			String defaultValue = "";
			public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
				ResultSetMetaData metaData = rs.getMetaData();
				while(rs.next()){
					defaultValue = rs.getString(1);
				}
				return defaultValue;
			}
		};
		return (String)jdbcTemplate.query(sourceQuery, mapper);
	}
	public List<RCReportFieldsVb> getCBKReportData(ReportsVb reportsVb, PromptTreeVb prompts,long min, long max) {
		String query = "";
		try{
			query = "SELECT TAB_ID, ROW_ID, COL_ID, CELL_DATA, COL_TYPE, CREATE_NEW,SHEET_NAME,FORMAT_TYPE,FILE_NAME FROM TEMPLATES_STG WHERE SORT_FIELD <=? AND SORT_FIELD >? " +
					"AND SESSION_ID=? ORDER BY TAB_ID,ROW_ID,COL_ID";
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
					List<RCReportFieldsVb> result = new ArrayList<RCReportFieldsVb>();
					while(rs.next()){
						RCReportFieldsVb fieldsVb = new RCReportFieldsVb();
						fieldsVb.setTabelId(rs.getString("TAB_ID"));
						fieldsVb.setRowId(rs.getString("ROW_ID"));
						fieldsVb.setColId(rs.getString("COL_ID"));
						fieldsVb.setValue1(rs.getString("CELL_DATA"));
						fieldsVb.setColType(rs.getString("COL_TYPE"));
						fieldsVb.setSheetName(rs.getString("SHEET_NAME"));
						fieldsVb.setRowStyle(rs.getString("FORMAT_TYPE"));
						fieldsVb.setExcelFileName(rs.getString("FILE_NAME"));
						//M - Mandatory create new Line. Y -- Create new Line only if does not exists N-- Do not create new Line.
						fieldsVb.setCreateNew("Y".equalsIgnoreCase(rs.getString("CREATE_NEW")) || "M".equalsIgnoreCase(rs.getString("CREATE_NEW")) ? true:false);
						fieldsVb.setCreateNewRow(rs.getString("CREATE_NEW"));
						result.add(fieldsVb);
					}
					return result;
				}
			};
			Object[] promptValues = new Object[3];
			promptValues[0] = max;
			promptValues[1] = min;
			promptValues[2] = prompts.getSessionId();
			/*int retVal =InsetAuditTrialDataForReports(reportWriterVb, prompts);
			if(retVal!=Constants.SUCCESSFUL_OPERATION){
				logger.error("Error  inserting into rs_Schedule Audit");
			}*/
			
			return (List<RCReportFieldsVb>)jdbcTemplate.query(query, promptValues, mapper);
		}catch(BadSqlGrammarException ex){
			ex.printStackTrace();
			logger.error(((query==null)? "query is Null":query));
			strErrorDesc = parseErrorMsg(new UncategorizedSQLException( "","", ex.getSQLException())); 
			throw buildRuntimeCustomException(getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION));
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error(((query==null)? "query is Null":query));
			strErrorDesc = ex.getMessage(); 
			throw buildRuntimeCustomException(getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION));
		}
	}
	public void callProcToCleanUpTables(PromptTreeVb promptTree) {
		Connection con = null;
		CallableStatement cs =  null;
		try{
			con = getConnection();
			cs = con.prepareCall("{call PR_RS_CLEANUP(?, ?, ?, ?, ?)}");
	        cs.setString(1, String.valueOf(intCurrentUserId));//Report Id
	        cs.setString(2, promptTree.getSessionId());//Group Report Id
	        cs.setString(3, promptTree.getReportId());//Group Report Id
	        cs.registerOutParameter(4, java.sql.Types.VARCHAR);//Chart type list
	        cs.registerOutParameter(5, java.sql.Types.VARCHAR); //Status 
	    	ResultSet rs = cs.executeQuery();
		    rs.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			JdbcUtils.closeStatement(cs);
			DataSourceUtils.releaseConnection(con, jdbcTemplate.getDataSource());
		}
	}
	public List<ReportsVb> findApplicationCategory() throws DataAccessException {
		VisionUsersVb visionUsersVb = SessionContextHolder.getContext();
		String applicationAccess = visionUsersVb.getApplicationAccess();
		if(!"REPORTS".equalsIgnoreCase(productName)) {
			applicationAccess = "'"+productName+"'";
		}
		String sql = " SELECT DISTINCT APPLICATION_ID ALPHA_SUB_TAB,(SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE "+
				" ALPHA_TAB = 8000 AND ALPHA_SUB_TAB = APPLICATION_ID) ALPHA_SUBTAB_DESCRIPTION "+
				" FROM PRD_REPORT_MASTER where APPLICATION_ID IN ("+applicationAccess+") ORDER BY ALPHA_SUBTAB_DESCRIPTION ";
		return  jdbcTemplate.query(sql, getAppCategoryMapper());
	}
	private RowMapper getAppCategoryMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setApplicationId(rs.getString("ALPHA_SUB_TAB"));
				vObject.setApplicationIdDesc(rs.getString("ALPHA_SUBTAB_DESCRIPTION"));
				return vObject;
			}
		};
		return mapper;
	}
	public List<ReportsVb> getWidgetList() throws DataAccessException {
		VisionUsersVb visionUsersVb = SessionContextHolder.getContext();
		String sql = "";
		try
		{
			if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				sql = " Select T2.Report_ID,T2.REPORT_TITLE,T2.FILTER_FLAG,T2.FILTER_REF_CODE,T2.APPLY_USER_RESTRCT, "
						+ " (SELECT "+new CommonDao(jdbcTemplate).getDbFunction("NVL")+"(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE REPORT_ID = T2.REPORT_ID) NEXT_LEVEL,T2.GROUPING_FLAG, "
						+ " T2.REPORT_ORDER, T2.Report_Type_AT, T2.Report_Type, T2.Template_ID,"
						+ " (SELECT VALUE FROM VISION_VARIABLES WHERE VARIABLE = 'PRD_REPORT_MAXROW') MAX_PERPAGE,T2.SCALING_FACTOR, "
						+ "	(SELECT "+new CommonDao(jdbcTemplate).getDbFunction("NVL")+"(REPORT_ORIENTATION,'P') FROM PRD_REPORT_DETAILS PRD WHERE PRD.REPORT_ID= T2.REPORT_ID AND "
	                    + " SUBREPORT_SEQ = (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS PR WHERE PR.REPORT_ID= T2.REPORT_ID) "
	                    + ") REPORT_ORIENTATION,(SELECT OBJECT_TYPE FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID  "
                        + "   AND SUBREPORT_SEQ= (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID)) OBJECT_TYPE, "
                        + " (SELECT CHART_TYPE FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID  "
                        + "    AND SUBREPORT_SEQ= (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID)) CHART_TYPE, "
                        + " WIDGET_THEME,REPORT_PERIOD, "
                        + " (SELECT SORTING_ENABLE FROM PRD_REPORT_DETAILS S1 "
                        + " WHERE S1.REPORT_ID = T2.REPORT_ID "
                        + " AND SUBREPORT_SEQ = (SELECT MIN (SUBREPORT_SEQ) "
                        + " FROM PRD_REPORT_DETAILS S1 "
                        + " WHERE S1.REPORT_ID = T2.REPORT_ID)) SORTING_ENABLE,"
                         + " (SELECT WIDGET_PAGINATION FROM PRD_REPORT_DETAILS S1 "
                        + " WHERE S1.REPORT_ID = T2.REPORT_ID "
                        + " AND SUBREPORT_SEQ = (SELECT MIN (SUBREPORT_SEQ) "
                        + " FROM PRD_REPORT_DETAILS S1 "
                        + " WHERE S1.REPORT_ID = T2.REPORT_ID)) WIDGET_PAGINATION"
						+ " FROM PRD_REPORT_ACCESS T1,PRD_REPORT_MASTER T2 "
						+ " WHERE t1.REPORT_ID  = t2.REPORT_ID AND T1.PRODUCT_NAME = T2.APPLICATION_ID AND T1.USER_GROUP||'-'||T1.USER_PROFILE = ? " + 
						" AND T2.STATUS = 0 AND REPORT_TYPE ='W' ";
			}else {
				sql = " Select T2.Report_ID,T2.REPORT_TITLE,T2.FILTER_FLAG,T2.FILTER_REF_CODE,T2.APPLY_USER_RESTRCT, "
						+ " (SELECT "+new CommonDao(jdbcTemplate).getDbFunction("NVL")+"(MIN(SUBREPORT_SEQ),0) FROM PRD_REPORT_DETAILS WHERE REPORT_ID = T2.REPORT_ID) NEXT_LEVEL,T2.GROUPING_FLAG, "
						+ " T2.REPORT_ORDER, T2.Report_Type_AT, T2.Report_Type, T2.Template_ID,"
						+ " (SELECT VALUE FROM VISION_VARIABLES WHERE VARIABLE = 'PRD_REPORT_MAXROW') MAX_PERPAGE,T2.SCALING_FACTOR, "
						+ "	(SELECT "+new CommonDao(jdbcTemplate).getDbFunction("NVL")+"(REPORT_ORIENTATION,'P') FROM PRD_REPORT_DETAILS PRD WHERE PRD.REPORT_ID= T2.REPORT_ID AND "
	                    + " SUBREPORT_SEQ = (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS PR WHERE PR.REPORT_ID= T2.REPORT_ID) "
	                    + ") REPORT_ORIENTATION,(SELECT OBJECT_TYPE FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID  "
                        + "   AND SUBREPORT_SEQ= (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID)) OBJECT_TYPE, "
                        + " (SELECT CHART_TYPE FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID  "
                        + "    AND SUBREPORT_SEQ= (SELECT MIN(SUBREPORT_SEQ) FROM PRD_REPORT_DETAILS S1 WHERE S1.REPORT_ID = T2.REPORT_ID)) CHART_TYPE, "
                        + " WIDGET_THEME,REPORT_PERIOD, "
                        + " (SELECT SORTING_ENABLE FROM PRD_REPORT_DETAILS S1 "
                        + " WHERE S1.REPORT_ID = T2.REPORT_ID "
                        + " AND SUBREPORT_SEQ = (SELECT MIN (SUBREPORT_SEQ) "
                        + " FROM PRD_REPORT_DETAILS S1 "
                        + " WHERE S1.REPORT_ID = T2.REPORT_ID)) SORTING_ENABLE,"
                        + " (SELECT WIDGET_PAGINATION FROM PRD_REPORT_DETAILS S1 "
                        + " WHERE S1.REPORT_ID = T2.REPORT_ID "
                        + " AND SUBREPORT_SEQ = (SELECT MIN (SUBREPORT_SEQ) "
                        + " FROM PRD_REPORT_DETAILS S1 "
                        + " WHERE S1.REPORT_ID = T2.REPORT_ID)) WIDGET_PAGINATION"
						+ " FROM PRD_REPORT_ACCESS T1,PRD_REPORT_MASTER T2 "
						+ " WHERE t1.REPORT_ID  = t2.REPORT_ID AND T1.PRODUCT_NAME = T2.APPLICATION_ID AND T1.USER_GROUP+'-'+T1.USER_PROFILE = ? " + 
						" AND T2.STATUS = 0 AND REPORT_TYPE ='W' ";
			}
			
			String orderBy = " ORDER BY REPORT_TITLE";
			sql = sql + orderBy;
			Object[] lParams = new Object[1];
			lParams[0] = visionUsersVb.getUserGrpProfile();
			return jdbcTemplate.query(sql, lParams, getWidgetListMapper());
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while getting Report List...!!");
			return null;
		}
			
	}
	protected RowMapper getWidgetListMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setReportId(rs.getString("REPORT_ID"));
				vObject.setReportTitle(rs.getString("REPORT_TITLE"));
				vObject.setFilterFlag(rs.getString("FILTER_FLAG"));
				vObject.setFilterRefCode(rs.getString("FILTER_REF_CODE"));
				vObject.setApplyUserRestrct(rs.getString("APPLY_USER_RESTRCT"));
				vObject.setCurrentLevel(rs.getString("NEXT_LEVEL").replaceAll(".0", ""));
				vObject.setNextLevel(rs.getString("NEXT_LEVEL").replaceAll(".0", ""));
				vObject.setGroupingFlag(rs.getString("GROUPING_FLAG"));
				vObject.setMaxRecords(rs.getInt("MAX_PERPAGE"));
				vObject.setReportTypeAT(rs.getInt("Report_Type_AT"));
				vObject.setReportType(rs.getString("Report_Type"));
				vObject.setTemplateId(rs.getString("Template_ID"));
				vObject.setReportOrientation(rs.getString("REPORT_ORIENTATION"));
				vObject.setObjectType(rs.getString("OBJECT_TYPE"));
				vObject.setChartType(rs.getString("CHART_TYPE"));
				String widgetTheme = rs.getString("WIDGET_THEME");
			    vObject.setWidgetTheme(widgetTheme);
				if(ValidationUtil.isValid(widgetTheme)) {
					String widgetThemePath = servletContext.getRealPath("/WEB-INF/classes/Widget_Theme/");
					widgetThemePath = widgetThemePath+widgetTheme+".txt";
					String data = "";
					try {
						data = new String(Files.readAllBytes(Paths.get(widgetThemePath)));
						JSONObject xmlJSONObj = XML.toJSONObject(data);
						int PRETTY_PRINT_INDENT_FACTOR = 4;
						String resultData = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR).replaceAll("[\\n\\t ]", "");
						vObject.setReportDesignXml(resultData);
					} catch (IOException e) {}
				}
				vObject.setScalingFactor(rs.getString("SCALING_FACTOR"));
				/*if(ValidationUtil.isValid(rs.getString("SCALING_FACTOR"))) {
					if("1000000".equalsIgnoreCase(rs.getString("SCALING_FACTOR"))) {
						vObject.setScalingFactor("(In Millions)");
					}else if("1000".equalsIgnoreCase(rs.getString("SCALING_FACTOR"))) {
						vObject.setScalingFactor("(In Thousands)");
					}else if("1000000000".equalsIgnoreCase(rs.getString("SCALING_FACTOR"))) {
						vObject.setScalingFactor("(In Billions)");
					}else {
						vObject.setScalingFactor("");
					}
				}*/
				if(ValidationUtil.isValid(rs.getString("REPORT_PERIOD"))) {
					VisionUsersVb visionUsers = SessionContextHolder.getContext();
					String country = "";
					String leBook = "";
					if(ValidationUtil.isValid(visionUsers.getCountry())) {
						country = visionUsers.getCountry();
					}else {
						country = new CommonDao(jdbcTemplate).findVisionVariableValue("DEFAULT_COUNTRY");
					}
					if(ValidationUtil.isValid(visionUsers.getLeBook())) {
						leBook = visionUsers.getLeBook();
					}else {
						leBook = new CommonDao(jdbcTemplate).findVisionVariableValue("DEFAULT_LE_BOOK");
					}
					vObject.setReportPeriod(new CommonDao(jdbcTemplate).getCurrentDateInfo(rs.getString("REPORT_PERIOD"), country+"-"+leBook));
					vObject.setSortFlag(rs.getString("SORTING_ENABLE"));
					vObject.setWidgetPagination(rs.getInt("WIDGET_PAGINATION"));
				}
				return vObject;
			}
		};
		return mapper;
	}
		public int saveWidget(ReportsVb dObj){
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "Insert Into PRD_USER_WIDGETS (APPLICATION_ID,VISION_ID, TEMPLATE_ID,WIDGET_IDS,WIDGET_THEME,USER_WIDGET_STATUS,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION) "
					+ "Values (?,?,?,?,?,?,?,?," + new CommonDao(jdbcTemplate).getDbFunction("SYSDATE") + ","
					+ new CommonDao(jdbcTemplate).getDbFunction("SYSDATE") + ")";

			Object[] args = { productName, intCurrentUserId, dObj.getTemplateId(), dObj.getSavedWidgetId(),
					dObj.getWidgetTheme(), 0, intCurrentUserId, intCurrentUserId };
			retVal = jdbcTemplate.update(query, args);
			return retVal;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while inserting into the PRD_USER_WIDGETS...!!");
			return 0;
		}
	}
	public int userWidgetExists(ReportsVb dObj){
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "SELECT COUNT(1) FROM PRD_USER_WIDGETS WHERE APPLICATION_ID = ? AND VISION_ID = ? ";	
			Object[] args = {productName,intCurrentUserId};
			retVal= jdbcTemplate.queryForObject(query,args,Integer.class);
			return retVal;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while inserting into the PRD_USER_WIDGETS...!!");
			return 0;
		}
	}
	public int deleteWidgetExists(ReportsVb dObj){
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "DELETE FROM PRD_USER_WIDGETS WHERE APPLICATION_ID = ? AND VISION_ID = ? ";	
			Object[] args = {productName,intCurrentUserId};
			retVal= jdbcTemplate.update(query,args);
			return retVal;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while inserting into the PRD_USER_WIDGETS...!!");
			return 0;
		}
	}
	public List<ReportsVb> getUserWidgets() throws DataAccessException {
		setServiceDefaults();
		try
		{
			String sql = " SELECT TEMPLATE_ID,WIDGET_IDS,WIDGET_THEME FROM PRD_USER_WIDGETS WHERE APPLICATION_ID = ? AND USER_WIDGET_STATUS = 0 AND VISION_ID = ? ";
			Object[] lParams = new Object[2];
			lParams[0] = productName;
			lParams[1] = intCurrentUserId;
			return jdbcTemplate.query(sql, lParams, getUserWidgetMapper());
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while getting Report List...!!");
			return null;
		}
			
	}
	protected RowMapper getUserWidgetMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportsVb vObject = new ReportsVb();
				vObject.setTemplateId(rs.getString("Template_ID"));
				vObject.setSavedWidgetId(rs.getString("WIDGET_IDS"));
				vObject.setWidgetTheme(rs.getString("WIDGET_THEME"));
				return vObject;
			}
		};
		return mapper;
	}
	private String replaceTagValues(String chartXml) {
		String chartReplaceXml = chartXml;
		chartReplaceXml=chartReplaceXml.replaceAll("categoryL1", "category");
		chartReplaceXml=chartReplaceXml.replaceAll("categoryL2", "category");
		chartReplaceXml=chartReplaceXml.replaceAll("categoryL3", "category");
		return chartReplaceXml;
	}
	public ExceptionCode getTilesReportData(ReportsVb vObject,String orginalQuery,Connection conExt) throws SQLException{
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"); 
		setServiceDefaults();
		Statement stmt = null; 
		ResultSet rs = null;
		String resultData = "";
		DecimalFormat dfDec = new DecimalFormat("0.00");
		DecimalFormat dfNoDec = new DecimalFormat("0");
		List<PrdQueryConfig> sqlQueryList = new ArrayList<PrdQueryConfig>();
		PrdQueryConfig prdQueryConfig = new PrdQueryConfig();
		ExceptionCode exceptionCode = new ExceptionCode();
		Statement stmt1 = null;
		try
		{	
			stmt1 = conExt.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
				    ResultSet.CONCUR_READ_ONLY);
			
			ResultSet rsData = stmt1.executeQuery(orginalQuery);
			ResultSetMetaData metaData = rsData.getMetaData();
			int colCount = metaData.getColumnCount();
			HashMap<String,String> columns = new HashMap<String,String>();
			Boolean dataAvail = false;
			while(rsData.next()){
				for(int cn = 1;cn <= colCount;cn++) {
					String columnName = metaData.getColumnName(cn);
					columns.put(columnName.toUpperCase(), rsData.getString(columnName));
				}
				dataAvail = true;
				break;
			}
			rsData.close();
			if(!dataAvail) {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
				exceptionCode.setErrorMsg("No Records Found");
				return exceptionCode;
			}
			String fieldProp = vObject.getColHeaderXml();
			fieldProp = ValidationUtil.isValid(fieldProp)?fieldProp.replaceAll("\n", "").replaceAll("\r", ""):"";
			for(int ctr = 1;ctr <= 5;ctr++) {
				String placeHolder = CommonUtils.getValueForXmlTag(fieldProp, "PLACEHOLDER"+ctr);
				String sourceCol = CommonUtils.getValueForXmlTag(placeHolder, "SOURCECOL");
				String dataType = CommonUtils.getValueForXmlTag(placeHolder, "DATA_TYPE");
				String numberFormat = CommonUtils.getValueForXmlTag(placeHolder, "NUMBERFORMAT");
				String scaling = CommonUtils.getValueForXmlTag(placeHolder, "SCALING");
				if(!ValidationUtil.isValid(placeHolder) || !ValidationUtil.isValid(sourceCol)) {
					continue;
				}
				if(ValidationUtil.isValid(sourceCol)) {
					String fieldValue = columns.get(sourceCol);
					if(!ValidationUtil.isValid(fieldValue))
						continue;
					/*Double val = 0.00;*/
					String prefix="";
					if(ValidationUtil.isValid(scaling) && "Y".equalsIgnoreCase(scaling) && ValidationUtil.isValid(fieldValue)) {
						Double dbValue = Math.abs(Double.parseDouble(fieldValue));
						if(dbValue > 1000000000) {
							dbValue = Double.parseDouble(fieldValue)/1000000000;
							prefix = "B";
						}else if(dbValue > 1000000) {
							dbValue = Double.parseDouble(fieldValue)/1000000;
							prefix = "M";
						}else if(dbValue > 10000) {
							dbValue = Double.parseDouble(fieldValue)/1000;
							prefix = "K";
						}
						String afterDecimalVal = String.valueOf(dbValue);
						if(!afterDecimalVal.contains("E")) {
							afterDecimalVal = afterDecimalVal.substring(afterDecimalVal.indexOf ( "." )+1);
							if(Double.parseDouble(afterDecimalVal) > 0)
								fieldValue = dfDec.format(dbValue)+" "+prefix;
							else
								fieldValue = dfNoDec.format(dbValue)+" "+prefix;
						}else {
							fieldValue = "0.00";
						}
					}
					if(ValidationUtil.isValid(fieldValue) && ValidationUtil.isValid(numberFormat) && "Y".equalsIgnoreCase(numberFormat) && !ValidationUtil.isValid(prefix)) {
						DecimalFormat decimalFormat = new DecimalFormat("#,###.##");
						double tmpVal = Double.parseDouble(fieldValue);
						fieldValue = decimalFormat.format(tmpVal);
					}
					/*if(ValidationUtil.isValid(fieldValue))*/
					fieldProp = fieldProp.replaceAll(sourceCol, fieldValue);
				}
			}
			String drillDownKey = CommonUtils.getValueForXmlTag(fieldProp,"DRILLDOWN_KEY");
			if(ValidationUtil.isValid(drillDownKey) && "DD_KEY_ID".equalsIgnoreCase(drillDownKey)) {
				String value = columns.get(drillDownKey);
				if(ValidationUtil.isValid(value))
					fieldProp = fieldProp.replaceAll(drillDownKey, value);
				else
					fieldProp = fieldProp.replaceAll(drillDownKey, "");
			}
			int PRETTY_PRINT_INDENT_FACTOR = 4;
			JSONObject xmlJSONObj = XML.toJSONObject(fieldProp);
			resultData = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR).replaceAll("[\\n\\t ]", "");
			exceptionCode.setResponse(resultData);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		}catch(Exception ex){
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ex.getMessage());
		}finally {
			if(stmt1 != null)
				stmt1.close();
			if(conExt != null)
				conExt.close();
		}
		return exceptionCode;
	}
	public int saveReportUserDef(ReportUserDefVb dObj){
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "Insert Into PRD_REPORT_USER_DEF (VISION_ID, REPORT_ID,SUB_REPORT_ID,GROUP_COLUMNS,SORTING_COLUMN,SEARCH_COLUMN,HIDE_COLUMNS,"+
					" DATA_FILTER_1,DATA_FILTER_2,DATA_FILTER_3,DATA_FILTER_4,DATA_FILTER_5, "+
					" DATA_FILTER_6,DATA_FILTER_7,DATA_FILTER_8,DATA_FILTER_9,DATA_FILTER_10, "+
					" PROMPT_VALUE_1,PROMPT_VALUE_2,PROMPT_VALUE_3,PROMPT_VALUE_4,PROMPT_VALUE_5, "+
					" PROMPT_VALUE_6,PROMPT_VALUE_7,PROMPT_VALUE_8,PROMPT_VALUE_9,PROMPT_VALUE_10, SCALING_FACTOR,GROUPING_FLAG, "+
					" SHOW_MEASURES,SHOW_DIMENSIONS,CHART_TYPE_AT,CHART_TYPE, "+
					" STATUS,RECORD_INDICATOR,MAKER,VERIFIER,DATE_LAST_MODIFIED,DATE_CREATION) "+
					"Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,?,?,"+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+","+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+")";	

			Object[] args = {intCurrentUserId,dObj.getReportId(),dObj.getSubReportId(),dObj.getGroupingColumn(),dObj.getSortColumn(),dObj.getSearchColumn(),
					dObj.getColumnsToHide(),dObj.getDataFilter1(),dObj.getDataFilter2(),dObj.getDataFilter3(),dObj.getDataFilter4(),dObj.getDataFilter5(),
					dObj.getDataFilter6(),dObj.getDataFilter7(),dObj.getDataFilter8(),dObj.getDataFilter9(),dObj.getDataFilter10(),	
					dObj.getPromptValue1(),dObj.getPromptValue2(),dObj.getPromptValue3(),dObj.getPromptValue4(),dObj.getPromptValue5(),
					dObj.getPromptValue6(),dObj.getPromptValue7(),dObj.getPromptValue8(),dObj.getPromptValue9(),dObj.getPromptValue10(),
					dObj.getScalingFactor(),dObj.getApplyGrouping(),dObj.getShowMeasures(),dObj.getShowDimensions(),dObj.getChartTypeAT(),
					dObj.getChartType(),intCurrentUserId,intCurrentUserId};
			retVal= jdbcTemplate.update(query,args);
			return retVal;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while inserting into the PRD_REPORT_USER_DEF...!!");
			return 0;
		}
	}
	public int deleteReportUserDef(ReportUserDefVb vObject){
		String query = "DELETE FROM PRD_REPORT_USER_DEF WHERE VISION_ID= ? AND  REPORT_ID = ?  And SUB_REPORT_ID = ? ";
		Object args[] = {intCurrentUserId,vObject.getReportId(), vObject.getSubReportId()};
		return jdbcTemplate.update(query,args);
	}
	public int reportUserDefExists(ReportUserDefVb vObject){
		setServiceDefaults();
		int retVal = 0;
		String query = "";
		try {
			query = "SELECT COUNT(1) FROM PRD_REPORT_USER_DEF WHERE  VISION_ID= ? AND  REPORT_ID = ?  And SUB_REPORT_ID = ? ";	
			Object args[] = {intCurrentUserId,vObject.getReportId(), vObject.getSubReportId()};
			retVal= jdbcTemplate.queryForObject(query,args,Integer.class);
			return retVal;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while inserting into the Report User Defs...!!");
			return 0;
		}
	}
	public List<ReportsVb> getSavedUserDefSetting(ReportsVb reportvb,Boolean checkFlag) throws DataAccessException {
		setServiceDefaults();
		try
		{
			String sql = "SELECT T1.REPORT_ID,T1.SUB_REPORT_ID,GROUP_COLUMNS,SORTING_COLUMN,SEARCH_COLUMN,HIDE_COLUMNS, "+
					" DATA_FILTER_1,DATA_FILTER_2,DATA_FILTER_3,DATA_FILTER_4,DATA_FILTER_5, "+
					" DATA_FILTER_6,DATA_FILTER_7,DATA_FILTER_8,DATA_FILTER_9,DATA_FILTER_10, "+
					" PROMPT_VALUE_1,PROMPT_VALUE_2,PROMPT_VALUE_3,PROMPT_VALUE_1,PROMPT_VALUE_4,PROMPT_VALUE_5, "+
					" PROMPT_VALUE_6,PROMPT_VALUE_7,PROMPT_VALUE_8,PROMPT_VALUE_9,PROMPT_VALUE_10, T1.SCALING_FACTOR,T1.GROUPING_FLAG,"+
					" SHOW_MEASURES,SHOW_DIMENSIONS,T1.CHART_TYPE "+
					" FROM PRD_REPORT_USER_DEF T1, PRD_REPORT_MASTER T2,PRD_REPORT_DETAILS T3"
					+ " WHERE T1.VISION_ID = ? AND T1.STATUS = 0 "+
					" AND T1.REPORT_ID = T3.REPORT_ID "+
                    " AND T1.SUB_REPORT_ID = T3.SUB_REPORT_ID "+
                    " AND T2.REPORT_ID = T3.REPORT_ID";
			if(checkFlag) {
				sql =  sql+" AND T2.REPORT_TYPE != 'W'  AND T1.REPORT_ID = '"+reportvb.getReportId()+"'  AND T1.SUB_REPORT_ID = '"+reportvb.getSubReportId()+"' ";
			}else {
				sql =  sql+" AND T2.REPORT_TYPE = 'W'";
			}
			Object[] lParams = new Object[1];
			lParams[0] = intCurrentUserId;
			return jdbcTemplate.query(sql, lParams, getUserDefSettingMapper());
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception while getting Report List...!!");
			return null;
		}
			
	}
	protected RowMapper getUserDefSettingMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportUserDefVb vObject = new ReportUserDefVb();
				vObject.setReportId(rs.getString("REPORT_ID"));
				vObject.setSubReportId(rs.getString("SUB_REPORT_ID"));
				vObject.setGroupingColumn(rs.getString("GROUP_COLUMNS"));
				vObject.setSortColumn(rs.getString("SORTING_COLUMN"));
				vObject.setSearchColumn(rs.getString("SEARCH_COLUMN"));
				vObject.setColumnsToHide(rs.getString("HIDE_COLUMNS"));
				vObject.setDataFilter1(rs.getString("DATA_FILTER_1"));
				vObject.setDataFilter2(rs.getString("DATA_FILTER_2"));
				vObject.setDataFilter3(rs.getString("DATA_FILTER_3"));
				vObject.setDataFilter4(rs.getString("DATA_FILTER_4"));
				vObject.setDataFilter5(rs.getString("DATA_FILTER_5"));
				vObject.setDataFilter6(rs.getString("DATA_FILTER_6"));
				vObject.setDataFilter7(rs.getString("DATA_FILTER_7"));
				vObject.setDataFilter8(rs.getString("DATA_FILTER_8"));
				vObject.setDataFilter9(rs.getString("DATA_FILTER_9"));
				vObject.setDataFilter10(rs.getString("DATA_FILTER_10"));
				vObject.setPromptValue1(rs.getString("PROMPT_VALUE_1"));
				vObject.setPromptValue2(rs.getString("PROMPT_VALUE_2"));
				vObject.setPromptValue3(rs.getString("PROMPT_VALUE_3"));
				vObject.setPromptValue4(rs.getString("PROMPT_VALUE_4"));
				vObject.setPromptValue5(rs.getString("PROMPT_VALUE_5"));
				vObject.setPromptValue6(rs.getString("PROMPT_VALUE_6"));
				vObject.setPromptValue7(rs.getString("PROMPT_VALUE_7"));
				vObject.setPromptValue8(rs.getString("PROMPT_VALUE_8"));
				vObject.setPromptValue9(rs.getString("PROMPT_VALUE_9"));
				vObject.setPromptValue10(rs.getString("PROMPT_VALUE_10"));
				vObject.setScalingFactor(rs.getString("SCALING_FACTOR"));
				vObject.setApplyGrouping(rs.getString("GROUPING_FLAG"));
				vObject.setShowMeasures(rs.getString("SHOW_MEASURES"));
				vObject.setShowDimensions(rs.getString("SHOW_DIMENSIONS"));
				vObject.setChartType(rs.getString("CHART_TYPE"));
				return vObject;
			}
		};
		return mapper;
	}
	public String getReportType(String reportId) {
		String reportType = "";
		String query = "";
		try {
			query = "SELECT REPORT_TYPE FROM PRD_REPORT_MASTER WHERE REPORT_ID = '"+reportId+"'";
		}catch(Exception e) {
			return null;
		}
		return jdbcTemplate.queryForObject(query, String.class);
	}
	public List getChartList(String chartType) {
		String sql = "";
		List collTemp = new ArrayList<>();
		try {
			sql = "SELECT (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 7005 AND ALPHA_SUB_TAB = S1.ALPHA_SUB_TAB ) ALPHA_SUB_TAB, "
					+ " (SELECT ALPHA_SUBTAB_DESCRIPTION FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = 7501 AND ALPHA_SUB_TAB = S1.ALPHA_SUB_TAB ) DESCR "
					+ " FROM ALPHA_SUB_TAB S1 WHERE ALPHA_TAB = 7500 AND ALPHA_SUBTAB_DESCRIPTION IN (  "
					+ " SELECT T2.ALPHA_SUBTAB_DESCRIPTION TYPE  FROM ALPHA_SUB_TAB T1,  " + " ALPHA_SUB_TAB T2  "
					+ " WHERE T1.ALPHA_TAB = 7005  " + " AND T2.ALPHA_TAB = 7500  " + " AND T1.ALPHA_SUB_TAB = '"
					+ chartType + "'  " + " AND T1.ALPHA_SUB_TAB = T2.ALPHA_SUB_TAB) ORDER BY DESCR ";
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					while (rs.next()) {
						LinkedHashMap<String,String> resultData = new LinkedHashMap<String,String>();
						for(int cn = 1;cn < 2;cn++) {
							resultData.put(rs.getString("ALPHA_SUB_TAB"), rs.getString("DESCR"));
						}
						collTemp.add(resultData);
					}
					return collTemp;
				}
			};
			return (List) jdbcTemplate.query(sql, mapper);
		} catch (Exception e) {
			logger.error("Error while getting chart List");
		}
		return collTemp;
	}
}