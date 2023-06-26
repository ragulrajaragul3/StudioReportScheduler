package com.vision.dao;

import java.io.IOException;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.SessionContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonVb;
import com.vision.vb.LevelOfDisplayVb;
import com.vision.vb.ProfileData;
import com.vision.vb.UserRestrictionVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.ScheduledReportWb;

@Component
public class CommonDao {
	
	public JdbcTemplate jdbcTemplate = null;

	public CommonDao(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	
	@Value("${app.productName}")
	private String productName;
	
	@Value("${app.client}")
	private String clientType;
	
	@Value("${app.client}")
	private String client;
	
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	@Autowired
	CommonApiDao commonApiDao;

	public List<CommonVb> findVerificationRequiredAndStaticDelete(String pTableName) throws DataAccessException {
		
		String sql = "select DELETE_TYPE,VERIFICATION_REQD FROM VISION_TABLES where UPPER(TABLE_NAME) = UPPER(?)";
		Object[] lParams = new Object[1];
		lParams[0] = pTableName;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				CommonVb commonVb = new CommonVb();
				commonVb.setStaticDelete(rs.getString("DELETE_TYPE") == null || rs.getString("DELETE_TYPE").equalsIgnoreCase("S") ? true : false);
				commonVb.setVerificationRequired(rs.getString("VERIFICATION_REQD") == null || rs.getString("VERIFICATION_REQD").equalsIgnoreCase("Y") ? true : false);
				return commonVb;
			}
		};
		List<CommonVb> commonVbs = jdbcTemplate.query(sql, lParams, mapper);
		if(commonVbs == null || commonVbs.isEmpty()){
			commonVbs = new ArrayList<CommonVb>();
			CommonVb commonVb = new CommonVb();
			commonVb.setStaticDelete(true);
			commonVb.setVerificationRequired(true);
			commonVbs.add(commonVb);
		}
		return commonVbs;
	}

	public List<ProfileData> getTopLevelMenu(int visionId) throws DataAccessException{
		String sql = "SELECT distinct NST.NUM_SUBTAB_DESCRIPTION, PP.MENU_GROUP,PP.MENU_ICON ,"+
 					 "PP.P_ADD, PP.P_MODIFY, PP.P_DELETE, PP.P_INQUIRY, PP.P_VERIFICATION , PP.P_EXCEL_UPLOAD "+
 					 "FROM PROFILE_PRIVILEGES PP, VISION_USERS MU, NUM_SUB_TAB NST "+ 
 					 "where PP.USER_GROUP = MU.USER_GROUP and PP.USER_PROFILE = MU.USER_PROFILE "+
 					 "and  NST.NUM_SUB_TAB = PP.MENU_GROUP and MU.VISION_ID = ? AND PP.PROFILE_STATUS = 0 AND NST.NUM_SUBTAB_STATUS=0 "+
 					 "and num_tab = 176 and pp.Application_Access Like '%"+productName+"%' order by PP.MENU_GROUP";
		Object[] lParams = new Object[1];
		lParams[0] = visionId;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ProfileData profileData = new ProfileData();
				profileData.setMenuItem(rs.getString("NUM_SUBTAB_DESCRIPTION"));
				profileData.setMenuGroup(rs.getInt("MENU_GROUP"));
				profileData.setProfileAdd(rs.getString("P_ADD"));
				profileData.setProfileModify(rs.getString("P_MODIFY"));
				profileData.setProfileDelete(rs.getString("P_DELETE"));
				profileData.setProfileInquiry(rs.getString("P_INQUIRY"));
				profileData.setProfileVerification(rs.getString("P_VERIFICATION"));
				profileData.setProfileUpload(rs.getString("P_EXCEL_UPLOAD"));
				profileData.setMenuIcon(rs.getString("MENU_ICON"));
				return profileData;
			}
		};
		List<ProfileData> profileData = jdbcTemplate.query(sql, lParams, mapper);
		return profileData;
	}
	public List<ProfileData> getTopLevelMenu() throws DataAccessException {
		String sql = " Select MENU_GROUP_SEQ,MENU_GROUP_NAME,MENU_GROUP_ICON from PRD_MENU_GROUP where MENU_GROUP_Status = 0 "
				+ "and Application_Access = ? ORDER BY MENU_GROUP_SEQ ";
		Object[] lParams = new Object[1];
		lParams[0] = productName;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ProfileData profileData = new ProfileData();
				profileData.setMenuItem(rs.getString("MENU_GROUP_NAME"));
				profileData.setMenuGroup(rs.getInt("MENU_GROUP_SEQ"));
				profileData.setMenuIcon(rs.getString("MENU_GROUP_ICON"));
				return profileData;
			}
		};
		List<ProfileData> profileData = jdbcTemplate.query(sql,lParams, mapper);
		return profileData;
	}

	public String findVisionVariableValue(String pVariableName) throws DataAccessException {
		if(!ValidationUtil.isValid(pVariableName)){
			return null;
		}
		String sql = "select VALUE FROM VISION_VARIABLES where UPPER(VARIABLE) = UPPER(?)";
		Object[] lParams = new Object[1];
		lParams[0] = pVariableName;
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				CommonVb commonVb = new CommonVb();
				commonVb.setMakerName(rs.getString("VALUE"));
				return commonVb;
			}
		};
		List<CommonVb> commonVbs = jdbcTemplate.query(sql, lParams, mapper);
		if(commonVbs != null && !commonVbs.isEmpty()){
			return commonVbs.get(0).getMakerName();
		}
		return null;
	}
	public void findDefaultHomeScreen(VisionUsersVb vObject) throws DataAccessException {
		int count = 0;
		String sql = "SELECT COUNT(1) FROM PWT_REPORT_SUITE WHERE VISION_ID = "+vObject.getVisionId();
		count = jdbcTemplate.queryForObject(sql, Integer.class);
		/*if(count>0){
			vObject.setDefaultHomeScreen(true);
		}*/
	}
	public int getMaxOfId(){
		String sql = "select max(vision_id) from (Select max(vision_id) vision_id from vision_users UNION ALL select Max(vision_id) from vision_users_pend)";
		int i = jdbcTemplate.queryForObject(sql, Integer.class);
		return i;
	}
	public String getVisionBusinessDayForExpAnalysis(String countryLeBook){
		Object args[] = {countryLeBook};
		return jdbcTemplate.queryForObject("select TO_CHAR(BUSINESS_DATE,'Mon-RRRR') BUSINESS_DATE  from Vision_Business_Day  WHERE COUNTRY ||'-'|| LE_BOOK=?",
				args,String.class);
	}
	
	public String getyearMonthForTop10Deals(String countryLeBook){
			Object args[] = {countryLeBook};
			return jdbcTemplate.queryForObject("select TO_CHAR(BUSINESS_DATE,'RRRRMM') BUSINESS_DATE  from Vision_Business_Day  WHERE COUNTRY ||'-'|| LE_BOOK=?",
					args,String.class);
	}
	public String getVisionBusinessDate(String countryLeBook){
		Object args[] = {countryLeBook};
		return jdbcTemplate.queryForObject("select TO_CHAR(BUSINESS_DATE,'DD-Mon-RRRR') BUSINESS_DATE  from Vision_Business_Day  WHERE COUNTRY ||'-'|| LE_BOOK=?",
				args,String.class);
	}
	
	public String getVisionCurrentYearMonth(){
		return jdbcTemplate.queryForObject("select to_char(to_date(CURRENT_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') CURRENT_YEAR_MONTH  from V_Curr_Year_Month",
				String.class);
	}
	public int getUploadCount(){
		  String sql = "Select count(1) from Vision_Upload where Upload_Status = 1 AND FILE_NAME LIKE '%XLSX'";
		  int i = jdbcTemplate.queryForObject(sql, Integer.class);
		  return i;
	}
	public int doPasswordResetInsertion(VisionUsersVb vObject){
    	Date oldDate = new Date(); 
        SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss") ;
        String resetValidity= df.format(oldDate);
    	if(ValidationUtil.isValid(vObject.getPwdResetTime())){
    	    Date newDate = DateUtils.addHours(oldDate, Integer.parseInt(vObject.getPwdResetTime()));
            resetValidity= df.format(newDate);
	    }
		String query = "Insert Into FORGOT_PASSWORD ( VISION_ID, RESET_DATE, RESET_VALIDITY, RS_STATUS_NT, RS_STATUS)" +
			"Values (?, SysDate, To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), ?, ?)";
		Object[] args = {vObject.getVisionId(), resetValidity, vObject.getUserStatusNt(), vObject.getUserStatus()};  
		return jdbcTemplate.update(query,args);
	}
	
	public List<LevelOfDisplayVb> getQueryUserGroupProfile() throws DataAccessException{
		String sql  = "SELECT USER_GROUP, USER_PROFILE FROM PROFILE_PRIVILEGES " + 
				" GROUP BY USER_GROUP, USER_PROFILE";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				LevelOfDisplayVb lodVb = new LevelOfDisplayVb();
				lodVb.setUserGroup(rs.getString("USER_GROUP"));
				lodVb.setUserProfile(rs.getString("USER_PROFILE"));
				return lodVb;
			}
		};
		List<LevelOfDisplayVb> lodVbList = jdbcTemplate.query(sql, mapper);
		return lodVbList;
	}
	
	public List<ProfileData> getQueryUserGroup() throws DataAccessException{
		String sql  = "SELECT DISTINCT USER_GROUP FROM PROFILE_PRIVILEGES ORDER BY USER_GROUP";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ProfileData profileData = new ProfileData();
				profileData.setUserGroup(rs.getString("USER_GROUP"));
				return profileData;
			}
		};
		List<ProfileData> profileData = jdbcTemplate.query(sql, mapper);
		return profileData;
	}
	
	public List<ProfileData> getQueryUserGroupBasedProfile(String userGroup) throws DataAccessException{
		String sql  = "SELECT DISTINCT USER_PROFILE,USER_GROUP FROM PROFILE_PRIVILEGES where USER_GROUP ='"+userGroup+"' ORDER BY USER_GROUP";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ProfileData profileData = new ProfileData();
				profileData.setUserGroup(rs.getString("USER_GROUP"));
				profileData.setUserProfile(rs.getString("USER_PROFILE"));
				return profileData;
			}
		};
		List<ProfileData> profileData = jdbcTemplate.query(sql, mapper);
		return profileData;
	}
	
	public String getSystemDate() {
		String sql = "SELECT To_Char(SysDate, 'DD-MM-YYYY HH24:MI:SS') AS SYSDATE1 FROM DUAL";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				return (rs.getString("SYSDATE1"));
			}
		};
		return (String) jdbcTemplate.queryForObject(sql, null, mapper);
	}
	public String getSystemDate12Hr() {
		String sql = "SELECT To_Char(SysDate, 'DD-MM-YYYY HH:MI:SS') AS SYSDATE1 FROM DUAL";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				return (rs.getString("SYSDATE1"));
			}
		};
		return (String) jdbcTemplate.queryForObject(sql, null, mapper);
	}
	
	public String getScriptValue(String pVariableName) throws DataAccessException, Exception {
		String returnValue = "";
		try {
			Object params[] = { pVariableName };
			returnValue = jdbcTemplate.queryForObject(
					"select VARIABLE_SCRIPT from VISION_DYNAMIC_HASH_VAR WHERE VARIABLE_TYPE = 2 AND SCRIPT_TYPE='MACROVAR' AND UPPER(VARIABLE_NAME)=UPPER(?) ",
					params, String.class);
			return returnValue;
		} catch (Exception e) {
			return null;
		}
	}
	
	//Deepak maintained seperately.Already avialble on AbstractCommonDao
	public String parseErrorMsgCommon(UncategorizedSQLException ecxception) {
		String strErrorDesc = ecxception.getSQLException() != null ? ecxception.getSQLException().getMessage()
				: ecxception.getMessage();
		String sqlErrorCodes[] = { "ORA-00928:", "ORA-00942:", "ORA-00998:", "ORA-01400:", "ORA-01722:", "ORA-04098:",
				"ORA-01810:", "ORA-01840:", "ORA-01843:", "ORA-20001:", "ORA-20002:", "ORA-20003:", "ORA-20004:",
				"ORA-20005:", "ORA-20006:", "ORA-20007:", "ORA-20008:", "ORA-20009:", "ORA-200010:", "ORA-20011:",
				"ORA-20012:", "ORA-20013:", "ORA-20014:", "ORA-20015:", "ORA-20016:", "ORA-20017:", "ORA-20018:",
				"ORA-20019:", "ORA-20020:", "ORA-20021:", "ORA-20022:", "ORA-20023:", "ORA-20024:", "ORA-20025:",
				"ORA-20102:", "ORA-20105:", "ORA-01422:", "ORA-06502:", "ORA-20082:", "ORA-20030:", "ORA-20010:",
				"ORA-20034:", "ORA-20043:", "ORA-20111:", "ORA-06512:", "ORA-04088:", "ORA-06552:", "ORA-00001:" };
		for (String sqlErrorCode : sqlErrorCodes) {
			if (ValidationUtil.isValid(strErrorDesc) && strErrorDesc.lastIndexOf(sqlErrorCode) >= 0) {
				strErrorDesc = strErrorDesc.substring(
						strErrorDesc.lastIndexOf(sqlErrorCode) + sqlErrorCode.length() + 1, strErrorDesc.length());
				if (strErrorDesc.indexOf("ORA-06512:") >= 0) {
					strErrorDesc = strErrorDesc.substring(0, strErrorDesc.indexOf("ORA-06512:"));
				}
			}
		}
		return strErrorDesc;
	}
	
	public String restrictedAoPermission(String module) {
		VisionUsersVb visionUsersVb = SessionContextHolder.getContext();
		String allowedAoAccess = "";
		if(ValidationUtil.isValid(visionUsersVb.getRestrictedAo())) {
			String aoBackuplst[] = 	visionUsersVb.getRestrictedAo().split(",");
			for(int ctr = 0;ctr < aoBackuplst.length;ctr++) {
				String accessXml = getAoAccessRights(visionUsersVb.getAccountOfficer(),aoBackuplst[ctr]);
				String permission = CommonUtils.getValueForXmlTag(accessXml, module);
				if(ValidationUtil.isValid(permission) && "Y".equalsIgnoreCase(permission)) {
					if(ValidationUtil.isValid(allowedAoAccess)) {
						allowedAoAccess = allowedAoAccess+"','"+aoBackuplst[ctr];
					}else {
						allowedAoAccess = aoBackuplst[ctr];
					}
				}
			}
			allowedAoAccess = ",'"+allowedAoAccess+"'";
		}
		return allowedAoAccess;
	}
	public String getAoAccessRights(String accountOfficer,String backupAo) {
		try {
			String query = " SELECT "+
					"  Access_Rights_Xml "+
					"  FROM MDM_AO_LEAVE_MGMT Where account_Officer = '"+backupAo+"' "+
				    "  and AO_back_up = '"+accountOfficer+"' and To_date(sysdate,'DD-MM-RRRR') >= To_date(leave_from,'DD-MM-RRRR') "+
				    "  and To_date(sysdate,'DD-MM-RRRR') <= To_date(leave_to,'DD-MM-RRRR') ";
			return jdbcTemplate.queryForObject(query,null,String.class);
		}catch(Exception e) {
			return null;
		}
	}
	/* Update to SQL Query */
	public String getUserHomeDashboard(VisionUsersVb visionUsersVb) {
		String homeDashboard = "NA";
		String query = "";
		try {
			query = " Select "+getDbFunction("NVL")+"(Home_dashboard,'NA') from PRD_PROFILE_DASHBOARDS where "
					+ "User_group ||'-'||User_Profile = ? and Application_access = ? ";

			Object[] lParams = new Object[2];
			lParams[0] = visionUsersVb.getUserGrpProfile();
			lParams[1] = productName;
			homeDashboard = jdbcTemplate.queryForObject(query, lParams, String.class);
			if (!ValidationUtil.isValid(homeDashboard)) {
				homeDashboard = "NA";
			}
		} catch (Exception e) {
		}
		return homeDashboard;
	}
	//This Function is Hard Coded for SBU Logic for Fidelity
	public String getVisionSbu(String parentVal) {
		String sbu="";
		try {
			sbu = jdbcTemplate.queryForObject(" SELECT DISTINCT "+
					" ''''|| LISTAGG (VISION_SBU, ''',''') WITHIN GROUP (ORDER BY VISION_SBU)|| '''' "+
					" Sbu FROM (SELECT DISTINCT VISION_SBU FROM VISION_SBU_MDM "+
					" WHERE    VISION_SBU IN ("+parentVal+") OR PARENT_SBU IN ("+parentVal+") OR DIVISON IN ("+parentVal+") "+
					" OR BANK_GROUP IN ("+parentVal+") ) ",null,String.class);
			
			System.out.println("Sbu Query:"+sbu);
		}catch(Exception e) {
			System.out.println("Error while getting SBU:"+e.getMessage());
			e.printStackTrace();
			sbu = "";
		}
		return sbu;
	}
	public static String getMacAddress(String ip) throws IOException {
		String address = null;
		String str = "";
		String macAddress = "";
		try {

			String cmd = "arp -a " + ip;
			Scanner s = new Scanner(Runtime.getRuntime().exec(cmd).getInputStream());
			Pattern pattern = Pattern
					.compile("(([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2})|(([0-9A-Fa-f]{4}\\.){2}[0-9A-Fa-f]{4})");
			try {
				while (s.hasNext()) {
					str = s.next();
					Matcher matcher = pattern.matcher(str);
					if (matcher.matches()) {
						break;
					} else {
						str = null;
					}
				}
			} finally {
				s.close();
			}
			if (!ValidationUtil.isValid(str)) {
				return ip;
			}
			return (str != null) ? str.toUpperCase() : null;
		} catch (SocketException ex) {
			ex.printStackTrace();
			return ip;
		}
	}
	public List getAllBusinessDate(){
		try
		{	
			String query = "";
			if ("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				query = " SELECT COUNTRY+'-'+LE_BOOK as COUNTRY, "+  
					    " FORMAT(BUSINESS_DATE,'dd-MMM-yyyy') VBD, "+ 
						" FORMAT(CONVERT(DATE,CONCAT(BUSINESS_YEAR_MONTH,'01')),'MMM-yyyy') VBM , "+
						" FORMAT(BUSINESS_WEEK_DATE,'dd-MMM-yyyy') VBW, "+  
						" FORMAT(CONVERT(DATE,CONCAT(BUSINESS_QTR_YEAR_MONTH,'01')),'MMM-yyyy') VBQ "+
						" FROM VISION_BUSINESS_DAY ";
			}else if ("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				query = " SELECT COUNTRY||'-'||LE_BOOK COUNTRY, "+
					    " TO_CHAR(BUSINESS_DATE,'DD-Mon-RRRR') VBD, "+
						" TO_CHAR(TO_DATE(BUSINESS_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') VBM, "+
						" TO_CHAR(BUSINESS_WEEK_DATE,'DD-Mon-RRRR') VBW, "+
						" TO_CHAR(TO_DATE(BUSINESS_QTR_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') VBQ "+
						" FROM VISION_BUSINESS_DAY";
			}
			ExceptionCode exceptionCode = commonApiDao.getCommonResultDataQuery(query);
			List resultlst = (List)exceptionCode.getResponse();
			return resultlst;
		}catch(Exception e) {
			return null;
		}
	}
	public ExceptionCode getReqdConnection(Connection conExt,String connectionName) {
		ExceptionCode exceptionCodeCon = new ExceptionCode();
		try {
			if (!ValidationUtil.isValid(connectionName)
					|| "DEFAULT".equalsIgnoreCase(connectionName)) {
				//conExt = commonApiDao.getConnection();
				conExt = jdbcTemplate.getDataSource().getConnection();
				exceptionCodeCon.setResponse(conExt);
			} else {
				String dbScript = getScriptValue(connectionName);
				if(!ValidationUtil.isValid(dbScript)) {
					exceptionCodeCon.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCodeCon.setErrorMsg("DB Connection Name is Invalid");
					return exceptionCodeCon;
				}
				exceptionCodeCon = CommonUtils.getConnection(dbScript);
			}
			exceptionCodeCon.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		}catch(Exception e) {
			exceptionCodeCon.setErrorMsg(e.getMessage());
			exceptionCodeCon.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		return exceptionCodeCon;
	}
	public int updateWidgetCreationStagingTable(String tableName) {
		String sql = "UPDATE VWC_STAGGING_TABLE_LOGGING SET PROCESSED = 'Y' , DATE_LAST_MODIFIED = sysdate WHERE TABLE_NAME = '"
				+ tableName + "'";
		return jdbcTemplate.update(sql);
	}
	public String getCurrentDateInfo(String option,String ctryLeBook) {
		String val ="";
		String sql = "";
		try {
			switch(option) {
				case "CYM":
					if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
						sql = "SELECT TO_CHAR(To_Date(BUSINESS_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') FROM VISION_BUSINESS_DAY "+
                        " WHERE COUNTRY||'-'||LE_BOOK = ? ";
					}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
						sql  = "SELECT BUSINESS_YEAR_MONTH   FROM VISION_BUSINESS_DAY  WHERE COUNTRY+'-'+LE_BOOK = ? ";
					}
				break;
				case "CY":
					if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
						sql = "SELECT TO_CHAR(To_Date(BUSINESS_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') FROM VISION_BUSINESS_DAY "+
	                        " WHERE COUNTRY||'-'||LE_BOOK = ? ";
					}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
						sql  = "SELECT BUSINESS_YEAR_MONTH   FROM VISION_BUSINESS_DAY  WHERE COUNTRY+'-'+LE_BOOK = ? ";
					}
				break;
				default:
					if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
						sql = "SELECT TO_CHAR(To_Date(BUSINESS_DATE),'DD-Mon-RRRR') FROM VISION_BUSINESS_DAY "+
		                    " WHERE COUNTRY||'-'||LE_BOOK = ? ";
					}else if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
						sql = "SELECT FORMAT(BUSINESS_DATE,'dd-MMM-yyyy')  BUSINESS_DATE FROM VISION_BUSINESS_DAY WHERE COUNTRY+'-'+LE_BOOK = ?";  
					}
				break;	
			}
			String args[] = {ctryLeBook};
			
			val = jdbcTemplate.queryForObject(sql, args,String.class);
		}catch(Exception e) {
			e.printStackTrace();
		}
		return val;
	}
	public String getDbFunction(String reqFunction) {
		String functionName = "";
		if("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
			switch(reqFunction) {
			case "DATEFUNC":
				functionName = "FORMAT";
				break;
			case "SYSDATE":
				functionName = "GetDate()";
				break;
			case "NVL":
				functionName = "ISNULL";
				break;
			case "TIME":
				functionName = "HH:mm:ss";
				break;
			case "DATEFORMAT":
				functionName = "dd-MMM-yyyy";
				break;
			case "CONVERT":
				functionName = "CONVERT";
				break;
			case "TYPE":
				functionName = "varchar,";
				break;
			case "TIMEFORMAT":
				functionName = "108";
				break;
			}
		}else if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
			switch(reqFunction) {
			case "DATEFUNC":
				functionName = "TO_CHAR";
				break;
			case "SYSDATE":
				functionName = "SYSDATE";
				break;
			case "NVL":
				functionName = "NVL";
				break;
			case "TIME":
				functionName = "HH24:MI:SS";
				break;
			case "DATEFORMAT":
				functionName = "DD-Mon-RRRR";
				break;
			case "CONVERT":
				functionName = "TO_CHAR";
				break;
			case "TYPE":
				functionName = "";
				break;
			case "TIMEFORMAT":
				functionName = "'HH:MM:SS'";
				break;
			}
		}
		
		return functionName;
	}
	public List getDateFormatforCaption(){
		try
		{	
			String query = "";
			VisionUsersVb visionUsersVb = SessionContextHolder.getContext();
			if ("MSSQL".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				query = " SELECT COUNTRY+'-'+LE_BOOK as COUNTRY, "+  
					    " FORMAT(BUSINESS_DATE,'dd-MMM-yyyy') VBD, "+ 
						" FORMAT(CONVERT(DATE,CONCAT(BUSINESS_YEAR_MONTH,'01')),'MMM-yyyy') VBM , "+
						" FORMAT(BUSINESS_WEEK_DATE,'dd-MMM-yyyy') VBW, "+ 
						" FORMAT(CONVERT(DATE,CONCAT(BUSINESS_QTR_YEAR_MONTH,'01')),'MMM-yyyy') VBQ "+
						" FORMAT(BUSINESS_DATE,'yyyy') CY, "+
						" (select value from vision_variables where variable = 'CURRENT_MONTH') CM "+
						" FROM VISION_BUSINESS_DAY "+
						"  WHERE COUNTRY = '"+visionUsersVb.getCountry()+"' AND LE_BOOK = '"+visionUsersVb.getLeBook()+"'";;
			}else if ("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE)) {
				query = " SELECT COUNTRY||'-'||LE_BOOK COUNTRY, "+
                        "  TO_CHAR(BUSINESS_DATE,'DD-Mon-RRRR') VBD, "+
                        "  TO_CHAR(TO_DATE(BUSINESS_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') VBM, "+
                        "  TO_CHAR(BUSINESS_WEEK_DATE,'DD-Mon-RRRR') VBW, "+
                        "  TO_CHAR(TO_DATE(BUSINESS_QTR_YEAR_MONTH,'RRRRMM'),'Mon-RRRR') VBQ, "+
                        "  TO_CHAR(BUSINESS_DATE,'RRRR') CY, "+
                        "  (select value from vision_variables where variable = 'CURRENT_MONTH') CM, "+
                        "  (SELECT TO_CHAR(CURRENT_TIMESTAMP, 'HH24:MI:SS') FROM dual) SYSTIME, "+
                        "  (SELECT SYSDATE FROM DUAL) SYSTEMDATE, "+
                        "  TO_CHAR(PREV_BUSINESS_DATE,'DD-Mon-RRRR') PVBD, "+ //#VBD-1#
                        "   TO_CHAR(ADD_MONTHS(BUSINESS_DATE,-1),'DD-Mon-RRRR') PMVBD , "+
                        "   TO_CHAR(ADD_MONTHS(BUSINESS_DATE,-12),'DD-Mon-RRRR') PYVBD, "+ 
                        "   TO_CHAR(ADD_MONTHS(BUSINESS_DATE,-1)-1,'DD-Mon-RRRR') PMPVBD "+ //#PMVBD-1#
                        "  FROM VISION_BUSINESS_DAY "+
                        "  WHERE COUNTRY = '"+visionUsersVb.getCountry()+"' AND LE_BOOK = '"+visionUsersVb.getLeBook()+"'";
			}
			ExceptionCode exceptionCode = commonApiDao.getCommonResultDataQuery(query);
			List resultlst = (List)exceptionCode.getResponse();
			return resultlst;
		}catch(Exception e) {
			return null;
		}
	}
	public List<VisionUsersVb> getSenderVisionIdEmailId(VisionUsersVb dObj) {
		List<VisionUsersVb> visionusersList = new ArrayList<VisionUsersVb>();
		StringBuffer query = new StringBuffer("Select Vision_ID,User_Email_ID from Vision_Users where Vision_ID ='"+dObj.getVisionId()+"' ");
		Object[] args = null;
		try{
			visionusersList = jdbcTemplate.query(query.toString(),getVisionuserLst());
			return visionusersList;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	private RowMapper getVisionuserLst() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VisionUsersVb visionUsersVb = new VisionUsersVb();
				visionUsersVb.setVisionId(rs.getInt("VISION_ID"));
				visionUsersVb.setUserEmailId(rs.getString("USER_EMAIL_ID"));
				return visionUsersVb;
			}
		};
		return mapper;
	}
	public int getAuditMaxSeq(String processId) throws Exception {
		int variableValue = 0;
		String query = "Select "+ new CommonDao(jdbcTemplate).getDbFunction("NVL")+"(Max(AUDIT_TRAIL_SEQUENCE_ID),0) Max_Seq from PRD_RSS_AUDIT_TRAIL where RSS_Process_ID = ?";
		Object[] args = {processId};
		try{
			variableValue = jdbcTemplate.queryForObject(query, args, Integer.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return variableValue;
	}
	public List<String> getDynamicHashVariable(String variableName) throws Exception {
		String query = "SELECT VARIABLE_SCRIPT, SCRIPT_TYPE FROM VISION_DYNAMIC_HASH_VAR WHERE VARIABLE_NAME = '"+variableName+"' ";
		List<String> commonVbs = new ArrayList<String>();
		try{
			 return jdbcTemplate.query(query, new ResultSetExtractor<List<String>>() {
				@Override
				public List<String> extractData(ResultSet rs) throws SQLException, DataAccessException {
					while (rs.next()) {
						commonVbs.add(rs.getString("VARIABLE_SCRIPT"));
						commonVbs.add(rs.getString("SCRIPT_TYPE"));
					}
					return commonVbs;
				}});
		} catch (Exception e) {
			e.printStackTrace();
		}
		return commonVbs;
	}
	public VisionUsersVb getVisionUserDetails (String visionId){
		VisionUsersVb visionUsersVb = new VisionUsersVb();
		List<VisionUsersVb> collTemp = new ArrayList<>();
		String query = "SELECT VISION_ID,USER_GROUP,USER_PROFILE FROM VISION_USERS WHERE VISION_ID = '"+ScheduledReportWb.currentUser+"'";
		try {
			collTemp =  jdbcTemplate.query(query, getVisionuserDetailMapper());
			visionUsersVb = collTemp.get(0);
		}catch(Exception e) {
			e.printStackTrace();
		}
		return visionUsersVb;
	}
	private RowMapper getVisionuserDetailMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VisionUsersVb visionUsersVb = new VisionUsersVb();
				visionUsersVb.setVisionId(rs.getInt("VISION_ID"));
				visionUsersVb.setUserGroup(rs.getString("USER_GROUP"));
				visionUsersVb.setUserProfile(rs.getString("USER_PROFILE"));
				return visionUsersVb;
			}
		};
		return mapper;
	}
	public List<UserRestrictionVb> getRestrictionTree() throws DataAccessException {
		String sql = "select MACROVAR_NAME,TAG_NAME, DISPLAY_NAME, MACROVAR_DESC from MACROVAR_TAGGING where MACROVAR_TYPE = 'DATA_RESTRICTION' order by MACROVAR_NAME, TAG_NO";
		return jdbcTemplate.query(sql, new ResultSetExtractor<List<UserRestrictionVb>>() {
			@Override
			public List<UserRestrictionVb> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<UserRestrictionVb> returnList = new ArrayList<UserRestrictionVb>();
				while (rs.next()) {
					String macroVar = rs.getString("MACROVAR_NAME");
					List<UserRestrictionVb> filteredList = returnList.stream()
							.filter(vb -> macroVar.equalsIgnoreCase(vb.getMacrovarName())).collect(Collectors.toList());
					if (filteredList != null && filteredList.size() > 0) {
						List<UserRestrictionVb> childrenList = filteredList.get(0).getChildren();
						childrenList.add(new UserRestrictionVb(macroVar, rs.getString("TAG_NAME"),
								rs.getString("DISPLAY_NAME"), rs.getString("MACROVAR_DESC")));
					} else {
						List<UserRestrictionVb> childrenList = new ArrayList<UserRestrictionVb>();
						childrenList.add(new UserRestrictionVb(macroVar, rs.getString("TAG_NAME"),
								rs.getString("DISPLAY_NAME"), rs.getString("MACROVAR_DESC")));
						UserRestrictionVb userRestrictionVb = new UserRestrictionVb();
						userRestrictionVb.setMacrovarName(macroVar);
						userRestrictionVb.setMacrovarDesc(rs.getString("MACROVAR_DESC"));
						userRestrictionVb.setChildren(childrenList);
						returnList.add(userRestrictionVb);
					}
				}
				return returnList;
			}
		});
	}
	public String replacehashPrompt(String query,String restrictStr,String restrictVal,String updateRestriction) {
		try {
			String replaceStr = "";
			String orgSbuStr = StringUtils.substringBetween(query, restrictStr, ")#");
			if(ValidationUtil.isValid(orgSbuStr)) {
				if ("Y".equalsIgnoreCase(updateRestriction) && ValidationUtil.isValid(restrictVal))
					replaceStr = " AND " + orgSbuStr + " IN (" + restrictVal + ")";
				
				restrictStr = restrictStr.replace("(", "\\(");
				orgSbuStr = orgSbuStr.replace("|", "\\|");
				query = query.replaceAll(restrictStr + orgSbuStr + "\\)#", replaceStr);	
			}
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		return query;
	}
	public String applyUserRestriction(String sqlQuery) {
		VisionUsersVb visionUserVb = SessionContextHolder.getContext();
		//VU_CLEB,VU_CLEB_AO,VU_CLEB_LV,VU_SBU,VU_PRODUCT,VU_OUC
		if(sqlQuery.contains("#VU_CLEB")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB(", visionUserVb.getCountry(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_CLEB_AO")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB_AO(", visionUserVb.getAccountOfficer(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_CLEB_LV")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_CLEB_LV(", visionUserVb.getLegalVehicle(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_LV_CLEB"))
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_LV_CLEB(", visionUserVb.getLegalVehicleCleb(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_SBU")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_SBU(", visionUserVb.getSbuCode(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_PRODUCT")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_PRODUCT(", visionUserVb.getProductAttribute(),visionUserVb.getUpdateRestriction());
		if(sqlQuery.contains("#VU_OUC")) 
			sqlQuery = replacehashPrompt(sqlQuery, "#VU_OUC(", visionUserVb.getOucAttribute(),visionUserVb.getUpdateRestriction());	
		return sqlQuery;
	}
	public String[][] PromptSplit(String promptValue,String query,String twoD_arr[][],int ctr1){
		String promptArrComma[] = promptValue.split(",");
		HashMap<String,String> prompt1Map = new HashMap<>();
		int ctr = 1;
		for(String str : promptArrComma) {
			prompt1Map.put(""+ctr, str);
			ctr++;
		}
		if(prompt1Map.size() > 0) {
			for (Map.Entry<String, String> set :prompt1Map.entrySet()) {
				String totalSplit[] = set.getValue().split("-");
				int ctr2 = 0;
				for(String str : totalSplit) {
					str = str.replaceAll("'", "");
					if(ValidationUtil.isValid(twoD_arr[ctr1][ctr2])) {
						twoD_arr[ctr1][ctr2] = twoD_arr[ctr1][ctr2]+","+"'"+str+"'";	
					}else {
						twoD_arr[ctr1][ctr2] = "'"+str+"'";
					}
					ctr2++;
				}
			}
		}
		return twoD_arr;
	}
	public String repSpecialCharExcelName(String workSheetName) {
		try {
			String[] strArr = { "[", "]", "/", "\\", "?", "*", ":" };
			for (String str : strArr) {
				if (workSheetName.contains(str)) {
					workSheetName = workSheetName.replaceAll(str, " ");
				}
			}
		}catch(Exception e) {
		}
		return workSheetName;
	}
}
