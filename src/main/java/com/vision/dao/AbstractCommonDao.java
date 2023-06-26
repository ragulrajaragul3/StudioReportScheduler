package com.vision.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.DCManualQueryVb;
import com.vision.vb.VcConfigMainColumnsVb;
import com.vision.vb.VcConfigMainTreeVb;
public abstract class AbstractCommonDao {
	
	@Autowired
	CommonDao commonDao;

	@Autowired
	JdbcTemplate jdbcTemplate;

	private int batchLimit = 5000;
	
	public Connection getConnection() {
		try {
			return getJdbcTemplate().getDataSource().getConnection();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	Logger logger = LoggerFactory.getLogger(this.getClass());

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	protected String strErrorDesc = "";
	protected String strCurrentOperation = "";
	protected String strApproveOperation = "";// Current operation for Audit purpose
	protected long intCurrentUserId = 0;// Current user Id of incoming request
	protected int retVal = 0;
	protected String serviceName = "";
	protected String serviceDesc = "";// Display Name of the service.
	protected String tableName = "";
	protected String childTableName = "";
	protected String userGroup = "";
	protected String userProfile = "";
	protected String userGrpProfile = "";
	
	@Autowired
	protected AuditTrailDataDao auditTrailDataDao;

	public AuditTrailDataDao getAuditTrailDataDao() {
		return auditTrailDataDao;
	}

	public void setAuditTrailDataDao(AuditTrailDataDao auditTrailDataDao) {
		this.auditTrailDataDao = auditTrailDataDao;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getChildTableName() {
		return childTableName;
	}

	public void setChildTableName(String childTableName) {
		this.childTableName = childTableName;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getServiceDesc() {
		return serviceDesc;
	}

	public void setServiceDesc(String serviceDesc) {
		this.serviceDesc = serviceDesc;
	}

	public String getStrErrorDesc() {
		return strErrorDesc;
	}

	public void setStrErrorDesc(String strErrorDesc) {
		this.strErrorDesc = strErrorDesc;
	}

	public String getStrCurrentOperation() {
		return strCurrentOperation;
	}

	public void setStrCurrentOperation(String strCurrentOperation) {
		this.strCurrentOperation = strCurrentOperation;
	}

	public String getStrApproveOperation() {
		return strApproveOperation;
	}

	public void setStrApproveOperation(String strApproveOperation) {
		this.strApproveOperation = strApproveOperation;
	}

	public long getIntCurrentUserId() {
		return intCurrentUserId;
	}

	public void setIntCurrentUserId(long intCurrentUserId) {
		this.intCurrentUserId = intCurrentUserId;
	}

	public int getRetVal() {
		return retVal;
	}

	public void setRetVal(int retVal) {
		this.retVal = retVal;
	}

	public String getSystemDate() {
		String sql = "SELECT To_Char(SysDate, 'DD-MM-YYYY HH24:MI:SS') AS SYSDATE1 FROM DUAL";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				return (rs.getString("SYSDATE1"));
			}
		};
		return (String) getJdbcTemplate().queryForObject(sql, null, mapper);
	}

	protected String getReferenceNo() {
		String sql = "SELECT TO_CHAR(SYSTIMESTAMP,'yyyymmddhh24missff') as timestamp FROM DUAL";
		try {
			RowMapper mapper = new RowMapper() {
				public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
					return (rs.getString("timestamp"));
				}
			};
			return (String) getJdbcTemplate().queryForObject(sql, null, mapper);
		} catch (Exception e) {
			return CommonUtils.getReferenceNo();
		}
	}

	protected ExceptionCode getResultObject(int intErrorId) {
		if (intErrorId == Constants.ERRONEOUS_OPERATION)
			return CommonUtils.getResultObject(serviceDesc, intErrorId, strCurrentOperation, strErrorDesc);
		else
			return CommonUtils.getResultObject(serviceDesc, intErrorId, strCurrentOperation, "");
	}

	protected String parseErrorMsg(UncategorizedSQLException ecxception) {
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

	protected RuntimeCustomException buildRuntimeCustomException(ExceptionCode rObject) {
		RuntimeCustomException lException = new RuntimeCustomException(rObject);
		return lException;
	}

	/**
	 * This method used to get the status of the Build Module
	 *
	 * @param String Service Name
	 * @returns String The running status of Build Module
	 */
	protected String getBuildModuleStatus(String country, String leBook) {
		int lockCount = 0;

		StringBuffer strBufApprove = new StringBuffer("Select count(LOCK_STATUS) STATUS_COUNT " + "FROM VISION_LOCKING "
				+ "WHERE LOCK_STATUS = 'Y' " + " AND SERVICE_NAME = ?" + " AND COUNTRY = ?" + " AND LE_BOOK = ?");

		StringBuffer defaultQuery = new StringBuffer(
				" Select count(LOCK_STATUS) STATUS_COUNT FROM VISION_LOCKING WHERE LOCK_STATUS = 'Y'  "
						+ "AND SERVICE_NAME = ? AND COUNTRY = NVL((SELECT VALUE FROM VISION_VARIABLES WHERE VARIABLE ='DEFAULT_COUNTRY'),'NG') "
						+ "AND LE_BOOK = NVL((SELECT VALUE FROM VISION_VARIABLES WHERE VARIABLE ='DEFAULT_LEBOOK'),'01')");
		try {

			if (ValidationUtil.isValid(country) && ValidationUtil.isValid(leBook)) {
				Object objParams[] = new Object[3];
				objParams[0] = getServiceName();
				objParams[1] = country;
				objParams[2] = leBook;
				lockCount = getJdbcTemplate().queryForObject(strBufApprove.toString(), objParams, Integer.class);
			} else {
				Object objParams[] = new Object[1];
				objParams[0] = getServiceDesc();
				lockCount = getJdbcTemplate().queryForObject(defaultQuery.toString(), objParams, Integer.class);
			}
			if (lockCount > 0)
				return "RUNNING";

			return "NOT-RUNNING";
		} catch (Exception ex) {
			ex.printStackTrace();
			return "NOT-RUNNING";
		}
	}

	public String removeDescLeBook(String promptString) {
		if (ValidationUtil.isValid(promptString)) {
			return promptString.substring(0,
					promptString.indexOf("-") > 0 ? promptString.indexOf("-") - 1 : promptString.length());
		} else
			return promptString;
	}

	public List<AlphaSubTabVb> greaterLesserSymbol(String ipString, boolean chgFlag) {
		List<AlphaSubTabVb> collTemp = null;
		try {
			if (chgFlag = true) {
				ipString = ipString.replaceAll("lessRep", "<").replaceAll("grtRep", ">");
			} else {
				ipString = ipString.replaceAll("<", "lessRep").replaceAll(">", "grtRep");
			}
			collTemp = getJdbcTemplate().query(ipString, getMapperforDropDown());
		} catch (Exception e) {
			e.printStackTrace();
			return collTemp;
		}

		return collTemp;
	}

	public RowMapper getMapperforDropDown() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				AlphaSubTabVb alphaSubTabVb = new AlphaSubTabVb();
				alphaSubTabVb.setAlphaSubTab(rs.getString(1));
				alphaSubTabVb.setDescription(rs.getString(2));
				return alphaSubTabVb;
			}
		};
		return mapper;
	}

	public String getServerCredentials(String enironmentVariable, String node, String columnName) {
		String sql = "SELECT " + columnName + " FROM VISION_NODE_CREDENTIALS WHERE SERVER_ENVIRONMENT='"
				+ enironmentVariable + "' AND NODE_NAME='" + node + "' AND ROWNUM<2";
		return getJdbcTemplate().queryForObject(sql, String.class);
	}

	public String getUserGroup() {
		return userGroup;
	}

	public void setUserGroup(String userGroup) {
		this.userGroup = userGroup;
	}

	public String getUserProfile() {
		return userProfile;
	}

	public void setUserProfile(String userProfile) {
		this.userProfile = userProfile;
	}
	public Map<String, List> getRestrictionTreeForLogin() throws DataAccessException {
		String sql = "select MACROVAR_NAME,TAG_NAME, DISPLAY_NAME, MACROVAR_DESC from MACROVAR_TAGGING where MACROVAR_TYPE = 'DATA_RESTRICTION' order by MACROVAR_NAME, TAG_NO";
		return getJdbcTemplate().query(sql, new ResultSetExtractor<Map<String, List>>(){
			@Override
			public Map<String, List> extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				Map<String, List> returnMap = new HashMap<String, List>();
				while(rs.next()){
					if(returnMap.get(rs.getString("MACROVAR_NAME"))==null){
						List<String> tagList = new ArrayList<String>();
						tagList.add(rs.getString("TAG_NAME"));
						returnMap.put(rs.getString("MACROVAR_NAME"), tagList);
					} else {
						List<String> tagList = returnMap.get(rs.getString("MACROVAR_NAME"));
						tagList.add(rs.getString("TAG_NAME"));
						returnMap.put(rs.getString("MACROVAR_NAME"), tagList);
					}
				}
				return returnMap;
			}
		});
	}
	public Connection returnConnection() {
		return getConnection();
	}
	private void dropTemporaryCreatedTable(String temporaryTableName) {
		try {
			String sql = "DROP TABLE "+temporaryTableName+" PURGE";
			getJdbcTemplate().execute(sql);
		}catch(Exception e) {}
	}
	
	private Map<String, String> returnDateConvertionSyntaxMap(String dataBaseType) {
		
		String sql = "SELECT * FROM MACROVAR_TAGGING WHERE MACROVAR_TYPE = 'DATE_CONVERT' AND UPPER(MACROVAR_NAME) = UPPER('"+dataBaseType+"')";
		
		return getJdbcTemplate().query(sql, new ResultSetExtractor<Map<String, String>>(){
			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				Map<String, String> map = new HashMap<String, String>();
				while(rs.next()) {
					map.put(rs.getString("TAG_NAME"), rs.getString("DISPLAY_NAME"));
				}
				return map;
			}
		});
		
	}
	
	private Map<String, String> returnDateFormatSyntaxMap(String dataBaseType) {
		
		String sql = "SELECT * FROM MACROVAR_TAGGING WHERE MACROVAR_TYPE = 'DATE_FORMAT' AND UPPER(MACROVAR_NAME) = UPPER('"+dataBaseType+"')";
		
		return getJdbcTemplate().query(sql, new ResultSetExtractor<Map<String, String>>(){
			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				Map<String, String> map = new HashMap<String, String>();
				while(rs.next()) {
					map.put(rs.getString("TAG_NAME"), rs.getString("DISPLAY_NAME"));
				}
				return map;
			}
		});
		
	}
	
	public int getBatchLimit() {
		return batchLimit;
	}
	
	public void setBatchLimit(int batchLimit) {
		this.batchLimit = batchLimit;
	}
	
	public String createRandomTableName() {
		String sessionId = ValidationUtil.generateRandomNumberForVcReport();
		return "VC_CV_"+intCurrentUserId+sessionId;
	}
	
	public String getVisionDynamicHashVariable(String variableName){
		try{
			String sql = "select VARIABLE_SCRIPT from vision_dynamic_hash_var where VARIABLE_NAME = 'VU_RESTRICTION_"+variableName+"'";
			return getJdbcTemplate().queryForObject(sql, String.class);
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	/*public Map<String, List> getRestrictionTree() throws DataAccessException {
		String sql = "select MACROVAR_NAME,TAG_NAME, DISPLAY_NAME, MACROVAR_DESC from MACROVAR_TAGGING where MACROVAR_TYPE = 'DATA_RESTRICTION' order by MACROVAR_NAME, TAG_NO";
		return getJdbcTemplate().query(sql, new ResultSetExtractor<Map<String, List>>(){
			@Override
			public Map<String, List> extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				Map<String, List> returnMap = new HashMap<String, List>();
				while(rs.next()){
					if(returnMap.get(rs.getString("MACROVAR_NAME"))==null){
						List<String> tagList = new ArrayList<String>();
						tagList.add(rs.getString("TAG_NAME"));
						returnMap.put(rs.getString("MACROVAR_NAME"), tagList);
					} else {
						List<String> tagList = returnMap.get(rs.getString("MACROVAR_NAME"));
						tagList.add(rs.getString("TAG_NAME"));
						returnMap.put(rs.getString("MACROVAR_NAME"), tagList);
					}
				}
				return returnMap;
			}
		});
	}*/
/*public List<UserRestrictionVb> getRestrictionTree() throws DataAccessException {
		String sql = "select MACROVAR_NAME,TAG_NAME, DISPLAY_NAME, MACROVAR_DESC from MACROVAR_TAGGING where MACROVAR_TYPE = 'DATA_RESTRICTION' order by MACROVAR_NAME, TAG_NO";
		return getJdbcTemplate().query(sql, new ResultSetExtractor<List<UserRestrictionVb>>() {
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
*/
	private Map<String, Object> formScriptsForTemporaryTable(VcConfigMainTreeVb treeVb, String srcTableName,
			String targetTableName, String dataBaseType) {
		StringBuffer createStr = new StringBuffer("CREATE TABLE " + targetTableName + " ( ");
		StringBuffer selectStr = new StringBuffer("SELECT ");
		StringBuffer selectStrForDirectInsert = new StringBuffer("SELECT ");
		StringBuffer insertStr = new StringBuffer("INSERT INTO " + targetTableName + " ( ");
		StringBuffer insertValueStr = new StringBuffer(" VALUES ( ");
		List<String> insertValuePlaceholderList = new ArrayList<String>();
		final String COMMA = ",";
		final String PLACE_HOLDER = "?";
		final String SPACE = " ";
		final String NUMBER_STRING = " NUMBER";
//		final String CHAR_STRING = " CHAR"; Commented bcz of fixed length - DD
		final String CHAR_STRING = " VARCHAR2";
		final String DECIMAL_STRING = " NUMBER";
		final String VARCHAR_STRING = " VARCHAR2";
		final String DATE_STRING = " DATE ";
		final String BYTE_STRING = " BYTE";
		final String NUMBER_MAX_LENGTH = "38";
		final String VARCHAR_MAX_LENGTH = "4000";
		final String DECIMAL_MAX_LENGTH = "38,10";
		final String CHAR_MAX_LENGTH = "2000";
		final String openBraket = "(";
		final String closeBraket = ")";
		final String QUOTE = "'";
		final String PLACE_HOLDER_VALUE = "v";
		int countIndex = 1;

		if (!ValidationUtil.isValid(dataBaseType))
			dataBaseType = "ORACLE";

		Map<String, String> dateConversionMap = returnDateConvertionSyntaxMap(dataBaseType);

		Map<String, String> dateFormatMap = returnDateFormatSyntaxMap(dataBaseType);

		for (VcConfigMainColumnsVb columnVb : treeVb.getChildren()) {
			String dateConvertSyntax = dateConversionMap.get(columnVb.getFormatType());
			String dateFormatSyntax = dateFormatMap.get(columnVb.getFormatType());
			String length = columnVb.getColLength();
			createStr.append(columnVb.getColName() + SPACE);
			if ("C".equalsIgnoreCase(columnVb.getColDisplayType())) {
				createStr.append(NUMBER_STRING + openBraket
						+ (ValidationUtil.isValid(length) ? length : NUMBER_MAX_LENGTH) + closeBraket);
			} else if ("D".equalsIgnoreCase(columnVb.getColDisplayType())) {
				createStr.append(DATE_STRING);
			} else if ("N".equalsIgnoreCase(columnVb.getColDisplayType())) {
				createStr.append(DECIMAL_STRING + openBraket
						+ (ValidationUtil.isValid(length) ? length : DECIMAL_MAX_LENGTH) + closeBraket);
			} else if ("T".equalsIgnoreCase(columnVb.getColDisplayType())) {
				createStr.append(CHAR_STRING + openBraket + (ValidationUtil.isValid(length) ? length : CHAR_MAX_LENGTH)
						+ SPACE + BYTE_STRING + closeBraket);
			} else if ("Y".equalsIgnoreCase(columnVb.getColDisplayType())) {
				createStr.append(
						VARCHAR_STRING + openBraket + (ValidationUtil.isValid(length) ? length : VARCHAR_MAX_LENGTH)
								+ SPACE + BYTE_STRING + closeBraket);
			} else {
				throw new RuntimeCustomException(
						"No proper data-type maintained for the column [" + columnVb.getAliasName() + "] of table ["
								+ treeVb.getAliasName() + "] from catalog [" + treeVb.getCatalogId() + "]");
			}
			if ("T".equalsIgnoreCase(columnVb.getColType())) {
				if ("D".equalsIgnoreCase(columnVb.getColDisplayType())) {

					/*
					 * selectStr.append("TO_CHAR(TO_DATE("+columnVb.getColName()+", '"+columnVb.
					 * getFormatTypeDesc()+"'), '"+columnVb.getFormatTypeDesc()+"') "+SPACE+columnVb
					 * .getColName());
					 * selectStrForDirectInsert.append("TO_DATE("+columnVb.getColName()+", '"
					 * +columnVb.getFormatTypeDesc()+"')"+SPACE+columnVb.getColName());
					 * insertValuePlaceholderList.add("TO_DATE('v',"+QUOTE+columnVb.
					 * getFormatTypeDesc()+QUOTE+closeBraket);
					 * insertValueStr.append("TO_DATE("+PLACE_HOLDER+COMMA+QUOTE+columnVb.
					 * getFormatTypeDesc()+QUOTE+closeBraket);
					 */

					dateConvertSyntax = dateConvertSyntax.replaceAll("#VALUE#", columnVb.getColName());
					dateFormatSyntax = dateFormatSyntax.replaceAll("#VALUE#", dateConvertSyntax);

					selectStr.append(dateFormatSyntax + SPACE + columnVb.getColName());
					selectStrForDirectInsert.append(dateConvertSyntax + SPACE + columnVb.getColName());
					insertValuePlaceholderList
							.add("TO_DATE('v'," + QUOTE + "DD-MM-RRRR HH24:MI:SS" + QUOTE + closeBraket);
					insertValueStr.append(
							"TO_DATE(" + PLACE_HOLDER + COMMA + QUOTE + "DD-MM-RRRR HH24:MI:SS" + QUOTE + closeBraket);
				} else {
					selectStr.append(columnVb.getColName());
					selectStrForDirectInsert.append(columnVb.getColName());
					insertValuePlaceholderList.add(QUOTE + PLACE_HOLDER_VALUE + QUOTE);
					insertValueStr.append(PLACE_HOLDER);
				}
			} else {
				if ("D".equalsIgnoreCase(columnVb.getColDisplayType())) {

					/*
					 * selectStr.append("TO_CHAR(TO_DATE("+openBraket+columnVb.getExperssionText()+
					 * closeBraket+", '"+columnVb.getFormatTypeDesc()+"'), '"+columnVb.
					 * getFormatTypeDesc()+"') "+SPACE+columnVb.getColName());
					 * selectStrForDirectInsert.append("TO_DATE("+openBraket+columnVb.
					 * getExperssionText()+closeBraket+", '"+columnVb.getFormatTypeDesc()+"')"+SPACE
					 * +columnVb.getColName());
					 * insertValuePlaceholderList.add("TO_DATE('v',"+QUOTE+columnVb.
					 * getFormatTypeDesc()+QUOTE+closeBraket);
					 * insertValueStr.append("TO_DATE("+PLACE_HOLDER+COMMA+QUOTE+columnVb.
					 * getFormatTypeDesc()+QUOTE+closeBraket);
					 */

					dateConvertSyntax = dateConvertSyntax.replaceAll("#VALUE#",
							openBraket + columnVb.getExperssionText() + closeBraket);
					dateFormatSyntax = dateFormatSyntax.replaceAll("#VALUE#", dateConvertSyntax);

					selectStr.append(dateFormatSyntax + SPACE + columnVb.getColName());
					selectStrForDirectInsert.append(dateConvertSyntax + SPACE + columnVb.getColName());
					insertValuePlaceholderList
							.add("TO_DATE('v'," + QUOTE + "DD-MM-RRRR HH24:MI:SS" + QUOTE + closeBraket);
					insertValueStr.append(
							"TO_DATE(" + PLACE_HOLDER + COMMA + QUOTE + "DD-MM-RRRR HH24:MI:SS" + QUOTE + closeBraket);
				} else {
					selectStr.append(
							openBraket + columnVb.getExperssionText() + closeBraket + SPACE + columnVb.getColName());
					selectStrForDirectInsert.append(
							openBraket + columnVb.getExperssionText() + closeBraket + SPACE + columnVb.getColName());
					insertValuePlaceholderList.add(QUOTE + PLACE_HOLDER_VALUE + QUOTE);
					insertValueStr.append(PLACE_HOLDER);
				}
			}
			insertStr.append(columnVb.getColName());

			if (countIndex == treeVb.getChildren().size()) {
				createStr.append(closeBraket);
				selectStr.append(SPACE + "FROM" + SPACE + srcTableName);
				selectStrForDirectInsert.append(SPACE + "FROM" + SPACE + srcTableName);
				insertStr.append(closeBraket);
				insertValueStr.append(closeBraket);
			} else {
				createStr.append(COMMA + SPACE);
				selectStr.append(COMMA + SPACE);
				selectStrForDirectInsert.append(COMMA + SPACE);
				insertStr.append(COMMA + SPACE);
				insertValueStr.append(COMMA + SPACE);
			}
			countIndex++;
		}

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("createString", String.valueOf(createStr));
		returnMap.put("selectString", String.valueOf(selectStr));
		returnMap.put("selectStrForDirectInsert", String.valueOf(selectStrForDirectInsert));
		returnMap.put("insertColumnString", String.valueOf(insertStr));
		returnMap.put("insertValueString", String.valueOf(insertValueStr));
		returnMap.put("insertStringWithPlaceHolder", String.valueOf(insertStr + SPACE + insertValueStr));
		returnMap.put("insertStringWithDirectSelect", String.valueOf(insertStr + SPACE + selectStrForDirectInsert));
		returnMap.put("insertValuePlaceholderList", insertValuePlaceholderList);
		return returnMap;
	}

	public ExceptionCode createTempTableForFile(VcConfigMainTreeVb treeVb, String srcTableName,
			String targetTableName) {

		String level = "Forming scripts";
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			Map<String, Object> returnMap = formScriptsForTemporaryTable(treeVb, srcTableName, targetTableName, null);
			level = "Executing creation script : [" + returnMap.get("createString") + "]";
			getJdbcTemplate().execute(String.valueOf(returnMap.get("createString")));

			level = "Executing insert script : [" + returnMap.get("insertStringWithDirectSelect") + "]";
			getJdbcTemplate().execute((String) returnMap.get("insertStringWithDirectSelect"));
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Exception during " + level + ". Cause [" + e.getMessage() + "].");
		}
		exceptionCode.setResponse(targetTableName);
		return exceptionCode;
	}

	public ExceptionCode createTempTableWithConnectionScript(String connectionScript, VcConfigMainTreeVb treeVb,
			String targetTableName) {

		ExceptionCode exceptionCode = new ExceptionCode();
		Connection srcCon = null;
		Statement stmt = null;
		ResultSet rs = null;
		if (!ValidationUtil.isValid(targetTableName))
			targetTableName = createRandomTableName();

		String level = "Create source connection";
		exceptionCode = CommonUtils.getConnection(connectionScript);
		if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
			srcCon = (Connection) exceptionCode.getResponse();
		} else {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Error during " + level + " .Reason [" + exceptionCode.getErrorMsg() + "]");
			return exceptionCode;
		}

		String dataBaseType = CommonUtils.getValue(connectionScript, "DATABASE_TYPE");

		String dbSetParam1 = CommonUtils.getValue(connectionScript, "DB_SET_PARAM1");
		String dbSetParam2 = CommonUtils.getValue(connectionScript, "DB_SET_PARAM2");
		String dbSetParam3 = CommonUtils.getValue(connectionScript, "DB_SET_PARAM3");
		Connection insertConnection = getConnection();
		try {
//			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			stmt = srcCon.createStatement();
			if (ValidationUtil.isValid(dbSetParam1)) {
				stmt.executeUpdate(dbSetParam1);
			}
			if (ValidationUtil.isValid(dbSetParam2)) {
				stmt.executeUpdate(dbSetParam2);
			}
			if (ValidationUtil.isValid(dbSetParam3)) {
				stmt.executeUpdate(dbSetParam3);
			}

			Map<String, Object> returnMap = formScriptsForTemporaryTable(treeVb, treeVb.getTableName(), targetTableName,
					dataBaseType);

			level = "Executing creation script : [" + returnMap.get("createString") + "]";
			getJdbcTemplate().execute(String.valueOf(returnMap.get("createString")));

			String selectSql = String.valueOf(returnMap.get("selectString"));

			String insertSql = (String) returnMap.get("insertStringWithPlaceHolder");

			level = "Fetch record from source table [" + treeVb.getTableName() + "]";
			rs = stmt.executeQuery(selectSql);
//			rs = stmt.executeQuery(selectSql + " WHERE rownum < 200000");
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();

			level = "Inserting record into target table [" + targetTableName + "]";

			setBatchLimit(5000);
			int batchIndex = 0;

			PreparedStatement pstmt = insertConnection.prepareStatement(insertSql);

			while (rs.next()) {
				int columnIndex = 1;
				while (columnIndex <= columnCount) {
					pstmt.setObject(columnIndex, rs.getObject(columnIndex));
					columnIndex++;
				}
				pstmt.addBatch();
				if (batchIndex == getBatchLimit()) {
					pstmt.executeBatch();
					pstmt.clearBatch();
					batchIndex = 0;
				}
				batchIndex++;
			}

			pstmt.executeBatch();

			pstmt.close();

			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Error during " + level + " .Reason [" + e.getMessage() + "]");
			dropTemporaryCreatedTable(targetTableName);
		} finally {
			try {
				insertConnection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		exceptionCode.setResponse(targetTableName);
		return exceptionCode;
	}

	public ExceptionCode createTempTableWithConnectionScriptForManualQuery(DCManualQueryVb mQueryVb,
			String connectionScript, VcConfigMainTreeVb treeVb, String targetTableName, String[] hashArray,
			String[] hashValueArray) {

		ExceptionCode exceptionCode = new ExceptionCode();
		List<String> tablesToDrop = new ArrayList<String>();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		if (!ValidationUtil.isValid(targetTableName))
			targetTableName = createRandomTableName();

		String level = "Create source connection";
		exceptionCode = CommonUtils.getConnection(connectionScript);
		if (exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
			con = (Connection) exceptionCode.getResponse();
		} else {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Error during " + level + " .Reason [" + exceptionCode.getErrorMsg() + "]");
			return exceptionCode;
		}
		String dbSetParam1 = CommonUtils.getValue(connectionScript, "DB_SET_PARAM1");
		String dbSetParam2 = CommonUtils.getValue(connectionScript, "DB_SET_PARAM2");
		String dbSetParam3 = CommonUtils.getValue(connectionScript, "DB_SET_PARAM3");
		Connection insertConnection = getConnection();
		try {
			stmt = con.createStatement();
			if (ValidationUtil.isValid(dbSetParam1)) {
				stmt.executeUpdate(dbSetParam1);
			}
			if (ValidationUtil.isValid(dbSetParam2)) {
				stmt.executeUpdate(dbSetParam2);
			}
			if (ValidationUtil.isValid(dbSetParam3)) {
				stmt.executeUpdate(dbSetParam3);
			}

			String sessionId = String.valueOf(System.currentTimeMillis());
			String stgQuery = "";
			tablesToDrop.add("TVC_" + sessionId + "_STG_1");
			tablesToDrop.add("TVC_" + sessionId + "_STG_2");
			tablesToDrop.add("TVC_" + sessionId + "_STG_3");
			exceptionCode.setOtherInfo(tablesToDrop);
			if (ValidationUtil.isValid(mQueryVb.getStgQuery1())) {
				level = "Staging 1";
				stgQuery = mQueryVb.getStgQuery1();
				stgQuery = returnParsedStagingQuery(stgQuery, sessionId, hashArray, hashValueArray);
				stmt.executeUpdate(stgQuery);
			}

			if (ValidationUtil.isValid(mQueryVb.getStgQuery2())) {
				level = "Staging 2";
				stgQuery = mQueryVb.getStgQuery2();
				stgQuery = returnParsedStagingQuery(stgQuery, sessionId, hashArray, hashValueArray);
				stmt.executeUpdate(stgQuery);
			}
			if (ValidationUtil.isValid(mQueryVb.getStgQuery3())) {
				level = "Staging 3";
				stgQuery = mQueryVb.getStgQuery3();
				stgQuery = returnParsedStagingQuery(stgQuery, sessionId, hashArray, hashValueArray);
				stmt.executeUpdate(stgQuery);
			}

			String sqlMainQuery = mQueryVb.getSqlQuery();
			sqlMainQuery = returnParsedStagingQuery(sqlMainQuery, sessionId, hashArray, hashValueArray);

			Map<String, Object> returnMap = formScriptsForTemporaryTable(treeVb, "(" + sqlMainQuery + ")",
					targetTableName, null);

			level = "Executing creation script : [" + returnMap.get("createString") + "]";
			getJdbcTemplate().execute(String.valueOf(returnMap.get("createString")));

			String selectSql = String.valueOf(returnMap.get("selectStrForDirectInsert"));

			if (CommonUtils.DEFAULT_DB.equalsIgnoreCase(mQueryVb.getDatabaseConnectivityDetails())) {
				targetTableName = "(" + selectSql + ")";
			} else {
				String insertSql = (String) returnMap.get("insertStringWithPlaceHolder");

				level = "Fetch record from source table [" + treeVb.getTableName() + "]";
				rs = stmt.executeQuery(selectSql);
				ResultSetMetaData metaData = rs.getMetaData();
				int columnCount = metaData.getColumnCount();

				level = "Inserting record into target table [" + targetTableName + "]";

				setBatchLimit(5000);
				int batchIndex = 1;
				Connection connection = getConnection();
				PreparedStatement pstmt = connection.prepareStatement(insertSql);

				while (rs.next()) {
					int columnIndex = 1;
					while (columnIndex <= columnCount) {
						pstmt.setObject(columnIndex, rs.getObject(columnIndex));
						columnIndex++;
					}
					pstmt.addBatch();
					if (batchIndex == getBatchLimit()) {
						pstmt.executeBatch();
						pstmt.clearBatch();
						batchIndex = 0;
					}
					batchIndex++;
				}

				pstmt.executeBatch();

				pstmt.close();
				connection.close();

			}

			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Error during " + level + " .Reason [" + e.getMessage() + "]");
			dropTemporaryCreatedTable(targetTableName);
		} finally {
			try {
				insertConnection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		exceptionCode.setResponse(targetTableName);
		return exceptionCode;
	}

	public String returnParsedStagingQuery(String sql, String sessionId, String[] hashArray, String[] hashValueArray)
			throws Exception {
		Pattern pattern = Pattern.compile("#(.*?)#", Pattern.DOTALL);
		Matcher matcher = pattern.matcher(sql);
		while (matcher.find()) {
			if ("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
				sql = sql.replaceAll("#" + matcher.group(1) + "#", "TVC_" + sessionId + "_STG_1");
			if ("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
				sql = sql.replaceAll("#" + matcher.group(1) + "#", "TVC_" + sessionId + "_STG_2");
			if ("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
				sql = sql.replaceAll("#" + matcher.group(1) + "#", "TVC_" + sessionId + "_STG_3");
		}
		sql = CommonUtils.replaceHashTag(sql, hashArray, hashValueArray);
		return sql;
	}
	public void deleteRecords(String tableName, String columnName, String parameter) {
		getJdbcTemplate().execute("delete from " + tableName + " where " + columnName + " ='" + parameter + "' ");
	}

	public Map<String, List> getRestrictionTreeLocal() throws DataAccessException {
		String sql = "select MACROVAR_NAME,TAG_NAME, DISPLAY_NAME, MACROVAR_DESC from MACROVAR_TAGGING where MACROVAR_TYPE = 'DATA_RESTRICTION' order by MACROVAR_NAME, TAG_NO";
		return getJdbcTemplate().query(sql, new ResultSetExtractor<Map<String, List>>() {
			@Override
			public Map<String, List> extractData(ResultSet rs) throws SQLException, DataAccessException {
				Map<String, List> returnMap = new HashMap<String, List>();
				while (rs.next()) {
					if (returnMap.get(rs.getString("MACROVAR_NAME")) == null) {
						List<String> tagList = new ArrayList<String>();
						tagList.add(rs.getString("TAG_NAME"));
						returnMap.put(rs.getString("MACROVAR_NAME"), tagList);
					} else {
						List<String> tagList = returnMap.get(rs.getString("MACROVAR_NAME"));
						tagList.add(rs.getString("TAG_NAME"));
						returnMap.put(rs.getString("MACROVAR_NAME"), tagList);
					}
				}
				return returnMap;
			}
		});
	}

	public String getUserGrpProfile() {
		return userGrpProfile;
	}

	public void setUserGrpProfile(String userGrpProfile) {
		this.userGrpProfile = userGrpProfile;
	}	
}