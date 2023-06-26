package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringEscapeUtils;
import org.bouncycastle.crypto.RuntimeCryptoException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.Paginationhelper;
import com.vision.util.ValidationUtil;
import com.vision.vb.ColumnHeadersVb;
import com.vision.vb.DCManualQueryVb;
import com.vision.vb.DSConnectorVb;
import com.vision.vb.DesignAnalysisVb;
import com.vision.vb.MaskingVb;
import com.vision.vb.PromptTreeVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.UserRestrictionVb;
import com.vision.vb.VcConfigMainColumnsVb;
import com.vision.vb.VcConfigMainTreeVb;
import com.vision.vb.VcConfigMainVb;
import com.vision.vb.VcForCatalogTableRelationVb;
import com.vision.vb.VcForQueryReportFieldsVb;
import com.vision.vb.VcForQueryReportFieldsWrapperVb;
import com.vision.vb.VcForQueryReportVb;
import com.vision.vb.VisionUsersVb;
import com.vision.vb.WidgetDesignVb;
import com.vision.vb.XmlJsonUploadVb;
import com.vision.wb.ScheduledReportWb;

@Component
public class DesignAnalysisDao extends AbstractDao<DesignAnalysisVb> {
	
	public JdbcTemplate jdbcTemplate = null;

	public DesignAnalysisDao(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	protected static String url = "";
	protected static String userName = "";
	protected static String password = "";

	 @Autowired
	 NumSubTabDao numSubTabDao;
	 
	 @Autowired
	 VisionUsersDao visionUsersDao;
	 
	 @Value("${app.databaseType}")
	 private String databaseType = "ORACLE";
		
		
	@Override
	public void setServiceDefaults() {
		serviceName = "Design And Analysis";
		serviceDesc = "Design And Analysis";
		tableName = "VCQD_QUERIES_ACCESS";
		childTableName = "VCQD_QUERIES";
		VisionUsersVb vObj = (VisionUsersVb) new CommonDao(jdbcTemplate).getVisionUserDetails(ScheduledReportWb.currentUser);
		//VisionUsersVb vObj = SessionContextHolder.getContext();
		intCurrentUserId = vObj.getVisionId();
		//intCurrentUserId = 9999;
		userGroup = vObj.getUserGroup();
		userProfile = vObj.getUserProfile();
		if (ValidationUtil.isValid(vObj.getUserGrpProfile())) {
			userGrpProfile = vObj.getUserGrpProfile();
			String[] grpProfile = new String[2];
			grpProfile = userGrpProfile.split("-");
			userGroup = grpProfile[0];
			userProfile = grpProfile[1];
		}
	}

	@SuppressWarnings("rawtypes")
	protected RowMapper getMQMapper() {
		RowMapper mapper = new RowMapper() {
			@Override
			@SuppressWarnings("deprecation")
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				DCManualQueryVb vObj = new DCManualQueryVb();
				vObj.setQueryId(rs.getString("QUERY_ID"));
				vObj.setQueryDescription(rs.getString("QUERY_DESCRIPTION"));
				vObj.setDatabaseType(rs.getString("DATABASE_TYPE"));
				vObj.setDatabaseConnectivityDetails(rs.getString("DATABASE_CONNECTIVITY_DETAILS"));
				vObj.setLookupDataLoading(rs.getString("LOOKUP_DATA_LOADING"));
				vObj.setSqlQuery(ValidationUtil.isValid(rs.getString("SQL_QUERY")) ? rs.getString("SQL_QUERY"): "");
				vObj.setStgQuery1(ValidationUtil.isValid(rs.getString("STG_QUERY1"))? rs.getString("STG_QUERY1"): "");
				vObj.setStgQuery2(ValidationUtil.isValid(rs.getString("STG_QUERY2"))? rs.getString("STG_QUERY2"): "");
				vObj.setStgQuery3(ValidationUtil.isValid(rs.getString("STG_QUERY3"))? rs.getString("STG_QUERY3"): "");
				vObj.setPostQuery(ValidationUtil.isValid(rs.getString("POST_QUERY"))? rs.getString("POST_QUERY"): "");
				vObj.setVcqStatusNt(rs.getInt("VCQ_STATUS_NT"));
				vObj.setVcqStatus(rs.getInt("VCQ_STATUS"));
				vObj.setDbStatus(rs.getInt("VCQ_STATUS"));
				vObj.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObj.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObj.setMaker(rs.getLong("MAKER"));
				vObj.setVerifier(rs.getLong("VERIFIER"));
				vObj.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				vObj.setDateCreation(rs.getString("DATE_CREATION"));
				vObj.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				vObj.setQueryColumnXML(rs.getString("COLUMNS_METADATA"));
				vObj.setHashVariableScript(rs.getString("HASH_VARIABLE_SCRIPT"));
				return vObj;
			}
		};
		return mapper;
	}
	@SuppressWarnings("rawtypes")
	protected RowMapper getVCQDTreeColumnsRelationMapper() {
		RowMapper mapper = new RowMapper() {
			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcForCatalogTableRelationVb vcForCatalogTableRelationVb = new VcForCatalogTableRelationVb();
				vcForCatalogTableRelationVb.setCatalogId(rs.getString("CATALOG_ID"));
				vcForCatalogTableRelationVb.setFromTableId(rs.getString("FROM_TABLE_ID"));
				vcForCatalogTableRelationVb.setToTableId(rs.getString("TO_TABLE_ID"));
				vcForCatalogTableRelationVb.setJoinTypeNt(rs.getString("JOIN_TYPE_NT"));
				vcForCatalogTableRelationVb.setRelJoinType(rs.getString("JOIN_TYPE"));
				vcForCatalogTableRelationVb.setJoinString(rs.getString("JOIN_STRING"));
				vcForCatalogTableRelationVb.setFilterCondition(rs.getString("FILTER_CONDITION"));				
				vcForCatalogTableRelationVb.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vcForCatalogTableRelationVb.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vcForCatalogTableRelationVb.setVcrStatusNt(rs.getInt("VCR_STATUS_NT"));
				vcForCatalogTableRelationVb.setVcrStatus(rs.getInt("VCR_STATUS"));
				vcForCatalogTableRelationVb.setMaker(rs.getInt("MAKER"));
				vcForCatalogTableRelationVb.setVerifier(rs.getInt("VERIFIER"));
				vcForCatalogTableRelationVb.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				vcForCatalogTableRelationVb.setDateCreation(rs.getString("DATE_CREATION"));
				return vcForCatalogTableRelationVb;
			}
		};
		return mapper;
	}
	
	@SuppressWarnings("unchecked")
	public List<DesignAnalysisVb> getDesignQueryFromVisionCatalog(DesignAnalysisVb designVb) throws DataAccessException {
		setServiceDefaults();
		designVb.setVerificationRequired(false);
		Vector<Object> params = new Vector<Object>();

		StringBuffer sql = new StringBuffer(" select DISTINCT T1.CATALOG_ID, T1.REPORT_ID, T1.REPORT_DESCRIPTION, T1.USER_ID, " +
				"  T2.MAKER, (select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = T2.MAKER) as MAKER_NAME ,"
				+ " T2.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = T2.VERIFIER) as VERIFIER_NAME, T1.VRD_STATUS, " + 
				" (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 1 AND NUM_SUB_TAB = T1.VRD_STATUS ) STATUS_DESC, " +
				" "+new CommonDao(jdbcTemplate).getDbFunction("DATEFUNC")+"(T1.DATE_LAST_MODIFIED, 'DD-MM-YYYY "+new CommonDao(jdbcTemplate).getDbFunction("TIME")+"') DATE_LAST_MODIFIED, " +
				" "+new CommonDao(jdbcTemplate).getDbFunction("DATEFUNC")+"(T1.DATE_CREATION, 'DD-MM-YYYY "+new CommonDao(jdbcTemplate).getDbFunction("TIME")+"') DATE_CREATION " +
				" from (VC_REPORT_DEFS_SELFBI T1 JOIN VCQD_QUERIES T2  " +
				"     ON (T1.REPORT_ID = T2.VCQD_QUERY_ID))  Left outer JOIN  VCQD_QUERIES_ACCESS T3 ON (T2.VCQD_QUERY_ID = T3.VCQD_QUERY_ID) " +
				" WHERE  " +
				" (T2.MAKER = ? OR (T3.USER_GROUP = ? AND T3.USER_PROFILE = ?)  ) ");
	
		String orderBy = " Order By REPORT_DESCRIPTION ";
		params.addElement( intCurrentUserId );
		params.addElement( userGroup );
		params.addElement( userProfile );
	 return getQueryPopupResults(designVb,new StringBuffer(), sql,new String(), orderBy, params,getMapperForDsQuery());
	 //return getQueryPopupResultsWithPend(designVb, null, sql, "", orderBy, params, getMapperForDsQuery());
	}
	

	protected RowMapper getMapperForDsQuery() {
		RowMapper mapper = new RowMapper() {
			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				DesignAnalysisVb vbObj = new DesignAnalysisVb();
				vbObj.setCatalogId(rs.getString("CATALOG_ID"));
				vbObj.setVcqdQueryId(rs.getString("REPORT_ID"));
				vbObj.setVcqdQueryDesc(rs.getString("REPORT_DESCRIPTION"));
				vbObj.setMaker(rs.getInt("MAKER"));
				vbObj.setMakerName(rs.getString("MAKER_NAME"));
				vbObj.setVerifier(rs.getInt("VERIFIER"));
				vbObj.setVerifierName(rs.getString("VERIFIER_NAME"));
				vbObj.setDateCreation(rs.getString("DATE_CREATION"));
				vbObj.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				vbObj.setVcqdStatus(rs.getInt("VRD_STATUS"));
				vbObj.setVcqdStatusDesc(rs.getString("STATUS_DESC"));
				return vbObj;
			}
		};
		return mapper;
	}


	@SuppressWarnings("unchecked")
	public List<VcConfigMainVb> getUserCatalogsDetails() throws DataAccessException {
		setServiceDefaults();
		String sql = " select T1.CATALOG_ID, T1.CATALOG_DESC, T1.JOIN_CLAUSE from VISION_CATALOG_SELFBI T1 LEFT JOIN CATALOG_ACCESS_SelfBI T2 ON (T1.CATALOG_ID = T2.CATALOG_ID) " + 
				" WHERE (T1.MAKER = ? OR (T2.USER_GROUP = ? AND T2.USER_PROFILE = ?)) " +
				" GROUP BY T1.CATALOG_ID, T1.CATALOG_DESC, T1.JOIN_CLAUSE"+
				" ORDER BY T1.CATALOG_ID ";
		Object[] args = {intCurrentUserId, userGroup, userProfile};
		return jdbcTemplate.query(sql, args, new RowMapper<VcConfigMainVb>() {
			@Override
			public VcConfigMainVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcConfigMainVb catalogVb = new VcConfigMainVb();
				catalogVb.setCatalogId(rs.getString("CATALOG_ID"));
				catalogVb.setCatalogDesc(rs.getString("CATALOG_DESC"));
				catalogVb.setJoinClause(rs.getInt("JOIN_CLAUSE"));
				return catalogVb;
			}
		});
	}
	@SuppressWarnings("unchecked")
	public List<VcConfigMainTreeVb> getQueryTreeDetails(String catalogId) throws DataAccessException {
		setServiceDefaults();
		String sql = " SELECT VC.CATALOG_ID, VCT.TABLE_ID, VCT.TABLE_NAME, VCT.ALIAS_NAME TABLE_ALIAS_NAME, VCT.BASE_TABLE_FLAG,  "+
				" VCT.QUERY_ID, VCT.DATABASE_TYPE, VCT.DATABASE_CONNECTIVITY_DETAILS, VCT.SORT_TREE, VCT.VCT_STATUS, "+
				" VCT.TABLE_SOURCE_TYPE, VCT.ACCESS_CONTROL_FLAG, VCT.ACCESS_CONTROL_SCRIPT "+
				" FROM VISION_CATALOG_SELFBI VC, VC_TREE_SELFBI VCT "+
				" WHERE VC.CATALOG_ID = VCT.CATALOG_ID "+
				" AND VC.CATALOG_ID = ? ORDER BY SORT_TREE, TABLE_ID";
		Object args[] = {catalogId};
		return jdbcTemplate.query(sql, args, new RowMapper<VcConfigMainTreeVb>() {
			@Override
			public VcConfigMainTreeVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcConfigMainTreeVb treeVb = new VcConfigMainTreeVb();
				treeVb.setCatalogId(rs.getString("CATALOG_ID"));
				treeVb.setTableId(rs.getString("TABLE_ID"));
				treeVb.setTableName(rs.getString("TABLE_NAME"));
				treeVb.setAliasName(ValidationUtil.isValid(rs.getString("TABLE_ALIAS_NAME"))?rs.getString("TABLE_ALIAS_NAME"):rs.getString("TABLE_NAME"));
				treeVb.setBaseTableFlag(rs.getString("BASE_TABLE_FLAG"));
				treeVb.setQueryId(rs.getString("QUERY_ID"));
				treeVb.setDatabaseType(rs.getString("DATABASE_TYPE"));
				treeVb.setDatabaseConnectivityDetails(rs.getString("DATABASE_CONNECTIVITY_DETAILS"));
				treeVb.setSortTree(rs.getString("SORT_TREE"));
				treeVb.setVctStatus(rs.getInt("VCT_STATUS"));
				treeVb.setTableSourceType(rs.getString("TABLE_SOURCE_TYPE"));
				treeVb.setAccessControlFlag(rs.getString("ACCESS_CONTROL_FLAG"));
				treeVb.setAccessControlScript(rs.getString("ACCESS_CONTROL_SCRIPT"));
				return treeVb;
			}
		});
	}
	
	@SuppressWarnings("unchecked")
	public List<VcConfigMainColumnsVb> getQueryTreeColumnsDetails(VcConfigMainTreeVb vcTreeVb) throws DataAccessException {
		setServiceDefaults();
		
		String sql = "SELECT VC.CATALOG_ID, VCT.TABLE_ID, VCT.TABLE_NAME, VCT.ALIAS_NAME TABLE_ALIAS_NAME, VCT.BASE_TABLE_FLAG, VCT.QUERY_ID, VCT.DATABASE_TYPE, " + 
				" VCT.DATABASE_CONNECTIVITY_DETAILS, VCT.SORT_TREE, VCT.VCT_STATUS, VCT.TABLE_SOURCE_TYPE, VCT.ACCESS_CONTROL_FLAG, " + 
				" VCT.ACCESS_CONTROL_SCRIPT, VCC.COL_ID, VCC.COL_NAME, VCC.ALIAS_NAME COLUMN_ALIAS_NAME, VCC.SORT_COLUMN, VCC.COL_DISPLAY_TYPE, " + 
				" VCC.COL_ATTRIBUTE_TYPE, VCC.COL_EXPERSSION_TYPE, VCC.FORMAT_TYPE, (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = VCC.FORMAT_TYPE) FORMAT_TYPE_DESC, "+
				" (SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = VCC.FORMAT_TYPE)JAVA_DATE_FORMAT, VCC.MAG_ENABLE_FLAG, VCC.MAG_TYPE, " + 
				" VCC.MAG_SELECTION_TYPE, VCC.VCC_STATUS, VCC.MAG_DEFAULT, VCC.MAG_QUERY_ID, VCC.MAG_DISPLAY_COLUMN, " + 
				" VCC.MAG_USE_COLUMN, VCC.FOLDER_IDS, VCC.EXPERSSION_TEXT, VCC.COL_TYPE, VCC.MASKING_FLAG, VCC.MASKING_SCRIPT, VCC.INCLUDE_GROUP_COL, " +
				" (select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = '"+databaseType+"' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = VCC.FORMAT_TYPE) DATE_FORMATTING_SYNTAX, " +
				" (select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = '"+databaseType+"' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = VCC.FORMAT_TYPE) DATE_CONVERSION_SYNTAX " +
				" FROM VISION_CATALOG_SELFBI VC, VC_TREE_SELFBI VCT, VC_COLUMNS_SELFBI VCC " + 
				" WHERE VC.CATALOG_ID = VCT.CATALOG_ID " + 
				" AND VCT.CATALOG_ID = VCC.CATALOG_ID " + 
				" AND VCT.TABLE_ID = VCC.TABLE_ID " + 
				" AND VC.CATALOG_ID = ? " +
				" AND VCT.TABLE_ID = ? ORDER BY CATALOG_ID, TABLE_ID, SORT_COLUMN";
		
		Object args[] = {vcTreeVb.getCatalogId(), vcTreeVb.getTableId()};
		
		return jdbcTemplate.query(sql, args, new RowMapper<VcConfigMainColumnsVb>() {
			@Override
			public VcConfigMainColumnsVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcConfigMainColumnsVb columnVb = new VcConfigMainColumnsVb();
				columnVb.setCatalogId(rs.getString("CATALOG_ID"));
				columnVb.setTableId(rs.getString("TABLE_ID"));
				columnVb.setColId(rs.getString("COL_ID"));
				columnVb.setColName(rs.getString("COL_NAME"));
				columnVb.setAliasName(ValidationUtil.isValid(rs.getString("COLUMN_ALIAS_NAME"))?rs.getString("COLUMN_ALIAS_NAME"):rs.getString("COL_NAME"));
				columnVb.setSortColumn(rs.getString("SORT_COLUMN"));
				columnVb.setColDisplayType(rs.getString("COL_DISPLAY_TYPE"));
				columnVb.setColAttributeType(rs.getString("COL_ATTRIBUTE_TYPE"));
				columnVb.setColExperssionType(rs.getString("COL_EXPERSSION_TYPE"));
				columnVb.setFormatType(rs.getString("FORMAT_TYPE"));
				columnVb.setFormatTypeDesc(rs.getString("FORMAT_TYPE_DESC"));
				columnVb.setMagEnableFlag(rs.getString("MAG_ENABLE_FLAG"));
				columnVb.setMagType(rs.getInt("MAG_TYPE"));
				columnVb.setMagSelectionType(rs.getString("MAG_SELECTION_TYPE"));
				columnVb.setVccStatus(rs.getInt("VCC_STATUS"));
				columnVb.setMagDefault(rs.getString("MAG_DEFAULT"));
				columnVb.setMagQueryId(rs.getString("MAG_QUERY_ID"));
				columnVb.setMagDisplayColumn(rs.getString("MAG_DISPLAY_COLUMN"));
				columnVb.setMagUseColumn(rs.getString("MAG_USE_COLUMN"));
				columnVb.setFolderIds(rs.getString("FOLDER_IDS"));
				columnVb.setJavaFormatDesc(rs.getString("JAVA_DATE_FORMAT"));
				if(ValidationUtil.isValid(rs.getString("EXPERSSION_TEXT"))){
					columnVb.setExperssionText(rs.getString("EXPERSSION_TEXT").replaceAll("#TABLE_ALIAS#", ValidationUtil.isValid(rs.getString("TABLE_ALIAS_NAME"))?rs.getString("TABLE_ALIAS_NAME"):rs.getString("TABLE_NAME")));
				}
//				columnVb.setExperssionText(rs.getString("EXPERSSION_TEXT"));
				columnVb.setColType(rs.getString("COL_TYPE"));
				columnVb.setMaskingFlag(rs.getString("MASKING_FLAG"));
				columnVb.setMaskingScript(rs.getString("MASKING_SCRIPT"));
				columnVb.setDateFormattingSyntax(rs.getString("DATE_FORMATTING_SYNTAX"));
				columnVb.setDateConversionSyntax(rs.getString("DATE_CONVERSION_SYNTAX"));
				if(ValidationUtil.isValid(rs.getString("INCLUDE_GROUP_COL")))
					columnVb.setIncludeGroupCol(rs.getString("INCLUDE_GROUP_COL").replaceAll("#TABLE_ALIAS#", ValidationUtil.isValid(rs.getString("TABLE_ALIAS_NAME"))?rs.getString("TABLE_ALIAS_NAME"):rs.getString("TABLE_NAME")));
				return columnVb;
			}
		});
	}
	
	@SuppressWarnings("unchecked")
	public List<VcForCatalogTableRelationVb> getQueryTreeColumnsRelationsDetails(VcForCatalogTableRelationVb vcForCatalogTableRelationVb) throws DataAccessException {
		setServiceDefaults();
		String sql = "select * from VC_RELATIONS_SELFBI where CATALOG_ID='"+vcForCatalogTableRelationVb.getCatalogId()+"' and FROM_TABLE_ID='"+vcForCatalogTableRelationVb.getFromTableId()+"' and TO_TABLE_ID='"+vcForCatalogTableRelationVb.getToTableId()+"'";
		return jdbcTemplate.query(sql, getVCQDTreeColumnsRelationMapper());
	}
	
	public String getMaxOfReportNamePerUser(String catalogId) {
		String sql = "";
		if("ORACLE".equalsIgnoreCase(databaseType))
			sql = "select MAX(To_NUMBER(substr(REPORT_ID,6,length(REPORT_ID)))) from VC_REPORT_DEFS_SELFBI";
		else if("MSSQL".equalsIgnoreCase(databaseType))
			sql = "select MAX(cast(substring(REPORT_ID,6,len(REPORT_ID)) AS numeric)) from VC_REPORT_DEFS_SELFBI";
		
		Long maxOfReportId = jdbcTemplate.queryForObject(sql, Long.class);
		maxOfReportId = (maxOfReportId==null)?0000000:maxOfReportId;
		return ("VCQD_" + String.format("%07d", maxOfReportId+1));
	}
	public List<DCManualQueryVb> getQueryIdDetails(String queryId){
		String query = "SELECT * FROM VC_QUERIES WHERE UPPER(QUERY_ID)=UPPER('"+queryId+"') AND VCQ_STATUS=0";
		return jdbcTemplate.query(query, getMQMapper());
	}	
	public String getScriptValue(String variableName){
		String sql = "SELECT VARIABLE_SCRIPT FROM VISION_DYNAMIC_HASH_VAR WHERE VARIABLE_TYPE = 2 AND SCRIPT_TYPE='MACROVAR' AND UPPER(VARIABLE_NAME)=UPPER(?)";
		Object params[] = new Object[1];
		params[0] = variableName;
		try{
			String returnValue =jdbcTemplate.queryForObject(sql, params,String.class);
			return (!ValidationUtil.isValid(returnValue) ? "" : returnValue);			
		}catch(Exception e){
			e.printStackTrace();
			return "";
		}
	}
	
	public List<Object> getReportDataAsXMLString(PromptTreeVb promptTree,final List<VcForQueryReportFieldsVb> reportFields, final String[] colTypebasket) {
		String query = "";
		try{
			query = "SELECT COUNT(1) FROM "+promptTree.getTableName();
			promptTree.setTotalRows(jdbcTemplate.queryForObject(query,int.class));
			query = "SELECT * FROM "+promptTree.getTableName()+" where rownum < "+(promptTree.getMaxCatalogDisplayCount()+1)+" ";
			ResultSetExtractor mapper = new ResultSetExtractor() {
				@Override
				public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
					StringBuffer lResult = new StringBuffer("");
					lResult.append("<tableData>");
					ResultSetMetaData metadata = rs.getMetaData();
					List<ColumnHeadersVb> columnHeaders = new ArrayList<ColumnHeadersVb>(metadata.getColumnCount());
					for(int i=1;i<= metadata.getColumnCount(); i++){
						ColumnHeadersVb columnHeader = new ColumnHeadersVb(); 
						columnHeader.setCaption(metadata.getColumnLabel(i));
						if(reportFields != null && !reportFields.isEmpty()){
							for(VcForQueryReportFieldsVb rCReportFieldsVb: reportFields){
								if(ValidationUtil.isValid(rCReportFieldsVb.getAlias())){
									if(columnHeader.getCaption().equalsIgnoreCase(rCReportFieldsVb.getAlias())){
										columnHeader.setColType(rCReportFieldsVb.getColDisplayType());
									}
								}else{
									if(columnHeader.getCaption().equalsIgnoreCase(rCReportFieldsVb.getColName())){
										columnHeader.setColType(rCReportFieldsVb.getColDisplayType());
									}	
								}
							}
						}else{
							switch(metadata.getColumnType(i)){
							case  -7: case  -6: case  5: case  -5: case  4: case  3: case  2: 
							case  6: case  7: case  8:
								if(ValidationUtil.isValid(colTypebasket) && colTypebasket.length > 0){
									int j=i-1;
									if(ValidationUtil.isValid(colTypebasket[j])){
										columnHeader.setColType(colTypebasket[j]);
									}else{
										 columnHeader.setColType("T");
									}
								}else{
									columnHeader.setColType("N");
								}
								break;
							default: columnHeader.setColType("T");
							}
						}
						columnHeaders.add(columnHeader);
					}
					while(rs.next()){
						lResult.append("<tableRow>");
						for(ColumnHeadersVb columnHeader:columnHeaders){
							String value = rs.getString(columnHeader.getCaption());
							if(value == null) value="";
							value =value.replaceAll("\\\\","@-@");
							lResult.append("<").append(columnHeader.getCaption().replaceAll(" ","_")).append(">").append(StringEscapeUtils.escapeXml(value)).append("</").append(columnHeader.getCaption().replaceAll(" ","_")).append(">");
						}
						lResult.append("</tableRow>");
					}
					lResult.append("</tableData>");
					String str2 = lResult.toString().replaceAll("@-@","/");
					StringBuffer str3 = new StringBuffer(str2);
					List<Object> result = new ArrayList<Object>(2);
					result.add(columnHeaders);
					//result.add(StringEscapeUtils.unescapeJava(lResult.toString()).replaceAll("\u0019",""));
					result.add(StringEscapeUtils.unescapeJava(str3.toString()));
					return result;
				}
			};
			return (List<Object>)jdbcTemplate.query(query, mapper);
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
	
	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode saveOrUpdate(VcForQueryReportVb rcReportVb) {
		ExceptionCode exceptionCode = null;
		strApproveOperation ="Save";
		strErrorDesc  = "";
		strCurrentOperation = "Save";
		setServiceDefaults();
		try {
			if(ValidationUtil.isValid(rcReportVb.getReportId())){
				//Update
				rcReportVb.setRecordIndicator(Constants.STATUS_ZERO);
				rcReportVb.setMaker(getIntCurrentUserId());
				rcReportVb.setVerifier(getIntCurrentUserId());
				rcReportVb.setRecordIndicatorNt(7);
				rcReportVb.setVrdStatusNt(1);
				rcReportVb.setVrdStatus(0);
				retVal = doUpdateReport(rcReportVb);
				if (retVal == Constants.SUCCESSFUL_OPERATION)
					retVal = doUpdateVcQueryDesign(rcReportVb);
//				retVal = doUpdateVcQueryDesignAccess(rcReportVb);
			}else{
				//Insert
				String rID=getMaxOfReportNamePerUser(rcReportVb.getCatalogId());
				rcReportVb.setReportName(rID);
				rcReportVb.setReportId(rID);
				rcReportVb.setRecordIndicator(Constants.STATUS_ZERO);
				rcReportVb.setMaker(getIntCurrentUserId());
				rcReportVb.setVerifier(getIntCurrentUserId());
				rcReportVb.setRecordIndicatorNt(7);
				rcReportVb.setVrdStatusNt(1);
				rcReportVb.setVrdStatus(0);
				retVal = doInsertionReport(rcReportVb);
				if (retVal == Constants.SUCCESSFUL_OPERATION)
					retVal = doInsertVcQueryDesign(rcReportVb);
				/*if (retVal == Constants.SUCCESSFUL_OPERATION)
					retVal = doInsertVcQueryDesignAccess(rcReportVb, String.valueOf(rcReportVb.getMaker()));*/
			}
			if (retVal != Constants.SUCCESSFUL_OPERATION){
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			//Delete all the Report Fields before Insert
			doDeleteReportFieldsByUserIdReportIdCatalogId(rcReportVb);
			if(!"A".equalsIgnoreCase(rcReportVb.getQueryType())){
				int sortOrder = 1;
				for(int i= 0;i< rcReportVb.getReportFields().size();i++){
					VcForQueryReportFieldsVb reportField = rcReportVb.getReportFields().get(i);
					reportField.setReportId(rcReportVb.getReportId());
					reportField.setCatalogId(rcReportVb.getCatalogId());
					reportField.setRecordIndicator(Constants.STATUS_ZERO);
					reportField.setMaker(getIntCurrentUserId());
					reportField.setVerifier(getIntCurrentUserId());
					reportField.setRecordIndicatorNt(7);
					reportField.setVrfStatusNt(1);
					reportField.setVrfStatus(0);
					reportField.setSortOrder(sortOrder);
					if(!ValidationUtil.isValid(reportField.getDisplayFlag())){
						reportField.setDisplayFlag("Y");
					}
					if(!ValidationUtil.isValid(reportField.getGroupBy())){
						reportField.setGroupBy("N");
					}
					
					retVal = doInsertionReportFields(reportField);
					if (retVal != Constants.SUCCESSFUL_OPERATION){
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
					sortOrder++;
				}
			}
			exceptionCode = getResultObject(retVal);
			return exceptionCode;
		}catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}catch(Exception ex){
			logger.error("Error in Modify.",ex);
			logger.error( ((rcReportVb==null)? "vObject is Null":rcReportVb.toString()));
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}
	private int doInsertionReportFields(VcForQueryReportFieldsVb vObject){
		String query = "Insert into  VC_REPORT_FIELDS_SELFBI (CATALOG_ID, USER_ID, REPORT_ID, TABLE_ID, COL_ID, COL_NAME, ALIAS, " +
				" OPERATOR, VALUE_1, VALUE_2, DISPLAY_FLAG, SORT_TYPE, SORT_ORDER, GROUP_BY, JOIN_CONDITION, VRF_STATUS_NT, " +
				" VRF_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, RECORD_INDICATOR_NT, RECORD_INDICATOR, "+ 
				" AGG_FUNCTION,SCALING_FLAG,SCALING_FORMAT,NUMBER_FORMAT,DECIMAL_FLAG,DECIMAL_COUNT,"+
				" DYNAMIC_START_FLAG,DYNAMIC_END_FLAG,DYNAMIC_START_DATE,DYNAMIC_END_DATE,DYNAMIC_START_OPERATOR,DYNAMIC_END_OPERATOR, "+
				" SUMMARY_CRITERIA, SUMMARY_VALUE_1, SUMMARY_VALUE_2, " +
				" SUMMARY_DYNAMIC_START_FLAG, SUMMARY_DYNAMIC_END_FLAG, SUMMARY_DYNAMIC_START_DATE, SUMMARY_DYNAMIC_END_DATE, " + 
				" SUMMARY_DYNAMIC_START_OPERATOR, SUMMARY_DYNAMIC_END_OPERATOR) "+
				" Values " +
				" (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Object[] args = { vObject.getCatalogId(),intCurrentUserId,vObject.getReportId(),vObject.getTabelId(), 
				vObject.getColId(),vObject.getColName(), vObject.getAlias(), vObject.getOperator(),vObject.getValue1(),vObject.getValue2(), 
				vObject.getDisplayFlag(), vObject.getSortType(), vObject.getSortOrder(), vObject.getGroupBy(), vObject.getJoinCondition(), 
				vObject.getVrfStatusNt(), vObject.getVrfStatus(), vObject.getMaker(), 
				vObject.getVerifier(),vObject.getInternalStatus(), vObject.getRecordIndicatorNt(),vObject.getRecordIndicator(),
				vObject.getAggFunction(),vObject.getScalingFlag(),vObject.getScalingFormat(),vObject.getNumberFormat(),vObject.getDecimalFlag(),vObject.getDecimalCount(),
				vObject.getDynamicStartFlag(),vObject.getDynamicEndFlag(),vObject.getDynamicStartDate(),vObject.getDynamicEndDate(),
				vObject.getDynamicStartOperator(),vObject.getDynamicEndOperator(), vObject.getSummaryCriteria(), vObject.getSummaryValue1(), vObject.getSummaryValue2(),
				vObject.getSummaryDynamicStartFlag(),vObject.getSummaryDynamicEndFlag(),vObject.getSummaryDynamicStartDate(),vObject.getSummaryDynamicEndDate(),
				vObject.getSummaryDynamicStartOperator(),vObject.getSummaryDynamicEndOperator()};
		return jdbcTemplate.update(query,args);
	}
	private int doUpdateReport(VcForQueryReportVb vObject){
		String query = "UPDATE VC_REPORT_DEFS_SELFBI SET REPORT_QUERY = ?, QUERY_WHERE = ?, QUERY_GROUP_BY = ?, QUERY_ORDER_BY = ?, REPORT_DESCRIPTION = ?, " +
				"QUERY_TYPE = ?, VRD_STATUS = ?, MAKER = ?, VERIFIER = ?, " +
				"DATE_LAST_MODIFIED = "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", RECORD_INDICATOR = ?, DISTINCT_FLAG = ? WHERE CATALOG_ID = ? AND REPORT_ID = ? AND USER_ID = ?";
		Object[] args = {vObject.getReportDescription(),vObject.getQueryType(), vObject.getVrdStatus(), 
				vObject.getMaker(),vObject.getVerifier(),  vObject.getRecordIndicator(), vObject.getDistinctFlag(), 
				vObject.getCatalogId(),vObject.getReportId(),intCurrentUserId};
		try{
			jdbcTemplate.update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int psIndex = 0;
					PreparedStatement ps = connection.prepareStatement(query);

					String clobData = ValidationUtil.isValid(vObject.getSelect()) ? vObject.getSelect() : ""; 
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getWhere()) ? vObject.getWhere() : ""; 
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

					clobData = ValidationUtil.isValid(vObject.getGroupBy()) ? vObject.getGroupBy() : ""; 
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getOrderBy()) ? vObject.getOrderBy() : ""; 
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					for (int i = 1; i <= args.length; i++) {
						ps.setObject(++psIndex, args[i - 1]);
					}
					return ps;
				}
			});
			
			return Constants.SUCCESSFUL_OPERATION;
		}catch(Exception e){
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			return Constants.ERRONEOUS_OPERATION;
		}
	}
	private int doUpdateVcQueryDesign(VcForQueryReportVb vObject) {
		String query = "UPDATE VCQD_QUERIES SET HASH_VARIABLE_SCRIPT = ?, VCQD_QUERY = ?, VCQD_QUERY_XML = ?, "+ 
				"VCQD_STATUS = 0, RECORD_INDICATOR = 0, MAKER = ?, "+
				"VERIFIER = ?, DATE_LAST_MODIFIED = "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", CATALOG_ID = ? "+
				"WHERE VCQD_QUERY_ID = ?";
		Object[] args = {vObject.getMaker(), vObject.getVerifier(), vObject.getCatalogId(), vObject.getReportId()};
		try{
			jdbcTemplate.update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int psIndex = 0;
					PreparedStatement ps = connection.prepareStatement(query);

					String clobData = ValidationUtil.isValid(vObject.getHashVariableScript()) ? vObject.getHashVariableScript() : ""; //HASH_VARIABLE_SCRIPT
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getQueryString()) ? vObject.getQueryString() : ""; //VCQD_QUERY
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getVcqdQueryXml()) ? vObject.getVcqdQueryXml() : ""; //VCQD_QUERY_XML
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

					for (int i = 1; i <= args.length; i++) {
						ps.setObject(++psIndex, args[i - 1]);
					}
					return ps;
				}
			});
			
			return Constants.SUCCESSFUL_OPERATION;
		}catch(Exception e){
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			return Constants.ERRONEOUS_OPERATION;
		}
	}

	private int doInsertVcQueryDesign(VcForQueryReportVb vObject){
		String query="INSERT INTO VCQD_QUERIES (CATALOG_ID, VCQD_QUERY_ID, "+
				"VCQD_STATUS_NT, VCQD_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, "+
				"VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, VCQD_QUERY_XML, VCQD_QUERY, HASH_VARIABLE_SCRIPT) "+
				"VALUES (?, ?, "+
				"1, 0, 7, 0, ?, "+
				"?, ?, "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", ?, ?, ?)";
		Object[] args = {vObject.getCatalogId(), vObject.getReportId(),vObject.getMaker(),vObject.getVerifier(),vObject.getInternalStatus()};
		try{
			jdbcTemplate.update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}

					String clobData = ValidationUtil.isValid(vObject.getVcqdQueryXml()) ? vObject.getVcqdQueryXml() : ""; //VCQD_QUERY_XML
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

					clobData = ValidationUtil.isValid(vObject.getQueryString()) ? vObject.getQueryString() : ""; //VCQD_QUERY
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getHashVariableScript()) ? vObject.getHashVariableScript() : ""; //HASH_VARIABLE_SCRIPT
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});
			return Constants.SUCCESSFUL_OPERATION;
		} catch(Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			return Constants.ERRONEOUS_OPERATION;
		}
	}
	private int doInsertVcQueryDesignAccess(VcForQueryReportVb vObject, String visionID){
		String sql = "INSERT INTO VCQD_QUERIES_ACCESS (VCQD_QUERY_ID, RECORD_INDICATOR, MAKER,"+
				" VERIFIER, DATE_LAST_MODIFIED, DATE_CREATION,QUERY_TYPE)"+
				" VALUES(?, 0, ?, ?, "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+",?)";
		try {
			Object[] args = {vObject.getReportId(), vObject.getMaker(), vObject.getVerifier(),1};
			return jdbcTemplate.update(sql, args);
		} catch(Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			return Constants.ERRONEOUS_OPERATION;
		}
	}
	private int doInsertionReport(VcForQueryReportVb vObject){
		String query = "Insert into VC_REPORT_DEFS_SELFBI (CATALOG_ID, USER_ID, REPORT_ID, REPORT_DESCRIPTION, " +
				" QUERY_TYPE, VRD_STATUS_NT, VRD_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, " +
				" DATE_CREATION, RECORD_INDICATOR_NT, RECORD_INDICATOR, DISTINCT_FLAG, " +
				" REPORT_QUERY, QUERY_WHERE, QUERY_GROUP_BY, QUERY_ORDER_BY) Values " +
				" (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", ?, ?, ?, ?, ?, ?, ?)";
		
		Object[] args = { vObject.getCatalogId(),intCurrentUserId,vObject.getReportId(), 
				vObject.getReportDescription(),vObject.getQueryType(),  
				vObject.getVrdStatusNt(), vObject.getVrdStatus(), vObject.getMaker(),vObject.getVerifier(),vObject.getInternalStatus(),
				vObject.getRecordIndicatorNt(),vObject.getRecordIndicator(), vObject.getDistinctFlag()};
		
		return jdbcTemplate.update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				int argumentLength = args.length;
				PreparedStatement ps = connection.prepareStatement(query);
				for (int i = 1; i <= argumentLength; i++) {
					ps.setObject(i, args[i - 1]);
				}

				String clobData = ValidationUtil.isValid(vObject.getSelect()) ? vObject.getSelect() : "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

				clobData = ValidationUtil.isValid(vObject.getWhere()) ? vObject.getWhere() : "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
				
				clobData = ValidationUtil.isValid(vObject.getGroupBy()) ? vObject.getGroupBy() : "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
				
				clobData = ValidationUtil.isValid(vObject.getOrderBy()) ? vObject.getOrderBy() : "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
				return ps;
			}
		});
		
	}
	private int doDeleteReportFieldsByUserIdReportIdCatalogId(VcForQueryReportVb vObject){
		String query = "DELETE FROM VC_REPORT_FIELDS_SELFBI WHERE CATALOG_ID = ? AND REPORT_ID = ? AND USER_ID = ?";
		Object[] args = {vObject.getCatalogId(),vObject.getReportId(),intCurrentUserId};
		return jdbcTemplate.update(query,args);
	}
	public List<VcForQueryReportVb> getAvailableActiveReportsForUser(DesignAnalysisVb designAnalysisVb){
		List<VcForQueryReportVb> result = null;
		String query = new String("SELECT  T1.CATALOG_ID, T1.REPORT_ID, T1.REPORT_DESCRIPTION, "+ 
				"T1.USER_ID, T1.REPORT_QUERY, T1.QUERY_TYPE FROM VC_REPORT_DEFS_SELFBI T1, VCQD_QUERIES_ACCESS T2 "+ 
				"WHERE T2.VCQDA_STATUS = 0 "+
				"AND T2.USER_GROUP=? "+
				"AND T2.USER_PROFILE=? "+
				"AND T1.CATALOG_ID = ? "+
				"AND T1.REPORT_ID = T2.VCQD_QUERY_ID " +
				"ORDER BY T1.CATALOG_ID, T1.REPORT_ID");
		try{
			setServiceDefaults();
			RowMapper mapper = new RowMapper() {
				@Override
				public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
					VcForQueryReportVb currentParent = new VcForQueryReportVb();
					currentParent.setCatalogId(rs.getString("CATALOG_ID"));
					currentParent.setReportName(rs.getString("REPORT_ID"));
					currentParent.setReportId(rs.getString("REPORT_ID"));
					currentParent.setReportDescription(rs.getString("REPORT_DESCRIPTION"));
					currentParent.setUserId(rs.getString("USER_ID"));
					currentParent.setQueryString(rs.getString("REPORT_QUERY"));
					currentParent.setQueryType(rs.getString("QUERY_TYPE"));
					return currentParent;
				}
			};
			Object objParams[] = new Object[2];
			objParams[0] = userGroup;
			objParams[1] = userProfile;
			objParams[2] = designAnalysisVb.getCatalogId();
			Paginationhelper<VcForQueryReportVb> paginationhelper = new Paginationhelper<VcForQueryReportVb>(); 
			if(designAnalysisVb.getTotalRows()  <= 0){
				result = paginationhelper.fetchPage(getJdbcTemplate(), query,  
						objParams, designAnalysisVb.getCurrentPage(), designAnalysisVb.getMaxRecords(),mapper);
				designAnalysisVb.setTotalRows(paginationhelper.getTotalRows());
			}else{
				result = paginationhelper.fetchPage(getJdbcTemplate(), query,  
						objParams, designAnalysisVb.getCurrentPage(), designAnalysisVb.getMaxRecords(), designAnalysisVb.getTotalRows(), mapper); 
			}
			result= jdbcTemplate.query(query,objParams, mapper);
			return result;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error(((query==null)? "query is Null":query));
			return null;
		}
	}
	
	public ExceptionCode executeSql(String sql, Object[] args){
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			if(args==null)
				jdbcTemplate.execute(sql);
			else
				jdbcTemplate.update(sql,args);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		}catch(Exception e){
			if(args!=null)
				e.printStackTrace();
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		return exceptionCode;
	}
	public static String getUrl() {
		return url;
	}
	public static void setUrl(String url) {
		DesignAnalysisDao.url = url;
	}
	public static String getUserName() {
		return userName;
	}
	public static void setUserName(String userName) {
		DesignAnalysisDao.userName = userName;
	}
	public static String getPassword() {
		return password;
	}
	public static void setPassword(String password) {
		DesignAnalysisDao.password = password;
	}
	
	public Integer returnBaseTableId(String catalogId){
		try{
			String baseFlagStr = jdbcTemplate.queryForObject("SELECT BASETABLE_JOINFLAG FROM VISION_CATALOG_SELFBI WHERE CATALOG_ID='"+catalogId+"'", String.class);
			if(ValidationUtil.isValid(baseFlagStr)){
				if("Y".equalsIgnoreCase(baseFlagStr)){
					return jdbcTemplate.queryForObject("SELECT TABLE_ID FROM VC_TREE_SELFBI WHERE CATALOG_ID='"+catalogId+"' AND BASE_TABLE_FLAG='Y' AND ROWNUM<2 ORDER BY TABLE_ID", Integer.class);
				}else{
					return null;
				}
			}else{
				return null;
			}
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	
	@SuppressWarnings("rawtypes")
	public ArrayList getData(String catalogId, Integer joinSyntaxType) {
		ArrayList returnArrayList = new ArrayList(3);
		final HashMap<String,List> linkedTblsForIndividualTblHM = new HashMap<String,List>();
		final HashMap<String,List<String>> relationHM = new HashMap<String,List<String>>();
//		final HashMap<String,String> aliasNameHM = new HashMap<String,String>();
//		final HashMap<String,String> tableNameHM = new HashMap<String,String>();
		final HashMap<String,VcConfigMainTreeVb> tableDetailsHM = new HashMap<String,VcConfigMainTreeVb>();
		String sql="SELECT FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE, RELATION_SCRIPT, FILTER_CONDITION FROM  VC_RELATIONS_SELFBI " + 
				"WHERE CATALOG_ID = '" +catalogId+ "' "+
				"ORDER BY FROM_TABLE_ID, TO_TABLE_ID" ;
	
		try {
			jdbcTemplate.query(sql, new RowCallbackHandler() {
				final String hyphen = "-";
				@Override
				public void processRow(ResultSet rs) throws SQLException, DataAccessException {
					String fromId = rs.getString("FROM_TABLE_ID");
					String toId = rs.getString("TO_TABLE_ID");
					if(linkedTblsForIndividualTblHM.get(fromId)==null){
						linkedTblsForIndividualTblHM.put(fromId, new ArrayList<String>(Arrays.asList(toId)));
					} else {
						List linkedTblIdList = linkedTblsForIndividualTblHM.get(fromId);
						linkedTblIdList.add(toId);
						linkedTblsForIndividualTblHM.put(fromId, linkedTblIdList);
					}
					
					if(linkedTblsForIndividualTblHM.get(toId)==null) {
						linkedTblsForIndividualTblHM.put(toId, new ArrayList<String>(Arrays.asList(fromId)));
					} else {
						List linkedTblIdList = linkedTblsForIndividualTblHM.get(toId);
						linkedTblIdList.add(fromId);
						linkedTblsForIndividualTblHM.put(toId, linkedTblIdList);
					}
					
					String key = fromId+hyphen+toId;
					String relationType = rs.getString("JOIN_TYPE");
					String relation = "";
					if (ValidationUtil.isValid(rs.getString("RELATION_SCRIPT"))) {
						if(!"4".equalsIgnoreCase(relationType)) {
							relation = CommonUtils.getValueForXmlTag(rs.getString("RELATION_SCRIPT"), "customjoin");
							if(!ValidationUtil.isValid(relation)) {
								if (joinSyntaxType == 1) 
									relation = CommonUtils.getValueForXmlTag(rs.getString("RELATION_SCRIPT"), "ansii_joinstring");
								else
									relation = CommonUtils.getValueForXmlTag(rs.getString("RELATION_SCRIPT"), "std_joinstring");
							}
						}
					}
					if (ValidationUtil.isValid(rs.getString("FILTER_CONDITION"))) {
						relation = ValidationUtil.isValid(relation) ? ( relation + " AND " +rs.getString("FILTER_CONDITION")) : rs.getString("FILTER_CONDITION");
					}
					
					relationHM.put(key, Arrays.asList(relationType, relation));
				}
				
			});
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		sql = "SELECT VC.CATALOG_ID, VCT.TABLE_ID, VCT.TABLE_NAME, VCT.ALIAS_NAME TABLE_ALIAS_NAME, VCT.BASE_TABLE_FLAG, VCT.QUERY_ID, VCT.DATABASE_TYPE, " + 
				" VCT.DATABASE_CONNECTIVITY_DETAILS, VCT.SORT_TREE, VCT.VCT_STATUS, VCT.TABLE_SOURCE_TYPE, VCT.ACCESS_CONTROL_FLAG, VCT.ACCESS_CONTROL_SCRIPT, " + 
				" VCT.ACCESS_CONTROL_SCRIPT, VCC.COL_ID, VCC.COL_NAME, VCC.ALIAS_NAME COLUMN_ALIAS_NAME, VCC.SORT_COLUMN, VCC.COL_DISPLAY_TYPE, VCC.COL_ATTRIBUTE_TYPE, " + 
				" VCC.COL_EXPERSSION_TYPE, FORMAT_TYPE, (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = VCC.FORMAT_TYPE) FORMAT_TYPE_DESC, " +
				" VCC.MAG_ENABLE_FLAG, VCC.MAG_TYPE, VCC.MAG_SELECTION_TYPE, VCC.VCC_STATUS, VCC.MAG_DEFAULT, VCC.MAG_QUERY_ID, " +
				" VCC.MAG_DISPLAY_COLUMN, VCC.MAG_USE_COLUMN, VCC.FOLDER_IDS, VCC.EXPERSSION_TEXT, VCC.COL_TYPE, VCC.MASKING_FLAG, VCC.MASKING_SCRIPT, VCC.INCLUDE_GROUP_COL, " +
				" (select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = '"+databaseType+"' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = VCC.FORMAT_TYPE) DATE_FORMATTING_SYNTAX, " +
				" (select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = '"+databaseType+"' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = VCC.FORMAT_TYPE) DATE_CONVERSION_SYNTAX " +
				" FROM VISION_CATALOG_SELFBI VC, VC_TREE_SELFBI VCT, VC_COLUMNS_SELFBI VCC " + 
				" WHERE VC.CATALOG_ID = VCT.CATALOG_ID " + 
				" AND VCT.CATALOG_ID = VCC.CATALOG_ID " + 
				" AND VCT.TABLE_ID = VCC.TABLE_ID " + 
				" AND VC.CATALOG_ID = '"+catalogId+"' " +
				" ORDER BY CATALOG_ID, TABLE_ID, COL_ID";
		try {
			jdbcTemplate.query(sql, new ResultSetExtractor<Object>() {
				List<VcConfigMainColumnsVb> colVbList = new ArrayList<VcConfigMainColumnsVb>();
				String chkTableId = null;
				VcConfigMainTreeVb treeDetailsVb = new VcConfigMainTreeVb();
				List<UserRestrictionVb> userRestList = new CommonDao(jdbcTemplate).getRestrictionTree();
				@Override
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					while(rs.next()) {
						if(chkTableId == null) {
							chkTableId = rs.getString("TABLE_ID");
							treeDetailsVb = new VcConfigMainTreeVb();
							treeDetailsVb.setCatalogId(rs.getString("CATALOG_ID"));
							treeDetailsVb.setTableId(rs.getString("TABLE_ID"));
							treeDetailsVb.setTableName(rs.getString("TABLE_NAME"));
							treeDetailsVb.setAliasName(rs.getString("TABLE_ALIAS_NAME"));
							treeDetailsVb.setBaseTableFlag(rs.getString("BASE_TABLE_FLAG"));
							treeDetailsVb.setQueryId(rs.getString("QUERY_ID"));
							treeDetailsVb.setDatabaseType(rs.getString("DATABASE_TYPE"));
							treeDetailsVb.setDatabaseConnectivityDetails(rs.getString("DATABASE_CONNECTIVITY_DETAILS"));
							treeDetailsVb.setTableSourceType(rs.getString("TABLE_SOURCE_TYPE"));
							treeDetailsVb.setAccessControlScript(rs.getString("ACCESS_CONTROL_SCRIPT"));
							if (ValidationUtil.isValid(treeDetailsVb.getAccessControlScript())) {
								List<UserRestrictionVb> restrictionListToReturn = new ArrayList<UserRestrictionVb>();
								Matcher authMatchObj = Pattern.compile("<auth>(.*?)<\\/auth>", Pattern.DOTALL)
										.matcher(treeDetailsVb.getAccessControlScript());
								while (authMatchObj.find()) {
									String category = CommonUtils.getValueForXmlTag(authMatchObj.group(1), "category");
									UserRestrictionVb filteredCategoryVb = userRestList.stream()
											.filter(vb -> category.equalsIgnoreCase(vb.getMacrovarName()))
											.collect(Collectors.collectingAndThen(Collectors.toList(), restrictionList -> {
												if (restrictionList != null && restrictionList.size() == 1) {
													return restrictionList.get(0);
												} else {
													return null;
												}
											}));

									if (filteredCategoryVb != null) {
										if (filteredCategoryVb.getChildren() != null) {
											Iterator childrenItr = filteredCategoryVb.getChildren().iterator();
											while (childrenItr.hasNext()) {
												UserRestrictionVb childVb = (UserRestrictionVb) childrenItr.next();
												childVb.setTagValue(CommonUtils.getValueForXmlTag(authMatchObj.group(1), childVb.getTagName()));
											}
										}
									}
									restrictionListToReturn.add(filteredCategoryVb);
								}
								treeDetailsVb.setAccessControlScriptParsed(restrictionListToReturn.size()>0?restrictionListToReturn:null);
							}
						}
						
						if(!chkTableId.equalsIgnoreCase(rs.getString("TABLE_ID"))) {
							
							treeDetailsVb.setChildren(colVbList);
							tableDetailsHM.put(treeDetailsVb.getTableId(), treeDetailsVb);
							colVbList = new ArrayList<VcConfigMainColumnsVb>();
							chkTableId = rs.getString("TABLE_ID");
							
							treeDetailsVb = new VcConfigMainTreeVb();
							treeDetailsVb.setCatalogId(rs.getString("CATALOG_ID"));
							treeDetailsVb.setTableId(rs.getString("TABLE_ID"));
							treeDetailsVb.setTableName(rs.getString("TABLE_NAME"));
							treeDetailsVb.setAliasName(rs.getString("TABLE_ALIAS_NAME"));
							treeDetailsVb.setBaseTableFlag(rs.getString("BASE_TABLE_FLAG"));
							treeDetailsVb.setQueryId(rs.getString("QUERY_ID"));
							treeDetailsVb.setDatabaseType(rs.getString("DATABASE_TYPE"));
							treeDetailsVb.setDatabaseConnectivityDetails(rs.getString("DATABASE_CONNECTIVITY_DETAILS"));
							treeDetailsVb.setTableSourceType(rs.getString("TABLE_SOURCE_TYPE"));
							treeDetailsVb.setAccessControlScript(rs.getString("ACCESS_CONTROL_SCRIPT"));
							if (treeDetailsVb != null && ValidationUtil.isValid(treeDetailsVb.getAccessControlScript())) {
								List<UserRestrictionVb> restrictionListToReturn = new ArrayList<UserRestrictionVb>();
								Matcher authMatchObj = Pattern.compile("<auth>(.*?)<\\/auth>", Pattern.DOTALL)
										.matcher(treeDetailsVb.getAccessControlScript());
								while (authMatchObj.find()) {
									String category = CommonUtils.getValueForXmlTag(authMatchObj.group(1), "category");
									UserRestrictionVb filteredCategoryVb = userRestList.stream()
											.filter(vb -> category.equalsIgnoreCase(vb.getMacrovarName()))
											.collect(Collectors.collectingAndThen(Collectors.toList(), restrictionList -> {
												if (restrictionList != null && restrictionList.size() == 1) {
													return restrictionList.get(0);
												} else {
													return null;
												}
											}));

									if (filteredCategoryVb != null) {
										if (filteredCategoryVb.getChildren() != null) {
											Iterator childrenItr = filteredCategoryVb.getChildren().iterator();
											while (childrenItr.hasNext()) {
												UserRestrictionVb childVb = (UserRestrictionVb) childrenItr.next();
												childVb.setTagValue(CommonUtils.getValueForXmlTag(authMatchObj.group(1), childVb.getTagName()));
											}
										}
									}
									restrictionListToReturn.add(filteredCategoryVb);
								}
								treeDetailsVb.setAccessControlScriptParsed(restrictionListToReturn.size()>0?restrictionListToReturn:null);
							}
						}
						
						VcConfigMainColumnsVb columnDetailsVb = new VcConfigMainColumnsVb();
						columnDetailsVb.setCatalogId(rs.getString("CATALOG_ID"));
						columnDetailsVb.setTableId(rs.getString("TABLE_ID"));
						columnDetailsVb.setColId(rs.getString("COL_ID"));
						columnDetailsVb.setColName(rs.getString("COL_NAME"));
						columnDetailsVb.setAliasName(rs.getString("COLUMN_ALIAS_NAME"));
						columnDetailsVb.setSortColumn(rs.getString("SORT_COLUMN"));
						columnDetailsVb.setColDisplayType(rs.getString("COL_DISPLAY_TYPE"));
						columnDetailsVb.setColAttributeType(rs.getString("COL_ATTRIBUTE_TYPE")); 
						columnDetailsVb.setColExperssionType(rs.getString("COL_EXPERSSION_TYPE"));
						if(ValidationUtil.isValid(rs.getString("EXPERSSION_TEXT")))
							columnDetailsVb.setExperssionText(rs.getString("EXPERSSION_TEXT").replaceAll("#TABLE_ALIAS#", ValidationUtil.isValid(rs.getString("TABLE_ALIAS_NAME"))?rs.getString("TABLE_ALIAS_NAME"):rs.getString("TABLE_NAME")));
						columnDetailsVb.setFormatType(rs.getString("FORMAT_TYPE"));
						columnDetailsVb.setFormatTypeDesc(rs.getString("FORMAT_TYPE_DESC"));
						columnDetailsVb.setDateFormattingSyntax(rs.getString("DATE_FORMATTING_SYNTAX"));
						columnDetailsVb.setDateConversionSyntax(rs.getString("DATE_CONVERSION_SYNTAX"));
						columnDetailsVb.setColType(rs.getString("COL_TYPE"));
						columnDetailsVb.setMaskingFlag(rs.getString("MASKING_FLAG"));
						columnDetailsVb.setMaskingScript(rs.getString("MASKING_SCRIPT"));
						if(ValidationUtil.isValid(rs.getString("INCLUDE_GROUP_COL")))
							columnDetailsVb.setIncludeGroupCol(rs.getString("INCLUDE_GROUP_COL").replaceAll("#TABLE_ALIAS#", ValidationUtil.isValid(rs.getString("TABLE_ALIAS_NAME"))?rs.getString("TABLE_ALIAS_NAME"):rs.getString("TABLE_NAME")));
						if(ValidationUtil.isValid(columnDetailsVb.getMaskingScript())) {
							List<MaskingVb> maskingVbList = new ArrayList<MaskingVb>();
							Matcher maskingMatchObj = Pattern.compile("<masking>(.*?)<\\/masking>", Pattern.DOTALL)
									.matcher(columnDetailsVb.getMaskingScript());
							while(maskingMatchObj.find()) {
								String userGroup = CommonUtils.getValueForXmlTag(maskingMatchObj.group(1), "usergroup");
								String userProfile = CommonUtils.getValueForXmlTag(maskingMatchObj.group(1), "userprofile");
								String pattern = CommonUtils.getValueForXmlTag(maskingMatchObj.group(1), "pattern");
								maskingVbList.add(new MaskingVb(userGroup, userProfile, pattern));
							}
							columnDetailsVb.setMaskingScriptParsed(maskingVbList.size()>0?maskingVbList:null);
						}
						colVbList.add(columnDetailsVb);
					}
					treeDetailsVb.setChildren(colVbList);
					tableDetailsHM.put(treeDetailsVb.getTableId(), treeDetailsVb);
					return null;
				}
			});
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		returnArrayList.add(linkedTblsForIndividualTblHM);
		returnArrayList.add(relationHM);
//		returnArrayList.add(aliasNameHM);
//		returnArrayList.add(tableNameHM);
		returnArrayList.add(tableDetailsHM);
		return returnArrayList;
	}
	
	public Integer getJoinTypeInCatalog(String catalogId) {
		String sql = "SELECT JOIN_CLAUSE FROM VISION_CATALOG_SELFBI where catalog_id='"+catalogId+"'";
		RowMapper mapper = new RowMapper() {
			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				return (rs.getInt("JOIN_CLAUSE"));
			}
		};
		return (Integer) jdbcTemplate.queryForObject(sql, null, mapper);
	}
	
	public String getBaseTableAliasName(Integer fromTableId, String catalogId)  throws DataAccessException{
		String sql = "SELECT TABLE_NAME, ALIAS_NAME FROM VC_TREE_SELFBI where table_id='"+fromTableId+"' AND catalog_id='"+catalogId+"'";
		RowMapper mapper = new RowMapper() {
			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				return (rs.getString("TABLE_NAME")+"@-@"+rs.getString("ALIAS_NAME"));
			}
		};
		return (String) jdbcTemplate.queryForObject(sql, null, mapper);
	}
	
	public String getCompleteReportDataAsXMLString(PromptTreeVb promptTree,final List<ColumnHeadersVb> columnHeaders, 
			long minRow, long maxRows) {
		String query = "";
		try{
			query = "SELECT T2.* FROM (SELECT rownum SEQ_VC_SEQ, T1.* FROM "+promptTree.getTableName()+" T1 ) T2 where "
				+ " SEQ_VC_SEQ > " + String.valueOf(minRow) + " AND SEQ_VC_SEQ <= " +String.valueOf(maxRows);
			ResultSetExtractor mapper = new ResultSetExtractor() {
				@Override
				public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
					StringBuffer lResult = new StringBuffer("");
					lResult.append("<tableData>");
					while(rs.next()){
						lResult.append("<tableRow>");
						for(ColumnHeadersVb columnHeader:columnHeaders){
							String value = rs.getString(columnHeader.getCaption());
							if(value == null) value="";
							value =value.replaceAll("\\\\","@-@");
							lResult.append("<").append(columnHeader.getCaption()).append(">").append(StringEscapeUtils.escapeXml(value)).append("</").append(columnHeader.getCaption()).append(">");
						}
						lResult.append("</tableRow>");
					}
					lResult.append("</tableData>");
					String str2 = lResult.toString().replaceAll("@-@","/");
					StringBuffer str3 = new StringBuffer(str2);
					return StringEscapeUtils.unescapeJava(str3.toString());
				}
			};
			return (String)jdbcTemplate.query(query, mapper);
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
	@SuppressWarnings("unchecked")
	public List<Object> getReportDataAsJSONString(PromptTreeVb promptTree,final List<VcForQueryReportFieldsVb> reportFields, final String[] colTypebasket) {
		String query = "";
		try{
			query = "SELECT COUNT(1) FROM "+promptTree.getTableName();
			promptTree.setTotalRows(jdbcTemplate.queryForObject(query,int.class));
			query = "SELECT * FROM "+promptTree.getTableName()+" where rownum < "+(promptTree.getMaxCatalogDisplayCount()+1)+" ";
			ResultSetExtractor mapper = new ResultSetExtractor() {
				@Override
				public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
					StringBuffer lResult = new StringBuffer("");
					lResult.append("<tableData>");
					ResultSetMetaData metadata = rs.getMetaData();
					List<ColumnHeadersVb> columnHeaders = new ArrayList<ColumnHeadersVb>(metadata.getColumnCount());
					for(int i=1;i<= metadata.getColumnCount(); i++){
						ColumnHeadersVb columnHeader = new ColumnHeadersVb(); 
						columnHeader.setCaption(metadata.getColumnLabel(i));
						if(reportFields != null && !reportFields.isEmpty()){
							for(VcForQueryReportFieldsVb rCReportFieldsVb: reportFields){
								if(ValidationUtil.isValid(rCReportFieldsVb.getAlias())){
									if(columnHeader.getCaption().equalsIgnoreCase(rCReportFieldsVb.getAlias())){
										columnHeader.setColType(rCReportFieldsVb.getColDisplayType());
									}
								}else{
									if(columnHeader.getCaption().equalsIgnoreCase(rCReportFieldsVb.getColName())){
										columnHeader.setColType(rCReportFieldsVb.getColDisplayType());
									}	
								}
							}
						}else{
							switch(metadata.getColumnType(i)){
							case  -7: case  -6: case  5: case  -5: case  4: case  3: case  2: 
							case  6: case  7: case  8:
								if(ValidationUtil.isValid(colTypebasket) && colTypebasket.length > 0) {
									int j=i-1;
									if(ValidationUtil.isValid(colTypebasket[j])){
										columnHeader.setColType(colTypebasket[j]);
									}else{
										 columnHeader.setColType("T");
									}
								}else{
									columnHeader.setColType("N");
								}
								break;
							default: columnHeader.setColType("T");
							}
						}
						columnHeaders.add(columnHeader);
					}
					while(rs.next()) {
						lResult.append("<tableRow>");
						for(ColumnHeadersVb columnHeader:columnHeaders){
							String value = rs.getString(columnHeader.getCaption());
							if(value == null) value="";
							value =value.replaceAll("\\\\","@-@");
							lResult.append("<").append(columnHeader.getCaption().replaceAll(" ","_")).append(">").append(StringEscapeUtils.escapeXml(value)).append("</").append(columnHeader.getCaption().replaceAll(" ","_")).append(">");
						}
						lResult.append("</tableRow>");
					}
					lResult.append("</tableData>");
					String str2 = lResult.toString().replaceAll("@-@","/");
					StringBuffer str3 = new StringBuffer(str2);
					List<Object> result = new ArrayList<Object>(2);
					result.add(columnHeaders);
					//result.add(StringEscapeUtils.unescapeJava(lResult.toString()).replaceAll("\u0019",""));
					result.add(StringEscapeUtils.unescapeJava(str3.toString()));
					return result;
				}
			};
			return (List<Object>)jdbcTemplate.query(query, mapper);
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
	
	public List<VcForQueryReportVb> getQueryAnalysisList() {
		setServiceDefaults();		
		String query="SELECT DISTINCT RS.REPORT_ID,RS.REPORT_DESCRIPTION,RS.MAKER,RS.VERIFIER,VU.USER_NAME " + 
				"FROM RS_ACCESS RSA JOIN SSBI_USER_VIEW VU ON RSA.USER_PROFILE = VU.USER_PROFILE AND RSA.USER_GROUP = VU.USER_GROUP " + 
				"JOIN report_suite RS on RS.REPORT_CATEGORY = RSA.REPORT_CATEGORY AND RS_STATUS =0 " +
				"WHERE  VU.VISION_ID ="+intCurrentUserId+" and VU.USER_GROUP ='"+userGroup+"' and VU.USER_PROFILE='"+userProfile+"'";
		try {
			RowMapper queryMapper = new RowMapper() {
			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcForQueryReportVb vcForQueryReportVb = new VcForQueryReportVb();
				vcForQueryReportVb.setReportId(rs.getString("REPORT_ID"));
				vcForQueryReportVb.setReportDescription(rs.getString("REPORT_DESCRIPTION"));
				vcForQueryReportVb.setMakerName(rs.getString("USER_NAME"));
				return vcForQueryReportVb;
			}
		};
			return jdbcTemplate.query(query,queryMapper);
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error(((query==null)? "query is Null":query));
			return null;
		}
	}
	
	public List<VcForQueryReportVb> getQueryReportsList() {
		setServiceDefaults();		
		String query="SELECT T3.USER_NAME, T1.CATALOG_ID, T1.REPORT_ID, T1.REPORT_DESCRIPTION,"+  
				"T1.REPORT_QUERY, T1.QUERY_TYPE FROM VC_REPORT_DEFS_SELFBI T1, VCQD_QUERIES_ACCESS T2,SSBI_USER_VIEW T3 "+
				"WHERE T2.VCQDA_STATUS = 0 AND T3.VISION_ID="+intCurrentUserId+" AND   T2.USER_GROUP = '"+userGroup+"' AND   T2.USER_PROFILE = '"+userProfile+"' AND"+
				" T2.USER_GROUP = T3.USER_GROUP AND   t2.USER_PROFILE = T3.USER_PROFILE AND T1.REPORT_ID = T2.VCQD_QUERY_ID "+
				"ORDER BY T1.CATALOG_ID, T1.REPORT_ID";		
		try {
			RowMapper queryMapper = new RowMapper() {
			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcForQueryReportVb vcForQueryReportVb = new VcForQueryReportVb();
				vcForQueryReportVb.setReportId(rs.getString("REPORT_ID"));
				vcForQueryReportVb.setReportDescription(rs.getString("REPORT_DESCRIPTION"));
				vcForQueryReportVb.setMakerName(rs.getString("USER_NAME"));
				return vcForQueryReportVb;
			}
		};
			return jdbcTemplate.query(query,queryMapper);
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error(((query==null)? "query is Null":query));
			return null;
		}
	}
	public List<Map<String, Object>> getQueryInteractiveDashboardList() {
		setServiceDefaults();		
		String query="SELECT MENU_PROGRAM, MENU_NAME AS DESCRIPTION,MENU_SEQUENCE,SEPARATOR,MENU_GROUP FROM VISION_MENU WHERE MENU_STATUS = 0 AND parent_sequence=5 ORDER BY MENU_NAME ASC";//WHERE parent_sequence=5";
		return jdbcTemplate.queryForList(query);
		/*try {
			RowMapper queryMapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcForQueryReportVb vcForQueryReportVb = new VcForQueryReportVb();
				vcForQueryReportVb.setReportId(rs.getString("MENU_NAME"));
				vcForQueryReportVb.setReportDescription(rs.getString("MENU_SEQUENCE"));
				vcForQueryReportVb.setMakerName(rs.getString("MENU_GROUP"));
				return vcForQueryReportVb;
				}
			};
		return jdbcTemplate.query(query,queryMapper);
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error(((query==null)? "query is Null":query));
			return null;
		}*/
	}
	
	@Transactional
	public ExceptionCode deleteReport(VcForQueryReportVb rcReportVb) {
		ExceptionCode exceptionCode = null;
		strApproveOperation ="Delete";
		strErrorDesc  = "";
		strCurrentOperation = "Delete";
		setServiceDefaults();
		try {
			//Delete all the Report Fields
			doDeleteReportFieldsByUserIdReportIdCatalogId(rcReportVb);
			doDeleteReportByUserIdReportIdCatalogId(rcReportVb);
			doDeleteReportByReportIdFromVcqdQueries(rcReportVb);
			doDeleteReportByUserIdReportIdFromVcqdAccess(rcReportVb);
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		}catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}catch(Exception ex){
			logger.error("Error in Delete.",ex);
			logger.error( ((rcReportVb==null)? "vObject is Null":rcReportVb.toString()));
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}
	
	private int doDeleteReportByUserIdReportIdCatalogId(VcForQueryReportVb vObject){
		String query = "DELETE FROM VC_REPORT_DEFS_SELFBI WHERE CATALOG_ID = ? AND REPORT_ID = ? AND USER_ID = '"+intCurrentUserId+"' ";
		Object[] args = {vObject.getCatalogId(),vObject.getReportId()};
		return jdbcTemplate.update(query,args);
	}
	private int doDeleteReportByUserIdReportIdFromVcqdAccess(VcForQueryReportVb vObject){
		String query = "DELETE FROM VCQD_QUERIES_ACCESS WHERE VCQD_QUERY_ID = ?  ";
		Object[] args = {vObject.getReportId()};
		return jdbcTemplate.update(query,args);
	}
	private int doDeleteReportByReportIdFromVcqdQueries(VcForQueryReportVb vObject){
		String query = "DELETE FROM VCQD_QUERIES WHERE VCQD_QUERY_ID = ? AND CATALOG_ID = ? ";
		Object[] args = {vObject.getReportId(),vObject.getCatalogId()};
		return jdbcTemplate.update(query,args);
	}
	
	public String fetchRecordFromConnectorFileUploadMapper(String connectorId, String fileName) {
		try {
			String sql = "SELECT SELF_BI_MAPPING_TABLE_NAME FROM CONNECTOR_FILE_UPLOAD_MAPPER WHERE UPPER(CONNECTOR_ID) = UPPER(?) AND UPPER(FILE_TABLE_NAME) = UPPER(?)";
			Object args[] = {connectorId, fileName};
			return jdbcTemplate.queryForObject(sql, args, String.class);
		} catch(Exception e) {
			throw new RuntimeCryptoException("No Record found in CONNECTOR_FILE_UPLOAD_MAPPER for connector ["+connectorId+"]");
		}
	}
	public String fetchConnectionScriptFromVisionDynamicHashVariable(String connectorId) {
		try {
			String sql = "SELECT VARIABLE_SCRIPT FROM VISION_DYNAMIC_HASH_VAR WHERE UPPER(VARIABLE_NAME) = UPPER(?)";
			Object args[] = {connectorId};
			return jdbcTemplate.queryForObject(sql, args, String.class);
		} catch(Exception e) {
			throw new RuntimeCryptoException("No Record found in VISION_DYNAMIC_HASH_VAR for connector ["+connectorId+"]");
		}
	}
	public DCManualQueryVb fetchManualQueryDetailFromVcQueries(String queryId) {
		try {
			String sql = "SELECT QUERY_ID, QUERY_DESCRIPTION, DATABASE_TYPE_AT, DATABASE_TYPE, "+
					" DATABASE_CONNECTIVITY_DETAILS,LOOKUP_DATA_LOADING_AT, LOOKUP_DATA_LOADING, QUERY_VALID_FLAG, SQL_QUERY, "+
					" STG_QUERY1, STG_QUERY2, STG_QUERY3, POST_QUERY, VCQ_STATUS_NT, VCQ_STATUS, "+
					" RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER,INTERNAL_STATUS, "+
					" "+new CommonDao(jdbcTemplate).getDbFunction("FORMAT")+"(DATE_LAST_MODIFIED, 'DD-MM-YYYY "+new CommonDao(jdbcTemplate).getDbFunction("TIME")+"') DATE_LAST_MODIFIED, "+
					" "+new CommonDao(jdbcTemplate).getDbFunction("FORMAT")+"(DATE_CREATION, 'DD-MM-YYYY "+new CommonDao(jdbcTemplate).getDbFunction("TIME")+"') DATE_CREATION, "+
					" COLUMNS_METADATA, HASH_VARIABLE_SCRIPT FROM VC_QUERIES TAPPR WHERE UPPER(QUERY_ID) = UPPER(?) ";
			Object args[] = {queryId.toUpperCase()};
			List<DCManualQueryVb> queryIdList = jdbcTemplate.query(sql, args, new RowMapper<DCManualQueryVb>() {
				@Override
				public DCManualQueryVb mapRow(ResultSet rs, int rowNum) throws SQLException {
					DCManualQueryVb vObj = new DCManualQueryVb();
					vObj.setQueryId(rs.getString("QUERY_ID"));
					vObj.setQueryDescription(rs.getString("QUERY_DESCRIPTION"));
					vObj.setDatabaseType(rs.getString("DATABASE_TYPE"));
					vObj.setDatabaseConnectivityDetails(rs.getString("DATABASE_CONNECTIVITY_DETAILS"));
					vObj.setLookupDataLoading(rs.getString("LOOKUP_DATA_LOADING"));
					vObj.setSqlQuery(ValidationUtil.isValid(rs.getString("SQL_QUERY"))?rs.getString("SQL_QUERY"):"");
					vObj.setStgQuery1(ValidationUtil.isValid(rs.getString("STG_QUERY1"))?rs.getString("STG_QUERY1"):"");
					vObj.setStgQuery2(ValidationUtil.isValid(rs.getString("STG_QUERY2"))?rs.getString("STG_QUERY2"):"");
					vObj.setStgQuery3(ValidationUtil.isValid(rs.getString("STG_QUERY3"))?rs.getString("STG_QUERY3"):"");
					vObj.setPostQuery(ValidationUtil.isValid(rs.getString("POST_QUERY"))?rs.getString("POST_QUERY"):"");
					vObj.setVcqStatusNt(rs.getInt("VCQ_STATUS_NT"));
					vObj.setVcqStatus(rs.getInt("VCQ_STATUS"));
					vObj.setDbStatus(rs.getInt("VCQ_STATUS"));
					vObj.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
					vObj.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
					vObj.setMaker(rs.getLong("MAKER"));
					vObj.setVerifier(rs.getLong("VERIFIER"));
					vObj.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
					vObj.setDateCreation(rs.getString("DATE_CREATION"));
					vObj.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
					vObj.setQueryColumnXML(ValidationUtil.isValid(rs.getString("COLUMNS_METADATA"))?rs.getString("COLUMNS_METADATA"):"");
					vObj.setHashVariableScript(ValidationUtil.isValid(rs.getString("HASH_VARIABLE_SCRIPT"))?rs.getString("HASH_VARIABLE_SCRIPT"):"");
					vObj.setQueryValidFlag(ValidationUtil.isValid(rs.getString("QUERY_VALID_FLAG"))?"TRUE":"FALSE");
					return vObj;
				}
			});
			if(ValidationUtil.isValidList(queryIdList))
				return queryIdList.get(0);
			return null;
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	public ExceptionCode formResponceJsonForExecute(VcForQueryReportFieldsWrapperVb vcForQueryReportFieldsWrapperVb, String TableName) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			JSONObject headerObj = new JSONObject();
			List<XmlJsonUploadVb> columnNameList = new ArrayList<XmlJsonUploadVb>();
			for(VcForQueryReportFieldsVb fieldVb : vcForQueryReportFieldsWrapperVb.getReportFields()) {
				if("Y".equalsIgnoreCase(fieldVb.getDisplayFlag())) {
					String colDisplayType = (fieldVb.getColDisplayType().equalsIgnoreCase("C") || fieldVb.getColDisplayType().equalsIgnoreCase("N"))?"M":"D";
					String colScaleFormatLabel = "";
					if("M".equalsIgnoreCase(colDisplayType) && "y".equalsIgnoreCase(fieldVb.getScalingFlag()) && ValidationUtil.isValid(fieldVb.getScalingFormat())) {
						switch(String.valueOf(fieldVb.getScalingFormat())) {
							case "1000":
								colScaleFormatLabel = "(t)";
								break;
							case "1000000":
								colScaleFormatLabel = "(m)";
								break;
							case "1000000000":
								colScaleFormatLabel = "(b)";
								break;
							case "1000000000000":
								colScaleFormatLabel = "(tr)";
								break;
						}
					}
					Map<String, Object> propertyMap = new HashMap<String, Object>();
					propertyMap.put("colDisplayType",colDisplayType);
					propertyMap.put("colScaleFormatLabel",colScaleFormatLabel);
					columnNameList.add(new XmlJsonUploadVb(fieldVb.getAlias(), propertyMap));
//					columnNameList.add(new XmlJsonUploadVb(fieldVb.getAlias()));
				}
			}
			headerObj.put("ROW1", columnNameList);

			StringBuffer selectStr = new StringBuffer(" ");
			int columnIndx=0;
			for(VcForQueryReportFieldsVb reportsObj : vcForQueryReportFieldsWrapperVb.getReportFields()) {
			
				if(ValidationUtil.isValid(reportsObj.getDisplayFlag()) && reportsObj.getDisplayFlag().equals("N")) {
					continue;
				}

				String colDisplayType = reportsObj.getColDisplayType();
				String scalingFlag = reportsObj.getScalingFlag();
				String scalingFormat = String.valueOf(reportsObj.getScalingFormat());
				String numberFormat = reportsObj.getNumberFormat();
				String decimalFlag = reportsObj.getDecimalFlag();
				String decimalCount = String.valueOf(reportsObj.getDecimalCount());
				
				String columnAlias = reportsObj.getAlias();
				Map<String, Object> propertyMap = new HashMap<String, Object>();
				propertyMap.put("colDisplayType",colDisplayType);
				
				if(ValidationUtil.isValid(reportsObj.getOperator()) && !"null".equalsIgnoreCase(reportsObj.getOperator())
				&& ("sum".equalsIgnoreCase(reportsObj.getOperator()) || "max".equalsIgnoreCase(reportsObj.getOperator())
                || "min".equalsIgnoreCase(reportsObj.getOperator()) || "avg".equalsIgnoreCase(reportsObj.getOperator()) 
                || "count".equalsIgnoreCase(reportsObj.getOperator())) ) {
					
					/* Aggrigation already applied in the temp table. So, no configured aggrigation needs to be included here*/
					
//					selectStr.append("TO_CHAR("+aggFunction+"("+columnAlias+") /"+scalingFormat+", '999,999,999,999.90') "+columnAlias+", ");
					String tempColumn = "("+"T"+"."+columnAlias+")";
					if ("y".equalsIgnoreCase(scalingFlag)) {
						tempColumn = tempColumn+"/"+scalingFormat;
					}
					if("y".equalsIgnoreCase(numberFormat)) {
						if("y".equalsIgnoreCase(decimalFlag)) {
							String decimalFormat = CommonUtils.returnDecimalFormat(decimalCount);
							if("ORACLE".equalsIgnoreCase(databaseType))
								tempColumn = "trim(TO_CHAR("+tempColumn+", '999,999,999,990."+decimalFormat+"'))";
							else if("MSSQL".equalsIgnoreCase(databaseType))
								tempColumn = "LTRIM(RTRIM(FORMAT("+tempColumn+", '###,###,###,###."+decimalFormat+"')))";

						} else {
							if("ORACLE".equalsIgnoreCase(databaseType))
								tempColumn = "trim(TO_CHAR("+tempColumn+", '999,999,999,990'))";
							else if("MSSQL".equalsIgnoreCase(databaseType))
								tempColumn = "LTRIM(RTRIM(FORMAT("+tempColumn+", '###,###,###,###')))";
						}
					}
					selectStr.append(tempColumn+" "+columnAlias+", ");
/*					isGroupByEnabled = true;
*/				} else {
						String tempColumn = "T"+"."+columnAlias;
						if ("y".equalsIgnoreCase(scalingFlag)) {
							tempColumn = tempColumn+"/"+scalingFormat;
						}
						if("y".equalsIgnoreCase(numberFormat)) {
							if("y".equalsIgnoreCase(decimalFlag)) {
								String decimalFormat = CommonUtils.returnDecimalFormat(decimalCount);
								if("ORACLE".equalsIgnoreCase(databaseType))
									tempColumn = "trim(TO_CHAR("+tempColumn+", '999,999,999,990."+decimalFormat+"'))";
								else if("MSSQL".equalsIgnoreCase(databaseType))
									tempColumn = "LTRIM(RTRIM(FORMAT("+tempColumn+", '###,###,###,###."+decimalFormat+"')))";

							} else {
								if("ORACLE".equalsIgnoreCase(databaseType))
									tempColumn = "trim(TO_CHAR("+tempColumn+", '999,999,999,990'))";
								else if("ORACLE".equalsIgnoreCase(databaseType))
									tempColumn = "LTRIM(RTRIM(FORMAT("+tempColumn+", '###,###,###,###')))";
							}
						}
						if((ValidationUtil.isValid(reportsObj.getColDisplayType()) && ( "d".equalsIgnoreCase(reportsObj.getColDisplayType())) )){
							//TO_DATE("+filterVb.getColumnName()+",'DD-MON-RRRR')
//							selectStr.append("FORMAT("+tempColumn+",'"+reportsObj.getDynamicDateFormat()+"')" +columnAlias+", ");
							selectStr.append(reportsObj.getDateFormattingSyntax().replaceAll("#VALUE#", tempColumn)+" "+columnAlias+", ");
						}
						else {	
						selectStr.append(tempColumn+" "+columnAlias+", ");
				     }
              }	
			}
			String selectvar = selectStr.substring(0, (selectStr.length()-2));
			
			Long totalRows =0l;
			if(vcForQueryReportFieldsWrapperVb.getMainModel().getTotalRows() == 0l)
				totalRows = jdbcTemplate.queryForObject("SELECT  COUNT(*) FROM "+TableName,Long.class);
			else 
				totalRows = vcForQueryReportFieldsWrapperVb.getMainModel().getTotalRows();
			
			vcForQueryReportFieldsWrapperVb.getMainModel().setTotalRows(totalRows);
			List<String> rowStringList = new ArrayList<String>();
			/*jdbcTemplate.query("SELECT * FROM (SELECT ROWNUM SID, "+selectvar+" FROM  "+TableName+" T "+(ValidationUtil.isValid(vcForQueryReportFieldsWrapperVb.getMainModel().getGroupBy1())?" GROUP BY ROWNUM, "+vcForQueryReportFieldsWrapperVb.getMainModel().getGroupBy1():"")+" ) WHERE SID >="+vcForQueryReportFieldsWrapperVb.getMainModel().getStartIndex()+" AND SID <= "+vcForQueryReportFieldsWrapperVb.getMainModel().getLastIndex(), new ResultSetExtractor<Object>() {*/
			String orderBy = vcForQueryReportFieldsWrapperVb.getMainModel().getOrderBy();
			String sql = "";
			if("ORACLE".equalsIgnoreCase(databaseType))
				sql = "SELECT * FROM (SELECT ROWNUM SID, "+selectvar+" FROM  "+TableName+" T "+(ValidationUtil.isValid(orderBy)?("ORDER BY "+orderBy):"")+" ) WHERE SID >="+vcForQueryReportFieldsWrapperVb.getMainModel().getStartIndex()+" AND SID <= "+vcForQueryReportFieldsWrapperVb.getMainModel().getLastIndex();
			else if("MSSQL".equalsIgnoreCase(databaseType))
				sql = "SELECT * FROM (SELECT * FROM (select ROW_NUMBER() OVER( ORDER BY "+(ValidationUtil.isValid(orderBy.trim())?orderBy:"(SELECT NULL)")+" ) num, rowtemp.* from("+
					"SELECT "+selectvar+" FROM "+ TableName + " T) rowtemp) TEMP WHERE NUM <="+vcForQueryReportFieldsWrapperVb.getMainModel().getLastIndex()+") T1 WHERE NUM>="+vcForQueryReportFieldsWrapperVb.getMainModel().getStartIndex()+" ORDER BY NUM";

			
			jdbcTemplate.query(sql, new ResultSetExtractor<Object>() {
				@Override
				public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
					int rowIndex = 1;
					rowLoop:while(rs.next()) {
						List<XmlJsonUploadVb> cellValueList = new ArrayList<XmlJsonUploadVb>();
						for(XmlJsonUploadVb headerVb:columnNameList) {
							cellValueList.add(new XmlJsonUploadVb(rs.getString(headerVb.getData())));
						}
						JSONObject rowObj = new JSONObject();
						rowObj.put("Row"+rowIndex, cellValueList);
						rowStringList.add(rowObj.toString());
						if(rowIndex==10000) {
							break rowLoop;
						}
						rowIndex++;
					}
					return null;
				}
			});
			Map<String, Object> returnMap = new HashMap<String, Object>();
			returnMap.put("HEADER", headerObj.toString());
			returnMap.put("BODY", rowStringList);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(returnMap);
		} catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	
	public ExceptionCode executeSqlString(String sql) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			jdbcTemplate.execute(sql);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}

	public void dropTable(String tableName) {
		try {
			String sql = "DROP TABLE "+tableName;
			jdbcTemplate.execute(sql);
		} catch(Exception e) {}
	}
	
	public List<VcForQueryReportVb> getReportDetailListingFromReportDefs(VcForQueryReportVb vObj) throws DataAccessException {
		StringBuffer sql = new StringBuffer(" SELECT T1.CATALOG_ID, T1.REPORT_ID, T1.USER_ID, T1.REPORT_DESCRIPTION, T1.REPORT_QUERY, T1.QUERY_WHERE, " +
				" T1.QUERY_GROUP_BY, T1.QUERY_ORDER_BY, T2.HASH_VARIABLE_SCRIPT, T1.DISTINCT_FLAG FROM VC_REPORT_DEFS_SELFBI T1, VCQD_QUERIES T2 " +
				" WHERE (T1.REPORT_ID = T2.VCQD_QUERY_ID) ");
		List<Object> argumentList = new ArrayList<Object>();
		if(ValidationUtil.isValid(vObj.getCatalogId())) {
			argumentList.add(vObj.getCatalogId());
			CommonUtils.addToQuery("UPPER(T1.CATALOG_ID) = UPPER(?) ", sql);
		}
		
		if(ValidationUtil.isValid(vObj.getReportId())) {
			argumentList.add(vObj.getReportId());
			CommonUtils.addToQuery("UPPER(T1.REPORT_ID) = UPPER(?) ", sql);
		}
		
		Object[] args = new Object[argumentList.size()];
		argumentList.toArray(args);
		return jdbcTemplate.query(String.valueOf(sql), args, new RowMapper<VcForQueryReportVb>() {
			@Override
			public VcForQueryReportVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcForQueryReportVb vbObj = new VcForQueryReportVb();
				vbObj.setCatalogId(rs.getString("CATALOG_ID"));
				vbObj.setReportId(rs.getString("REPORT_ID"));
				vbObj.setReportDescription(rs.getString("REPORT_DESCRIPTION"));
				vbObj.setSelect(rs.getString("REPORT_QUERY"));
				vbObj.setWhere(rs.getString("QUERY_WHERE"));
				vbObj.setGroupBy(rs.getString("QUERY_GROUP_BY"));
				vbObj.setOrderBy(rs.getString("QUERY_ORDER_BY"));
				vbObj.setHashVariableScript(rs.getString("HASH_VARIABLE_SCRIPT"));
				vbObj.setDistinctFlag(rs.getString("DISTINCT_FLAG"));
				return vbObj;
			}
		});
	}
	
	public List<VcForQueryReportFieldsVb> getReportFieldsDetailFromReportFields(VcForQueryReportVb vObj) throws DataAccessException {
		StringBuffer sql = new StringBuffer(" SELECT T1.CATALOG_ID, T1.USER_ID, T1.REPORT_ID, T1.REPORT_DESCRIPTION, T1.REPORT_QUERY, " +
				" T1.QUERY_WHERE, T1.QUERY_GROUP_BY, T1.QUERY_ORDER_BY, " +
				" T2.TABLE_ID, T2.COL_ID, T3.COL_NAME, T2.ALIAS, T2.OPERATOR, " +
				" T2.VALUE_1, T2.VALUE_2, T2.DISPLAY_FLAG, T2.SORT_TYPE, T2.SORT_ORDER, T2.JOIN_CONDITION, " +
				" T2.GROUP_BY, T3.COL_DISPLAY_TYPE, T3.COL_ATTRIBUTE_TYPE, T3.COL_EXPERSSION_TYPE, T3.FORMAT_TYPE, " +
				" (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = T3.FORMAT_TYPE) FORMAT_TYPE_DESC, " +
				" T3.MAG_ENABLE_FLAG, T3.MAG_QUERY_ID, T3.MAG_TYPE, T3.MAG_SELECTION_TYPE, T3.MAG_USE_COLUMN, " +
				" T3.MAG_DISPLAY_COLUMN, T3.MAG_DEFAULT, T3.EXPERSSION_TEXT, T3.COL_TYPE, T3.MASKING_FLAG, " +
				" T3.MASKING_SCRIPT, T3.COL_LENGTH, T4.TABLE_NAME, T4.ALIAS_NAME TABLE_ALIAS_NAME,"+ 
				" T2.AGG_FUNCTION,T2.SCALING_FLAG,T2.SCALING_FORMAT,T2.NUMBER_FORMAT,T2.DECIMAL_FLAG,T2.DECIMAL_COUNT, " +
                " T2.DYNAMIC_START_FLAG,T2.DYNAMIC_END_FLAG,T2.DYNAMIC_START_DATE,T2.DYNAMIC_END_DATE,T2.DYNAMIC_START_OPERATOR,T2.DYNAMIC_END_OPERATOR, "+
                " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = T3.FORMAT_TYPE) DYNAMIC_DATE_FORMAT, "+
                " (SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = T3.FORMAT_TYPE)JAVA_DATE_FORMAT,  T3.INCLUDE_GROUP_COL, " +
                " (select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = '"+databaseType+"' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = T3.FORMAT_TYPE) DATE_FORMATTING_SYNTAX, " +
                " (select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = '"+databaseType+"' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = T3.FORMAT_TYPE) DATE_CONVERSION_SYNTAX," +
                " T2.SUMMARY_CRITERIA, T2.SUMMARY_VALUE_1, T2.SUMMARY_VALUE_2, " +
                " T2.SUMMARY_DYNAMIC_START_FLAG, T2.SUMMARY_DYNAMIC_END_FLAG, T2.SUMMARY_DYNAMIC_START_DATE, T2.SUMMARY_DYNAMIC_END_DATE, " +
                " T2.SUMMARY_DYNAMIC_START_OPERATOR, T2.SUMMARY_DYNAMIC_END_OPERATOR "+
				" FROM VC_REPORT_DEFS_SELFBI T1, VC_REPORT_FIELDS_SELFBI T2, VC_COLUMNS_SELFBI T3, VC_TREE_SELFBI T4 " +
				" WHERE (T1.CATALOG_ID = T2.CATALOG_ID AND T1.REPORT_ID = T2.REPORT_ID " +
				" AND T2.CATALOG_ID = T3.CATALOG_ID AND T2.TABLE_ID = T3.TABLE_ID AND T2.COL_ID = T3.COL_ID " +
				" AND T3.CATALOG_ID = T4.CATALOG_ID AND T3.TABLE_ID = T4.TABLE_ID) ");
		List<Object> argumentList = new ArrayList<Object>();
		if(ValidationUtil.isValid(vObj.getCatalogId())) {
			argumentList.add(vObj.getCatalogId());
			CommonUtils.addToQuery("UPPER(T1.CATALOG_ID) = UPPER(?) ", sql);
		}
		
		if(ValidationUtil.isValid(vObj.getReportId())) {
			argumentList.add(vObj.getReportId());
			CommonUtils.addToQuery("UPPER(T1.REPORT_ID) = UPPER(?) ", sql);
		}
		
		sql.append(" ORDER BY T2.SORT_ORDER");
		
		Object[] args = new Object[argumentList.size()];
		argumentList.toArray(args);
		return jdbcTemplate.query(String.valueOf(sql), args, new RowMapper<VcForQueryReportFieldsVb>() {
			@Override
			public VcForQueryReportFieldsVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcForQueryReportFieldsVb vbObj = new VcForQueryReportFieldsVb();
				vbObj.setCatalogId(rs.getString("CATALOG_ID"));
				vbObj.setReportId(rs.getString("REPORT_ID"));
				vbObj.setTabelId(rs.getString("TABLE_ID"));
				vbObj.setColId(rs.getString("COL_ID"));
				vbObj.setColName(rs.getString("COL_NAME"));
				vbObj.setAlias(rs.getString("ALIAS"));
				vbObj.setOperator(rs.getString("OPERATOR"));
				vbObj.setValue1(rs.getString("VALUE_1"));
				vbObj.setValue2(rs.getString("VALUE_2"));
				vbObj.setDisplayFlag(rs.getString("DISPLAY_FLAG"));
				vbObj.setSortType(rs.getString("SORT_TYPE"));
				vbObj.setSortOrder(rs.getInt("SORT_ORDER"));
				vbObj.setJoinCondition(rs.getString("JOIN_CONDITION"));
				vbObj.setGroupBy(rs.getString("GROUP_BY"));
				vbObj.setColDisplayType(rs.getString("COL_DISPLAY_TYPE"));
				vbObj.setColAttributeType(rs.getString("COL_ATTRIBUTE_TYPE")); 
				vbObj.setColExpressionType(rs.getString("COL_EXPERSSION_TYPE"));
				vbObj.setFormatType(rs.getInt("FORMAT_TYPE"));
				vbObj.setFormatTypeDesc(rs.getString("FORMAT_TYPE_DESC"));
				vbObj.setMagEnableFlag(rs.getString("MAG_ENABLE_FLAG"));
				vbObj.setMagQueryId(rs.getString("MAG_QUERY_ID"));
				vbObj.setMagType(rs.getString("MAG_TYPE"));
				vbObj.setMagSelectionType(rs.getString("MAG_SELECTION_TYPE"));
				vbObj.setMagUseColumn(rs.getString("MAG_USE_COLUMN"));
				vbObj.setMagDisplayColumn(rs.getString("MAG_DISPLAY_COLUMN"));
				vbObj.setMagDefault(rs.getString("MAG_DEFAULT"));
				vbObj.setExperssionText(rs.getString("EXPERSSION_TEXT"));
				vbObj.setColType(rs.getString("COL_TYPE"));
				vbObj.setMaskingFlag(rs.getString("MASKING_FLAG"));
				vbObj.setMaskingScript(rs.getString("MASKING_SCRIPT"));
				vbObj.setColLength(rs.getString("COL_LENGTH"));
				vbObj.setTableName(rs.getString("TABLE_NAME"));
				vbObj.setTabelAliasName(rs.getString("TABLE_ALIAS_NAME"));
				vbObj.setAggFunction(rs.getString("AGG_FUNCTION"));
				vbObj.setScalingFlag(rs.getString("SCALING_FLAG"));
				vbObj.setScalingFormat(rs.getLong("SCALING_FORMAT"));
				vbObj.setNumberFormat(rs.getString("NUMBER_FORMAT"));
				vbObj.setDecimalFlag(rs.getString("DECIMAL_FLAG"));
				vbObj.setDecimalCount(rs.getInt("DECIMAL_COUNT"));
				vbObj.setDynamicStartFlag(rs.getString("DYNAMIC_START_FLAG"));
				vbObj.setDynamicEndFlag(rs.getString("DYNAMIC_END_FLAG"));
				vbObj.setDynamicStartDate(rs.getString("DYNAMIC_START_DATE"));
				vbObj.setDynamicEndDate(rs.getString("DYNAMIC_END_DATE"));
				vbObj.setDynamicStartOperator(rs.getInt("DYNAMIC_START_OPERATOR"));
				vbObj.setDynamicEndOperator(rs.getInt("DYNAMIC_END_OPERATOR"));
				vbObj.setDynamicDateFormat(rs.getString("DYNAMIC_DATE_FORMAT"));
				vbObj.setJavaFormatDesc(rs.getString("JAVA_DATE_FORMAT"));
				vbObj.setDateFormattingSyntax(rs.getString("DATE_FORMATTING_SYNTAX"));
				vbObj.setDateConversionSyntax(rs.getString("DATE_CONVERSION_SYNTAX"));
				if(ValidationUtil.isValid(rs.getString("INCLUDE_GROUP_COL")))
					vbObj.setIncludeGroupCol(rs.getString("INCLUDE_GROUP_COL").replaceAll("#TABLE_ALIAS#", ValidationUtil.isValid(rs.getString("TABLE_ALIAS_NAME"))?rs.getString("TABLE_ALIAS_NAME"):rs.getString("TABLE_NAME")));
				vbObj.setSummaryCriteria(rs.getString("SUMMARY_CRITERIA"));
				vbObj.setSummaryValue1(rs.getString("SUMMARY_VALUE_1"));
				vbObj.setSummaryValue2(rs.getString("SUMMARY_VALUE_2"));
				vbObj.setSummaryDynamicStartFlag(rs.getString("SUMMARY_DYNAMIC_START_FLAG"));
				vbObj.setSummaryDynamicEndFlag(rs.getString("SUMMARY_DYNAMIC_END_FLAG"));
				vbObj.setSummaryDynamicStartDate(rs.getString("SUMMARY_DYNAMIC_START_DATE"));
				vbObj.setSummaryDynamicEndDate(rs.getString("SUMMARY_DYNAMIC_END_DATE"));
				vbObj.setSummaryDynamicStartOperator(rs.getInt("SUMMARY_DYNAMIC_START_OPERATOR"));
				vbObj.setSummaryDynamicEndOperator(rs.getInt("SUMMARY_DYNAMIC_END_OPERATOR"));
				return vbObj;
			}
		});
	}
	
	public int insertRecordIntoStaggingTableLog(String tableName) {
		try {
			String sql = " Insert into VWC_STAGGING_TABLE_LOGGING "+
					   " (TABLE_NAME, PROCESSED, DATE_LAST_MODIFIED, DATE_CREATION) "+
					" Values "+
					" ('"+tableName+"', 'Y', "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+", "+new CommonDao(jdbcTemplate).getDbFunction("SYSDATE")+")";
			return jdbcTemplate.update(sql);
		} catch (Exception e) {
			e.printStackTrace();
			return Constants.ERRONEOUS_OPERATION;
		}
	}
	
	public List<WidgetDesignVb> validatingMainWidgetsData(String vcqdQuery) {
		return jdbcTemplate.query("select WIDGET_ID,DESCRIPTION from VWC_MAIN_WIDGETS WHERE QUERY_ID ='"+vcqdQuery+"' ", new RowMapper<WidgetDesignVb>() {
			@Override
			public WidgetDesignVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				WidgetDesignVb widgetVb = new WidgetDesignVb();
				widgetVb.setWidgetId(rs.getString("WIDGET_ID"));
				widgetVb.setDescription(rs.getString("DESCRIPTION"));
				return widgetVb;
			}
		});
		
	}

	public synchronized int getMaxVersionNumber(String macroVar) {
		String sql = "SELECT CASE WHEN MAX(VERSION_NO) IS NULL THEN 0 ELSE MAX(VERSION_NO) END VERSION_NO FROM VCQD_QUERIES_AD WHERE VCQD_QUERY_ID = ?";
		Object args[] = {macroVar};
		return jdbcTemplate.queryForObject(sql, args, Integer.class);
	}
	public void moveMainDataToAD(String macroVar) {
	Integer versionNumber = getMaxVersionNumber (macroVar)+1;
	
		jdbcTemplate.update("INSERT INTO  VCQD_QUERIES_AD(VCQD_QUERY_ID,QUERY_TYPE_AT,QUERY_TYPE,VCQD_QUERY,VCQD_QUERY_XML,VCQD_STATUS_NT,VCQD_STATUS, " + 
				" RECORD_INDICATOR_NT,RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION,CATALOG_ID, " + 
				" HASH_VARIABLE_SCRIPT,VERSION_NO) select VCQD_QUERY_ID,QUERY_TYPE_AT,QUERY_TYPE,VCQD_QUERY,VCQD_QUERY_XML,VCQD_STATUS_NT,VCQD_STATUS,RECORD_INDICATOR_NT, " + 
				" RECORD_INDICATOR,MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION,CATALOG_ID,HASH_VARIABLE_SCRIPT, '"+versionNumber+"'  from VCQD_QUERIES WHERE VCQD_QUERY_ID='"+macroVar+"' ");
		
		jdbcTemplate.update("INSERT INTO VC_REPORT_DEFS_SELFBI_AD(CATALOG_ID,USER_ID,REPORT_ID,REPORT_DESCRIPTION,QUERY_TYPE,REPORT_QUERY,RECORD_INDICATOR_NT, " + 
				" RECORD_INDICATOR,VRD_STATUS_NT,VRD_STATUS,MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION,QUERY_WHERE,QUERY_GROUP_BY, " + 
				" QUERY_ORDER_BY,VERSION_NO)select CATALOG_ID,USER_ID,REPORT_ID,REPORT_DESCRIPTION,QUERY_TYPE,REPORT_QUERY,RECORD_INDICATOR_NT,RECORD_INDICATOR,VRD_STATUS_NT, " + 
				" VRD_STATUS,MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION,QUERY_WHERE,QUERY_GROUP_BY,QUERY_ORDER_BY, ' "+versionNumber+ "'  " + 
				" from VC_REPORT_DEFS_SELFBI WHERE REPORT_ID ='"+macroVar+"' ");

		
		jdbcTemplate.update("INSERT INTO VC_REPORT_FIELDS_SELFBI_AD(CATALOG_ID,TABLE_ID,COL_ID,USER_ID,REPORT_ID,COL_NAME,ALIAS,OPERATOR,DISPLAY_FLAG,SORT_TYPE,SORT_ORDER,AGG_FUNCTION,CONDITION_OPERATOR,VALUE_1, " + 
				" VALUE_2,GROUP_BY,COL_TYPE,COL_DISPLAY_TYPE,FORMAT_TYPE_NT,FORMAT_TYPE,RECORD_INDICATOR_NT,RECORD_INDICATOR,VRF_STATUS_NT,VRF_STATUS,MAKER,VERIFIER, " + 
				" INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION,EXPERSSION_TEXT,VC_COL_ID,JOIN_CONDITION,SCALING_FLAG,SCALING_FORMAT,NUMBER_FORMAT,DECIMAL_FLAG,DECIMAL_COUNT,VERSION_NO, "+
				" DYNAMIC_START_FLAG,DYNAMIC_END_FLAG,DYNAMIC_START_DATE,DYNAMIC_END_DATE,DYNAMIC_START_OPERATOR,DYNAMIC_END_OPERATOR)SELECT CATALOG_ID,TABLE_ID,COL_ID,USER_ID,REPORT_ID,COL_NAME, " + 
				" ALIAS,OPERATOR,DISPLAY_FLAG,SORT_TYPE,SORT_ORDER,AGG_FUNCTION,CONDITION_OPERATOR,VALUE_1,VALUE_2,GROUP_BY,COL_TYPE,COL_DISPLAY_TYPE,FORMAT_TYPE_NT, " + 
				" FORMAT_TYPE,RECORD_INDICATOR_NT,RECORD_INDICATOR,VRF_STATUS_NT,VRF_STATUS,MAKER,VERIFIER,INTERNAL_STATUS,DATE_LAST_MODIFIED,DATE_CREATION,EXPERSSION_TEXT, " + 
				" VC_COL_ID,JOIN_CONDITION,SCALING_FLAG,SCALING_FORMAT,NUMBER_FORMAT,DECIMAL_FLAG,DECIMAL_COUNT, '"+versionNumber+"' , "+
				" DYNAMIC_START_FLAG,DYNAMIC_END_FLAG,DYNAMIC_START_DATE,DYNAMIC_END_DATE,DYNAMIC_START_OPERATOR,DYNAMIC_END_OPERATOR from VC_REPORT_FIELDS_SELFBI WHERE REPORT_ID = '"+macroVar+"' ");

		
	}

	public List<DesignAnalysisVb> getQuerySmartSearchFilter(DesignAnalysisVb designVb) {
		Vector<Object> params = new Vector<Object>();
		
		setServiceDefaults();
		
		StringBuffer strBufApprove = new StringBuffer(" select T1.CATALOG_ID, T1.REPORT_ID, T1.REPORT_DESCRIPTION, T1.USER_ID, T3.USER_GROUP, T3.USER_PROFILE, " +
				"  T1.MAKER, (select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = T1.MAKER) as MAKER_NAME ,"
				+ " T1.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = T1.VERIFIER) as VERIFIER_NAME, T1.VRD_STATUS, " + 
				" (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 1 AND NUM_SUB_TAB = T1.VRD_STATUS ) STATUS_DESC, " +
				" "+new CommonDao(jdbcTemplate).getDbFunction("DATEFUNC")+"(T1.DATE_LAST_MODIFIED, 'DD-MM-YYYY "+new CommonDao(jdbcTemplate).getDbFunction("TIME")+"') DATE_LAST_MODIFIED, " +
				" "+new CommonDao(jdbcTemplate).getDbFunction("DATEFUNC")+"(T1.DATE_CREATION, 'DD-MM-YYYY "+new CommonDao(jdbcTemplate).getDbFunction("TIME")+"') DATE_CREATION " +
				" from (VC_REPORT_DEFS_SELFBI T1 JOIN VCQD_QUERIES T2  " +
				"     ON (T1.REPORT_ID = T2.VCQD_QUERY_ID))  Left outer JOIN  VCQD_QUERIES_ACCESS T3 ON (T2.VCQD_QUERY_ID = T3.VCQD_QUERY_ID) " +
				" WHERE (T2.MAKER = ? OR (T3.USER_GROUP = ? AND T3.USER_PROFILE = ?)) AND ");
		
		/*StringBuffer strBufApprove = new StringBuffer(
				" SELECT TAPPR.CATALOG_ID, TAPPR.CATALOG_DESC, TAPPR.JOIN_CLAUSE, TAPPR.BASETABLE_JOINFLAG, "
						+ " TAPPR.VC_STATUS_NT, TAPPR.VC_STATUS, "
						+ " (Select NUM_SUBTAB_DESCRIPTION from NUM_SUB_TAB where NUM_TAB = TAppr.VC_STATUS_NT and TAPPR.VC_STATUS=NUM_SUB_TAB ) AS STATUS_DESC,"
						+ " TAPPR.RECORD_INDICATOR_NT, TAPPR.RECORD_INDICATOR, TAPPR.MAKER, (select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TAPPR.MAKER) as MAKER_NAME,"
						+ " TAPPR.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TAPPR.VERIFIER) as VERIFIER_NAME, TAPPR.INTERNAL_STATUS, "
						+ " TO_CHAR(TAPPR.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, "
						+ " TO_CHAR(TAPPR.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION "
						+ " FROM VISION_CATALOG_SELFBI TAPPR LEFT JOIN CATALOG_ACCESS_SelfBI T2 ON (TAPPR.CATALOG_ID = T2.CATALOG_ID) "
						+ " WHERE (TAPPR.MAKER = '"+intCurrentUserId+"'  OR (T2.USER_GROUP = '"+userGroup+"' AND T2.USER_PROFILE = '"+userProfile+"')) AND ");*/
		/*StringBuffer strBufPending = new StringBuffer(
				" SELECT TWIP.CATALOG_ID, TWIP.CATALOG_DESC, TWIP.JOIN_CLAUSE, TWIP.BASETABLE_JOINFLAG, "
						+ " TWIP.VC_STATUS_NT, TWIP.VC_STATUS, "
						+ " (Select NUM_SUBTAB_DESCRIPTION from NUM_SUB_TAB where NUM_TAB = TWIP.VC_STATUS_NT and TWIP.VC_STATUS=NUM_SUB_TAB ) AS STATUS_DESC,"
						+ " TWIP.RECORD_INDICATOR_NT, TWIP.RECORD_INDICATOR, TWIP.MAKER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TWIP.MAKER) as MAKER_NAME,"
						+ " TWIP.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TWIP.VERIFIER) as VERIFIER_NAME ,TWIP.INTERNAL_STATUS, "
						+ " TO_CHAR(TWIP.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, "
						+ " TO_CHAR(TWIP.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION "
						+ " FROM VISION_CATALOG_WIP TWIP LEFT JOIN CATALOG_ACCESS_SelfBI T2 ON (TWIP.CATALOG_ID = T2.CATALOG_ID) "
						+ " WHERE (TWIP.MAKER = '"+intCurrentUserId+"'  OR (T2.USER_GROUP = '"+userGroup+"' AND T2.USER_PROFILE = '"+userProfile+"')) AND ");*/
		try {

			params.addElement( intCurrentUserId );
			params.addElement( userGroup );
			params.addElement( userProfile );
			
			if (designVb.getSmartSearchOpt().size() > 0) {
				int count = 1;
				for (SmartSearchVb data: designVb.getSmartSearchOpt()){
					if(count == designVb.getSmartSearchOpt().size()) {
						data.setJoinType("");
					} else {
						if(!ValidationUtil.isValid(data.getJoinType()) && !("AND".equalsIgnoreCase(data.getJoinType()) || "OR".equalsIgnoreCase(data.getJoinType()))) {
							data.setJoinType("AND");
						}
					}
					String val = CommonUtils.criteriaBasedVal(data.getCriteria(), data.getValue().trim());
					switch (data.getObject()) {
					case "catalogId":
						CommonUtils.addToQuerySearch(" upper(T1.CATALOG_ID) "+ val, strBufApprove, data.getJoinType());
						break;

					case "vcqdQueryId":
						CommonUtils.addToQuerySearch(" upper(T1.REPORT_ID) "+ val, strBufApprove, data.getJoinType());
						break;

					case "vcqdQueryDesc":
					/*	List<NumSubTabVb> numSTData = numSubTabDao.findNumSubTabByDesc(val);
						String actData="";
						for(int k=0; k< numSTData.size(); k++) {
							int numsubtab = numSTData.get(k).getNumSubTab();
							if(!ValidationUtil.isValid(actData)) {
								actData = "'"+Integer.toString(numsubtab)+"'";
							}else {
								actData =actData+ ","+ "'"+Integer.toString(numsubtab)+"'";
							}
						}*/
						CommonUtils.addToQuerySearch(" upper(T1.REPORT_DESCRIPTION) "+val, strBufApprove, data.getJoinType());
						break;

					case "dateCreation":
						CommonUtils.addToQuerySearch(" "+new CommonDao(jdbcTemplate).getDbFunction("DATEFUNC")+"(T1.DATE_CREATION,'DD-MM-YYYY "+new CommonDao(jdbcTemplate).getDbFunction("TIME")+"') " + val, strBufApprove, data.getJoinType());
						break;

					case "dateLastModified":
						CommonUtils.addToQuerySearch(" "+new CommonDao(jdbcTemplate).getDbFunction("DATEFUNC")+"(T1.DATE_LAST_MODIFIED,'DD-MM-YYYY "+new CommonDao(jdbcTemplate).getDbFunction("TIME")+"') " + val, strBufApprove, data.getJoinType());
						break;

					case "makerName":
						List<VisionUsersVb> muData = visionUsersDao.findUserIdByDesc(val);
						String actMData="";
						for(int k=0; k< muData.size(); k++) {
							int visId = muData.get(k).getVisionId();
							if(!ValidationUtil.isValid(actMData)) {
								actMData = "'"+Integer.toString(visId)+"'";
							}else {
								actMData =actMData+ ","+ "'"+Integer.toString(visId)+"'";
							}
						}
						CommonUtils.addToQuerySearch(" (T1.MAKER) IN ("+ actMData+") ", strBufApprove, data.getJoinType());
						break;
					
					default:
					}
					count++;
				}
			}
			String orderBy = " Order By CATALOG_ID ";
			return getQueryPopupResultsWithPend(designVb, null, strBufApprove, "", orderBy, params, getMapperForDsQuery());

		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(((strBufApprove == null) ? "strBufApprove is Null" : strBufApprove.toString()));

			if (params != null)
				for (int i = 0; i < params.size(); i++)
					logger.error("objParams[" + i + "]" + params.get(i).toString());
			return null;

		}
	}
	
	public DSConnectorVb getQueriesForDatasource() {
		DSConnectorVb connector =null;
		try {
			String sql = "SELECT TABLE_LIST_QUERY,TABLE_FILTER_LIST_QUERY,COLUMN_LIST_QUERY FROM  VISION_DYNAMIC_HASH_VAR WHERE VARIABLE_NAME = '"+CommonUtils.DEFAULT_DB+"' ";
			connector = jdbcTemplate.queryForObject(sql, new RowMapper<DSConnectorVb>() {
				@Override
				public DSConnectorVb mapRow(ResultSet rs, int rowNum) throws SQLException {
					DSConnectorVb connector = new DSConnectorVb();
					connector.setTableQuery(rs.getString("TABLE_LIST_QUERY"));
					connector.setTableExcludeQuery(rs.getString("TABLE_FILTER_LIST_QUERY"));
					connector.setTableColumnQuery(rs.getString("COLUMN_LIST_QUERY"));
					return connector; 
				}
			});
		}catch(Exception e){
			e.printStackTrace();
		}
		return connector;
		
	}

}
