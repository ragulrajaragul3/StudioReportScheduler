package com.vision.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import com.vision.authentication.SessionContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonApiModel;
import com.vision.vb.CommonVb;
import com.vision.vb.VisionUsersVb;

@Component
public class CommonApiDao extends AbstractDao<CommonVb>{
	
	@Value("${app.productName}")
	private String productName;
	public JdbcTemplate jdbcTemplate = null;
	public CommonApiDao(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	
	public ExceptionCode getCommonResultDataFetch(CommonApiModel vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		ArrayList result = new ArrayList(); 
		try
		{	
			String orginalQuery = getPrdQueryConfig(vObject.getQueryId());
			if(!ValidationUtil.isValid(orginalQuery)) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("No Queries maintained for the Query Id["+vObject.getQueryId()+"]");
				return exceptionCode;
			}
			orginalQuery = replacePromptVariables(orginalQuery, vObject);
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
					ResultSetMetaData metaData = rs.getMetaData();
					int colCount = metaData.getColumnCount();
					Boolean dataPresent = false;
					while(rs.next()){
						HashMap<String,String> resultData = new HashMap<String,String>();
						dataPresent = true;
						for(int cn = 1;cn <= colCount;cn++) {
							String columnName = metaData.getColumnName(cn);
							resultData.put(columnName.toUpperCase(), rs.getString(columnName));
						}
						result.add(resultData);
					}
					if(dataPresent) {
						exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
						exceptionCode.setResponse(result);
					}else {
						exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
					}
					return exceptionCode;
				}
			};
			return (ExceptionCode)getJdbcTemplate().query(orginalQuery, mapper);
		}catch(Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
	}
	public String replacePromptVariables(String query, CommonApiModel vObj) {
		query = query.replaceAll("#PROMPT_VALUE_1#", vObj.getParam1());
		query = query.replaceAll("#PROMPT_VALUE_2#", vObj.getParam2());
		query = query.replaceAll("#PROMPT_VALUE_3#", vObj.getParam3());
		query = query.replaceAll("#PROMPT_VALUE_4#", vObj.getParam4());
		query = query.replaceAll("#PROMPT_VALUE_5#", vObj.getParam5());
		query = query.replaceAll("#PROMPT_VALUE_6#", vObj.getParam6());
		query = query.replaceAll("#PROMPT_VALUE_7#", vObj.getParam7());
		query = query.replaceAll("#PROMPT_VALUE_8#", vObj.getParam8());
		query = query.replaceAll("#PROMPT_VALUE_9#", vObj.getParam9());
		query = query.replaceAll("#PROMPT_VALUE_10#", vObj.getParam10());
		query = query.replaceAll("#VISION_ID#", "" + SessionContextHolder.getContext().getVisionId());
		return query;
	}
	public String getPrdQueryConfig(String queryId){
		String resultQuery = "";
		try
		{			
			String sql = "SELECT QUERY from PRD_QUERY_CONFIG WHERE DATA_REF_ID = '"+queryId+"' AND STATUS = 0 AND APPLICATION_ID = '"+productName+"' ";
			resultQuery = getJdbcTemplate().queryForObject(sql,String.class);
			return resultQuery;
		}catch(Exception ex){
			logger.error("Exception while getting the Query for the Query ID["+queryId+"]");
			return null;
		}
	}
	public int checkSaveTheme(VisionUsersVb vObj){
		int cnt = 0;
		try
		{			
			String sql = "SELECT  count(1) FROM PRD_APP_THEME where Vision_ID = "+vObj.getVisionId()+" AND APPLICATION_ID = '"+productName+"' ";
			cnt = getJdbcTemplate().queryForObject(sql,Integer.class);
			if(cnt > 0 ) {
				deleteSaveTheme(vObj);
			}
			return cnt;
		}catch(Exception ex){
			return 0;
		}
	}
	public int deleteSaveTheme(VisionUsersVb vObj){
		try
		{			
			String sql = "DELETE FROM PRD_APP_THEME where Vision_ID = "+vObj.getVisionId()+" AND APPLICATION_ID = '"+productName+"' ";
			int retVal = getJdbcTemplate().update(sql);
			return retVal;
		}catch(Exception ex){
			return 0;
		}
	}
	public ExceptionCode getCommonResultDataQuery(String query){
		ExceptionCode exceptionCode = new ExceptionCode();
		ArrayList result = new ArrayList(); 
		try
		{	
			if(!ValidationUtil.isValid(query)) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Query Invalid");
				return exceptionCode;
			}
			//orginalQuery = replacePromptVariables(query, vObject);
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
					ResultSetMetaData metaData = rs.getMetaData();
					int colCount = metaData.getColumnCount();
					Boolean dataPresent = false;
					while(rs.next()){
						LinkedHashMap<String,String> resultData = new LinkedHashMap<String,String>();
						dataPresent = true;
						for(int cn = 1;cn <= colCount;cn++) {
							String columnName = metaData.getColumnName(cn);
							resultData.put(columnName.toUpperCase(), rs.getString(columnName));
						}
						result.add(resultData);
					}
					if(dataPresent) {
						exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
						exceptionCode.setResponse(result);
					}else {
						exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
					}
					return exceptionCode;
				}
			};
			return (ExceptionCode)getJdbcTemplate().query(query, mapper);
		}catch(Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
	}
	public ExceptionCode insertAppTheme(VisionUsersVb vObj){
		ExceptionCode exceptionCode = new  ExceptionCode();
		try
		{		
			checkSaveTheme(vObj);
			String sql = "insert into PRD_APP_THEME(Vision_ID,App_Theme,Report_Slide_Theme, Language,Application_ID)"
					+ "values(?,?,?,? ,?) ";
			Object[] args = {vObj.getVisionId(),vObj.getAppTheme(),vObj.getReportSliderTheme(),vObj.getLanguage(),productName};
			
			int retVal =  getJdbcTemplate().update(sql,args);
			
			if(retVal == Constants.SUCCESSFUL_OPERATION) {
				exceptionCode.setErrorCode(retVal);
				exceptionCode.setErrorMsg("Theme Saved Successfully");
			}else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Theme Not Saved");
			}
		}catch(Exception ex){
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Theme Not Saved");
		}
		return exceptionCode;
	}
	public ExceptionCode getCommonResultDataQuery(String query,Connection conExt){
		ExceptionCode exceptionCode = new ExceptionCode();
		ArrayList result = new ArrayList(); 
		try
		{	
			if(!ValidationUtil.isValid(query)) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Query Invalid");
				return exceptionCode;
			}
			//orginalQuery = replacePromptVariables(query, vObject);
			ResultSetExtractor mapper = new ResultSetExtractor() {
				public Object extractData(ResultSet rs)  throws SQLException, DataAccessException {
					ResultSetMetaData metaData = rs.getMetaData();
					int colCount = metaData.getColumnCount();
					Boolean dataPresent = false;
					while(rs.next()){
						LinkedHashMap<String,String> resultData = new LinkedHashMap<String,String>();
						dataPresent = true;
						for(int cn = 1;cn <= colCount;cn++) {
							String columnName = metaData.getColumnName(cn);
							resultData.put(columnName.toUpperCase(), rs.getString(columnName));
						}
						result.add(resultData);
					}
					if(dataPresent) {
						exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
						exceptionCode.setResponse(result);
					}else {
						exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
					}
					return exceptionCode;
				}
			};
			return (ExceptionCode)jdbcTemplate.query(query, mapper);
		}catch(Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
	}	
}
