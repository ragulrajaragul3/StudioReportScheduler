/**
 * 
 */
package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.SessionContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.TabVb;

@Component
public class AlphaSubTabDao extends AbstractDao<AlphaSubTabVb> {
	
	@SuppressWarnings("unchecked")
	public List<AlphaSubTabVb> findActiveAlphaSubTabsByAlphaTab(int pAlphaTab) throws DataAccessException {
		String sql = "SELECT * FROM ALPHA_SUB_TAB WHERE ALPHA_SUBTAB_STATUS = 0 AND ALPHA_TAB = ?  AND ALPHA_SUB_TAB !='Z' ORDER BY ALPHA_SUB_TAB";
		Object[] lParams = new Object[1];
		lParams[0] = pAlphaTab;
		return  getJdbcTemplate().query(sql, lParams, getMapper());
	}
	
	@SuppressWarnings("rawtypes")
	public List findActiveAlphaSubTabsByAlphaTabCols(int pNumTab, String cols) throws DataAccessException {
		if(!ValidationUtil.isValid(cols))
			cols="*";

		String sql = "SELECT "+cols+" FROM ALPHA_SUB_TAB WHERE ALPHA_SUBTAB_STATUS = 0 AND ALPHA_TAB = ?  AND ALPHA_SUB_TAB !='Z' ORDER BY ALPHA_SUB_TAB";
		Object[] lParams = new Object[1];
		lParams[0] = pNumTab;
		return  jdbcTemplate.queryForList(sql,lParams);
	}
	
	@SuppressWarnings("unchecked")
	public List<AlphaSubTabVb> findActiveAlphaSubTabsByAlphaTab(int pAlphaTab, String orderBy) throws DataAccessException {
		if(!ValidationUtil.isValid(orderBy)){
			orderBy = "ALPHA_SUB_TAB";
		}
		String sql = "SELECT * FROM ALPHA_SUB_TAB WHERE ALPHA_SUBTAB_STATUS = 0 AND ALPHA_TAB = ? ORDER BY "+orderBy;
		Object[] lParams = new Object[1];
		lParams[0] = pAlphaTab;
		return  getJdbcTemplate().query(sql, lParams, getMapper());
	}
	@SuppressWarnings("unchecked")
	public List<AlphaSubTabVb> findAlphaSubTabsByAlphaTab(int pAlphaTab) throws DataAccessException {
		
		String sql = "SELECT * FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = ? ORDER BY ALPHA_SUB_TAB";
		Object[] lParams = new Object[1];
		lParams[0] = pAlphaTab;
		return  getJdbcTemplate().query(sql, lParams, getMapper());
	}
	@SuppressWarnings("unchecked")
	public List<AlphaSubTabVb> findAlphaSubTabsByAlphaSubTabs(int pAlphaTab, String alphaSubTab) throws DataAccessException {
		
		String sql = "SELECT * FROM ALPHA_SUB_TAB WHERE ALPHA_TAB = ? AND ALPHA_SUB_TAB != '"+alphaSubTab+"' ORDER BY ALPHA_SUB_TAB";
		Object[] lParams = new Object[1];
		lParams[0] = pAlphaTab;
		return  getJdbcTemplate().query(sql, lParams, getMapper());
	}	
	@SuppressWarnings("unchecked")
	public List<AlphaSubTabVb> findAlphaSubTabsByAlphaTabAndStatus(int pAlphaTab, int[] status, String orderBy) throws DataAccessException {
		String query = "SELECT * FROM ALPHA_SUB_TAB WHERE ALPHA_SUBTAB_STATUS IN (";
		StringBuffer sqlParams = new StringBuffer();
		Object arr[] = new Integer[status.length+1];
		int i = 0;
		for(;i < status.length; i++){
			sqlParams.append("?");
			if (i < status.length-1) sqlParams.append(",");
			arr[i] = new Integer(status[i]);
		}
		if(!ValidationUtil.isValid(orderBy)){
			orderBy = "ALPHA_SUB_TAB";
		}
		query = query + sqlParams.toString() + ") AND ALPHA_TAB = ? ORDER BY "+orderBy;
		arr[i] = new Integer(pAlphaTab);
		return getJdbcTemplate().query(query, arr, getMapper());
	}
	@SuppressWarnings("unchecked")
	public List<AlphaSubTabVb> findAlphaSubTabsByAlphaTab(int pAlphaTab, String[] pSubtabs, String orderBy) throws DataAccessException {
		String query = "SELECT * FROM ALPHA_SUB_TAB WHERE ALPHA_SUB_TAB IN (";
		StringBuffer sqlParams = new StringBuffer();
		Object arr[] = new String[pSubtabs.length+1];
		int i = 0;
		for(;i < pSubtabs.length; i++){
			sqlParams.append("?");
			if (i < pSubtabs.length-1) sqlParams.append(",");
			arr[i] = new String(pSubtabs[i]);
		}
		if(!ValidationUtil.isValid(orderBy)){
			orderBy = "ALPHA_SUB_TAB";
		}
		query = query + sqlParams.toString() + ") AND ALPHA_TAB = ? ORDER BY "+orderBy;
		arr[i] = new String(pAlphaTab+"");
		return getJdbcTemplate().query(query, arr, getMapper());
	}
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				AlphaSubTabVb alphaSubTabVb = new AlphaSubTabVb();
				alphaSubTabVb.setAlphaTab(rs.getInt("ALPHA_TAB"));
				alphaSubTabVb.setAlphaSubTab(rs.getString("ALPHA_SUB_TAB"));
				alphaSubTabVb.setDescription(rs.getString("ALPHA_SUBTAB_DESCRIPTION"));
				alphaSubTabVb.setAlphaSubTabStatusNt(rs.getInt("ALPHA_SUBTAB_STATUS_NT"));
				alphaSubTabVb.setAlphaSubTabStatus(rs.getInt("ALPHA_SUBTAB_STATUS"));
				alphaSubTabVb.setDbStatus(rs.getInt("ALPHA_SUBTAB_STATUS"));
				alphaSubTabVb.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				alphaSubTabVb.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				alphaSubTabVb.setMaker(rs.getLong("MAKER"));
				alphaSubTabVb.setVerifier(rs.getLong("VERIFIER"));
				alphaSubTabVb.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				alphaSubTabVb.setDateCreation(rs.getString("DATE_CREATION"));
				alphaSubTabVb.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				return alphaSubTabVb;
			}
		};
		return mapper;
	}
	public List<AlphaSubTabVb> getQueryPopupResults(AlphaSubTabVb dObj){
		Vector<Object> params = new Vector<Object>();
		setServiceDefaults();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.ALPHA_TAB, TAppr.ALPHA_SUB_TAB, TAppr.ALPHA_SUBTAB_DESCRIPTION, TAppr.ALPHA_SUBTAB_STATUS_NT, " +
				"TAppr.ALPHA_SUBTAB_STATUS, TAppr.RECORD_INDICATOR_NT, TAppr.RECORD_INDICATOR,  TAppr.MAKER, TAppr.VERIFIER, To_Char(TAppr.DATE_LAST_MODIFIED, 'DD-MM-RRRR HH24:MI:SS') DATE_LAST_MODIFIED, " +
				"To_Char(TAppr.DATE_CREATION, 'DD-MM-RRRR HH24:MI:SS') DATE_CREATION, INTERNAL_STATUS From ALPHA_SUB_TAB TAppr ");
		String strWhereNotExists = new String( " Not Exists " +
				"(Select 'X' From ALPHA_SUB_TAB_PEND TPend Where TPend.ALPHA_TAB = TAppr.ALPHA_TAB AND TAppr.ALPHA_SUB_TAB=TPend.ALPHA_SUB_TAB) ");
		StringBuffer strBufPending = new StringBuffer("Select TPend.ALPHA_TAB, TPend.ALPHA_SUB_TAB, TPend.ALPHA_SUBTAB_DESCRIPTION, TPend.ALPHA_SUBTAB_STATUS_NT, " +
				"TPend.ALPHA_SUBTAB_STATUS,  TPend.RECORD_INDICATOR_NT, TPend.RECORD_INDICATOR, TPend.MAKER,TPend.VERIFIER, " +
				"To_Char(TPend.DATE_LAST_MODIFIED, 'DD-MM-RRRR HH24:MI:SS') DATE_LAST_MODIFIED, " +
				"To_Char(TPend.DATE_CREATION, 'DD-MM-RRRR HH24:MI:SS') DATE_CREATION, INTERNAL_STATUS From ALPHA_SUB_TAB_Pend TPend ");
		try
		{
			//check if the column [ALPHA_TAB] should be included in the query
			if (dObj.getAlphaTab() != -1)
			{
				params.addElement(new Integer(dObj.getAlphaTab()));
				CommonUtils.addToQuery("TAppr.ALPHA_TAB = ?", strBufApprove);
				CommonUtils.addToQuery("TPend.ALPHA_TAB = ?", strBufPending);
			}
			//check if the column [ALPHA_SUB_TAB] should be included in the query
			if (ValidationUtil.isValid(dObj.getAlphaSubTab()) && !"-1".equalsIgnoreCase(dObj.getAlphaSubTab()))
			{
				params.addElement(dObj.getAlphaSubTab());
				CommonUtils.addToQuery("TAppr.ALPHA_SUB_TAB = ?", strBufApprove);
				CommonUtils.addToQuery("TPend.ALPHA_SUB_TAB = ?", strBufPending);
			}
			//check if the column [ALPHA_SUBTAB_DESCRIPTION] should be included in the query
			if (ValidationUtil.isValid(dObj.getDescription()))
			{
				params.addElement("%" + dObj.getDescription() + "%");
				CommonUtils.addToQuery("TAppr.ALPHA_SUBTAB_DESCRIPTION LIKE ?", strBufApprove);
				CommonUtils.addToQuery("TPend.ALPHA_SUBTAB_DESCRIPTION LIKE ?", strBufPending);
			}
			//check if the column [ALPHA_SUBTAB_STATUS] should be included in the query
			if (dObj.getAlphaSubTabStatus() != -1)
			{
				params.addElement(dObj.getAlphaSubTabStatus());
				CommonUtils.addToQuery("TAppr.ALPHA_SUBTAB_STATUS = ?", strBufApprove);
				CommonUtils.addToQuery("TPend.ALPHA_SUBTAB_STATUS = ?", strBufPending);
			}
			//check if the column [RECORD_INDICATOR] should be included in the query
			if (dObj.getRecordIndicator() != -1)
			{
				if (dObj.getRecordIndicator() > 3)
				{
					params.addElement(new Integer(0));
					CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
					CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
				}
				else
				{
					params.addElement(new Integer(dObj.getRecordIndicator()));
					CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
					CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
				}
			}
			String orderBy = " Order By ALPHA_TAB, ALPHA_SUB_TAB ";
			return getQueryPopupResults(dObj,strBufPending, strBufApprove, strWhereNotExists, orderBy, params, getMapper());
			
		}catch(Exception ex){
			
			ex.printStackTrace();
			logger.error(((strBufApprove==null)? "strBufApprove is Null":strBufApprove.toString()));
			logger.error("UNION");
			logger.error(((strBufPending==null)? "strBufPending is Null":strBufPending.toString()));

			if (params != null)
				for(int i=0 ; i< params.size(); i++)
					logger.error("objParams[" + i + "]" + params.get(i).toString());
			return null;

		}
	}
	@Override
	public List<AlphaSubTabVb> getQueryResults(AlphaSubTabVb dObj, int intStatus){
		return getQueryResultsForReview(dObj, intStatus);
	}
	public List<AlphaSubTabVb> getQueryResultsForReview(AlphaSubTabVb dObj, int intStatus){

		List<AlphaSubTabVb> collTemp = null;
		final int intKeyFieldsCount = 2;

		String strQueryAppr = new String("Select TAppr.ALPHA_TAB, TAppr.ALPHA_SUB_TAB, TAppr.ALPHA_SUBTAB_DESCRIPTION, " +
				"TAppr.ALPHA_SUBTAB_STATUS_NT, TAppr.ALPHA_SUBTAB_STATUS, TAppr.RECORD_INDICATOR_NT, TAppr.RECORD_INDICATOR, " +
				"TAppr.MAKER, TAppr.VERIFIER, To_Char(TAppr.DATE_LAST_MODIFIED, 'DD-MM-RRRR HH24:MI:SS') DATE_LAST_MODIFIED, " +
				"To_Char(TAppr.DATE_CREATION, 'DD-MM-RRRR HH24:MI:SS') DATE_CREATION, INTERNAL_STATUS From ALPHA_SUB_TAB TAppr " +
				"Where TAppr.ALPHA_TAB= ? AND TAppr.ALPHA_SUB_TAB =?");
		String strQueryPend = new String("Select TPend.ALPHA_TAB, TPend.ALPHA_SUB_TAB, TPend.ALPHA_SUBTAB_DESCRIPTION, " +
				"TPend.ALPHA_SUBTAB_STATUS_NT, TPend.ALPHA_SUBTAB_STATUS, TPend.RECORD_INDICATOR_NT, TPend.RECORD_INDICATOR, " +
				"TPend.MAKER, TPend.VERIFIER, To_Char(TPend.DATE_LAST_MODIFIED, 'DD-MM-RRRR HH24:MI:SS') DATE_LAST_MODIFIED, " +
				"To_Char(TPend.DATE_CREATION, 'DD-MM-RRRR HH24:MI:SS') DATE_CREATION, INTERNAL_STATUS From ALPHA_SUB_TAB_PEND TPend " +
				"Where TPend.ALPHA_TAB= ? AND TPend.ALPHA_SUB_TAB =?");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getAlphaTab();
		objParams[1] = dObj.getAlphaSubTab();

		try
		{	if(!dObj.isVerificationRequired()){intStatus =0;}
			if(intStatus == 0){
				logger.info("Executing approved query");
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(),objParams,getMapper());
			}else{
				logger.info("Executing pending query");
				collTemp = getJdbcTemplate().query(strQueryPend.toString(),objParams,getMapper());
			}
			return collTemp;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Error: getQueryResultsForReview Exception :   ");
			if(intStatus == 0)
				logger.error(((strQueryAppr == null) ? "strQueryAppr is Null" : strQueryAppr.toString()));
			else
				logger.error(((strQueryPend == null) ? "strQueryPend is Null" : strQueryPend.toString()));

			if (objParams != null)
				for(int i=0 ; i< objParams.length; i++)
					logger.error("objParams[" + i + "]" + objParams[i].toString());
			return null;
		}
	}
	public List<AlphaSubTabVb> getQueryResultsByParent(AlphaSubTabVb dObj, int intStatus){

		List<AlphaSubTabVb> collTemp = null;
		final int intKeyFieldsCount = 1;

		String strQueryAppr = new String("Select TAppr.ALPHA_TAB, TAppr.ALPHA_SUB_TAB, TAppr.ALPHA_SUBTAB_DESCRIPTION, " +
				"TAppr.ALPHA_SUBTAB_STATUS_NT, TAppr.ALPHA_SUBTAB_STATUS, TAppr.RECORD_INDICATOR_NT, TAppr.RECORD_INDICATOR, " +
				"TAppr.MAKER, TAppr.VERIFIER, To_Char(TAppr.DATE_LAST_MODIFIED, 'DD-MM-RRRR HH24:MI:SS') DATE_LAST_MODIFIED, " +
				"To_Char(TAppr.DATE_CREATION, 'DD-MM-RRRR HH24:MI:SS') DATE_CREATION, INTERNAL_STATUS From ALPHA_SUB_TAB TAppr " +
				"Where TAppr.ALPHA_TAB= ?");
		String strQueryPend = new String("Select TPend.ALPHA_TAB, TPend.ALPHA_SUB_TAB, TPend.ALPHA_SUBTAB_DESCRIPTION, " +
				"TPend.ALPHA_SUBTAB_STATUS_NT, TPend.ALPHA_SUBTAB_STATUS, TPend.RECORD_INDICATOR_NT, TPend.RECORD_INDICATOR, " +
				"TPend.MAKER, TPend.VERIFIER, To_Char(TPend.DATE_LAST_MODIFIED, 'DD-MM-RRRR HH24:MI:SS') DATE_LAST_MODIFIED, " +
				"To_Char(TPend.DATE_CREATION, 'DD-MM-RRRR HH24:MI:SS') DATE_CREATION, INTERNAL_STATUS From ALPHA_SUB_TAB_PEND TPend " +
				"Where TPend.ALPHA_TAB= ?");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getAlphaTab();

		try
		{	if(!dObj.isVerificationRequired()){intStatus =0;}
			if(intStatus == 0){
				logger.info("Executing approved query");
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(),objParams,getMapper());
			}else{
				logger.info("Executing pending query");
				collTemp = getJdbcTemplate().query(strQueryPend.toString(),objParams,getMapper());
			}
			return collTemp;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Error: getQueryResultsForReview Exception :   ");
			if(intStatus == 0)
				logger.error(((strQueryAppr == null) ? "strQueryAppr is Null" : strQueryAppr.toString()));
			else
				logger.error(((strQueryPend == null) ? "strQueryPend is Null" : strQueryPend.toString()));

			if (objParams != null)
				for(int i=0 ; i< objParams.length; i++)
					logger.error("objParams[" + i + "]" + objParams[i].toString());
			return null;
		}
	}
	@Override
	protected List<AlphaSubTabVb> selectApprovedRecord(AlphaSubTabVb vObject){
		return getQueryResultsForReview(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<AlphaSubTabVb> doSelectPendingRecord(AlphaSubTabVb vObject){
		return getQueryResultsForReview(vObject, Constants.STATUS_PENDING);
	}
	
	public void setPendingRecordCount(TabVb objectTemp) {
		final int intKeyFieldsCount = 1;
		int intStatus = 1;
		StringBuffer strQueryPend = new StringBuffer("Select COUNT('X')" +
			" From ALPHA_SUB_TAB_PEND TPend Where TPend.ALPHA_TAB = '"+objectTemp.getTab()+"' ");
		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = objectTemp.getTab();//[ALPHA_TAB]
		try
		{
			if(!objectTemp.isVerificationRequired()){intStatus =0;}
			if(intStatus != 0){
				logger.info("Executing pending query");
				int count = getJdbcTemplate().queryForObject(strQueryPend.toString(),Integer.class);
				//int count = getJdbcTemplate().queryForInt(strQueryPend.toString(),objParams);
				objectTemp.setPendingRecordsCount(count);
			}
		}
		catch(Exception ex){
			ex.printStackTrace();
			logger.error("Error: getQueryResultsForReview Exception :   ");
			logger.error(((strQueryPend == null) ? "strQueryPend is null" : strQueryPend.toString()));

		}
	}
	public void setTotRecordCount(TabVb objectTemp) {
		final int intKeyFieldsCount = 1;
		int intStatus = 1;
		StringBuffer strQueryAppr = new StringBuffer("Select COUNT('X')" +
			" From ALPHA_SUB_TAB TAppr Where TAppr.ALPHA_TAB = '"+objectTemp.getTab()+"' ");
		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = objectTemp.getTab();//[ALPHA_TAB]
		try
		{
			if(!objectTemp.isVerificationRequired()){intStatus =0;}
			if(intStatus != 0){
				logger.info("Executing approved query");
				int count = getJdbcTemplate().queryForObject(strQueryAppr.toString(),Integer.class);
				//int count = getJdbcTemplate().queryForInt(strQueryPend.toString(),objParams);
				objectTemp.setTotRecordsCount(count);
			}
		}
		catch(Exception ex){
			ex.printStackTrace();
			logger.error("Error: getQueryResultsForReview Exception :   ");
			logger.error(((strQueryAppr == null) ? "strQueryAppr is null" : strQueryAppr.toString()));
		}
	}
	@Override
	protected void setServiceDefaults(){
		serviceName = "AlphaSubTab";
		serviceDesc = "Alpha Sub Tab";
		tableName = "ALPHA_TAB";
		childTableName = "ALPHA_SUB_TAB";
		intCurrentUserId = SessionContextHolder.getContext().getVisionId();
	}

	@Override
	protected int getStatus(AlphaSubTabVb records){return records.getAlphaSubTabStatus();}

	@Override
	protected void setStatus(AlphaSubTabVb vObject,int status){vObject.setAlphaSubTabStatus(status);}

	@Override
	protected int doInsertionAppr(AlphaSubTabVb vObject){
		String query = "Insert Into ALPHA_SUB_TAB( " + 
			"ALPHA_TAB, ALPHA_SUB_TAB, ALPHA_SUBTAB_DESCRIPTION, ALPHA_SUBTAB_STATUS_NT, ALPHA_SUBTAB_STATUS, " + 
			" RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, " + 
			" DATE_CREATION) " + 
			" Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SysDate, SysDate)";

		Object args[] = {vObject.getAlphaTab(), vObject.getAlphaSubTab(), vObject.getDescription(),
			vObject.getAlphaSubTabStatusNt(), vObject.getAlphaSubTabStatus(), vObject.getRecordIndicatorNt(),
			vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus()};

		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int doInsertionPend(AlphaSubTabVb vObject){
		String query = "Insert Into ALPHA_SUB_TAB_PEND( " + 
			"ALPHA_TAB, ALPHA_SUB_TAB, ALPHA_SUBTAB_DESCRIPTION, ALPHA_SUBTAB_STATUS_NT, ALPHA_SUBTAB_STATUS, " + 
			" RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, " + 
			" DATE_CREATION) " + 
			" Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SysDate, SysDate)";

		Object args[] = {vObject.getAlphaTab(), vObject.getAlphaSubTab(), vObject.getDescription(),
			vObject.getAlphaSubTabStatusNt(), vObject.getAlphaSubTabStatus(), vObject.getRecordIndicatorNt(),
			vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus()};

		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int doInsertionPendWithDc(AlphaSubTabVb vObject){
		String query = "Insert Into ALPHA_SUB_TAB_PEND( " + 
			"ALPHA_TAB, ALPHA_SUB_TAB, ALPHA_SUBTAB_DESCRIPTION, ALPHA_SUBTAB_STATUS_NT, ALPHA_SUBTAB_STATUS, " + 
			" RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, " + 
			" DATE_CREATION) " + 
			"Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SysDate,  " + 
			"To_Date(?, 'DD-MM-RRRR HH24:MI:SS'))";

		Object args[] = {vObject.getAlphaTab(), vObject.getAlphaSubTab(), vObject.getDescription(), 
			vObject.getAlphaSubTabStatusNt(), vObject.getAlphaSubTabStatus(), vObject.getRecordIndicatorNt(), 
			vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), 
			vObject.getDateCreation()};

		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int doUpdateAppr(AlphaSubTabVb vObject){
		String query = "Update ALPHA_SUB_TAB Set " + 
			"ALPHA_SUBTAB_DESCRIPTION = ?, ALPHA_SUBTAB_STATUS_NT = ?, ALPHA_SUBTAB_STATUS = ?, " + 
			" RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, " + 
			" DATE_LAST_MODIFIED = SysDate " + 
			"Where ALPHA_TAB = ? " +
			" And ALPHA_SUB_TAB = ? ";

		Object args[] = {vObject.getDescription(), vObject.getAlphaSubTabStatusNt(),
			vObject.getAlphaSubTabStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
			vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), 
			vObject.getAlphaTab(), 
			vObject.getAlphaSubTab()};

		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int doUpdatePend(AlphaSubTabVb vObject){
		String query = "Update ALPHA_SUB_TAB_PEND Set " + 
			"ALPHA_SUBTAB_DESCRIPTION = ?, ALPHA_SUBTAB_STATUS_NT = ?, ALPHA_SUBTAB_STATUS = ?, "+
			" RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, "+
			" DATE_LAST_MODIFIED = SysDate " + 
			"Where ALPHA_TAB = ? " +
			" And ALPHA_SUB_TAB = ? ";

		Object args[] = {vObject.getDescription(), vObject.getAlphaSubTabStatusNt(), 
			vObject.getAlphaSubTabStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), 
			vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
			vObject.getAlphaTab(), vObject.getAlphaSubTab()};

		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int doDeleteAppr(AlphaSubTabVb vObject){
		String query = "Delete From ALPHA_SUB_TAB Where " + 
		"ALPHA_TAB = ? AND ALPHA_SUB_TAB = ? " ;
		Object args[] = {vObject.getAlphaTab(), vObject.getAlphaSubTab()};
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int deletePendingRecord(AlphaSubTabVb vObject){
		String query = "Delete From ALPHA_SUB_TAB_PEND Where " + 
		"ALPHA_TAB = ? AND ALPHA_SUB_TAB = ? " ;
		Object args[] = {vObject.getAlphaTab(), vObject.getAlphaSubTab()};
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected String frameErrorMessage(AlphaSubTabVb vObject, String strOperation)
	{
		// specify all the key fields and their values first
		String strErrMsg = new String("");
		try {
			strErrMsg =  strErrMsg + " ALPHA_TAB:" + vObject.getAlphaTab();
			strErrMsg =  strErrMsg + " ALPHA_SUB_TAB:" + vObject.getAlphaSubTab();
			// Now concatenate the error message that has been sent
			if ("Approve".equalsIgnoreCase(strOperation))
				strErrMsg = strErrMsg + " failed during approve Operation. Bulk Approval aborted !!";
			else
				strErrMsg = strErrMsg + " failed during reject Operation. Bulk Rejection aborted !!";
		}catch(Exception ex){
			strErrorDesc = ex.getMessage();
			strErrMsg = strErrMsg + strErrorDesc;
			logger.error(strErrMsg, ex);
		}
		// Return back the error message string
		return strErrMsg;
	}


	@Override
	protected String getAuditString(AlphaSubTabVb vObject)
	{
		StringBuffer strAudit = new StringBuffer("");
		try
		{
			strAudit.append(vObject.getAlphaTab());
			strAudit.append("!|#");
			if(vObject.getAlphaSubTab() != null)
				strAudit.append(vObject.getAlphaSubTab().trim());
			else
				strAudit.append("NULL");
			strAudit.append("!|#");

			if(vObject.getDescription() != null)
				strAudit.append(vObject.getDescription().trim());
			else
				strAudit.append("NULL");
			strAudit.append("!|#");

			strAudit.append(vObject.getAlphaSubTabStatusNt());
			strAudit.append("!|#");
			strAudit.append(vObject.getAlphaSubTabStatus());
			strAudit.append("!|#");
			strAudit.append(vObject.getRecordIndicatorNt());
			strAudit.append("!|#");
			strAudit.append(vObject.getRecordIndicator());
			strAudit.append("!|#");
			strAudit.append(vObject.getMaker());
			strAudit.append("!|#");
			strAudit.append(vObject.getVerifier());
			strAudit.append("!|#");
			strAudit.append(vObject.getInternalStatus());
			strAudit.append("!|#");
			if(vObject.getDateLastModified() != null)
				strAudit.append(vObject.getDateLastModified().trim());
			else
				strAudit.append("NULL");
			strAudit.append("!|#");

			if(vObject.getDateCreation() != null)
				strAudit.append(vObject.getDateCreation().trim());
			else
				strAudit.append("NULL");
			strAudit.append("!|#");

		}
		catch(Exception ex)
		{
			strErrorDesc = ex.getMessage();
			strAudit = strAudit.append(strErrorDesc);
			ex.printStackTrace();
		}
		return strAudit.toString();
	}
}
