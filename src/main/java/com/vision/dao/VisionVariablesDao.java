package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.SessionContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.VisionVariablesVb;

@Component
public class VisionVariablesDao extends AbstractDao<VisionVariablesVb> {
	
	
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VisionVariablesVb visionVariablesVb = new VisionVariablesVb();
				visionVariablesVb.setVariable(rs.getString("VARIABLE"));
				visionVariablesVb.setValue(rs.getString("VALUE"));
				visionVariablesVb.setVariableStatusNt(rs.getInt("VARIABLE_STATUS_NT"));
				visionVariablesVb.setVariableStatus(rs.getInt("VARIABLE_STATUS"));
				visionVariablesVb.setDbStatus(rs.getInt("VARIABLE_STATUS"));
				visionVariablesVb.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				visionVariablesVb.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				visionVariablesVb.setMaker(rs.getLong("MAKER"));
				visionVariablesVb.setVerifier(rs.getLong("VERIFIER"));
				visionVariablesVb.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				visionVariablesVb.setDateCreation(rs.getString("DATE_CREATION"));
				visionVariablesVb.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				visionVariablesVb.setReadOnly(rs.getString("READ_ONLY"));
				return visionVariablesVb;
			}
		};
		return mapper;
	}
	
	public List<VisionVariablesVb> getQueryPopupResults(VisionVariablesVb dObj){
		
		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.VARIABLE," +
			"TAppr.VALUE, TAppr.VARIABLE_STATUS_NT, TAppr.VARIABLE_STATUS," +
			"TAppr.RECORD_INDICATOR_NT, TAppr.RECORD_INDICATOR, TAppr.MAKER, TAppr.VERIFIER, TAppr.INTERNAL_STATUS, " +
			"To_Char(TAppr.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, " +
			"To_Char(TAppr.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION, READ_ONLY" +
			" From VISION_VARIABLES TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From VISION_VARIABLES_PEND TPend Where TPend.VARIABLE = TAppr.VARIABLE)");
		StringBuffer strBufPending = new StringBuffer("Select TPend.VARIABLE, TPend.VALUE, TPend.VARIABLE_STATUS_NT, TPend.VARIABLE_STATUS, " +
			"TPend.RECORD_INDICATOR_NT, TPend.RECORD_INDICATOR, TPend.MAKER, TPend.VERIFIER, TPend.INTERNAL_STATUS, " +
			"To_Char(TPend.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, " +
			"To_Char(TPend.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION, READ_ONLY" +
			" From VISION_VARIABLES_PEND TPend ");
		try
		{
			
			//check if the column [VARIABLE_STATUS] should be included in the query
			if (dObj.getVariableStatus() != -1){
				params.addElement(new Integer(dObj.getVariableStatus()));
				CommonUtils.addToQuery("TAppr.VARIABLE_STATUS = ?", strBufApprove);
				CommonUtils.addToQuery("TPend.VARIABLE_STATUS = ?", strBufPending);
			}

			//check if the column [VARIABLE] should be included in the query
			if (ValidationUtil.isValid(dObj.getVariable())){
				params.addElement("%" + dObj.getVariable().toUpperCase() + "%");
				CommonUtils.addToQuery("UPPER(TAppr.VARIABLE) LIKE ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.VARIABLE) LIKE ?", strBufPending);
			}

			//check if the column [VALUE] should be included in the query
			if (ValidationUtil.isValid(dObj.getValue())){
				params.addElement("%" + dObj.getValue().toUpperCase() + "%");
				CommonUtils.addToQuery("UPPER(TAppr.VALUE) LIKE ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.VALUE) LIKE ?", strBufPending);
			}

			//check if the column [RECORD_INDICATOR] should be included in the query
			if (dObj.getRecordIndicator() != -1){
				if (dObj.getRecordIndicator() > 3){
					params.addElement(new Integer(0));
					CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
					CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
				}else{
					params.addElement(new Integer(dObj.getRecordIndicator()));
					CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
					CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
				}
			}
			String orderBy=" Order By VARIABLE ";
			return getQueryPopupResults(dObj,strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
			
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
	public List<VisionVariablesVb> getQueryResults(VisionVariablesVb dObj, int intStatus){
		setServiceDefaults();
		List<VisionVariablesVb> collTemp = null;
		final int intKeyFieldsCount = 1;

		String strQueryAppr = new String("Select TAppr.VARIABLE," +
			"TAppr.VALUE, TAppr.VARIABLE_STATUS_NT, TAppr.VARIABLE_STATUS," +
			"TAppr.RECORD_INDICATOR_NT, TAppr.RECORD_INDICATOR," +
			"TAppr.MAKER, TAppr.VERIFIER, TAppr.INTERNAL_STATUS," +
			"To_Char(TAppr.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, To_Char(TAppr.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION, READ_ONLY" +
			" From VISION_VARIABLES TAppr " + 
			"Where TAppr.VARIABLE = ?");
		String strQueryPend = new String("Select TPend.VARIABLE," +
			"TPend.VALUE, TPend.VARIABLE_STATUS_NT, TPend.VARIABLE_STATUS," +
			"TPend.RECORD_INDICATOR_NT, TPend.RECORD_INDICATOR," +
			"TPend.MAKER, TPend.VERIFIER, TPend.INTERNAL_STATUS," +
			"To_Char(TPend.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, To_Char(TPend.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION, READ_ONLY" +
			" From VISION_VARIABLES_PEND TPend " + 
		"Where TPend.VARIABLE = ?");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = new String(dObj.getVariable());//[VARIABLE]

		try
		{if(!dObj.isVerificationRequired()){intStatus =0;}
			if(intStatus == 0)
			{
				logger.info("Executing approved query");
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(),objParams,getMapper());
			}else{
				logger.info("Executing pending query");
				collTemp = getJdbcTemplate().query(strQueryPend.toString(),objParams,getMapper());
			}
			return collTemp;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Error: getQueryResults Exception :   ");
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
	protected List<VisionVariablesVb> selectApprovedRecord(VisionVariablesVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}
	@Override
	protected List<VisionVariablesVb> doSelectPendingRecord(VisionVariablesVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}
	@Override
	protected int getStatus(VisionVariablesVb records){return records.getVariableStatus();}
	@Override
	protected void setStatus(VisionVariablesVb vObject,int status){vObject.setVariableStatus(status);}
	@Override
	protected int doInsertionAppr(VisionVariablesVb vObject){
		String query = "Insert Into VISION_VARIABLES ( VARIABLE, VALUE, VARIABLE_STATUS_NT, VARIABLE_STATUS,"+
			"RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"+
			"Values (?, ?, ?, ?, ?, ?, ?, ?, ?, SysDate, SysDate)";
		Object[] args = {vObject.getVariable(),vObject.getValue(),vObject.getVariableStatusNt(),vObject.getVariableStatus(),vObject.getRecordIndicatorNt(),vObject.getRecordIndicator()
			,vObject.getMaker(),
			vObject.getVerifier(), vObject.getInternalStatus()};  
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected int doInsertionPend(VisionVariablesVb vObject){
		String query = "Insert Into VISION_VARIABLES_PEND ( VARIABLE, VALUE, VARIABLE_STATUS_NT, "+
			"VARIABLE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) "+
			"Values (?, ?, ?, ?, ?, ?, ?, ?, ?, SysDate, SysDate)";
		Object[] args = {vObject.getVariable(),vObject.getValue(),vObject.getVariableStatusNt(),vObject.getVariableStatus(),
			vObject.getRecordIndicatorNt(),vObject.getRecordIndicator(),vObject.getMaker(),
			vObject.getVerifier(), vObject.getInternalStatus()};  
		return getJdbcTemplate().update(query,args);	
	}
	@Override
	protected int doInsertionPendWithDc(VisionVariablesVb vObject){
		String query = "Insert Into VISION_VARIABLES_PEND ( VARIABLE, VALUE, VARIABLE_STATUS_NT,"+
			"VARIABLE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) "+
			"Values (?, ?, ?, ?, ?, ?, ?, ?, ?, SysDate, To_Date(?, 'DD-MM-YYYY HH24:MI:SS'))";
		Object[] args = {vObject.getVariable(),vObject.getValue(),vObject.getVariableStatusNt(),vObject.getVariableStatus(), 
			vObject.getRecordIndicatorNt(),vObject.getRecordIndicator(),vObject.getMaker(),
			vObject.getVerifier(), vObject.getInternalStatus(),vObject.getDateCreation()};  
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected int doUpdateAppr(VisionVariablesVb vObject){
		String query = "Update VISION_VARIABLES Set VALUE = ?, VARIABLE_STATUS_NT = ?, VARIABLE_STATUS = ?, "+
			"RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, "+
			"DATE_LAST_MODIFIED = SysDate Where VARIABLE = ?";
		Object[] args = {vObject.getValue(),vObject.getVariableStatusNt(),vObject.getVariableStatus(), vObject.getRecordIndicatorNt(),vObject.getRecordIndicator(),vObject.getMaker(),
			vObject.getVerifier(), vObject.getInternalStatus(), vObject.getVariable()};
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected int doUpdatePend(VisionVariablesVb vObject){
		String query = "Update VISION_VARIABLES_PEND Set VALUE = ?, VARIABLE_STATUS_NT = ?, VARIABLE_STATUS = ?,"+
			"RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = SysDate Where VARIABLE = ?";
		Object[] args = {vObject.getValue(),vObject.getVariableStatusNt(),vObject.getVariableStatus(), vObject.getRecordIndicatorNt(),vObject.getRecordIndicator(),vObject.getMaker(),
			vObject.getVerifier(), vObject.getInternalStatus(), vObject.getVariable()};
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected int doDeleteAppr(VisionVariablesVb vObject){
		String query = "Delete From VISION_VARIABLES	Where VARIABLE = ?";
		Object[] args = {vObject.getVariable()};
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected int deletePendingRecord(VisionVariablesVb vObject){
		String query = "Delete From VISION_VARIABLES_PEND Where VARIABLE = ?";
		Object[] args = {vObject.getVariable()};
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected String frameErrorMessage(VisionVariablesVb vObject, String strOperation){
		// specify all the key fields and their values first
		String strErrMsg = new String("");
		try{
			strErrMsg =  strErrMsg + "VARIABLE: " + vObject.getVariable();
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
	protected String getAuditString(VisionVariablesVb vObject){
		StringBuffer strAudit = new StringBuffer("");
		strAudit.append(vObject.getVariable().trim());
		strAudit.append("!|#");
		strAudit.append(vObject.getValue().trim());
		strAudit.append("!|#");
		strAudit.append(vObject.getVariableStatusNt());
		strAudit.append("!|#");
		if(vObject.getVariableStatus()== -1)
			vObject.setVariableStatus(0);
		strAudit.append(vObject.getVariableStatus());
		strAudit.append("!|#");
		strAudit.append(vObject.getRecordIndicatorNt());
		strAudit.append("!|#");
		if(vObject.getRecordIndicator()== -1)
			vObject.setRecordIndicator(0);
		strAudit.append(vObject.getRecordIndicator());
		strAudit.append("!|#");
		strAudit.append(vObject.getMaker());
		strAudit.append("!|#");
		strAudit.append(vObject.getVerifier());
		strAudit.append("!|#");
		strAudit.append(vObject.getInternalStatus());
		strAudit.append("!|#");
		if(vObject.getDateLastModified() != null  && !vObject.getDateLastModified().equalsIgnoreCase(""))
			strAudit.append(vObject.getDateLastModified().trim());
		else
			strAudit.append("NULL");
		strAudit.append("!|#");

		if(vObject.getDateCreation() != null && !vObject.getDateCreation().equalsIgnoreCase(""))
			strAudit.append(vObject.getDateCreation().trim());
		else
			strAudit.append("NULL");
		strAudit.append("!|#");
		return strAudit.toString();
	}
	@Override
	protected void setServiceDefaults(){
		serviceName = "VisionVariables";
		/*serviceDesc = CommonUtils.getResourceManger().getString("visionVariables");*/
		serviceDesc = "VisionVariables";
		tableName = "VISION_VARIABLES";
		childTableName = "VISION_VARIABLES";
		intCurrentUserId = SessionContextHolder.getContext().getVisionId();
		
	}
}