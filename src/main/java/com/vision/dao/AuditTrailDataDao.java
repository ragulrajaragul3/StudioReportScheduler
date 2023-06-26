package com.vision.dao;


import java.io.Reader;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.SessionContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.ValidationUtil;
import com.vision.vb.AuditTrailDataVb;
import com.vision.vb.CommonVb;

@Component
public class AuditTrailDataDao extends AbstractDao<AuditTrailDataVb> {
	
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				AuditTrailDataVb auditTrailDataVb = new AuditTrailDataVb();
				auditTrailDataVb.setReferenceNo(rs.getString("REFERENCE_NO"));
				auditTrailDataVb.setSubReferenceNo(rs.getString("SUB_REFERENCE_NO"));
				auditTrailDataVb.setTableName(rs.getString("TABLE_NAME"));
				auditTrailDataVb.setChildTableName(rs.getString("CHILD_TABLE_NAME"));
				auditTrailDataVb.setTableSequence(rs.getInt("TABLE_SEQUENCE"));
				auditTrailDataVb.setAuditMode(rs.getString("AUDIT_MODE"));
				auditTrailDataVb.setDateCreation(rs.getString("DATE_CREATION"));
				auditTrailDataVb.setAuditDataOld(rs.getString("AUDIT_DATA_OLD"));
				auditTrailDataVb.setAuditDataNew(rs.getString("AUDIT_DATA_NEW"));
				int auditOldDataOldLogic = rs.getInt("AUDIT_OLD_OLD_LOGIC_FLAG");
				int auditNewDataOldLogic = rs.getInt("AUDIT_NEW_OLD_LOGIC_FLAG");
				if(auditOldDataOldLogic == 0 && auditNewDataOldLogic == 0){
					auditTrailDataVb.setOldLogic(true);
				}
				auditTrailDataVb.setMaker(rs.getLong("MAKER"));
				if(rs.getString("IP_ADDRESS")!=null)
					auditTrailDataVb.setRemoteAddress(rs.getString("IP_ADDRESS"));
				return auditTrailDataVb;
			}
		};
		return mapper;
	}
	private RowMapper getRowMapperForReview(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				AuditTrailDataVb auditTrailDataVb = new AuditTrailDataVb();
				auditTrailDataVb.setAuditMode(rs.getString("AUDIT_MODE"));
				auditTrailDataVb.setAuditDataOld(rs.getString("AUDIT_DATA_OLD"));
				auditTrailDataVb.setAuditDataNew(rs.getString("AUDIT_DATA_NEW"));
				auditTrailDataVb.setTablecolumns(rs.getString("TABLE_COLUMN"));
				return auditTrailDataVb;
			}
		};
		return mapper;
	}
	@Override
	protected void setServiceDefaults(){
		serviceName = "AuditTrail";
		serviceDesc = "Audit Trail";
		tableName = "AUDIT_TRAIL_DATA";
		childTableName = "AUDIT_TRAIL_DATA";
		intCurrentUserId = SessionContextHolder.getContext().getVisionId();
	}
	public int insertAuditTrail(AuditTrailDataVb auditTrailDataVb){
		int result=0;
		PreparedStatement ps= null;
		String remoteIpAddress = "";
		if(ValidationUtil.isValid(SessionContextHolder.getContext())){
			remoteIpAddress = SessionContextHolder.getContext().getRemoteAddress();	
		}
		
		try{
			String query = "Insert Into AUDIT_TRAIL_DATA(REFERENCE_NO, SUB_REFERENCE_NO, TABLE_NAME, CHILD_TABLE_NAME,"+
				"TABLE_SEQUENCE, AUDIT_MODE, DATE_CREATION,MAKER, IP_ADDRESS, AUDIT_DATA_OLD, AUDIT_DATA_NEW) Values (?, ?, ?, ?, ?, ?,  SysDate, ?, ?, ?, ?)";
			Object[] args = {auditTrailDataVb.getReferenceNo(), auditTrailDataVb.getSubReferenceNo(),auditTrailDataVb.getTableName(),
				auditTrailDataVb.getChildTableName(),getMaxSequence(auditTrailDataVb.getChildTableName()),auditTrailDataVb.getAuditMode(),
				auditTrailDataVb.getMaker(), remoteIpAddress};
			ps= getConnection().prepareStatement(query);
			for(int i=1;i<=args.length;i++){
				ps.setObject(i,args[i-1]);	
			}
			
			String auditDataOld = auditTrailDataVb.getAuditDataOld();
			try{
				if(auditDataOld.equalsIgnoreCase(null))
					auditDataOld="";
			}catch(Exception e){
				auditDataOld="";
			}
			Reader reader = new StringReader(auditDataOld);
			ps.setCharacterStream(args.length+1, reader, auditDataOld.length());
			
			String auditDataNew = auditTrailDataVb.getAuditDataNew();
			try{
				if(auditDataNew.equalsIgnoreCase(null))
					auditDataNew="";
			}catch(Exception e){
				auditDataNew="";			
			}
			reader = new StringReader(auditDataNew);
			ps.setCharacterStream(args.length+2, reader, auditDataNew.length());
			result = ps.executeUpdate();
		}catch(Exception e){
			System.out.println(e.getMessage());
			strErrorDesc = e.getMessage();
			logger.error("Update Error : "+e.getMessage());
		}finally{
			if (ps != null) {
				try {
					ps.close();
					ps=null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}
	public int getMaxSequence(String strTableName){
		int tableSequence  =1;
		if (strTableName == null || strTableName.length() == 0)
			return 1;
		StringBuffer strBufApprove = new StringBuffer("Select (case when max(TABLE_SEQUENCE) is null then 0 else max(TABLE_SEQUENCE) end) as TABLE_SEQUENCE FROM Vision_Table_Columns ");
		strBufApprove.append(" Where TABLE_NAME = ? ");
		Object objParams[] = {strTableName};
		int tableSequnceTemp = getJdbcTemplate().queryForObject(strBufApprove.toString(), objParams, Integer.class);
		if(tableSequnceTemp == 0){
			return tableSequence;
		}
		return tableSequnceTemp;
	}
	public List<AuditTrailDataVb> getQueryResults(AuditTrailDataVb dObj, int intStatus){

		Vector<Object> params = new Vector<Object>();
		setServiceDefaults();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.REFERENCE_NO, TAppr.SUB_REFERENCE_NO, TAppr.TABLE_NAME, TAppr.CHILD_TABLE_NAME, "+
		" TAppr.TABLE_SEQUENCE, TAppr.AUDIT_MODE, To_Char(TAppr.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION, "+
		" TAppr.AUDIT_DATA_OLD, INSTR(TAppr.AUDIT_DATA_OLD, '^!#') AUDIT_OLD_OLD_LOGIC_FLAG, "+
		" TAppr.AUDIT_DATA_NEW, INSTR(TAppr.AUDIT_DATA_NEW, '^!#') AUDIT_NEW_OLD_LOGIC_FLAG, "+
		" TAppr.MAKER, TAppr.IP_ADDRESS From AUDIT_TRAIL_DATA TAppr");
		try
		{
			
			if (ValidationUtil.isValid(dObj.getTableName())){
				params.addElement("%"+dObj.getTableName().toUpperCase()+"%");
				CommonUtils.addToQuery("UPPER(TAppr.TABLE_NAME) like ?", strBufApprove);
			}
			if (!"-1".equalsIgnoreCase(dObj.getAuditMode())){
				params.addElement(dObj.getAuditMode());
				CommonUtils.addToQuery("TAppr.AUDIT_MODE = ?", strBufApprove);
			}
			if (ValidationUtil.isValid(dObj.getChildTableName())){
				params.addElement("%"+ dObj.getChildTableName().toUpperCase() +"%");
				CommonUtils.addToQuery("UPPER(TAppr.CHILD_TABLE_NAME) LIKE ?", strBufApprove);
			}
			if (ValidationUtil.isValid(dObj.getRemoteAddress())){
				params.addElement("%"+ dObj.getRemoteAddress().toUpperCase() +"%");
				CommonUtils.addToQuery("UPPER(TAppr.IP_ADDRESS) LIKE ?", strBufApprove);
			}			
			if (dObj.getMaker() != 0){
				params.addElement(dObj.getMaker());
				CommonUtils.addToQuery("TAppr.MAKER = ?", strBufApprove);
			}
			if (ValidationUtil.isValid(dObj.getFromDate()) && ValidationUtil.isValid(dObj.getToDate())){
				String startDate = dObj.getFromDate();
				String endDate   = dObj.getToDate();
				if(startDate.trim().indexOf(" ") > -1){	
					params.addElement(startDate);
				}else{
					params.addElement(startDate + "00:00:00");
				}
				if(endDate.trim().indexOf(" ") > -1 ){   
					params.addElement(endDate);
				}else{
					params.addElement(endDate+ "23:59:59");
				}
				CommonUtils.addToQuery(" TAppr.DATE_CREATION BETWEEN To_Date(?, 'DD-MM-YYYY HH24:MI:SS') AND To_Date(?, 'DD-MM-YYYY HH24:MI:SS') ", strBufApprove);
			}
			dObj.setVerificationRequired(false);
			String orderBy = " Order By TAppr.DATE_CREATION desc, TAppr.TABLE_NAME ";
			return getQueryPopupResults(dObj, new StringBuffer(), strBufApprove, "", orderBy, params);
			
		}catch(Exception ex){
			
			ex.printStackTrace();
			logger.error(((strBufApprove==null)? "strBufApprove is Null":strBufApprove.toString()));
			if (params != null)
				for(int i=0 ; i< params.size(); i++)
					logger.error("objParams[" + i + "]" + params.get(i).toString());
			return null;

		}
	}
	public List<AuditTrailDataVb> getQueryResultsForReview(AuditTrailDataVb dObj, int intStatus){
        setServiceDefaults();
        List<AuditTrailDataVb> collTemp = null;
        Vector<Object> params = new Vector<Object>();
        StringBuffer strQueryAppr = new StringBuffer("select AD.AUDIT_DATA_OLD , AD.AUDIT_DATA_NEW , AD.AUDIT_MODE ");
        if(intStatus == 1)
        	strQueryAppr.append(" ,TC.TABLE_COLUMN " +
                     " from AUDIT_TRAIL_DATA AD, VISION_TABLE_COLUMNS TC " +
                     " Where  AD.REFERENCE_NO = ? AND AD.AUDIT_MODE= ?  and " +
                     " AD.DATE_CREATION =  To_Date(?, 'DD-MM-YYYY HH24:MI:SS') ");
        else
        	strQueryAppr.append(" , '' as TABLE_COLUMN from AUDIT_TRAIL_DATA AD " +
                    " Where  AD.REFERENCE_NO = ? AND AD.AUDIT_MODE= ?  and " +
                    " AD.DATE_CREATION =  To_Date(?, 'DD-MM-YYYY HH24:MI:SS') ");
        try{
              params.add(dObj.getReferenceNo());
              params.add(dObj.getAuditMode());
              params.add(dObj.getDateCreation());
              if(dObj.getTableName().equalsIgnoreCase(dObj.getChildTableName())){
                    params.add(dObj.getTableName());
                    CommonUtils.addToQuery("AD.TABLE_NAME = ?", strQueryAppr);
                    if(intStatus == 1){
                    	CommonUtils.addToQuery(" TC.TABLE_NAME = AD.TABLE_NAME ", strQueryAppr);
                    }
              }else{
                    params.add(dObj.getTableName());
                    params.add(dObj.getChildTableName());
                    CommonUtils.addToQuery("AD.TABLE_NAME = ?", strQueryAppr);
                    CommonUtils.addToQuery("AD.CHILD_TABLE_NAME = ?", strQueryAppr);
                    if(intStatus == 1){
                    	CommonUtils.addToQuery(" TC.TABLE_NAME = AD.CHILD_TABLE_NAME ", strQueryAppr);
                    }
              }
              Object objParams[] = new Object[params.size()];
              for(int Ctr=0; Ctr < params.size(); Ctr++)
                    objParams[Ctr] = (Object) params.elementAt(Ctr);
              logger.info("Executing approved query");
              collTemp = getJdbcTemplate().query(strQueryAppr.toString(),objParams,getRowMapperForReview());
              return collTemp;
        }catch(Exception ex){
              ex.printStackTrace();
              logger.error("Error: getQueryResultsForReview Exception :   ");
              logger.error(((strQueryAppr == null) ? "strQueryAppr is Null" : strQueryAppr.toString()));
              if (params != null)
                    for(int i=0 ; i< params.size(); i++)
                          logger.error("objParams[" + i + "]" + params.get(i).toString());
              return null;
        }
	}
	public String fetchMakerVerifierNames(long UserId){
//		String resultValue = getJdbcTemplate().queryForObject("SELECT USER_NAME FROM VISION_USERS WHERE VISION_ID = '"+UserId+"' ",String.class);
		String sql = "SELECT USER_NAME FROM VISION_USERS WHERE VISION_ID = '"+UserId+"' ";
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				CommonVb commonVb = new CommonVb();
				commonVb.setMakerName(rs.getString("USER_NAME"));
				return commonVb;
			}
		};
		List<CommonVb> commonVbs = getJdbcTemplate().query(sql, mapper);
		if(commonVbs != null && !commonVbs.isEmpty()){
			return commonVbs.get(0).getMakerName();
		}
		return "";
//		return (ValidationUtil.isValid(resultValue)?resultValue:"");
	}
}