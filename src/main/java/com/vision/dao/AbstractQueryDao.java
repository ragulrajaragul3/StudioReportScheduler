package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.Paginationhelper;
import com.vision.util.ValidationUtil;
import com.vision.vb.AuditTrailDataVb;
import com.vision.vb.CommonVb;

@Component
public class AbstractQueryDao<E extends CommonVb> extends AbstractCommonDao {
	
	/**
	 * Any subclass of this class must overwrite the bellow methods.
	 * Can be taken as Abstract but not all the DAO class would be using these methods.
	 */
	protected List<E> selectApprovedRecord(E vObject){return null;}
	protected List<E> doSelectPendingRecord(E vObject){return null;}
	protected RowMapper getMapper(){return null;}
	protected int getStatus(E records){return 0;}
	protected void setStatus(E vObject,int status){}
	protected int doInsertionAppr(E vObject){return 0;}
	protected int doInsertionPend(E vObject){return 0;}
	protected int doInsertionPendWithDc(E vObject){return 1;}
	protected int doUpdateAppr(E vObject){return 1;}
	protected int doUpdatePend(E vObject){return 1;}
	protected int doDeleteAppr(E vObject){return 1;}
	protected int deletePendingRecord(E vObject){return 1;}
	protected String frameErrorMessage(E vObject, String strOperation){return null;}
	protected String getAuditString(E vObject){return null;}
	protected void setServiceDefaults(){}
	protected String getBuildStatus(E vObject){
		return getBuildModuleStatus("", "");
	}
	protected ExceptionCode writeAuditLog(E vNewObject, E vOldObject){
		AuditTrailDataVb auditTrailDataVb = new AuditTrailDataVb();
		String strReferenceNo = CommonUtils.getReferenceNo();// Generate Unique Reference No for the Audit Entry
		if (vNewObject != null){
			auditTrailDataVb.setAuditDataNew(getAuditString(vNewObject));//New Audit Data
			auditTrailDataVb.setMaker(vNewObject.getMaker());
		}else{
			auditTrailDataVb.setAuditDataNew("NULL");//New Audit Data
			if (vOldObject != null)
				auditTrailDataVb.setMaker(vOldObject.getMaker());
		}
		if (vOldObject != null){
			auditTrailDataVb.setAuditDataOld(getAuditString(vOldObject));//OLD Audit Data
		}else{
			auditTrailDataVb.setAuditDataOld("NULL");//OLD Audit Data as NULL
		}
		delay(strReferenceNo);
		auditTrailDataVb.setReferenceNo(strReferenceNo); // Reference Number
		String strSubReferenceNo = CommonUtils.getReferenceNo();
		auditTrailDataVb.setSubReferenceNo(strSubReferenceNo);
		//System.out.println("ReferenceNo: "+auditTrailDataVb.getReferenceNo()+", SubReferenceNo: "+auditTrailDataVb.getSubReferenceNo());
		auditTrailDataVb.setTableName(getTableName());//Table Name
		auditTrailDataVb.setChildTableName(getChildTableName());
		if ("Add".equalsIgnoreCase(getStrApproveOperation()))
			auditTrailDataVb.setAuditMode("SAS");
		else if ("Modify".equalsIgnoreCase(getStrApproveOperation()))
			auditTrailDataVb.setAuditMode("SMS");
		else if ("Delete".equalsIgnoreCase(getStrApproveOperation()))
			auditTrailDataVb.setAuditMode("SDS");
		
		retVal = auditTrailDataDao.insertAuditTrail(auditTrailDataVb);
		if(retVal != Constants.SUCCESSFUL_OPERATION){
			return getResultObject(Constants.AUDIT_TRAIL_ERROR);
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}
	private synchronized void delay(String strReferenceNo){
		try {
			while(CommonUtils.getReferenceNo().equalsIgnoreCase(strReferenceNo)){
				
			}
		} catch (Exception e) {}
	}
	/**
	 * 
	 * @param dObj
	 * @param pendingQuery
	 * @param approveQuery
	 * @param whereNotExistsQuery
	 * @param orderBy
	 * @param params
	 * @return list of Records
	 */
	protected List<E> getQueryPopupResults(E dObj,StringBuffer pendingQuery, StringBuffer approveQuery, 
			String whereNotExistsQuery,	String orderBy, Vector<Object> params){
		return getQueryPopupResults(dObj, pendingQuery, approveQuery, whereNotExistsQuery, orderBy, params, getMapper());
	}
	protected List<E> getQueryPopupResults(E dObj,StringBuffer pendingQuery, StringBuffer approveQuery, 
			String whereNotExistsQuery,	String orderBy, Vector<Object> params, RowMapper rowMapper){
		Object objParams[]=null;
		int Ctr = 0;
		int Ctr2 = 0;
		List<E> result;
		// Now the params are ready with the values.Create an object array and insert the values into it
		if(dObj.isVerificationRequired() && dObj.getRecordIndicator() != 0){
			objParams = new Object[params.size()*2];
		}else{
			objParams = new Object[params.size()];
		}

		for(Ctr=0; Ctr < params.size(); Ctr++)
			objParams[Ctr] = params.elementAt(Ctr);
		
		//pendingQuery.append(orderBy);
		
		Paginationhelper<E> paginationhelper = new Paginationhelper<E>(); 
		if(dObj.isVerificationRequired() && dObj.getRecordIndicator() != 0){
			//Same set of parameters are needed for the Pending table query too
			//So add another set of similar values to the objects array
			for(Ctr2=0 ; Ctr2 < params.size(); Ctr2++, Ctr++)
				objParams[Ctr] = params.elementAt(Ctr2);
			if(whereNotExistsQuery != null && !whereNotExistsQuery.isEmpty() && approveQuery != null)
				CommonUtils.addToQuery(whereNotExistsQuery, approveQuery);
			String query = "";
			if(approveQuery == null || pendingQuery == null){
				if(approveQuery == null){
					query = pendingQuery.toString();
				}else{
					query = approveQuery.toString();
				}
				//query = ValidationUtil.convertQuery(query, orderBy);
			}else{
				query = approveQuery.toString() + " Union " + pendingQuery.toString();
				//query = ValidationUtil.convertQuery(approveQuery.toString(), orderBy) + " Union " + ValidationUtil.convertQuery(pendingQuery.toString(), orderBy);
			}
			query = ValidationUtil.convertQuery(query, orderBy);
			if(dObj.getTotalRows()  <= 0){
				result = paginationhelper.fetchPage(getJdbcTemplate(), query, 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), rowMapper == null ? getMapper(): rowMapper);
				dObj.setTotalRows(paginationhelper.getTotalRows());
			}else{
				result = paginationhelper.fetchPage(getJdbcTemplate(), query, 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), dObj.getTotalRows(), rowMapper == null ? getMapper(): rowMapper); 
			}
		}else{
			if(dObj.getTotalRows()  <= 0){
				result = paginationhelper.fetchPage(getJdbcTemplate(), ValidationUtil.convertQuery(approveQuery.toString(), orderBy), 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), rowMapper == null ? getMapper(): rowMapper);
				dObj.setTotalRows(paginationhelper.getTotalRows());
			}else{
				result = paginationhelper.fetchPage(getJdbcTemplate(), ValidationUtil.convertQuery(approveQuery.toString(), orderBy), 
						objParams, dObj.getStartIndex(), dObj.getLastIndex(), dObj.getTotalRows(), rowMapper == null ? getMapper(): rowMapper); 
			}
		}
		return result;
	}
	protected List<E> getQueryPopupResultsPgn(E dObj,StringBuffer pendingQuery, StringBuffer approveQuery, 
			String whereNotExistsQuery,	String orderBy, Vector<Object> params, RowMapper rowMapper){
		Object objParams[]=null;
		int Ctr = 0;
		int Ctr2 = 0;
		List<E> result;
		// Now the params are ready with the values.Create an object array and insert the values into it
		if(dObj.isVerificationRequired()){
			objParams = new Object[params.size()*3];
		}else{
			objParams = new Object[params.size()*2];
		}

		for(Ctr=0; Ctr < params.size(); Ctr++)
			objParams[Ctr] = (Object) params.elementAt(Ctr);

		pendingQuery.append(orderBy);
		
		Paginationhelper<E> paginationhelper = new Paginationhelper<E>(); 
		if(dObj.isVerificationRequired()){
			//Same set of parameters are needed for the Pending table query too
			//So add another set of similar values to the objects array
			for(Ctr2=0 ; Ctr2 < params.size(); Ctr2++, Ctr++)
				objParams[Ctr] = (Object) params.elementAt(Ctr2);
			for(Ctr2=0 ; Ctr2 < params.size(); Ctr2++, Ctr++)
				objParams[Ctr] = (Object) params.elementAt(Ctr2);
			if(whereNotExistsQuery != null && !whereNotExistsQuery.isEmpty() && approveQuery != null)
				CommonUtils.addToQuery(whereNotExistsQuery, approveQuery);
			String query = "";
			if(approveQuery == null || pendingQuery == null){
				if(approveQuery == null){
					query = pendingQuery.toString();
				}else{
					query = approveQuery.toString();
				}
			}else{
				query = approveQuery.toString() + " Union " + pendingQuery.toString();
			}
			if(dObj.getTotalRows()  <= 0){
				result = paginationhelper.fetchPage(getJdbcTemplate(), query, 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), rowMapper == null ? getMapper(): rowMapper);
				dObj.setTotalRows(paginationhelper.getTotalRows());
			}else{
				result = paginationhelper.fetchPage(getJdbcTemplate(), query, 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), dObj.getTotalRows(), rowMapper == null ? getMapper(): rowMapper); 
			}
		}else{
			for(Ctr2=0 ; Ctr2 < params.size(); Ctr2++, Ctr++)
				objParams[Ctr] = (Object) params.elementAt(Ctr2);
			if(dObj.getTotalRows()  <= 0){
				result = paginationhelper.fetchPage(getJdbcTemplate(), pendingQuery.toString(), 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), rowMapper == null ? getMapper(): rowMapper);
				dObj.setTotalRows(paginationhelper.getTotalRows());
			}else{
				result = paginationhelper.fetchPage(getJdbcTemplate(), pendingQuery.toString(), 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), dObj.getTotalRows(), rowMapper == null ? getMapper(): rowMapper); 
			}
		}
		return result;
	}
	protected List<E> getQueryPopupResultsGroup(E dObj,StringBuffer pendingQuery, StringBuffer approveQuery, 
			String whereNotExistsQuery,	String orderBy, Vector<Object> params, RowMapper rowMapper){
		Object objParams[]=null;
		int Ctr = 0;
		int Ctr2 = 0;
		List<E> result;
		// Now the params are ready with the values.Create an object array and insert the values into it
		if(dObj.isVerificationRequired()){
			objParams = new Object[params.size()*2];
		}else{
			objParams = new Object[params.size()];
		}

		for(Ctr=0; Ctr < params.size(); Ctr++)
			objParams[Ctr] = (Object) params.elementAt(Ctr);
		
		pendingQuery.append(orderBy);
		
		Paginationhelper<E> paginationhelper = new Paginationhelper<E>(); 
		if(dObj.isVerificationRequired()){
			//Same set of parameters are needed for the Pending table query too
			//So add another set of similar values to the objects array
			for(Ctr2=0 ; Ctr2 < params.size(); Ctr2++, Ctr++)
				objParams[Ctr] = (Object) params.elementAt(Ctr2);
			if(whereNotExistsQuery != null && !whereNotExistsQuery.isEmpty() && approveQuery != null)
				CommonUtils.addToQuery(whereNotExistsQuery, approveQuery);
			String query = "";
			if(approveQuery == null || pendingQuery == null){
				if(approveQuery == null){
					query = pendingQuery.toString();
				}else{
					query = approveQuery.toString();
				}
			}else{
				query = approveQuery.toString() + " Union " + pendingQuery.toString();
			}
			if(dObj.getTotalRows()  <= 0){
				result = paginationhelper.fetchPage(getJdbcTemplate(), query, 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), rowMapper == null ? getMapper(): rowMapper);
				dObj.setTotalRows(paginationhelper.getTotalRows());
			}else{
				result = paginationhelper.fetchPage(getJdbcTemplate(), query, 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), dObj.getTotalRows(), rowMapper == null ? getMapper(): rowMapper); 
			}
		}else{
			if(dObj.getTotalRows()  <= 0){
				result = paginationhelper.fetchPage(getJdbcTemplate(), approveQuery.toString()+orderBy, 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), rowMapper == null ? getMapper(): rowMapper);
				dObj.setTotalRows(paginationhelper.getTotalRows());
			}else{
				result = paginationhelper.fetchPage(getJdbcTemplate(), approveQuery.toString()+orderBy, 
						objParams, dObj.getCurrentPage(), dObj.getMaxRecords(), dObj.getTotalRows(), rowMapper == null ? getMapper(): rowMapper); 
			}
		}
		return result;
	}
	public void fetchMakerVerifierNames(E object){
		String sql = " select USER_NAME FROM VISION_USERS where VISION_ID = ?";
		ArrayList<String> lLocaList = new ArrayList<String>(1);
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				return (rs.getString("USER_NAME"));
			}
		};
		if(object.getMaker() >0){
			Object[] args = {object.getMaker()};
			lLocaList = (ArrayList<String>) getJdbcTemplate().query(sql, args, mapper);
			if(lLocaList == null || lLocaList.isEmpty()){
				object.setMakerName("");
			}else{
				object.setMakerName(lLocaList.get(0));
			}
			
		}
		if(object.getVerifier() > 0){
			Object[] args = {object.getVerifier()};
			lLocaList = (ArrayList<String>) getJdbcTemplate().query(sql, args, mapper);
			if(lLocaList == null || lLocaList.isEmpty()){
				object.setVerifierName("");
			}else{
				object.setVerifierName(lLocaList.get(0));
			}
		}
	}
	protected List<E> getQueryPopupResultsWithPend(E dObj,StringBuffer pendingQuery, StringBuffer approveQuery, 
			String whereNotExistsQuery,	String orderBy, Vector<Object> params){
		return getQueryPopupResultsWithPend(dObj, pendingQuery, approveQuery, whereNotExistsQuery, orderBy, params, getMapper());
	}
	
	protected List<E> getQueryPopupResultsWithPend(E dObj, StringBuffer pendingQuery,
			StringBuffer approveQuery, String whereNotExistsQuery, String orderBy, Vector<Object> params,
			RowMapper rowMapper) {
		if (!ValidationUtil.isValid(dObj.getTotalRows()))
			dObj.setTotalRows(0);
		Object objParams[] = null;
		int Ctr = 0;
		int Ctr2 = 0;
		List<E> result;

		Paginationhelper<E> paginationhelper = new Paginationhelper<E>();

		if (whereNotExistsQuery != null && !whereNotExistsQuery.isEmpty() && approveQuery != null)
			CommonUtils.addToQuery(whereNotExistsQuery, approveQuery);
		String query = "";
		if (approveQuery == null || pendingQuery == null) {
			
			objParams = new Object[params.size()];
			for (Ctr = 0; Ctr < params.size(); Ctr++)
				objParams[Ctr] = (Object) params.elementAt(Ctr);
			
			if (approveQuery == null) {
				pendingQuery.append(orderBy);
				query = pendingQuery.toString();
			} else {
				approveQuery.append(orderBy);
				query = approveQuery.toString();
			}
		} else {
			
			objParams = new Object[params.size() * 2];
			for (Ctr = 0; Ctr < params.size(); Ctr++)
				objParams[Ctr] = (Object) params.elementAt(Ctr);
			for (Ctr2 = 0; Ctr2 < params.size(); Ctr2++, Ctr++)
				objParams[Ctr] = (Object) params.elementAt(Ctr2);
			
			query = approveQuery.toString() + " Union " + pendingQuery.toString() + " " + orderBy;
		}
		if (dObj.getTotalRows() <= 0) {
			result = paginationhelper.fetchPage(getJdbcTemplate(), query, objParams, dObj.getStartIndex(),
					dObj.getLastIndex(), rowMapper == null ? getMapper() : rowMapper);
			dObj.setTotalRows(paginationhelper.getTotalRows());
		} else {
			result = paginationhelper.fetchPage(getJdbcTemplate(), query, objParams, dObj.getStartIndex(),
					dObj.getLastIndex(), dObj.getTotalRows(), rowMapper == null ? getMapper() : rowMapper);
		}
		return result;
	}

}
