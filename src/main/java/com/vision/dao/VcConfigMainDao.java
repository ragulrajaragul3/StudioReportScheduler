package com.vision.dao;

import java.io.StringReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vision.authentication.SessionContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.ColumnDeleteVb;
import com.vision.vb.DesignAnalysisVb;
import com.vision.vb.LevelOfDisplayVb;
import com.vision.vb.MaskingVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.RelationDeleteVb;
import com.vision.vb.RelationMapVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.TableDeleteVb;
import com.vision.vb.UserRestrictionVb;
import com.vision.vb.VcConfigMainAliasVb;
import com.vision.vb.VcConfigMainCRUDVb;
import com.vision.vb.VcConfigMainColumnsVb;
import com.vision.vb.VcConfigMainLODWrapperVb;
import com.vision.vb.VcConfigMainRelationVb;
import com.vision.vb.VcConfigMainTreeVb;
import com.vision.vb.VcConfigMainVb;
import com.vision.vb.VcForCatalogTableRelationVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.DynamicJoinNew;

@Component
public class VcConfigMainDao extends AbstractDao<VcConfigMainVb> {
	public JdbcTemplate jdbcTemplate = null;

	public VcConfigMainDao(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	@Autowired
	NumSubTabDao numSubTabDao;
	@Autowired
	VisionUsersDao visionUsersDao;
	
	@Override
	protected void setServiceDefaults() {
		serviceName = "VC Configuration";
		serviceDesc = "VC Configuration";
		tableName = "VISION_CATALOG_WIP";
		childTableName = "VISION_CATALOG_WIP";
		intCurrentUserId = SessionContextHolder.getContext().getVisionId();
		userGroup = SessionContextHolder.getContext().getUserGroup();
		userProfile = SessionContextHolder.getContext().getUserProfile();
	}

	public List<VcConfigMainVb> getQueryResultsFromVisionCatalog(VcConfigMainVb dObj, int intStatus,
			boolean appendStatus) {
		List<VcConfigMainVb> collTemp = null;
		
		setServiceDefaults();
		
		final int intKeyFieldsCount = 1;
		String strQueryAppr = new String(
				" SELECT TAPPR.CATALOG_ID, TAPPR.CATALOG_DESC, TAPPR.JOIN_CLAUSE, TAPPR.BASETABLE_JOINFLAG, "
						+ " TAPPR.VC_STATUS_NT, TAPPR.VC_STATUS, "
						+ " (Select NUM_SUBTAB_DESCRIPTION from NUM_SUB_TAB where NUM_TAB = TAppr.VC_STATUS_NT and TAPPR.VC_STATUS=NUM_SUB_TAB ) AS STATUS_DESC, "
						+ " TAPPR.RECORD_INDICATOR_NT, TAPPR.RECORD_INDICATOR,  TAPPR.MAKER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TAPPR.MAKER) as MAKER_NAME,"  
						+ " TAPPR.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TAPPR.VERIFIER) as VERIFIER_NAME,TAPPR.INTERNAL_STATUS, "
						+ " TO_CHAR(TAPPR.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, "
						+ " TO_CHAR(TAPPR.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION "
						+ " FROM VISION_CATALOG_SELFBI TAPPR LEFT JOIN CATALOG_ACCESS_SelfBI T2 ON (TAPPR.CATALOG_ID = T2.CATALOG_ID) " 
						+ " WHERE TAPPR.CATALOG_ID = ? AND (TAPPR.MAKER = '"+intCurrentUserId+"' OR (T2.USER_GROUP = '"+userGroup+"' AND T2.USER_PROFILE = '"+userProfile+"')) ");
		String strQueryApprStatus = " AND TAppr.VC_STATUS = " + dObj.getVcStatus();
		String strQueryPend = new String(
				" SELECT TWIP.CATALOG_ID, TWIP.CATALOG_DESC, TWIP.JOIN_CLAUSE, TWIP.BASETABLE_JOINFLAG, "
						+ " TWIP.VC_STATUS_NT, TWIP.VC_STATUS, "
						+ " (Select NUM_SUBTAB_DESCRIPTION from NUM_SUB_TAB where NUM_TAB = TWIP.VC_STATUS_NT and TWIP.VC_STATUS=NUM_SUB_TAB ) AS STATUS_DESC, "
						+ " TWIP.RECORD_INDICATOR_NT, TWIP.RECORD_INDICATOR, TWIP.MAKER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TWIP.MAKER) as MAKER_NAME,"
						+ " TWIP.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TWIP.VERIFIER) as VERIFIER_NAME, TWIP.INTERNAL_STATUS, "
						+ " TO_CHAR(TWIP.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, "
						+ " TO_CHAR(TWIP.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION "
						+ " FROM VISION_CATALOG_WIP TWIP LEFT JOIN CATALOG_ACCESS_SelfBI T2 ON (TWIP.CATALOG_ID = T2.CATALOG_ID) "
						+ " WHERE TWIP.CATALOG_ID = ? AND (TWIP.MAKER = '"+intCurrentUserId+"' OR (T2.USER_GROUP = '"+userGroup+"' AND T2.USER_PROFILE = '"+userProfile+"')) ");
		String strQueryPendStatus = " AND TWIP.VC_STATUS = " + dObj.getVcStatus();

		if (appendStatus) {
			strQueryAppr += strQueryApprStatus;
			strQueryPend += strQueryPendStatus;
		}

		try {
			Object objParams[] = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCatalogId();

			// if(!dObj.isVerificationRequired()){intStatus =0;}
			if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getMapperForVC());
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getMapperForVC());
			}
			return collTemp;
		} catch (Exception ex) {
			if (ValidationUtil.isValid(ex.getMessage()) && !ValidationUtil.isValid(strErrorDesc)) {
				strErrorDesc = ex.getCause().toString();
			}
			logger.error("Error: getQueryResults Exception :   ");
			if (intStatus == 0)
				logger.error(((strQueryAppr == null) ? "strQueryAppr is Null" : strQueryAppr.toString()));
			else
				logger.error(((strQueryPend == null) ? "strQueryPend is Null" : strQueryPend.toString()));
			throw new RuntimeCustomException(ex.getMessage());
//			return null;
		}
	}

	protected RowMapper getMapperForVC() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				VcConfigMainVb vcConfigMainVb = new VcConfigMainVb();
				vcConfigMainVb.setCatalogId(rs.getString("CATALOG_ID"));
				vcConfigMainVb.setCatalogDesc(rs.getString("CATALOG_DESC"));
				vcConfigMainVb.setJoinClause(rs.getInt("JOIN_CLAUSE"));
				vcConfigMainVb.setBaseTableJoinFlag(rs.getString("BASETABLE_JOINFLAG"));
				vcConfigMainVb.setVcStatus(rs.getInt("VC_STATUS"));
				vcConfigMainVb.setDbStatus(rs.getInt("VC_STATUS"));
				vcConfigMainVb.setVcStatusDesc(rs.getString("STATUS_DESC"));
				vcConfigMainVb.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vcConfigMainVb.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vcConfigMainVb.setMaker(rs.getLong("MAKER"));
				vcConfigMainVb.setMakerName(rs.getString("MAKER_NAME"));
				vcConfigMainVb.setVerifier(rs.getLong("VERIFIER"));
				vcConfigMainVb.setVerifierName(rs.getString("VERIFIER_NAME"));
				vcConfigMainVb.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				vcConfigMainVb.setDateCreation(rs.getString("DATE_CREATION"));
				vcConfigMainVb.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				return vcConfigMainVb;
			}
		};
		return mapper;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doInsertApprRecordforVcCatalogWIP(VcConfigMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		List<VcConfigMainVb> collTemp = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		try {
			/* Check if record already published */
			collTemp = getQueryResultsFromVisionCatalog(vObject, 0, false);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (collTemp.size() > 0) {
				int intStaticDeletionFlag = collTemp.get(0).getVcStatus();

				/* If record already in [Published & Deleted] status, throw error */
				if (intStaticDeletionFlag == Constants.PASSIVATE) {
					logger.error("Collection size is greater than zero - Duplicate record found, but inactive");
					exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
					throw buildRuntimeCustomException(exceptionCode);
				} else {

					// Change of logic Start //
					/* If record already published, set status as [Published & Work In Progress] */
					// vObject.setVcStatus(2);
					// Change of logic End //

					/* If record already published, throw error */
					logger.error("Collection size is greater than zero - Duplicate record found");
					exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			} else {
				/* If record not in main table, set status as [Work In Progress] */
				vObject.setVcStatus(1);
			}
			/* Check if record exists in WIP table */
			collTemp = getQueryResultsFromVisionCatalog(vObject, 1, false);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			/* If record exists in WIP table, throw error */
			if (collTemp.size() > 0) {
				logger.error("Collection size is greater than zero - Duplicate record found");
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			String sysDate = getSystemDate();
			vObject.setDateCreation(sysDate);
			vObject.setDateLastModified(sysDate);
			retVal = doInsertionApprforVcCatalogIntoWIP(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				if(ValidationUtil.isValid(strErrorDesc)) {
					retVal=20;
				}
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				
				exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			}
//			retVal = doInsertionApprforVcCatalogAccessIntoWIP(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				if(ValidationUtil.isValid(strErrorDesc)) {
					retVal=20;
				}
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			}
			return exceptionCode;
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			logger.error("Error in Add.", ex);
			logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	protected int doInsertionApprforVcCatalogIntoWIP(VcConfigMainVb vObject) {
		int result = 0;
		String query = "INSERT INTO VISION_CATALOG_WIP "
				+ " (CATALOG_ID, CATALOG_DESC, RECORD_INDICATOR_NT, RECORD_INDICATOR, VC_STATUS_NT, VC_STATUS, MAKER, VERIFIER, "
				+ " INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,JOIN_CLAUSE,BASETABLE_JOINFLAG) VALUES "
				+ " (?, ?, ?, ?, ?, ?, ?, ?, ?, To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), ?, ?)";
		Object[] args = { vObject.getCatalogId(), vObject.getCatalogDesc(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getVcStatusNt(), vObject.getVcStatus(), intCurrentUserId,
				intCurrentUserId, vObject.getInternalStatus(), vObject.getDateLastModified(), vObject.getDateCreation(),
				vObject.getJoinClause(), vObject.getBaseTableJoinFlag() };
		try {
			return getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Insert Error in VISION_CATALOG_WIP: " + e.getMessage());
		}
		return result;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doUpdateRecordVcCatalogIntoWIP(VcConfigMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		setServiceDefaults();
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		try {
			return doUpdateRecordForNonTransVcCatalogIntoWIP(vObject);
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Error in Modify.", ex);
			logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	protected ExceptionCode doUpdateRecordForNonTransVcCatalogIntoWIP(VcConfigMainVb vObject)
			throws RuntimeCustomException {
		List<VcConfigMainVb> collTempMain = null;
		List<VcConfigMainVb> collTempWIP = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
			exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* Check record existence in main table */
		collTempMain = getQueryResultsFromVisionCatalog(vObject, 0, false);
		if (collTempMain == null) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		if (collTempMain.size() > 0) {
			/*
			 * Check to block modification of deleted record before changing it to [Work In
			 * Progress] status
			 */
			int intStaticDeletionFlag = collTempMain.get(0).getVcStatus();
			if (intStaticDeletionFlag == Constants.PASSIVATE && vObject.getVcStatus() == Constants.PASSIVATE) {
				logger.error("Collection size is greater than zero - Duplicate record found, but inactive");
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				vObject.setVcStatus(2);
				vObject.setDateCreation(collTempMain.get(0).getDateCreation());
			}
		}

		/* Check record existence in _WIP table */
		collTempWIP = getQueryResultsFromVisionCatalog(vObject, 1, false);
		if (collTempWIP == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* If record not available in both MAIN and _WIP table, throw exception */
		if (collTempWIP.size() == 0 && collTempMain.size() == 0) {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/*
		 * Unless the status of record being modified is [Published & Work In Progress],
		 * the status of records in _WIP table must be [Work In Progress]
		 */
		if (!(2 == vObject.getVcStatus())) {
			vObject.setVcStatus(1);
		}

		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		vObject.setMaker(getIntCurrentUserId());
		vObject.setVerifier(getIntCurrentUserId());
		if (collTempWIP.size() > 0) {
			retVal = doUpdateApprForVcCatalogInPend(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
//			retVal = doUpdateApprForVcCatalogAccessInPend(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			String sysDate = getSystemDate();
			vObject.setDateCreation(collTempMain.get(0).getDateCreation());
			vObject.setDateLastModified(sysDate);
			retVal = doInsertionApprforVcCatalogIntoWIP(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				vObject.setVcStatus(2);
			}

//			retVal = doInsertionApprforVcCatalogAccessIntoWIP(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				vObject.setVcStatus(2);
			}
		}
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
	}

	protected int doUpdateApprForVcCatalogInPend(VcConfigMainVb vObject) {
		int result = 0;
		String query = "Update VISION_CATALOG_WIP Set CATALOG_DESC = ?, JOIN_CLAUSE = ?, BASETABLE_JOINFLAG = ?, "
				+ "VC_STATUS_NT = ?, VC_STATUS = ?, RECORD_INDICATOR_NT = ?,"
				+ "RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, DATE_LAST_MODIFIED = SysDate Where CATALOG_ID = ?";
		Object[] args = { vObject.getCatalogDesc(), vObject.getJoinClause(), vObject.getBaseTableJoinFlag(),
				vObject.getVcStatusNt(), vObject.getVcStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getCatalogId() };
		try {
			return getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Update Error in VISION_CATALOG_WIP: " + e.getMessage());
		}
		return result;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doDeleteRecordVcCatalogFromPend(VcConfigMainVb vObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = null;
		setServiceDefaults();
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		try {
			return doDeleteRecordForNonTransVcCatalogFromWIP(vObject);
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			logger.error("Error in Delete.", ex);
			logger.error(((vObject == null) ? "vObject is Null" : vObject.toString()));
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	protected ExceptionCode doDeleteRecordForNonTransVcCatalogFromWIP(VcConfigMainVb vObject)
			throws RuntimeCustomException {
		List<VcConfigMainVb> collTempMain = null;
		List<VcConfigMainVb> collTempWIP = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();

		VcConfigMainVb vObjectlocal = null;
		vObject.setMaker(getIntCurrentUserId());
		if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
			exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* Fetch record from MAIN table */
		collTempMain = getQueryResultsFromVisionCatalog(vObject, 0, false);
		if (collTempMain == null) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* Fetch record from WIP table */
		collTempWIP = getQueryResultsFromVisionCatalog(vObject, 1, false);
		if (collTempWIP == null) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* If record not in both MAIN and WIP table, throw exception */
		if (collTempMain.size() == 0 && collTempWIP.size() == 0) {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}

		/* If record exist in MAIN table */
		if (collTempMain.size() > 0) {
			/*
			 * If current status equal to status from MAIN table => attempt to delete record
			 * from MAIN table
			 */
			if (vObject.getVcStatus() == collTempMain.get(0).getVcStatus()) {
				/* If parallel record exist in WIP table, throw exception */
				if (collTempWIP.size() > 0) {
					exceptionCode = CommonUtils.getResultObject(serviceDesc, Constants.WE_HAVE_ERROR_DESCRIPTION,
							Constants.DELETE,
							"Cannot delete record from MAIN table when parallel record exist in WIP table");
					throw buildRuntimeCustomException(exceptionCode);
				} else if (vObject.getVcStatus() == Constants.PASSIVATE) {
					exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
					throw buildRuntimeCustomException(exceptionCode);
				} else {
					/* Update MAIN table record with status [Published & Deleted] */
					vObjectlocal = collTempMain.get(0);
					vObjectlocal.setVcStatus(9);
					retVal = doUpdateApprForVcCatalogInMain(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					} else {
						vObject.setVcStatus(9);
					}
				}
			}
		}

		/*
		 * If record exist only in pend table, delete record from WIP table Also remove
		 * configurations from other tables based on the current CATALOG_ID
		 */
		if (collTempWIP.size() > 0) {
			retVal = deleteFromTablesWithCatalogId(vObject.getCatalogId(), "VISION_CATALOG_WIP");
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			retVal = deleteFromTablesWithCatalogId(vObject.getCatalogId(), "VC_TREE_WIP");
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				strErrorDesc = "Unable to delete record from VC_TREE_WIP";
				exceptionCode = getResultObject(20);
				throw buildRuntimeCustomException(exceptionCode);
			}
			retVal = deleteFromTablesWithCatalogId(vObject.getCatalogId(), "VC_COLUMNS_WIP");
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				strErrorDesc = "Unable to delete record from VC_COLUMNS_WIP";
				exceptionCode = getResultObject(20);
				throw buildRuntimeCustomException(exceptionCode);
			}
			retVal = deleteFromTablesWithCatalogId(vObject.getCatalogId(), "VC_RELATIONS_WIP");
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				strErrorDesc = "Unable to delete record from VC_RELATIONS_WIP";
				exceptionCode = getResultObject(20);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	protected int doUpdateApprForVcCatalogInMain(VcConfigMainVb vObject) {
		int result = 0;
		String query = "Update VISION_CATALOG_SELFBI Set CATALOG_DESC = ?, JOIN_CLAUSE = ?, BASETABLE_JOINFLAG = ?, "
				+ "VC_STATUS_NT = ?, VC_STATUS = ?, RECORD_INDICATOR_NT = ?,"
				+ "RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, DATE_LAST_MODIFIED = SysDate Where CATALOG_ID = ?";
		Object[] args = { vObject.getCatalogDesc(), vObject.getJoinClause(), vObject.getBaseTableJoinFlag(),
				vObject.getVcStatusNt(), vObject.getVcStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getCatalogId() };
		try {
			return getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Update Error in VISION_CATALOG_SELFBI: " + e.getMessage());
		}
		return result;
	}

	protected int deleteFromTablesWithCatalogId(String catalogId, String tableName) {
		int result = 0;
		String query = "Delete From " + tableName + " Where CATALOG_ID = ?";
		Object[] args = { catalogId };
		try {
			 getJdbcTemplate().update(query, args);
			 result=1;
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Delete Error in " + tableName + ": " + e.getMessage());
		}
		return result;
	}

	protected int deleteFromTablesWithCatalogType(String catalogId, String tableName) {
		int result = 0;
		String query = "Delete From " + tableName + " Where CATALOG_ID = ?";
		Object[] args = { catalogId };
		try {
			return getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Delete Error in " + tableName + ": " + e.getMessage());
		}
		return result;
	}

	public List<VcConfigMainVb> getQueryPopupResultsFromVisionCatalog(VcConfigMainVb dObj) {
		Vector<Object> params = new Vector<Object>();
		
		setServiceDefaults();
		
		StringBuffer strBufApprove = new StringBuffer(
				" SELECT TAPPR.CATALOG_ID, TAPPR.CATALOG_DESC, TAPPR.JOIN_CLAUSE, TAPPR.BASETABLE_JOINFLAG, "
						+ " TAPPR.VC_STATUS_NT, TAPPR.VC_STATUS, "
						+ " (Select NUM_SUBTAB_DESCRIPTION from NUM_SUB_TAB where NUM_TAB = TAppr.VC_STATUS_NT and TAPPR.VC_STATUS=NUM_SUB_TAB ) AS STATUS_DESC,"
						+ " TAPPR.RECORD_INDICATOR_NT, TAPPR.RECORD_INDICATOR, TAPPR.MAKER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TAPPR.MAKER) as MAKER_NAME," 
						+ " TAPPR.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TAPPR.VERIFIER) as VERIFIER_NAME, TAPPR.INTERNAL_STATUS, "
						+ " TO_CHAR(TAPPR.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, "
						+ " TO_CHAR(TAPPR.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION "
						+ " FROM VISION_CATALOG_SELFBI TAPPR LEFT JOIN CATALOG_ACCESS_SelfBI T2 ON (TAPPR.CATALOG_ID = T2.CATALOG_ID) "
						+ " WHERE (TAPPR.MAKER = '"+intCurrentUserId+"'  OR (T2.USER_GROUP = '"+userGroup+"' AND T2.USER_PROFILE = '"+userProfile+"')) ");
		StringBuffer strBufPending = new StringBuffer(
				" SELECT TWIP.CATALOG_ID, TWIP.CATALOG_DESC, TWIP.JOIN_CLAUSE, TWIP.BASETABLE_JOINFLAG, "
						+ " TWIP.VC_STATUS_NT, TWIP.VC_STATUS, "
						+ " (Select NUM_SUBTAB_DESCRIPTION from NUM_SUB_TAB where NUM_TAB = TWIP.VC_STATUS_NT and TWIP.VC_STATUS=NUM_SUB_TAB ) AS STATUS_DESC,"
						+ " TWIP.RECORD_INDICATOR_NT, TWIP.RECORD_INDICATOR,  TWIP.MAKER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TWIP.MAKER) as MAKER_NAME, "
						+ " TWIP.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TWIP.VERIFIER) as VERIFIER_NAME, TWIP.INTERNAL_STATUS, "
						+ " TO_CHAR(TWIP.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, "
						+ " TO_CHAR(TWIP.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION "
						+ " FROM VISION_CATALOG_WIP TWIP LEFT JOIN CATALOG_ACCESS_SelfBI T2 ON (TWIP.CATALOG_ID = T2.CATALOG_ID) "
						+ " WHERE (TWIP.MAKER = '"+intCurrentUserId+"' OR (T2.USER_GROUP = '"+userGroup+"' AND T2.USER_PROFILE = '"+userProfile+"')) ");
		try {

			if (ValidationUtil.isValid(dObj.getCatalogId())) {
				params.addElement("%" + dObj.getCatalogId() + "%");
				CommonUtils.addToQuery("UPPER(TAppr.CATALOG_ID) LIKE ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TWIP.CATALOG_ID) LIKE ?", strBufPending);
			}

			if (ValidationUtil.isValid(dObj.getCatalogDesc())) {
				params.addElement("%" + dObj.getCatalogDesc().toUpperCase() + "%");
				CommonUtils.addToQuery("UPPER(TAppr.CATALOG_DESC) LIKE ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TWIP.CATALOG_DESC) LIKE ?", strBufPending);
			}

			if (ValidationUtil.isValid(dObj.getJoinClause()) && dObj.getJoinClause() != -1) {
				params.addElement(dObj.getJoinClause());
				CommonUtils.addToQuery("TAppr.JOIN_CLAUSE = ?", strBufApprove);
				CommonUtils.addToQuery("TWIP.JOIN_CLAUSE = ?", strBufPending);
			}

			// check if the column [RECORD_INDICATOR] should be included in the query
			if (dObj.getRecordIndicator() != -1) {
				if (dObj.getRecordIndicator() > 3) {
					params.addElement(new Integer(0));
					CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
					CommonUtils.addToQuery("TWIP.RECORD_INDICATOR > ?", strBufPending);
				} else {
					params.addElement(new Integer(dObj.getRecordIndicator()));
					CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
					CommonUtils.addToQuery("TWIP.RECORD_INDICATOR = ?", strBufPending);
				}
			}
			String orderBy = " Order By CATALOG_ID ";
			return getQueryPopupResultsWithPend(dObj, strBufPending, strBufApprove, "", orderBy, params, getMapperForVC());

		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(((strBufApprove == null) ? "strBufApprove is Null" : strBufApprove.toString()));
			logger.error("UNION");
			logger.error(((strBufPending == null) ? "strBufPending is Null" : strBufPending.toString()));

			if (params != null)
				for (int i = 0; i < params.size(); i++)
					logger.error("objParams[" + i + "]" + params.get(i).toString());
			return null;

		}
	}

	/*public Connection getDbConnection(String jdbcUrl, String username, String password, String type, String version)
			throws ClassNotFoundException, SQLException, Exception {
		Connection connection = null;
		if ("ORACLE".equalsIgnoreCase(type))
			Class.forName("oracle.jdbc.driver.OracleDriver");
		else if ("MSSQL".equalsIgnoreCase(type))
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		else if ("MYSQL".equalsIgnoreCase(type))
			Class.forName("com.mysql.jdbc.Driver");
		else if ("POSTGRESQL".equalsIgnoreCase(type))
			Class.forName("org.postgresql.Driver");
		else if ("SYBASE".equalsIgnoreCase(type))
			Class.forName("com.sybase.jdbc4.jdbc.SybDataSource");
		else if ("INFORMIX".equalsIgnoreCase(type))
			Class.forName("com.informix.jdbc.IfxDriver");

		connection = DriverManager.getConnection(jdbcUrl, username, password);
		return connection;
	}*/

	public NumSubTabVb getActiveNumTab(int ntValue, String databaseFilter) {
		String sql = "select * from NUM_SUB_TAB t where NUM_TAB='" + ntValue + "' AND NUM_SUBTAB_DESCRIPTION='"
				+ databaseFilter + "'";
		ResultSetExtractor rseObj = new ResultSetExtractor() {
			public NumSubTabVb extractData(ResultSet rs) throws SQLException, DataAccessException {
				NumSubTabVb numSubTabVb = new NumSubTabVb();
				while (rs.next()) {
					numSubTabVb.setNumSubTab(rs.getInt("NUM_SUB_TAB"));
					numSubTabVb.setDescription(rs.getString("NUM_SUBTAB_DESCRIPTION"));
				}
				return numSubTabVb;
			}
		};
		return (NumSubTabVb) getJdbcTemplate().query(sql, rseObj);
	}

	public Map<String, String> getDataTypeMap(String databaseType) {
		String sql = "select TAG_NAME, DISPLAY_NAME from MACROVAR_TAGGING t where MACROVAR_NAME = 'DATA_TYPE' AND UPPER(MACROVAR_TYPE) = UPPER('"+databaseType+"') ORDER BY TAG_NO";
		ResultSetExtractor rseObj = new ResultSetExtractor() {
			Map<String, String> returnMap = new HashMap<String, String>();

			@Override
			public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
				while (rs.next()) {
					returnMap.put(rs.getString("TAG_NAME"), rs.getString("DISPLAY_NAME"));
				}
				return returnMap;
			}
		};
		return (Map<String, String>) getJdbcTemplate().query(sql, rseObj);
	}

	public String getQueryForTableAndView(String variableName) throws DataAccessException {
		String sql = "SELECT VARIABLE_SCRIPT FROM VISION_DYNAMIC_HASH_VAR WHERE VARIABLE_NAME = '" + variableName + "'";
		return getJdbcTemplate().queryForObject(sql, String.class);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doPublishToMain(VcConfigMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			setServiceDefaults();
			strCurrentOperation = "Publish";
			/* Do Validate For Relation */
			doValidateRelation(vObject.getCatalogId());
			
			/* Get version number to be used */
			int versionNumber = getMaxVersionNumber(vObject);
			versionNumber++;

			/* Move MAIN tables data to AD tables */
			moveMainDataToAD(vObject, versionNumber);

			/* Delete data from MAIN tables */
			deleteCatalogData(vObject, true);

			/* Move WIP tables data to MAIN tables */
			moveWipDataToMain(vObject);

			/* Delete data from WIP tables */
			deleteCatalogData(vObject, false);

		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (Exception ex) {
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	private ExceptionCode doValidateRelation(String catalogId) {
		
		ExceptionCode exceptionCode = new ExceptionCode();
		
		ArrayList relationDataList = getDataForTableRelation(catalogId, 1);
		
		HashMap<String,VcConfigMainTreeVb> tableDetailsHM = (HashMap<String,VcConfigMainTreeVb>) relationDataList.get(2);
		
		List<String> tableIdList = tableDetailsHM.entrySet().stream().map(entry -> entry.getKey()).collect(Collectors.toList());
		
		VcConfigMainVb vcConfigMainVb = new VcConfigMainVb();
		vcConfigMainVb.setCatalogId(catalogId);
		
		VcConfigMainVb catalogVb = getQueryResultsFromVisionCatalog(vcConfigMainVb, 1, false).get(0);
		
		Optional<Entry<String, VcConfigMainTreeVb>> baseTableMap = tableDetailsHM.entrySet().stream().filter(entry -> "Y".equalsIgnoreCase(entry.getValue().getBaseTableFlag())).findFirst();
		
		String baseTableId = baseTableMap.isPresent()?baseTableMap.get().getKey():null;
		
		if("Y".equalsIgnoreCase(catalogVb.getBaseTableJoinFlag()) && baseTableId == null) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Base-table falg is set for Catalog ["+catalogId+"] and yet no base-table is found.");
			throw buildRuntimeCustomException(exceptionCode);
		}
		
		List<String> chkTableList = null;
		/*for(int iIndex = 0; iIndex<tableIdList.size();iIndex++) {
			chkTableList = new ArrayList<String>(2);
			for(int jIndex = (iIndex+1); jIndex<tableIdList.size();jIndex++) {
				chkTableList.add(tableIdList.get(iIndex));
				chkTableList.add(tableIdList.get(jIndex));
				try {
					DynamicJoinNew.formDynamicJoinString(catalogId, 1, chkTableList, relationDataList, (ValidationUtil.isValid(baseTableId)?Integer.parseInt(baseTableId):null));
				} catch (Exception e) {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg(e.getMessage());
					throw buildRuntimeCustomException(exceptionCode);
				}
				chkTableList = new ArrayList<String>(2);
			}
		}*/
		if("Y".equalsIgnoreCase(catalogVb.getBaseTableJoinFlag())) {
			chkTableList = new ArrayList<String>(2);
			for(int jIndex = 0; jIndex<tableIdList.size();jIndex++) {
				if(baseTableId!=tableIdList.get(jIndex)) {
					chkTableList.add(baseTableId);
					chkTableList.add(tableIdList.get(jIndex));
					try {
						DynamicJoinNew.formDynamicJoinString(catalogId, 1, chkTableList, relationDataList, (ValidationUtil.isValid(baseTableId)?Integer.parseInt(baseTableId):null));
					} catch (Exception e) {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg(e.getMessage());
						throw buildRuntimeCustomException(exceptionCode);
					}
					chkTableList = new ArrayList<String>(2);
				}
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}
	
	@SuppressWarnings("rawtypes")
	public ArrayList getDataForTableRelation(String catalogId, Integer joinSyntaxType) {
		ArrayList returnArrayList = new ArrayList(3);
		final HashMap<String,List> linkedTblsForIndividualTblHM = new HashMap<String,List>();
		final HashMap<String,List<String>> relationHM = new HashMap<String,List<String>>();
		final HashMap<String,VcConfigMainTreeVb> tableDetailsHM = new HashMap<String,VcConfigMainTreeVb>();
		String sql="SELECT FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE, RELATION_SCRIPT, FILTER_CONDITION FROM  VC_RELATIONS_WIP " + 
				"WHERE CATALOG_ID = '" +catalogId+ "' "+
				"ORDER BY FROM_TABLE_ID, TO_TABLE_ID" ;
	
		try {
			getJdbcTemplate().query(sql, new RowCallbackHandler() {
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
		sql = " SELECT VTS.CATALOG_ID, VTS.TABLE_ID, VTS.TABLE_NAME, VTS.ALIAS_NAME, " +
				"        VTS.QUERY_ID, VTS.DATABASE_TYPE, VTS.DATABASE_CONNECTIVITY_DETAILS, VTS.TABLE_SOURCE_TYPE, " +
				"        CASE WHEN VTS.DATABASE_TYPE = 'MACROVAR' " +
				"        then (select VARIABLE_SCRIPT from VISION_DYNAMIC_HASH_VAR VDHV where VDHV.VARIABLE_NAME = VTS.DATABASE_CONNECTIVITY_DETAILS) " +
				"        else '' " +
				"        end VARIABLE_SCRIPT,BASE_TABLE_FLAG " +
				"   FROM VC_TREE_WIP VTS " +
				"  WHERE catalog_id = '"+ catalogId +"' AND VCT_STATUS = 0";
		
		try {
			
			getJdbcTemplate().query(sql, new RowCallbackHandler() {
				@Override
				public void processRow(ResultSet rs) throws SQLException, DataAccessException {
					VcConfigMainTreeVb treeDetailsVb = new VcConfigMainTreeVb();
					treeDetailsVb.setCatalogId(rs.getString("CATALOG_ID"));
					treeDetailsVb.setTableId(rs.getString("TABLE_ID"));
					treeDetailsVb.setTableName(rs.getString("TABLE_NAME"));
					treeDetailsVb.setAliasName(rs.getString("ALIAS_NAME"));
					treeDetailsVb.setQueryId(rs.getString("QUERY_ID"));
					treeDetailsVb.setDatabaseType(rs.getString("DATABASE_TYPE"));
					treeDetailsVb.setDatabaseConnectivityDetails(rs.getString("DATABASE_CONNECTIVITY_DETAILS"));
					treeDetailsVb.setTableSourceType(rs.getString("TABLE_SOURCE_TYPE"));
					treeDetailsVb.setVariableScript(rs.getString("VARIABLE_SCRIPT"));
					treeDetailsVb.setBaseTableFlag(rs.getString("BASE_TABLE_FLAG"));
					tableDetailsHM.put(rs.getString("TABLE_ID"), treeDetailsVb);
				}
			});
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		returnArrayList.add(linkedTblsForIndividualTblHM);
		returnArrayList.add(relationHM);
		returnArrayList.add(tableDetailsHM);
		return returnArrayList;
	}
	
	public synchronized int getMaxVersionNumber(VcConfigMainVb vObject) {
		String sql = "SELECT CASE WHEN MAX(VERSION_NO) IS NULL THEN 0 ELSE MAX(VERSION_NO) END VERSION_NO FROM VISION_CATALOG_AD WHERE CATALOG_ID = ?";
		Object args[] = {vObject.getCatalogId()};
		return getJdbcTemplate().queryForObject(sql, args, Integer.class);
	}

	public void moveMainDataToAD(VcConfigMainVb vObject, int versionNumber) {
		
		/* For VISION_CONFIGURATION */
		String sql = "INSERT INTO VISION_CATALOG_AD (VERSION_NO, CATALOG_ID, CATALOG_DESC, JOIN_CLAUSE_NT, JOIN_CLAUSE, " +
				" BASETABLE_JOINFLAG, RECORD_INDICATOR_NT, RECORD_INDICATOR, VC_STATUS_NT, VC_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, " +
				" DATE_LAST_MODIFIED, DATE_CREATION) " +
				" SELECT '" + versionNumber+ "' AS VERSION_NO, CATALOG_ID, CATALOG_DESC, JOIN_CLAUSE_NT, JOIN_CLAUSE, " +
				" BASETABLE_JOINFLAG, RECORD_INDICATOR_NT, RECORD_INDICATOR, VC_STATUS_NT, VC_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, " +
				" DATE_CREATION FROM VISION_CATALOG_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

		/* For VC_TREE_SELFBI */
		sql = "INSERT INTO VC_TREE_AD (VERSION_NO, CATALOG_ID, TABLE_ID, TABLE_NAME, ALIAS_NAME, BASE_TABLE_FLAG,"+
				" QUERY_ID, DATABASE_TYPE_AT, DATABASE_TYPE, DATABASE_CONNECTIVITY_DETAILS, SORT_TREE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCT_STATUS_NT, VCT_STATUS," + 
				" MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, TABLE_SOURCE_TYPE, ACCESS_CONTROL_SCRIPT, ACCESS_CONTROL_FLAG )" +
				" SELECT '" + versionNumber+ "' AS VERSION_NO, CATALOG_ID, TABLE_ID, TABLE_NAME, ALIAS_NAME, BASE_TABLE_FLAG, " + 
				" QUERY_ID, DATABASE_TYPE_AT, DATABASE_TYPE, DATABASE_CONNECTIVITY_DETAILS, SORT_TREE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCT_STATUS_NT, VCT_STATUS, " + 
				" MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, TABLE_SOURCE_TYPE, ACCESS_CONTROL_SCRIPT, ACCESS_CONTROL_FLAG " +
				" FROM VC_TREE_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

		/* For VC_COLUMNS */
		sql = "INSERT INTO VC_COLUMNS_AD (VERSION_NO, CATALOG_ID, TABLE_ID, COL_ID, COL_NAME, ALIAS_NAME, SORT_COLUMN," + 
				" COL_DISPLAY_TYPE_AT, COL_DISPLAY_TYPE, COL_ATTRIBUTE_TYPE_AT, COL_ATTRIBUTE_TYPE, COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, FORMAT_TYPE_NT," + 
				" FORMAT_TYPE, MAG_ENABLE_FLAG, MAG_TYPE_NT, MAG_TYPE, MAG_SELECTION_TYPE_AT, MAG_SELECTION_TYPE, RECORD_INDICATOR_NT, RECORD_INDICATOR," + 
				" VCC_STATUS_NT, VCC_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, MAG_DEFAULT, MAG_QUERY_ID," + 
				" MAG_DISPLAY_COLUMN, MAG_USE_COLUMN, FOLDER_IDS, EXPERSSION_TEXT, COL_TYPE_AT, COL_TYPE, MASKING_SCRIPT, MASKING_FLAG,COL_LENGTH)"+
	            " SELECT '" + versionNumber+ "' AS VERSION_NO, CATALOG_ID, TABLE_ID, COL_ID, COL_NAME, ALIAS_NAME, SORT_COLUMN, " +
				" COL_DISPLAY_TYPE_AT, COL_DISPLAY_TYPE, COL_ATTRIBUTE_TYPE_AT, COL_ATTRIBUTE_TYPE, COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, FORMAT_TYPE_NT, " +
				" FORMAT_TYPE, MAG_ENABLE_FLAG, MAG_TYPE_NT, MAG_TYPE, MAG_SELECTION_TYPE_AT, MAG_SELECTION_TYPE, RECORD_INDICATOR_NT, RECORD_INDICATOR, " +
				" VCC_STATUS_NT, VCC_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, MAG_DEFAULT, MAG_QUERY_ID, " +
				" MAG_DISPLAY_COLUMN, MAG_USE_COLUMN, FOLDER_IDS, EXPERSSION_TEXT, COL_TYPE_AT, COL_TYPE, MASKING_SCRIPT, MASKING_FLAG,COL_LENGTH" +
				" FROM VC_COLUMNS_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

		/* For VC_RELATIONS */
		sql = "INSERT INTO VC_RELATIONS_AD (VERSION_NO, CATALOG_ID, FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE," + 
				" JOIN_STRING, FILTER_CONDITION, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCR_STATUS_NT, VCR_STATUS, MAKER, VERIFIER, INTERNAL_STATUS," + 
				" DATE_LAST_MODIFIED, DATE_CREATION, RELATION_SCRIPT ) SELECT '" + versionNumber+ "' AS VERSION_NO, CATALOG_ID, FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE," + 
				" JOIN_STRING, FILTER_CONDITION, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCR_STATUS_NT, VCR_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, " +
				" DATE_LAST_MODIFIED, DATE_CREATION, RELATION_SCRIPT FROM VC_RELATIONS_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

	}

	public void deleteDataFromTables(VcConfigMainVb vObject, boolean mainTable) {
		if (mainTable) {
			/* For VISION_CONFIGURATION */
			String sql = "DELETE FROM VISION_CATALOG_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_TREE_SELFBI */
			sql = "DELETE FROM VC_TREE_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_COLUMNS */
			sql = "DELETE FROM VC_COLUMNS_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_RELATIONS */
			sql = "DELETE FROM VC_RELATIONS_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For CATALOG_ACCESS_SelfBI */
			sql = "DELETE FROM CATALOG_ACCESS_SelfBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

		} else {
			/* For VISION_CATALOG_WIP */
			String sql = "DELETE FROM VISION_CATALOG_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_TREE_WIP */
			sql = "DELETE FROM VC_TREE_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_COLUMNS_WIP */
			sql = "DELETE FROM VC_COLUMNS_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_RELATIONS_WIP */
			sql = "DELETE FROM VC_RELATIONS_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For CATALOG_ACCESS_SelfBI_WIP */
			/*sql = "DELETE FROM CATALOG_ACCESS_SelfBI_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);*/
		}
	}

	public void deleteCatalogData(VcConfigMainVb vObject, boolean mainTable) {
		if (mainTable) {
			/* For VISION_CONFIGURATION */
			String sql = "DELETE FROM VISION_CATALOG_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_TREE_SELFBI */
			sql = "DELETE FROM VC_TREE_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_COLUMNS */
			sql = "DELETE FROM VC_COLUMNS_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_RELATIONS */
			sql = "DELETE FROM VC_RELATIONS_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For CATALOG_ACCESS_SelfBI 
			sql = "DELETE FROM CATALOG_ACCESS_SelfBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);*/

		} else {
			/* For VISION_CATALOG_WIP */
			String sql = "DELETE FROM VISION_CATALOG_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_TREE_WIP */
			sql = "DELETE FROM VC_TREE_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_COLUMNS_WIP */
			sql = "DELETE FROM VC_COLUMNS_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For VC_RELATIONS_WIP */
			sql = "DELETE FROM VC_RELATIONS_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);

			/* For CATALOG_ACCESS_SelfBI_WIP 
			sql = "DELETE FROM CATALOG_ACCESS_SelfBI_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "' ";
			getJdbcTemplate().update(sql);*/
		}
	}
	public void moveWipDataToMain(VcConfigMainVb vObject) {
		ExceptionCode exceptionCode = new ExceptionCode();
		/* For VISION_CONFIGURATION */
		String sql = "SELECT COUNT(1) FROM VISION_CATALOG_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		int existanceCount = getJdbcTemplate().queryForObject(sql, Integer.class);
		if (existanceCount == 0) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("No records in work in progress table(VISION_CATALOG_WIP)");
			throw buildRuntimeCustomException(exceptionCode);
		}
		sql = "INSERT INTO VISION_CATALOG_SELFBI (CATALOG_ID, CATALOG_DESC, JOIN_CLAUSE_NT, JOIN_CLAUSE, BASETABLE_JOINFLAG," + 
				" RECORD_INDICATOR_NT, RECORD_INDICATOR, VC_STATUS_NT, VC_STATUS,  MAKER, VERIFIER, " + 
				" INTERNAL_STATUS,  DATE_LAST_MODIFIED,  DATE_CREATION) "
				+ "SELECT CATALOG_ID, CATALOG_DESC, JOIN_CLAUSE_NT, JOIN_CLAUSE, BASETABLE_JOINFLAG, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, VC_STATUS_NT,0 VC_STATUS, " + intCurrentUserId + " as MAKER, "
				+ intCurrentUserId + " as VERIFIER, "
				+ "INTERNAL_STATUS, sysdate as DATE_LAST_MODIFIED, sysdate as DATE_CREATION FROM VISION_CATALOG_WIP "
				+ " WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

		/* For VC_TREE_SELFBI */
		sql = "SELECT COUNT(1) FROM VC_TREE_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		existanceCount = getJdbcTemplate().queryForObject(sql, Integer.class);
		if (existanceCount == 0) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("No records in work in progress table(VC_TREE_WIP)");
			throw buildRuntimeCustomException(exceptionCode);
		}
		sql = "INSERT INTO VC_TREE_SELFBI  (CATALOG_ID, TABLE_ID, TABLE_NAME, ALIAS_NAME, BASE_TABLE_FLAG," + 
				" QUERY_ID, DATABASE_TYPE_AT, DATABASE_TYPE, DATABASE_CONNECTIVITY_DETAILS," + 
				" SORT_TREE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCT_STATUS_NT, VCT_STATUS," + 
				" MAKER, VERIFIER, INTERNAL_STATUS, " + 
				" DATE_LAST_MODIFIED, DATE_CREATION, TABLE_SOURCE_TYPE, ACCESS_CONTROL_SCRIPT,ACCESS_CONTROL_FLAG)" +
				" SELECT CATALOG_ID, TABLE_ID, TABLE_NAME, ALIAS_NAME, BASE_TABLE_FLAG, "
				+ "QUERY_ID, DATABASE_TYPE_AT, DATABASE_TYPE, DATABASE_CONNECTIVITY_DETAILS, "
				+ "SORT_TREE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCT_STATUS_NT, VCT_STATUS, " + intCurrentUserId
				+ " as MAKER, " + intCurrentUserId + " as VERIFIER, INTERNAL_STATUS, "
				+ "sysdate as DATE_LAST_MODIFIED, sysdate as DATE_CREATION, TABLE_SOURCE_TYPE, ACCESS_CONTROL_SCRIPT,ACCESS_CONTROL_FLAG FROM VC_TREE_WIP "
				+ "WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

		/* For VC_COLUMNS */
		sql = "SELECT COUNT(1) FROM VC_COLUMNS_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		existanceCount = getJdbcTemplate().queryForObject(sql, Integer.class);
		if (existanceCount == 0) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("No records in work in progress table(VC_COLUMNS_WIP)");
			throw buildRuntimeCustomException(exceptionCode);
		}
		sql = "INSERT INTO VC_COLUMNS_SELFBI ( CATALOG_ID, TABLE_ID, COL_ID, COL_NAME, ALIAS_NAME, SORT_COLUMN, COL_DISPLAY_TYPE_AT, COL_DISPLAY_TYPE, COL_ATTRIBUTE_TYPE_AT," + 
				" COL_ATTRIBUTE_TYPE, COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, FORMAT_TYPE_NT, FORMAT_TYPE, MAG_ENABLE_FLAG, MAG_TYPE_NT,"+
				" MAG_TYPE, MAG_SELECTION_TYPE_AT, MAG_SELECTION_TYPE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCC_STATUS_NT, VCC_STATUS," + 
				" MAKER,VERIFIER, INTERNAL_STATUS,  DATE_LAST_MODIFIED,  DATE_CREATION, MAG_DEFAULT, MAG_QUERY_ID, MAG_DISPLAY_COLUMN, "+
				" MAG_USE_COLUMN, FOLDER_IDS, EXPERSSION_TEXT, COL_TYPE_AT, COL_TYPE, MASKING_SCRIPT, MASKING_FLAG ,COL_LENGTH ) "
				+ "SELECT CATALOG_ID, TABLE_ID, COL_ID, COL_NAME, ALIAS_NAME, SORT_COLUMN, COL_DISPLAY_TYPE_AT, COL_DISPLAY_TYPE, COL_ATTRIBUTE_TYPE_AT, "
				+ "COL_ATTRIBUTE_TYPE, COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, FORMAT_TYPE_NT, FORMAT_TYPE, MAG_ENABLE_FLAG, MAG_TYPE_NT, "
				+ "MAG_TYPE, MAG_SELECTION_TYPE_AT, MAG_SELECTION_TYPE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCC_STATUS_NT, VCC_STATUS, "
				+ intCurrentUserId + " as MAKER, " + "" + intCurrentUserId
				+ " as VERIFIER, INTERNAL_STATUS, sysdate as DATE_LAST_MODIFIED, sysdate as DATE_CREATION, MAG_DEFAULT, MAG_QUERY_ID, MAG_DISPLAY_COLUMN, "
				+ "MAG_USE_COLUMN, FOLDER_IDS, EXPERSSION_TEXT, COL_TYPE_AT, COL_TYPE,MASKING_SCRIPT, MASKING_FLAG,COL_LENGTH FROM VC_COLUMNS_WIP "
				+ "WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);
/*
		 For VC_RELATIONS 
		sql = "SELECT COUNT(1) FROM VC_RELATIONS_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		existanceCount = getJdbcTemplate().queryForObject(sql, Integer.class);
		if (existanceCount == 0) {
			sql = "SELECT COUNT(DISTINCT TABLE_ID) COUNT FROM VC_TREE_WIP WHERE CATALOG_ID='" + vObject.getCatalogId()
					+ "'";
			if (getJdbcTemplate().queryForObject(sql, Integer.class) > 1) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("No records in work in progress table(VC_RELATIONS_WIP)");
				throw buildRuntimeCustomException(exceptionCode);
			}
		}*/
		sql = "INSERT INTO VC_RELATIONS_SELFBI (CATALOG_ID, FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE, JOIN_STRING, " + 
				" FILTER_CONDITION, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCR_STATUS_NT, VCR_STATUS," + 
				" MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, " + 
				" DATE_CREATION,RELATION_SCRIPT)"
				+ "SELECT CATALOG_ID, FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE, JOIN_STRING, "
				+ "FILTER_CONDITION, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCR_STATUS_NT, VCR_STATUS, " + ""
				+ intCurrentUserId + " as MAKER, " + intCurrentUserId
				+ " as VERIFIER, INTERNAL_STATUS, sysdate as DATE_LAST_MODIFIED, "
				+ "sysdate as DATE_CREATION,RELATION_SCRIPT FROM VC_RELATIONS_WIP " + "WHERE CATALOG_ID='" + vObject.getCatalogId()
				+ "'";
		getJdbcTemplate().update(sql);

/*		 For CATALOG_ACCESS_SelfBI 
		sql = "SELECT COUNT(1) FROM CATALOG_ACCESS_SelfBI_WIP WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		existanceCount = getJdbcTemplate().queryForObject(sql, Integer.class);
		if (existanceCount == 0) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("No records in work in progress table(CATALOG_ACCESS_SelfBI_WIP)");
			throw buildRuntimeCustomException(exceptionCode);
		}
		sql = "INSERT INTO CATALOG_ACCESS_SelfBI (CATALOG_ID, CATALOG_STATUS_NT, CATALOG_STATUS, " + 
				"  MAKER, VERIFIER, INTERNAL_STATUS," + 
				"  DATE_LAST_MODIFIED,  DATE_CREATION, RECORD_INDICATOR_NT, RECORD_INDICATOR, " + 
				"  USER_GROUP_AT, USER_GROUP, USER_PROFILE, USER_PROFILE_AT ) " + "SELECT CATALOG_ID, CATALOG_STATUS_NT, CATALOG_STATUS, "
				+ intCurrentUserId + " as MAKER, " + intCurrentUserId + " as VERIFIER, INTERNAL_STATUS, "
				+ " sysdate as DATE_LAST_MODIFIED, sysdate as DATE_CREATION, RECORD_INDICATOR_NT, RECORD_INDICATOR, "
				+ " USER_GROUP_AT, USER_GROUP, USER_PROFILE, USER_PROFILE_AT FROM CATALOG_ACCESS_SelfBI_WIP "
				+ "WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);*/
	}

	protected int doInsertRecordToVcCatalogWIPFromMain(VcConfigMainVb vObject) {
		int result = 0;
		String query = "INSERT INTO VISION_CATALOG_WIP (CATALOG_ID, CATALOG_DESC, JOIN_CLAUSE_NT, JOIN_CLAUSE, BASETABLE_JOINFLAG, RECORD_INDICATOR_NT, "
				+ " RECORD_INDICATOR, VC_STATUS_NT, VC_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) "
				+ " SELECT CATALOG_ID, CATALOG_DESC, JOIN_CLAUSE_NT, JOIN_CLAUSE, BASETABLE_JOINFLAG, RECORD_INDICATOR_NT, RECORD_INDICATOR, "
				+ " VC_STATUS_NT, " + vObject.getVcStatus() + ", " + intCurrentUserId + ", " + intCurrentUserId
				+ ", INTERNAL_STATUS, "
				+ " SYSDATE AS DATE_LAST_MODIFIED, SYSDATE AS DATE_CREATION FROM VISION_CATALOG_SELFBI where CATALOG_ID='"
				+ vObject.getCatalogId() + "'";
		try {
			result = getJdbcTemplate().update(query);
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Insert Error : " + e.getMessage());
		}
		return result;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doSaveOperationsWIP(VcConfigMainCRUDVb vcMainVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			setServiceDefaults();
			vcMainVb.setMaker(intCurrentUserId);
			vcMainVb.setVerifier(intCurrentUserId);
			strCurrentOperation = "add".equalsIgnoreCase(vcMainVb.getActionType())?"Add":"Modify";
			/* Validate and Insert or Modify Catalog WIP */
			doValidateAndSaveCatalogMain(vcMainVb);
			
			/* Delete tables, columns and relations as selected */
			doDeleteVcWIP(vcMainVb);

			/* Insert/modify tables and columns */
			if (ValidationUtil.isValidList(vcMainVb.getAddModifyMetadata())) {
				doInsertUpdateTreeAndColumnsWthRegExp(vcMainVb);
			}

			/* Insert/modify relations */
			if (ValidationUtil.isValidList(vcMainVb.getRelationAddModifyMetadata())) {
				doInsertUpdateRelationsWthRegExp(vcMainVb);
			}
			/* Insert/modify hash variables */
//			 doInsertUpdateInVcHashVariableWIP(vcForCatalogWrapperVb);
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (Exception ex) {
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	public void doDeleteVcWIP(VcConfigMainCRUDVb vcMainVb) {
		strCurrentOperation = Constants.DELETE;

		if(vcMainVb.getTableDeleteMetadata()!=null && vcMainVb.getTableDeleteMetadata().size()>0) {
			for(TableDeleteVb tableDeleteVb : vcMainVb.getTableDeleteMetadata()) {
				doDeleteForTableVcWIP(tableDeleteVb);
			}
		}
		
		if(vcMainVb.getColumnDeleteMetadata()!=null && vcMainVb.getColumnDeleteMetadata().size()>0) {
			for(ColumnDeleteVb columnDeleteVb : vcMainVb.getColumnDeleteMetadata()) {
				doDeleteForColumnVcWIP(columnDeleteVb);
			}
		}
		
		if(vcMainVb.getRelationDeleteMetadata()!=null && vcMainVb.getRelationDeleteMetadata().size()>0) {
			for(RelationDeleteVb relationDeleteVb : vcMainVb.getRelationDeleteMetadata()) {
				doDeleteForRelationsVcWIP(relationDeleteVb);
			}
		}
		
	}

	protected void doDeleteForTableVcWIP(TableDeleteVb tableDeleteVb) {
		
		Object[] args = { tableDeleteVb.getCatalogId(), tableDeleteVb.getTableId()};

		String query = "DELETE FROM VC_TREE_WIP WHERE CATALOG_ID = ? AND TABLE_ID = ?";
		getJdbcTemplate().update(query, args);

		query = "DELETE FROM VC_COLUMNS_WIP WHERE CATALOG_ID = ? AND TABLE_ID = ?";
		getJdbcTemplate().update(query, args);

		query = "DELETE FROM VC_RELATIONS_WIP WHERE CATALOG_ID = ? AND FROM_TABLE_ID = ?";
		getJdbcTemplate().update(query, args);

		query = "DELETE FROM VC_RELATIONS_WIP WHERE CATALOG_ID = ? AND TO_TABLE_ID = ?";
		getJdbcTemplate().update(query, args);
		
	}

	protected void doDeleteForColumnVcWIP(ColumnDeleteVb columnDeleteVb) {
		
		String query = "DELETE FROM VC_COLUMNS_WIP WHERE CATALOG_ID = ? AND TABLE_ID = ? AND COL_ID = ?";
		Object[] args = { columnDeleteVb.getCatalogId(), columnDeleteVb.getTableId(), columnDeleteVb.getColumnId() };
		getJdbcTemplate().update(query, args);
		
	}

	protected void doDeleteForRelationsVcWIP(RelationDeleteVb relationDeleteVb) {
		
		if(ValidationUtil.isValid(relationDeleteVb.getFromTableId()) && ValidationUtil.isValid(relationDeleteVb.getToTableId())) {
			String query = "DELETE FROM VC_RELATIONS_WIP WHERE CATALOG_ID = ? AND FROM_TABLE_ID = ? AND TO_TABLE_ID = ?";
			Object[] args = { relationDeleteVb.getCatalogId(), relationDeleteVb.getFromTableId(), relationDeleteVb.getToTableId() };
			getJdbcTemplate().update(query, args);
		}
		
	}

	public ArrayList getData(String catalogId) {
		String sql = "SELECT * FROM VC_RELATIONS_SELFBI WHERE CATALOG_ID=? AND VCR_STATUS=0 ORDER BY FROM_TABLE_ID, TO_TABLE_ID";
		Object lParams[] = { catalogId };
		ResultSetExtractor<ArrayList> rse = new ResultSetExtractor<ArrayList>() {
			@Override
			public ArrayList extractData(ResultSet rs) throws SQLException, DataAccessException {
				ArrayList returnList = new ArrayList(2);
				HashMap<String, String> linkedTblsForIndividualTblHM = new HashMap<String, String>();
				HashMap<String, String> relationHM = new HashMap<String, String>();
				while (rs.next()) {
					String fromId = String.valueOf(rs.getObject("FROM_TABLE_ID"));
					String toId = String.valueOf(rs.getObject("TO_TABLE_ID"));
					if (linkedTblsForIndividualTblHM.get(fromId) == null) {
						linkedTblsForIndividualTblHM.put(fromId, toId);
					} else {
						String linkString = linkedTblsForIndividualTblHM.get(fromId);
						linkString = linkString + "," + toId;
						linkedTblsForIndividualTblHM.put(fromId, linkString);
					}
					if (linkedTblsForIndividualTblHM.get(toId) == null) {
						linkedTblsForIndividualTblHM.put(toId, fromId);
					} else {
						String linkString = linkedTblsForIndividualTblHM.get(toId);
						linkString = linkString + "," + fromId;
						linkedTblsForIndividualTblHM.put(toId, linkString);
					}
					String key = fromId + "-" + toId;
					String relation = "";
					String filterCondition = "";
					Clob clob = rs.getClob("JOIN_STRING");
					if (clob != null)
						relation = clob.getSubString(1, (int) clob.length());

					clob = rs.getClob("FILTER_CONDITION");
					if (clob != null)
						filterCondition = clob.getSubString(1, (int) clob.length());
					if (ValidationUtil.isValid(filterCondition)) {
						relation = relation + " AND " + filterCondition;
					}
					relationHM.put(key, relation);
				}
				returnList.add(linkedTblsForIndividualTblHM);
				returnList.add(relationHM);
				return returnList;
			}
		};
		return getJdbcTemplate().query(sql, lParams, rse);
	}

	public Integer returnBaseTableId(String catalogId) {
		try {
			String baseFlagStr = getJdbcTemplate().queryForObject(
					"SELECT BASETABLE_JOINFLAG FROM VISION_CATALOG_SELFBI WHERE CATALOG_ID='" + catalogId + "'",
					String.class);
			if (ValidationUtil.isValid(baseFlagStr)) {
				if ("Y".equalsIgnoreCase(baseFlagStr)) {
					return getJdbcTemplate().queryForObject("SELECT TABLE_ID FROM VC_TREE_SELFBI WHERE CATALOG_ID='"
							+ catalogId + "' AND BASE_TABLE_FLAG='Y' AND ROWNUM<2 ORDER BY TABLE_ID", Integer.class);
				} else {
					return null;
				}
			} else {
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public List getDistinctOfDatadourceFromVcTree(VcConfigMainVb dObj) {
		List<VcConfigMainVb> collTemp = null;
		String query = "SELECT DATABASE_TYPE \"databaseType\", " + 
				"       CASE " + 
				"          WHEN DATABASE_TYPE = 'M_QUERY' THEN QUERY_ID " + 
				"          ELSE DATABASE_CONNECTIVITY_DETAILS " + 
				"       END " + 
				"          \"databaseConnectivityDetails\" " + 
				"  FROM VC_TREE_WIP " + 
				" WHERE catalog_id = '"+ dObj.getCatalogId() + "' " + 
				" group by  DATABASE_TYPE, " + 
				"       CASE " + 
				"          WHEN DATABASE_TYPE = 'M_QUERY' THEN QUERY_ID " + 
				"          ELSE DATABASE_CONNECTIVITY_DETAILS " + 
				"       END";
		if (dObj.getVcStatus() == 0 || dObj.getVcStatus() == 9) {
			query = "SELECT DATABASE_TYPE \"databaseType\", " + 
					"       CASE " + 
					"          WHEN DATABASE_TYPE = 'M_QUERY' THEN QUERY_ID " + 
					"          ELSE DATABASE_CONNECTIVITY_DETAILS " + 
					"       END " + 
					"          \"databaseConnectivityDetails\" " + 
					"  FROM VC_TREE_SELFBI " + 
					" WHERE catalog_id = '"+ dObj.getCatalogId() + "' " + 
					" group by  DATABASE_TYPE, " + 
					"       CASE " + 
					"          WHEN DATABASE_TYPE = 'M_QUERY' THEN QUERY_ID " + 
					"          ELSE DATABASE_CONNECTIVITY_DETAILS " + 
					"       END";
		}
		try {
			return getJdbcTemplate().queryForList(query);
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
	
	public List<VcConfigMainVb> getQuerySmartSearchFilter(VcConfigMainVb dObj) {
		Vector<Object> params = new Vector<Object>();
		
		setServiceDefaults();
		
		StringBuffer strBufApprove = new StringBuffer(
				" SELECT TAPPR.CATALOG_ID, TAPPR.CATALOG_DESC, TAPPR.JOIN_CLAUSE, TAPPR.BASETABLE_JOINFLAG, "
						+ " TAPPR.VC_STATUS_NT, TAPPR.VC_STATUS, "
						+ " (Select NUM_SUBTAB_DESCRIPTION from NUM_SUB_TAB where NUM_TAB = TAppr.VC_STATUS_NT and TAPPR.VC_STATUS=NUM_SUB_TAB ) AS STATUS_DESC,"
						+ " TAPPR.RECORD_INDICATOR_NT, TAPPR.RECORD_INDICATOR, TAPPR.MAKER, (select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TAPPR.MAKER) as MAKER_NAME,"
						+ " TAPPR.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TAPPR.VERIFIER) as VERIFIER_NAME, TAPPR.INTERNAL_STATUS, "
						+ " TO_CHAR(TAPPR.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, "
						+ " TO_CHAR(TAPPR.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION "
						+ " FROM VISION_CATALOG_SELFBI TAPPR LEFT JOIN CATALOG_ACCESS_SelfBI T2 ON (TAPPR.CATALOG_ID = T2.CATALOG_ID) "
						+ " WHERE (TAPPR.MAKER = '"+intCurrentUserId+"'  OR (T2.USER_GROUP = '"+userGroup+"' AND T2.USER_PROFILE = '"+userProfile+"')) AND ");
		StringBuffer strBufPending = new StringBuffer(
				" SELECT TWIP.CATALOG_ID, TWIP.CATALOG_DESC, TWIP.JOIN_CLAUSE, TWIP.BASETABLE_JOINFLAG, "
						+ " TWIP.VC_STATUS_NT, TWIP.VC_STATUS, "
						+ " (Select NUM_SUBTAB_DESCRIPTION from NUM_SUB_TAB where NUM_TAB = TWIP.VC_STATUS_NT and TWIP.VC_STATUS=NUM_SUB_TAB ) AS STATUS_DESC,"
						+ " TWIP.RECORD_INDICATOR_NT, TWIP.RECORD_INDICATOR, TWIP.MAKER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TWIP.MAKER) as MAKER_NAME,"
						+ " TWIP.VERIFIER,(select USER_NAME FROM SSBI_USER_VIEW where VISION_ID = TWIP.VERIFIER) as VERIFIER_NAME ,TWIP.INTERNAL_STATUS, "
						+ " TO_CHAR(TWIP.DATE_LAST_MODIFIED, 'DD-MM-YYYY HH24:MI:SS') DATE_LAST_MODIFIED, "
						+ " TO_CHAR(TWIP.DATE_CREATION, 'DD-MM-YYYY HH24:MI:SS') DATE_CREATION "
						+ " FROM VISION_CATALOG_WIP TWIP LEFT JOIN CATALOG_ACCESS_SelfBI T2 ON (TWIP.CATALOG_ID = T2.CATALOG_ID) "
						+ " WHERE (TWIP.MAKER = '"+intCurrentUserId+"'  OR (T2.USER_GROUP = '"+userGroup+"' AND T2.USER_PROFILE = '"+userProfile+"')) AND ");
		try {

			
			if (dObj.getSmartSearchVb().size() > 0) {
				int count = 1;
				for (SmartSearchVb data: dObj.getSmartSearchVb()){
					if(count == dObj.getSmartSearchVb().size()) {
						data.setJoinType("");
					} else {
						if(!ValidationUtil.isValid(data.getJoinType()) && !("AND".equalsIgnoreCase(data.getJoinType()) || "OR".equalsIgnoreCase(data.getJoinType()))) {
							data.setJoinType("AND");
						}
					}
					String val = CommonUtils.criteriaBasedVal(data.getCriteria(), data.getValue());
					switch (data.getObject()) {
					case "catalogId":
						CommonUtils.addToQuerySearch(" upper(TAPPR.CATALOG_ID) "+ val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TWIP.CATALOG_ID) "+ val, strBufPending, data.getJoinType());
						break;

					case "catalogDesc":
						CommonUtils.addToQuerySearch(" upper(TAPPR.CATALOG_DESC) "+ val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TWIP.CATALOG_DESC) "+ val, strBufPending, data.getJoinType());
						break;

					case "vcStatusDesc":
						List<NumSubTabVb> numSTData = numSubTabDao.findNumSubTabByDesc(val);
						String actData="";
						for(int k=0; k< numSTData.size(); k++) {
							int numsubtab = numSTData.get(k).getNumSubTab();
							if(!ValidationUtil.isValid(actData)) {
								actData = "'"+Integer.toString(numsubtab)+"'";
							}else {
								actData =actData+ ","+ "'"+Integer.toString(numsubtab)+"'";
							}
						}
						CommonUtils.addToQuerySearch(" upper(TAPPR.VC_STATUS) IN ("+ actData+") ", strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TWIP.VC_STATUS) IN ("+ actData+") ", strBufPending, data.getJoinType());
						break;

					case "dateCreation":
						CommonUtils.addToQuerySearch(" TO_CHAR(TAPPR.DATE_CREATION,'DD-MM-YYYY HH24:MI:SS') " + val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" TO_CHAR(TWIP.DATE_CREATION,'DD-MM-YYYY HH24:MI:SS') " + val, strBufPending, data.getJoinType());
						break;
									
					case "dateLastModified":
						CommonUtils.addToQuerySearch(" TO_CHAR(TAPPR.DATE_LAST_MODIFIED,'DD-MM-YYYY HH24:MI:SS') " + val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" TO_CHAR(TWIP.DATE_LAST_MODIFIED,'DD-MM-YYYY HH24:MI:SS') " + val, strBufPending, data.getJoinType());
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
						CommonUtils.addToQuerySearch(" (TAPPR.MAKER) IN ("+ actMData+") ", strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" (TWIP.MAKER) IN ("+ actMData+") ", strBufPending, data.getJoinType());
						break;

					case "verifierName":
						List<VisionUsersVb> vuData = visionUsersDao.findUserIdByDesc(val);
						String actVData="";
						for(int k=0; k< vuData.size(); k++) {
							int visId = vuData.get(k).getVisionId();
							if(!ValidationUtil.isValid(actVData)) {
								actVData = "'"+Integer.toString(visId)+"'";
							}else {
								actVData =actVData+ ","+ "'"+Integer.toString(visId)+"'";
							}
						}
						CommonUtils.addToQuerySearch( " upper(TAPPR.VERIFIER) IN upper("+ actVData+")", strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch( " upper(TWIP.VERIFIER) IN upper("+ actVData+")", strBufPending, data.getJoinType());
						break;

					default:
					}
					count++;
				}
			}
			String orderBy = " Order By CATALOG_ID ";
			return getQueryPopupResultsWithPend(dObj, strBufPending, strBufApprove, "", orderBy, params, getMapperForVC());

		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(((strBufApprove == null) ? "strBufApprove is Null" : strBufApprove.toString()));
			logger.error("UNION");
			logger.error(((strBufPending == null) ? "strBufPending is Null" : strBufPending.toString()));

			if (params != null)
				for (int i = 0; i < params.size(); i++)
					logger.error("objParams[" + i + "]" + params.get(i).toString());
			return null;

		}
	}	
	
	@SuppressWarnings("unchecked")
	public VcConfigMainTreeVb getTableColumnByTableIdCatalogId(VcConfigMainTreeVb vcTreeVb) {
		setServiceDefaults();
		String catalogTable = "VISION_CATALOG_WIP";
		String treeTable = "VC_TREE_WIP";
		String columnTable = "VC_COLUMNS_WIP";
		if (vcTreeVb.getVctStatus() == 0 || vcTreeVb.getVctStatus() == 9) {
			catalogTable = "VISION_CATALOG_SELFBI";
			treeTable = "VC_TREE_SELFBI";
			columnTable = "VC_COLUMNS_SELFBI";
		}
		String sql = "SELECT VC.CATALOG_ID, VCT.TABLE_ID, VCT.TABLE_NAME, VCT.ALIAS_NAME TABLE_ALIAS_NAME, VCT.BASE_TABLE_FLAG, VCT.QUERY_ID, VCT.DATABASE_TYPE, " + 
				" VCT.DATABASE_CONNECTIVITY_DETAILS, VCT.SORT_TREE, VCT.VCT_STATUS, VCT.TABLE_SOURCE_TYPE, VCT.ACCESS_CONTROL_FLAG, " + 
				" VCT.ACCESS_CONTROL_SCRIPT, VCC.COL_ID, VCC.COL_NAME, VCC.ALIAS_NAME COLUMN_ALIAS_NAME, VCC.SORT_COLUMN, VCC.COL_DISPLAY_TYPE, " + 
				" VCC.COL_ATTRIBUTE_TYPE, VCC.COL_EXPERSSION_TYPE, VCC.FORMAT_TYPE, VCC.MAG_ENABLE_FLAG, VCC.MAG_TYPE, " + 
				" VCC.MAG_SELECTION_TYPE, VCC.VCC_STATUS, VCC.MAG_DEFAULT, VCC.MAG_QUERY_ID, VCC.MAG_DISPLAY_COLUMN, " + 
				" VCC.MAG_USE_COLUMN, VCC.FOLDER_IDS, VCC.EXPERSSION_TEXT, VCC.COL_TYPE, VCC.MASKING_FLAG, VCC.MASKING_SCRIPT,VCC.COL_LENGTH " + 
				" FROM "+catalogTable+" VC, "+treeTable+" VCT, "+columnTable+" VCC " + 
				" WHERE VC.CATALOG_ID = VCT.CATALOG_ID " + 
				" AND VCT.CATALOG_ID = VCC.CATALOG_ID " + 
				" AND VCT.TABLE_ID = VCC.TABLE_ID " + 
				" AND VC.CATALOG_ID = '"+vcTreeVb.getCatalogId()+"'"+ 
				" AND VCT.TABLE_ID = "+vcTreeVb.getTableId();
		
		try(Connection con = getConnection();
				Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				ResultSet rs = stmt.executeQuery(sql);) {
				List<UserRestrictionVb> userRestList = new CommonDao(jdbcTemplate).getRestrictionTree();
				VcConfigMainTreeVb treeVb = null;
				List<VcConfigMainColumnsVb> columnList = new ArrayList<VcConfigMainColumnsVb>();
				while(rs.next()) {
				VcConfigMainColumnsVb columnVb = new VcConfigMainColumnsVb();
				columnVb.setCatalogId(rs.getString("CATALOG_ID"));
				columnVb.setTableId(rs.getString("TABLE_ID"));
				columnVb.setColId(rs.getString("COL_ID"));
				columnVb.setColName(rs.getString("COL_NAME"));
				columnVb.setAliasName(rs.getString("COLUMN_ALIAS_NAME"));
				columnVb.setSortColumn(rs.getString("SORT_COLUMN"));
				columnVb.setColDisplayType(rs.getString("COL_DISPLAY_TYPE"));
				columnVb.setColAttributeType(rs.getString("COL_ATTRIBUTE_TYPE"));
				columnVb.setColExperssionType(rs.getString("COL_EXPERSSION_TYPE"));
				columnVb.setFormatType(rs.getString("FORMAT_TYPE"));
				columnVb.setMagEnableFlag(rs.getString("MAG_ENABLE_FLAG"));
				columnVb.setMagType(rs.getInt("MAG_TYPE"));
				columnVb.setMagSelectionType(rs.getString("MAG_SELECTION_TYPE"));
				columnVb.setVccStatus(rs.getInt("VCC_STATUS"));
				columnVb.setMagDefault(rs.getString("MAG_DEFAULT"));
				columnVb.setMagQueryId(rs.getString("MAG_QUERY_ID"));
				columnVb.setMagDisplayColumn(rs.getString("MAG_DISPLAY_COLUMN"));
				columnVb.setMagUseColumn(rs.getString("MAG_USE_COLUMN"));
				columnVb.setFolderIds(rs.getString("FOLDER_IDS"));
				columnVb.setExperssionText(rs.getString("EXPERSSION_TEXT"));
				columnVb.setColType(rs.getString("COL_TYPE"));
				columnVb.setMaskingFlag(rs.getString("MASKING_FLAG"));
				columnVb.setMaskingScript(rs.getString("MASKING_SCRIPT"));
				columnVb.setColLength(rs.getString("COL_LENGTH"));
				/* Sample Script
				----------------
				<maskings>
					<masking>
						<usergroup>ADMIN</usergroup>
						<userprofile>MANAGEMENT</userprofile>
						<pattern>___XXX___</pattern>
					</masking>
				</maskings>
				----------------
				Sample Script */
				
				if(ValidationUtil.isValid(columnVb.getMaskingScript())) {
					List<MaskingVb> maskingVbList = new ArrayList<MaskingVb>();
					Matcher maskingMatchObj = Pattern.compile("<masking>(.*?)<\\/masking>", Pattern.DOTALL)
							.matcher(columnVb.getMaskingScript());
					while(maskingMatchObj.find()) {
						String userGroup = CommonUtils.getValueForXmlTag(maskingMatchObj.group(1), "usergroup");
						String userProfile = CommonUtils.getValueForXmlTag(maskingMatchObj.group(1), "userprofile");
						String pattern = CommonUtils.getValueForXmlTag(maskingMatchObj.group(1), "pattern");
						maskingVbList.add(new MaskingVb(userGroup, userProfile, pattern));
					}
					columnVb.setMaskingScriptParsed(maskingVbList.size()>0?maskingVbList:null);
				}
				
				columnList.add(columnVb);
			}
			if(rs.last()) {
				treeVb = new VcConfigMainTreeVb();
				treeVb.setCatalogId(rs.getString("CATALOG_ID"));
				treeVb.setTableId(rs.getString("TABLE_ID"));
				treeVb.setTableName(rs.getString("TABLE_NAME"));
				treeVb.setAliasName(rs.getString("TABLE_ALIAS_NAME"));
				treeVb.setBaseTableFlag(rs.getString("BASE_TABLE_FLAG"));
				treeVb.setQueryId(rs.getString("QUERY_ID"));
				treeVb.setDatabaseType(rs.getString("DATABASE_TYPE"));
				treeVb.setDatabaseConnectivityDetails(rs.getString("DATABASE_CONNECTIVITY_DETAILS"));
				treeVb.setSortTree(rs.getString("SORT_TREE"));
				treeVb.setVctStatus(rs.getInt("VCT_STATUS"));
				treeVb.setTableSourceType(rs.getString("TABLE_SOURCE_TYPE"));
				treeVb.setAccessControlFlag(rs.getString("ACCESS_CONTROL_FLAG"));
				treeVb.setAccessControlScript(rs.getString("ACCESS_CONTROL_SCRIPT"));
				treeVb.setChildren(columnList);
			}
			
			/* Sample Script
			----------------
			<categories>
				<auth>
					<category>COUNTRY-LE_BOOK</category>
					<COUNTRY>COUNTRY</COUNTRY>
					<LE_BOOK>LE_BOOK</LE_BOOK>
				</auth>
				<auth>
					<category>COUNTRY</category>
					<COUNTRY>COUNTRY</COUNTRY>
				</auth>
			</categories>
			----------------
			Sample Script */
			
			if (treeVb != null && ValidationUtil.isValid(treeVb.getAccessControlScript())) {
				List<UserRestrictionVb> restrictionListToReturn = new ArrayList<UserRestrictionVb>();
				Matcher authMatchObj = Pattern.compile("<auth>(.*?)<\\/auth>", Pattern.DOTALL)
						.matcher(treeVb.getAccessControlScript());
				while (authMatchObj.find()) {
					String category = CommonUtils.getValueForXmlTag(authMatchObj.group(1), "category");
					UserRestrictionVb filteredCategoryVb = (UserRestrictionVb) userRestList.stream()
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
				treeVb.setAccessControlScriptParsed(restrictionListToReturn.size()>0?restrictionListToReturn:null);
			}
			
			return treeVb;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
	
	@SuppressWarnings("rawtypes")
	public List getTableColumnAliasByCatalogId(VcConfigMainVb vcConfigMainVb) {
		setServiceDefaults();
		Integer status=vcConfigMainVb.getVcStatus();
		String columnTable = "VC_COLUMNS_WIP";
		String treeTable = "VC_TREE_WIP";
		if (status == 0 || status == 9) {
			columnTable = "VC_COLUMNS_SELFBI";
			treeTable = "VC_TREE_SELFBI";
		}
		
		String sql = " select T1.CATALOG_ID, T1.TABLE_ID, T1.TABLE_NAME, T1.ALIAS_NAME TABLE_ALIAS_NAME, T1.DATABASE_TYPE , "+
				"  CASE "+
				"     WHEN T1.DATABASE_TYPE = 'M_QUERY' THEN T1.QUERY_ID "+
				"     ELSE T1.DATABASE_CONNECTIVITY_DETAILS "+
				"  END DATABASE_CONNECTIVITY_DETAILS, T1.SORT_TREE, T2.COL_ID, T2.COL_NAME, "+
				"  CASE "+
				"     WHEN T2.ALIAS_NAME is null THEN T2.COL_NAME "+
				"     ELSE T2.ALIAS_NAME "+
				"  END COLUMN_ALIAS_NAME, T2.SORT_COLUMN from "+treeTable+" T1, "+columnTable+" T2 "+
				" where T1.CATALOG_ID = T2.CATALOG_ID "+
				" AND T1.TABLE_ID = T2.TABLE_ID "+
				" AND T1.CATALOG_ID = '"+vcConfigMainVb.getCatalogId()+"' order by T1.SORT_TREE, T1.TABLE_ID, T2.SORT_COLUMN, T2.COL_ID";
		try{
			ResultSetExtractor<List> rse = new ResultSetExtractor<List>() {
				@Override
				public List extractData(ResultSet rs) throws SQLException, DataAccessException {
					List returnList = new ArrayList();
					VcConfigMainAliasVb treeVb = new VcConfigMainAliasVb();
					List<VcConfigMainAliasVb> children = new ArrayList<VcConfigMainAliasVb>();
					Integer tempId = null;
					if(rs.next()) {
						tempId = rs.getInt("TABLE_ID");
						treeVb = new VcConfigMainAliasVb(tempId, rs.getInt("SORT_TREE"), rs.getString("TABLE_NAME"), rs.getString("TABLE_ALIAS_NAME"), rs.getString("DATABASE_TYPE"), rs.getString("DATABASE_CONNECTIVITY_DETAILS"), null);
						children.add(new VcConfigMainAliasVb(rs.getInt("COL_ID"), rs.getInt("SORT_COLUMN"), rs.getString("COL_NAME"), rs.getString("COLUMN_ALIAS_NAME"), null, null, null));
					}
					while(rs.next()) {
						if(tempId==rs.getInt("TABLE_ID")) {
							children.add(new VcConfigMainAliasVb(rs.getInt("COL_ID"), rs.getInt("SORT_COLUMN"), rs.getString("COL_NAME"), rs.getString("COLUMN_ALIAS_NAME"), null, null, null));
						} else {
							treeVb.setChildren(children);
							returnList.add(treeVb);
							tempId = rs.getInt("TABLE_ID");
							treeVb = new VcConfigMainAliasVb(tempId, rs.getInt("SORT_TREE"), rs.getString("TABLE_NAME"), rs.getString("TABLE_ALIAS_NAME"), rs.getString("DATABASE_TYPE"), rs.getString("DATABASE_CONNECTIVITY_DETAILS"), null);
							children = new ArrayList<VcConfigMainAliasVb>();
							children.add(new VcConfigMainAliasVb(rs.getInt("COL_ID"), rs.getInt("SORT_COLUMN"), rs.getString("COL_NAME"), rs.getString("COLUMN_ALIAS_NAME"), null, null, null));
						}
					}
					
					if (children != null && children.size() > 0) {
						treeVb.setChildren(children);
						returnList.add(treeVb);
					}
					
					return returnList;
				}
			};
			return getJdbcTemplate().query(sql, rse);
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error(((sql==null)? "query is Null":sql));
			return null;
		}
	}
	
	@SuppressWarnings("rawtypes")
	public List<VcForCatalogTableRelationVb> getToTableIdsByFromTableId(VcConfigMainRelationVb vcConfigMainRelationVb) { 
		setServiceDefaults();
		String catalogId=vcConfigMainRelationVb.getCatalogId();
		String fromTableId=vcConfigMainRelationVb.getFromTableId();
		Integer status=vcConfigMainRelationVb.getVcrStatus();
		String tableName = "VC_RELATIONS_WIP";
		if (status == 0 || status == 9) {
			tableName = "VC_RELATIONS_SELFBI";
		}
		String query = "SELECT t1.to_table_id,t1.catalog_Id,(SELECT num_subtab_description" + 
				" FROM num_sub_tab" + 
				" WHERE num_tab = 41 AND num_sub_tab = t1.join_type) AS jointypedesc" + 
				" FROM  "+tableName+" t1" + 
				" WHERE  t1.vcr_status = "+status + 
				" AND t1.catalog_id ='"+catalogId+"'" + 
				" AND t1.from_table_id = "+fromTableId; 
				
	/*	String query = " SELECT TO_TABLE_ID FROM " + tableName
						+ " WHERE VCR_STATUS="+status+" AND CATALOG_ID='"+catalogId+"' AND FROM_TABLE_ID="+fromTableId;*/
		try{
			@SuppressWarnings("rawtypes")
			RowMapper mapper = new RowMapper() {
				public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
					VcForCatalogTableRelationVb vcForCatalogTableRelationVb=new VcForCatalogTableRelationVb();
					vcForCatalogTableRelationVb.setCatalogId(rs.getString("catalog_Id"));
					vcForCatalogTableRelationVb.setToTableId(rs.getString("to_table_id"));
					vcForCatalogTableRelationVb.setJoinString(rs.getString("jointypedesc"));
					return vcForCatalogTableRelationVb;
				}
			};
			return getJdbcTemplate().query(query, mapper);
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error(((query==null)? "query is Null":query));
			return null;
		}
	}

	public String getColumnMetaDataScriptForVcQueries(String macroVar) {
		try {
			String sql = "SELECT COLUMNS_METADATA FROM VC_QUERIES WHERE UPPER(QUERY_ID)='"+macroVar.toUpperCase()+"'";
			return getJdbcTemplate().queryForObject(sql, String.class);
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public Map<Integer, List<VcConfigMainRelationVb>> getRelationDetailsForCatalog(VcConfigMainVb configMainVb){
		
		String relationTableName = (configMainVb.getVcStatus() == 0 || configMainVb.getVcStatus() == 9)?"VC_RELATIONS_SELFBI":"VC_RELATIONS_WIP";
		String treeTableName = (configMainVb.getVcStatus() == 0 || configMainVb.getVcStatus() == 9)?"VC_TREE_SELFBI":"VC_TREE_WIP";
		
		String sql = " SELECT CATALOG_ID, FROM_TABLE_ID, (SELECT TABLE_NAME FROM "+treeTableName+" WHERE CATALOG_ID = ? AND TABLE_ID = R1.FROM_TABLE_ID) FROM_TABLE_NAME, " +
				" TO_TABLE_ID, (SELECT TABLE_NAME FROM "+treeTableName+" WHERE CATALOG_ID = ? AND TABLE_ID = R1.TO_TABLE_ID) TO_TABLE_NAME, " +
				" JOIN_TYPE_NT, JOIN_TYPE, JOIN_STRING, FILTER_CONDITION, " + 
				" RECORD_INDICATOR_NT, RECORD_INDICATOR, VCR_STATUS_NT, VCR_STATUS, MAKER, VERIFIER, " + 
				" INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, RELATION_SCRIPT " + 
				" FROM "+relationTableName+" R1 WHERE CATALOG_ID = ? ORDER BY CATALOG_ID, FROM_TABLE_ID, TO_TABLE_ID";
		
		Object args[] = {configMainVb.getCatalogId(), configMainVb.getCatalogId(), configMainVb.getCatalogId()};
		
		try {
			ResultSetExtractor<Map<Integer, List<VcConfigMainRelationVb>>> rowmapper = new ResultSetExtractor<Map<Integer, List<VcConfigMainRelationVb>>>() {

				@Override
				public Map<Integer, List<VcConfigMainRelationVb>> extractData(ResultSet rs) throws SQLException, DataAccessException {
					Map<Integer, List<VcConfigMainRelationVb>> returnMap = new HashMap<Integer, List<VcConfigMainRelationVb>>();
					while(rs.next()) {
						List<VcConfigMainRelationVb> relationList = returnMap.get(rs.getInt("FROM_TABLE_ID")) != null
								? returnMap.get(rs.getInt("FROM_TABLE_ID"))
								: new ArrayList<VcConfigMainRelationVb>();
						VcConfigMainRelationVb relationVb = new VcConfigMainRelationVb();
						relationVb.setCatalogId(rs.getString("CATALOG_ID"));
						relationVb.setFromTableId(rs.getString("FROM_TABLE_ID"));
						relationVb.setFromTableName(rs.getString("FROM_TABLE_NAME"));
						relationVb.setToTableId(rs.getString("TO_TABLE_ID"));
						relationVb.setToTableName(rs.getString("TO_TABLE_NAME"));
						relationVb.setJoinType(rs.getInt("JOIN_TYPE"));
						relationVb.setFilterCondition(rs.getString("FILTER_CONDITION"));
						relationVb.setVcrStatus(rs.getInt("VCR_STATUS"));
						relationVb.setRelationScript(rs.getString("RELATION_SCRIPT"));
						if(ValidationUtil.isValid(relationVb.getRelationScript())) {
							List<RelationMapVb> mapList = new ArrayList<RelationMapVb>();
							
							Matcher matchObj = Pattern.compile("<customjoin>(.*?)<\\/customjoin>", Pattern.DOTALL).matcher(relationVb.getRelationScript());
							if(matchObj.find()) {
								relationVb.setCustomJoinString(matchObj.group(1));
							}							
							
							if(!ValidationUtil.isValid(relationVb.getCustomJoinString())) {
								matchObj = Pattern.compile("<column>(.*?)<\\/column>", Pattern.DOTALL).matcher(relationVb.getRelationScript());
								while(matchObj.find()) {
									mapList.add(new RelationMapVb(CommonUtils.getValueForXmlTag(matchObj.group(1), "fcolumn"), CommonUtils.getValueForXmlTag(matchObj.group(1), "tcolumn")));
								}
							}
							relationVb.setRelationScriptParsed(ValidationUtil.isValidList(mapList)?mapList:null);
						}
						relationList.add(relationVb);
						returnMap.put(rs.getInt("FROM_TABLE_ID"),relationList);
					}
					return returnMap;
				}
			};
			return getJdbcTemplate().query(sql, args, rowmapper);
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public ExceptionCode doValidateAndSaveCatalogMain(VcConfigMainCRUDVb vcMainVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String operation = "add".equalsIgnoreCase(vcMainVb.getActionType())?"Add":"Modify";
		VcConfigMainVb dObj = new VcConfigMainVb();
		dObj.setCatalogId(vcMainVb.getCatalogId());
		List<VcConfigMainVb> catalogWIPList = getQueryResultsFromVisionCatalog(dObj, 1, false);
		List<VcConfigMainVb> catalogMainList = getQueryResultsFromVisionCatalog(dObj, 0, false);
		if(ValidationUtil.isValidList(catalogWIPList)) {
			if("add".equalsIgnoreCase(operation)) {
				exceptionCode = CommonUtils.getResultObject("Catalog", Constants.DUPLICATE_KEY_INSERTION, operation, null);
				throw new RuntimeCustomException(exceptionCode);
			} else {
				if(0 == vcMainVb.getVcStatus() && ValidationUtil.isValidList(catalogMainList)) {
					exceptionCode = CommonUtils.getResultObject("Catalog", Constants.WE_HAVE_ERROR_DESCRIPTION, operation, "Cannot create multiple instance for same catalog in pending Table.");
					throw new RuntimeCustomException(exceptionCode);
				}
				retVal =  doUpdateApprForVcCatalogInWIP(vcMainVb);
				if(retVal == Constants.SUCCESSFUL_OPERATION)
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				else
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			}
			return exceptionCode;
		}
		
		
		if(ValidationUtil.isValidList(catalogMainList)) {
			VcConfigMainVb catalogMainVb = catalogMainList.get(0);
			if("add".equalsIgnoreCase(operation)) {
				exceptionCode = CommonUtils.getResultObject("Catalog", (catalogMainVb.getVcStatus() == 9?Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE:Constants.DUPLICATE_KEY_INSERTION), operation, null);
				throw new RuntimeCustomException(exceptionCode);
			} else {
				if(catalogMainVb.getVcStatus() == 9 && vcMainVb.getVcStatus() == 9) {
					exceptionCode = CommonUtils.getResultObject("Catalog", Constants.CANNOT_DELETE_AN_INACTIVE_RECORD, operation, null);
					throw new RuntimeCustomException(exceptionCode);
				} else {
					moveMainDataToWIP(vcMainVb);
					vcMainVb.setVcStatus(2);
					retVal =  doUpdateApprForVcCatalogInWIP(vcMainVb);
					if(retVal == Constants.SUCCESSFUL_OPERATION)
						exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					else
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				}
			}
			return exceptionCode;
		} else {
			if("add".equalsIgnoreCase(operation)) {
				retVal =  doInsertionApprforVcCatalogIntoWIP(vcMainVb);
				if(retVal == Constants.SUCCESSFUL_OPERATION)
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				else
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			} else {
				exceptionCode = CommonUtils.getResultObject("Catalog", Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD, operation, null);
				throw new RuntimeCustomException(exceptionCode);
			}
			return exceptionCode;
		}
	}

	protected int doUpdateApprForVcCatalogInWIP(VcConfigMainCRUDVb vObject) {
		String query = "Update VISION_CATALOG_WIP Set CATALOG_DESC = ?, JOIN_CLAUSE = ?, BASETABLE_JOINFLAG = ?, "
				+ "VC_STATUS = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, DATE_LAST_MODIFIED = To_Date(?, 'DD-MM-YYYY HH24:MI:SS') Where CATALOG_ID = ?";
		Object[] args = { vObject.getCatalogDesc(), vObject.getJoinClause(), vObject.getBaseTableJoinFlag(), 
				vObject.getVcStatus(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getDateLastModified(), vObject.getCatalogId()};
		return getJdbcTemplate().update(query, args);
	}
	
	protected int doInsertionApprforVcCatalogIntoWIP(VcConfigMainCRUDVb vObject) {
		String query = "INSERT INTO VISION_CATALOG_WIP "
				+ " (CATALOG_ID, CATALOG_DESC, RECORD_INDICATOR_NT, RECORD_INDICATOR, VC_STATUS_NT, VC_STATUS, MAKER, VERIFIER, "
				+ " INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,JOIN_CLAUSE,BASETABLE_JOINFLAG) VALUES "
				+ " (?, ?, ?, ?, ?, ?, ?, ?, ?, To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), ?, ?)";
		Object[] args = { vObject.getCatalogId(), vObject.getCatalogDesc(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getVcStatusNt(), vObject.getVcStatus(), intCurrentUserId,
				intCurrentUserId, vObject.getInternalStatus(), vObject.getDateLastModified(), vObject.getDateCreation(),
				vObject.getJoinClause(), vObject.getBaseTableJoinFlag() };
		return getJdbcTemplate().update(query, args);
	}
	
	public void moveMainDataToWIP(VcConfigMainCRUDVb vObject) {
		
		/* For VISION_CONFIGURATION */
		String sql = "INSERT INTO VISION_CATALOG_WIP (CATALOG_ID, CATALOG_DESC, JOIN_CLAUSE_NT, JOIN_CLAUSE, BASETABLE_JOINFLAG, RECORD_INDICATOR_NT," + 
				"   RECORD_INDICATOR, VC_STATUS_NT,  VC_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"+
			    " SELECT CATALOG_ID, CATALOG_DESC, JOIN_CLAUSE_NT, JOIN_CLAUSE, BASETABLE_JOINFLAG, RECORD_INDICATOR_NT, " + 
				"RECORD_INDICATOR, VC_STATUS_NT, 2 VC_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION FROM VISION_CATALOG_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

		/* For VC_TREE_SELFBI */
		sql = "INSERT INTO VC_TREE_WIP (CATALOG_ID, TABLE_ID, TABLE_NAME, ALIAS_NAME, BASE_TABLE_FLAG, QUERY_ID, DATABASE_TYPE_AT, DATABASE_TYPE," + 
				" DATABASE_CONNECTIVITY_DETAILS, SORT_TREE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCT_STATUS_NT, VCT_STATUS," + 
				" MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, TABLE_SOURCE_TYPE, ACCESS_CONTROL_SCRIPT," + 
				" ACCESS_CONTROL_FLAG) SELECT CATALOG_ID, TABLE_ID, TABLE_NAME, ALIAS_NAME, BASE_TABLE_FLAG, QUERY_ID, DATABASE_TYPE_AT, DATABASE_TYPE, " + 
				" DATABASE_CONNECTIVITY_DETAILS, SORT_TREE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCT_STATUS_NT, VCT_STATUS, " + 
				" MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, TABLE_SOURCE_TYPE, ACCESS_CONTROL_SCRIPT, " +
				" ACCESS_CONTROL_FLAG FROM VC_TREE_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

		/* For VC_COLUMNS */
		sql = "INSERT INTO VC_COLUMNS_WIP (CATALOG_ID, TABLE_ID, COL_ID, COL_NAME, ALIAS_NAME, SORT_COLUMN, COL_DISPLAY_TYPE_AT, COL_DISPLAY_TYPE, COL_ATTRIBUTE_TYPE_AT," + 
				" COL_ATTRIBUTE_TYPE, COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, FORMAT_TYPE_NT, FORMAT_TYPE, MAG_ENABLE_FLAG, MAG_TYPE_NT, MAG_TYPE," + 
				" MAG_SELECTION_TYPE_AT, MAG_SELECTION_TYPE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCC_STATUS_NT, VCC_STATUS, MAKER, VERIFIER," + 
				" INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, MAG_DEFAULT, MAG_QUERY_ID, MAG_DISPLAY_COLUMN, MAG_USE_COLUMN," + 
				" FOLDER_IDS, EXPERSSION_TEXT, COL_TYPE_AT, COL_TYPE, MASKING_SCRIPT, MASKING_FLAG,COL_LENGTH ) SELECT CATALOG_ID, TABLE_ID, COL_ID, COL_NAME, ALIAS_NAME, SORT_COLUMN, COL_DISPLAY_TYPE_AT, COL_DISPLAY_TYPE, COL_ATTRIBUTE_TYPE_AT, " + 
				" COL_ATTRIBUTE_TYPE, COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, FORMAT_TYPE_NT, FORMAT_TYPE, MAG_ENABLE_FLAG, MAG_TYPE_NT, MAG_TYPE, " + 
				" MAG_SELECTION_TYPE_AT, MAG_SELECTION_TYPE, RECORD_INDICATOR_NT, RECORD_INDICATOR, VCC_STATUS_NT, VCC_STATUS, MAKER, VERIFIER, " + 
				" INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, MAG_DEFAULT, MAG_QUERY_ID, MAG_DISPLAY_COLUMN, MAG_USE_COLUMN, " + 
				" FOLDER_IDS, EXPERSSION_TEXT, COL_TYPE_AT, COL_TYPE, MASKING_SCRIPT, MASKING_FLAG,COL_LENGTH  FROM VC_COLUMNS_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

		/* For VC_RELATIONS */
		sql = "INSERT INTO VC_RELATIONS_WIP (CATALOG_ID, FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE, JOIN_STRING, FILTER_CONDITION," + 
				" RECORD_INDICATOR_NT, RECORD_INDICATOR, VCR_STATUS_NT, VCR_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED," + 
				" DATE_CREATION, RELATION_SCRIPT ) SELECT CATALOG_ID, FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE, JOIN_STRING, FILTER_CONDITION, " +
				" RECORD_INDICATOR_NT, RECORD_INDICATOR, VCR_STATUS_NT, VCR_STATUS, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, " + 
				" DATE_CREATION, RELATION_SCRIPT FROM VC_RELATIONS_SELFBI WHERE CATALOG_ID='" + vObject.getCatalogId() + "'";
		getJdbcTemplate().update(sql);

	}

	public ExceptionCode doInsertUpdateTreeAndColumnsWthRegExp(VcConfigMainCRUDVb vcMainVb) throws Exception{
		ExceptionCode exceptionCode = new ExceptionCode();
		List<VcConfigMainTreeVb> catalogtablelist = vcMainVb.getAddModifyMetadata();
		for (VcConfigMainTreeVb treeVb : catalogtablelist) {
			if(ValidationUtil.isValid(treeVb.getVctStatus()) && treeVb.getVctStatus() != -1) {
				if(ValidationUtil.isValidList(treeVb.getAccessControlScriptParsed())) {
					StringBuffer accessControlStrBuf = new StringBuffer("<categories>");
					for(UserRestrictionVb categoryVb : treeVb.getAccessControlScriptParsed()) {
						accessControlStrBuf.append("<auth>");
						accessControlStrBuf.append("<category>"+categoryVb.getMacrovarName()+"</category>");
						if(ValidationUtil.isValidList(categoryVb.getChildren())) {
							for(UserRestrictionVb tagVb : categoryVb.getChildren()) {
								accessControlStrBuf.append("<"+tagVb.getTagName()+">"+tagVb.getTagValue()+"</"+tagVb.getTagName()+">");
							}
						}
						accessControlStrBuf.append("</auth>");
					}
					accessControlStrBuf.append("</categories>");
					treeVb.setAccessControlScript(String.valueOf(accessControlStrBuf));
				}
				if (!checkTblExistance(treeVb, 1)) {
					doInsertionApprForVcTreeWIP(vcMainVb, treeVb);
				} else {
					doUpdateApprForVcTreeWIP(vcMainVb, treeVb);
				}
				
				if(ValidationUtil.isValidList(treeVb.getChildren())) {
					for(VcConfigMainColumnsVb columnVb : treeVb.getChildren()) {
						if(ValidationUtil.isValidList(columnVb.getMaskingScriptParsed())) {
							StringBuffer maskingStrBuf = new StringBuffer("<maskings>");
							for(MaskingVb maskVb : columnVb.getMaskingScriptParsed()) {
								maskingStrBuf.append("<masking>");
								maskingStrBuf.append("<usergroup>"+maskVb.getUserGroup()+"</usergroup>");
								maskingStrBuf.append("<userprofile>"+maskVb.getUserProfile()+"</userprofile>");
								maskingStrBuf.append("<pattern>"+maskVb.getPattern()+"</pattern>");
								maskingStrBuf.append("</masking>");
							}
							maskingStrBuf.append("</maskings>");
							columnVb.setMaskingScript(String.valueOf(maskingStrBuf));
						}
						if (!checkColExistance(columnVb, 1)) {
							doInsertionApprForVcColumnsWIP(vcMainVb, columnVb);
						} else {
							doUpdateApprForVcColumnsWIP(vcMainVb, columnVb);
						}
					}
				}
			}
		}
		exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		return exceptionCode;
	}
	
	protected boolean checkTblExistance(VcConfigMainTreeVb vObj, int intStatus) {
		String tableName = (intStatus == 0) ? "VC_TREE_SELFBI" : "VC_TREE_WIP";
		boolean existCheck = false;
		String sql = "SELECT COUNT(1) FROM " + tableName + " WHERE CATALOG_ID = '" + vObj.getCatalogId()
				+ "' AND TABLE_ID = '" + vObj.getTableId() + "'";
		try {
			int result = getJdbcTemplate().queryForObject(sql, Integer.class);
			existCheck = result != 0 ? true : false;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return existCheck;
	}
	
	protected void doInsertionApprForVcTreeWIP(VcConfigMainCRUDVb vcMainVb, VcConfigMainTreeVb treeVb) throws Exception{
		
		if("M_QUERY".equalsIgnoreCase(treeVb.getDatabaseType())) {
			treeVb.setQueryId(treeVb.getDatabaseConnectivityDetails());
		}
		
		String query = " Insert into VC_TREE_WIP " + " (CATALOG_ID, TABLE_ID, TABLE_NAME, ALIAS_NAME, BASE_TABLE_FLAG, "
				+ " QUERY_ID, DATABASE_TYPE, DATABASE_CONNECTIVITY_DETAILS, SORT_TREE, "
				+ " RECORD_INDICATOR, VCT_STATUS_NT, VCT_STATUS, MAKER, "
				+ " VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, TABLE_SOURCE_TYPE, ACCESS_CONTROL_FLAG, ACCESS_CONTROL_SCRIPT) "
				+ " Values (?, ?, ?, ?, ?, " + " ?, ?, ?, ?, ?, " + " ?, ?, ?, ?, ?, "
				+ "  TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS'), TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS'), ?, ?, ?)";
		Object[] args = { treeVb.getCatalogId(), treeVb.getTableId(), treeVb.getTableName(), treeVb.getAliasName(),
				treeVb.getBaseTableFlag(), treeVb.getQueryId(), treeVb.getDatabaseType(),
				treeVb.getDatabaseConnectivityDetails(), treeVb.getSortTree(), vcMainVb.getRecordIndicator(),
				treeVb.getVctStatusNt(), treeVb.getVctStatus(), intCurrentUserId, intCurrentUserId,
				vcMainVb.getInternalStatus(), vcMainVb.getDateLastModified(), vcMainVb.getDateCreation(), treeVb.getTableSourceType(),
				treeVb.getAccessControlFlag() };
		
		getJdbcTemplate().update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				int argumentLength = args.length;
				PreparedStatement ps = connection.prepareStatement(query);
				for (int i = 1; i <= argumentLength; i++) {
					ps.setObject(i, args[i - 1]);
				}

				String clobData = ValidationUtil.isValid(treeVb.getAccessControlScript())
						? treeVb.getAccessControlScript()
						: "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
				return ps;
			}
		});
	}
	
	protected void doUpdateApprForVcTreeWIP(VcConfigMainCRUDVb vcMainVb, VcConfigMainTreeVb treeVb) throws Exception {
		
		if("M_QUERY".equalsIgnoreCase(treeVb.getDatabaseType())) {
			treeVb.setQueryId(treeVb.getDatabaseConnectivityDetails());
		}
		
		String query = " UPDATE VC_TREE_WIP SET ACCESS_CONTROL_SCRIPT = ?, TABLE_NAME = ?, ALIAS_NAME = ?, BASE_TABLE_FLAG = ?, "
				+ " QUERY_ID = ?, DATABASE_TYPE = ?, DATABASE_CONNECTIVITY_DETAILS = ?, SORT_TREE = ?, "
				+ " VCT_STATUS_NT = ?, VCT_STATUS = ?, MAKER = ?, "
				+ " VERIFIER = ?, DATE_LAST_MODIFIED = TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS'), TABLE_SOURCE_TYPE = ?, "
				+ " ACCESS_CONTROL_FLAG = ? "
				+ " WHERE CATALOG_ID = ? AND TABLE_ID = ? ";
		Object[] args = { treeVb.getTableName(), treeVb.getAliasName(), treeVb.getBaseTableFlag(), treeVb.getQueryId(),
				treeVb.getDatabaseType(), treeVb.getDatabaseConnectivityDetails(), treeVb.getSortTree(),
				treeVb.getVctStatusNt(), treeVb.getVctStatus(), intCurrentUserId, intCurrentUserId,
				vcMainVb.getDateLastModified(), treeVb.getTableSourceType(), 
				treeVb.getAccessControlFlag(), 
				treeVb.getCatalogId(), treeVb.getTableId() };
		getJdbcTemplate().update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				int psIndex = 0;
				PreparedStatement ps = connection.prepareStatement(query);

				String clobData = ValidationUtil.isValid(treeVb.getAccessControlScript()) ? treeVb.getAccessControlScript() : "";
				ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
				
				for (int i = 1; i <= args.length; i++) {
					ps.setObject(++psIndex, args[i - 1]);
				}
				return ps;
			}
		});
	}
	
	protected boolean checkColExistance(VcConfigMainColumnsVb vObj, int intStatus) {
		String tableName = (intStatus == 0) ? "VC_COLUMNS_SELFBI" : "VC_COLUMNS_WIP";
		boolean existCheck = false;
		String sql = "SELECT COUNT(1) FROM " + tableName + " WHERE CATALOG_ID = '" + vObj.getCatalogId()
				+ "' AND TABLE_ID = '" + vObj.getTableId() + "'" + "AND COL_ID = '" + vObj.getColId() + "'";
		try {
			int result = getJdbcTemplate().queryForObject(sql, Integer.class);
			existCheck = result != 0 ? true : false;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return existCheck;
	}

	protected void doInsertionApprForVcColumnsWIP(VcConfigMainCRUDVb vcMainVb, VcConfigMainColumnsVb columnsVb) throws Exception {
		String query = " Insert into VC_COLUMNS_WIP (CATALOG_ID, TABLE_ID, COL_ID, COL_NAME, ALIAS_NAME, "
				+ " SORT_COLUMN, COL_DISPLAY_TYPE_AT, COL_DISPLAY_TYPE, COL_ATTRIBUTE_TYPE_AT, COL_ATTRIBUTE_TYPE, "
				+ " COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, FORMAT_TYPE_NT, FORMAT_TYPE, MAG_ENABLE_FLAG, "
				+ " MAG_TYPE_NT, MAG_TYPE, MAG_SELECTION_TYPE_AT, MAG_SELECTION_TYPE, RECORD_INDICATOR_NT, "
				+ " RECORD_INDICATOR, VCC_STATUS_NT, VCC_STATUS, MAKER, VERIFIER, "
				+ " INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, MAG_DEFAULT, MAG_QUERY_ID, "
				+ " MAG_DISPLAY_COLUMN, MAG_USE_COLUMN, FOLDER_IDS, EXPERSSION_TEXT, COL_TYPE, MASKING_FLAG, COL_LENGTH,MASKING_SCRIPT) Values "
				+ " (?, ?, ?, ?, ?, " 
				+ " ?, ?, ?, ?, ?, " + " ?, ?, ?, ?, ?, " + " ?, ?, ?, ?, ?, "
				+ " ?, ?, ?, ?, ?, "
				+ " ?, TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS'), TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS'), ?, ?, "
				+ " ?, ?, ?, ?, ?, ?,?,?)";
		Object[] args = { columnsVb.getCatalogId(), columnsVb.getTableId(), columnsVb.getColId(), columnsVb.getColName(),
				columnsVb.getAliasName(), columnsVb.getSortColumn(), columnsVb.getColDisplayTypeAt(), columnsVb.getColDisplayType(),
				columnsVb.getColAttributeTypeAt(), columnsVb.getColAttributeType(), columnsVb.getColExperssionTypeAt(),
				columnsVb.getColExperssionType(), columnsVb.getFormatTypeNt(), columnsVb.getFormatType(), columnsVb.getMagEnableFlag(),
				columnsVb.getMagTypeNt(), columnsVb.getMagType(), columnsVb.getMagSelectionTypeAt(), columnsVb.getMagSelectionType(),
				vcMainVb.getRecordIndicatorNt(), vcMainVb.getRecordIndicator(), columnsVb.getVccStatusNt(), columnsVb.getVccStatus(),
				intCurrentUserId, intCurrentUserId, vcMainVb.getInternalStatus(), vcMainVb.getDateLastModified(),
				vcMainVb.getDateCreation(), columnsVb.getMagDefault(), columnsVb.getMagQueryId(), columnsVb.getMagDisplayColumn(),
				columnsVb.getMagUseColumn(), columnsVb.getFolderIds(), columnsVb.getExperssionText(), columnsVb.getColType(), columnsVb.getMaskingFlag(),columnsVb.getColLength()};
		
		getJdbcTemplate().update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				int argumentLength = args.length;
				PreparedStatement ps = connection.prepareStatement(query);
				for (int i = 1; i <= argumentLength; i++) {
					ps.setObject(i, args[i - 1]);
				}

				String clobData = ValidationUtil.isValid(columnsVb.getMaskingScript()) ? columnsVb.getMaskingScript() : "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
				return ps;
			}
		});
	}

	protected void doUpdateApprForVcColumnsWIP(VcConfigMainCRUDVb vcMainVb, VcConfigMainColumnsVb columnsVb) throws Exception{
		String query = " UPDATE VC_COLUMNS_WIP SET MASKING_SCRIPT = ?, COL_NAME = ?, ALIAS_NAME = ?, "
				+ " SORT_COLUMN = ?, COL_DISPLAY_TYPE_AT = ?, COL_DISPLAY_TYPE = ?, COL_ATTRIBUTE_TYPE_AT = ?, COL_ATTRIBUTE_TYPE = ?, "
				+ " COL_EXPERSSION_TYPE_AT = ?, COL_EXPERSSION_TYPE = ?, FORMAT_TYPE_NT = ?, FORMAT_TYPE = ?, MAG_ENABLE_FLAG = ?, "
				+ " MAG_TYPE_NT = ?, MAG_TYPE = ?, MAG_SELECTION_TYPE_AT = ?, MAG_SELECTION_TYPE = ?, "
				+ " VCC_STATUS = ?, MAKER = ?, VERIFIER = ?, "
				+ " DATE_LAST_MODIFIED = TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS'), MAG_DEFAULT = ?, MAG_QUERY_ID = ?, "
				+ " MAG_DISPLAY_COLUMN = ?, MAG_USE_COLUMN = ?, FOLDER_IDS = ?, EXPERSSION_TEXT = ?, COL_TYPE = ?, MASKING_FLAG = ?,COL_LENGTH = ? "
				+ " WHERE CATALOG_ID = ? AND TABLE_ID = ? AND COL_ID = ? ";
		Object[] args = { columnsVb.getColName(), columnsVb.getAliasName(), columnsVb.getSortColumn(),
				columnsVb.getColDisplayTypeAt(), columnsVb.getColDisplayType(), columnsVb.getColAttributeTypeAt(),
				columnsVb.getColAttributeType(), columnsVb.getColExperssionTypeAt(), columnsVb.getColExperssionType(),
				columnsVb.getFormatTypeNt(), columnsVb.getFormatType(), columnsVb.getMagEnableFlag(), columnsVb.getMagTypeNt(),
				columnsVb.getMagType(), columnsVb.getMagSelectionTypeAt(), columnsVb.getMagSelectionType(), columnsVb.getVccStatus(),
				intCurrentUserId, intCurrentUserId, vcMainVb.getDateLastModified(), columnsVb.getMagDefault(),
				columnsVb.getMagQueryId(), columnsVb.getMagDisplayColumn(), columnsVb.getMagUseColumn(), columnsVb.getFolderIds(),
				columnsVb.getExperssionText(), columnsVb.getColType(), columnsVb.getMaskingFlag(),columnsVb.getColLength(),columnsVb.getCatalogId(), columnsVb.getTableId(), columnsVb.getColId()};
		
		getJdbcTemplate().update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				int psIndex = 0;
				PreparedStatement ps = connection.prepareStatement(query);

				String clobData = ValidationUtil.isValid(columnsVb.getMaskingScript()) ? columnsVb.getMaskingScript() : "";
				ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
				
				for (int i = 1; i <= args.length; i++) {
					ps.setObject(++psIndex, args[i - 1]);
				}
				return ps;
			}
		});
	}
	
	private String returnTableAliasName(VcConfigMainCRUDVb vcMainVb, String tableId) {
		return vcMainVb.getAddModifyMetadata().stream().filter(treeVb -> tableId.equalsIgnoreCase(treeVb.getTableId()))
				.collect(Collectors.collectingAndThen(Collectors.toList(), treeVbList -> {
					return ValidationUtil.isValidList(treeVbList)?treeVbList.get(0).getAliasName():"";
				}));
	}
	
	public ExceptionCode doInsertUpdateRelationsWthRegExp(VcConfigMainCRUDVb vcMainVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		final String DOT = ".";
		final String EQUAL = " = ";
		final String AND = " AND ";
		final String JOIN_SYMBOL = " (+) ";
		try {
			for (VcConfigMainRelationVb relationVb : vcMainVb.getRelationAddModifyMetadata()) {
				StringBuffer relationStr = new StringBuffer("<relation>");
				StringBuffer ansiiJoinStr = new StringBuffer();
				StringBuffer standardJoinStr = new StringBuffer();
				String fromTblAliasName = returnTableAliasName(vcMainVb, relationVb.getFromTableId());
				String toTblAliasName = returnTableAliasName(vcMainVb, relationVb.getToTableId());
				int index = 1;
				boolean isAndRequired = true;
				if(5 != relationVb.getJoinType()) { //Cross Join Chk - No join string for cross join
					if(ValidationUtil.isValid(relationVb.getCustomJoinString())) {
						relationStr.append("<customjoin>"+relationVb.getCustomJoinString()+"</customjoin>");
					} else if(ValidationUtil.isValidList(relationVb.getRelationScriptParsed())) {
						relationStr.append("<columnmapping>");
						for (RelationMapVb relationMapVb:relationVb.getRelationScriptParsed()) {
							isAndRequired = (index==relationVb.getRelationScriptParsed().size())?false:true;
							relationStr.append("<column>");
							relationStr.append("<fcolumn>"+relationMapVb.getfColumn()+"</fcolumn>");
							relationStr.append("<tcolumn>"+relationMapVb.gettColumn()+"</tcolumn>");
							
							ansiiJoinStr.append(fromTblAliasName+DOT+relationMapVb.getfColumn());
							ansiiJoinStr.append(EQUAL);
							ansiiJoinStr.append(toTblAliasName+DOT+relationMapVb.gettColumn());
							
							if(1 == relationVb.getJoinType()) {//Inner Join
								standardJoinStr.append(fromTblAliasName+DOT+relationMapVb.getfColumn());
								standardJoinStr.append(EQUAL);
								standardJoinStr.append(toTblAliasName+DOT+relationMapVb.gettColumn());
							} else if(2 == relationVb.getJoinType()) {//Left Join
								standardJoinStr.append(fromTblAliasName+DOT+relationMapVb.getfColumn());
								standardJoinStr.append(EQUAL);
								standardJoinStr.append(toTblAliasName+DOT+relationMapVb.gettColumn()+JOIN_SYMBOL);
							} else if(3 == relationVb.getJoinType()) {//Right Join
								standardJoinStr.append(fromTblAliasName+DOT+relationMapVb.getfColumn()+JOIN_SYMBOL);
								standardJoinStr.append(EQUAL);
								standardJoinStr.append(toTblAliasName+DOT+relationMapVb.gettColumn());
							}
							
							if(isAndRequired) {
								ansiiJoinStr.append(AND);
								if(4 != relationVb.getJoinType())//Outer Join(Not possible with standard join syntax)
									standardJoinStr.append(AND);
							}
							relationStr.append("</column>");
							index++;
						}
						relationStr.append("</columnmapping>");
						relationStr.append("<customjoin></customjoin>");
						relationStr.append("<ansii_joinstring>"+ansiiJoinStr+"</ansii_joinstring>");
						relationStr.append("<std_joinstring>"+standardJoinStr+"</std_joinstring>");
					}
				}
				relationStr.append("</relation>");
				relationVb.setRelationScript(String.valueOf(relationStr));
				
				if (!checkRelationExistance(relationVb, 1)) {
					doInsertionApprVcRelWIP(vcMainVb, relationVb);
				} else {
					doUpdateApprVcRelWIP(vcMainVb, relationVb);
				}
			}
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (UncategorizedSQLException uSQLEcxception) {
			uSQLEcxception.printStackTrace();
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}
	
	protected boolean checkRelationExistance(VcConfigMainRelationVb vObj, int intStatus) {
		String tableName = (intStatus == 0) ? "VC_RELATIONS_SELFBI" : "VC_RELATIONS_WIP";
		boolean existCheck = false;
		String sql = "SELECT COUNT(1) FROM " + tableName + " WHERE CATALOG_ID = '" + vObj.getCatalogId() + "' "
				+ "AND FROM_TABLE_ID = '" + vObj.getFromTableId() + "' AND TO_TABLE_ID = '" + vObj.getToTableId() + "'";
		try {
			int result = getJdbcTemplate().queryForObject(sql, Integer.class);
			existCheck = result != 0 ? true : false;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return existCheck;
	}
	
	protected void doInsertionApprVcRelWIP(VcConfigMainCRUDVb vcMainVb, VcConfigMainRelationVb relationVb) throws Exception {
		String query = "INSERT INTO VC_RELATIONS_WIP (CATALOG_ID, FROM_TABLE_ID, TO_TABLE_ID, JOIN_TYPE, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, VCR_STATUS_NT, VCR_STATUS, MAKER, "
				+ "VERIFIER, DATE_LAST_MODIFIED, DATE_CREATION, FILTER_CONDITION, RELATION_SCRIPT) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS'), TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS'), ?, ?)";
		Object[] args = { relationVb.getCatalogId(), relationVb.getFromTableId(), relationVb.getToTableId(),
				relationVb.getJoinType(), vcMainVb.getRecordIndicatorNt(), vcMainVb.getRecordIndicator(),
				relationVb.getVcrStatusNt(), relationVb.getVcrStatus(), intCurrentUserId, intCurrentUserId, vcMainVb.getDateLastModified(), vcMainVb.getDateCreation()};
		
		getJdbcTemplate().update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				int argumentLength = args.length;
				PreparedStatement ps = connection.prepareStatement(query);
				for (int i = 1; i <= argumentLength; i++) {
					ps.setObject(i, args[i - 1]);
				}

				String clobData = ValidationUtil.isValid(relationVb.getFilterCondition()) ? relationVb.getFilterCondition() : "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
							
				clobData = ValidationUtil.isValid(relationVb.getRelationScript()) ? relationVb.getRelationScript() : "";
				ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
				return ps;
			}
		});
	}
	
	protected void doUpdateApprVcRelWIP(VcConfigMainCRUDVb vcMainVb, VcConfigMainRelationVb relationVb) throws Exception {
		String query = " UPDATE VC_RELATIONS_WIP SET RELATION_SCRIPT = ?, FILTER_CONDITION = ?, JOIN_TYPE = ?, "
				+ "VCR_STATUS = ?, MAKER = ?, VERIFIER = ?, DATE_LAST_MODIFIED = TO_DATE(?, 'DD-MM-YYYY HH24:MI:SS') "
				+ "WHERE CATALOG_ID = ? AND FROM_TABLE_ID = ? AND TO_TABLE_ID = ? ";
		Object[] args = { relationVb.getJoinType(), relationVb.getVcrStatus(), intCurrentUserId, intCurrentUserId, vcMainVb.getDateLastModified(), 
				relationVb.getCatalogId(), relationVb.getFromTableId(), relationVb.getToTableId() };
		
		
		getJdbcTemplate().update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
				int psIndex = 0;
				PreparedStatement ps = connection.prepareStatement(query);

				String clobData = ValidationUtil.isValid(relationVb.getRelationScript()) ? relationVb.getRelationScript() : "";
				ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
				
				clobData = ValidationUtil.isValid(relationVb.getFilterCondition()) ? relationVb.getFilterCondition() : ""; // FILTER_CONDITION
				ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

				for (int i = 1; i <= args.length; i++) {
					ps.setObject(++psIndex, args[i - 1]);
				}
				return ps;
			}
		});
	}

	public int doInsertRecordForCatalogAccess(VcConfigMainLODWrapperVb wrapperVb, boolean isMain) throws Exception {
		VcConfigMainVb mainObject = wrapperVb.getMainModel();
		String tableName = "CATALOG_ACCESS_SelfBI_WIP";
		if (isMain)
			tableName = "CATALOG_ACCESS_SelfBI";

		String sql = "DELETE FROM " + tableName + " WHERE CATALOG_ID = '" + mainObject.getCatalogId() + "'";

		try {
			getJdbcTemplate().update(sql);
		} catch (Exception e) {
		}

		try {
			if(ValidationUtil.isValidList(wrapperVb.getLodProfileList())) {
				sql = "INSERT INTO " + tableName
						+ " (CATALOG_ID,  USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT, USER_PROFILE, CATALOG_STATUS,  "
						+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), To_Date(?, 'DD-MM-YYYY HH24:MI:SS'))";
				for (LevelOfDisplayVb vObject : wrapperVb.getLodProfileList()) {
					Object[] args = { mainObject.getCatalogId(), vObject.getUserGroupAt(), vObject.getUserGroup(),
							vObject.getUserProfileAt(), vObject.getUserProfile(), "0", mainObject.getRecordIndicatorNt(),
							mainObject.getRecordIndicator(), mainObject.getMaker(), mainObject.getVerifier(),
							mainObject.getInternalStatus(), mainObject.getDateLastModified(),
							mainObject.getDateCreation() };
					getJdbcTemplate().update(sql, args);
				}
			}
			
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			throw e;
		}
	}
	
	public VcConfigMainLODWrapperVb getRecordForCatalogLOD(VcConfigMainLODWrapperVb wrapperVb) {
		VcConfigMainVb mainObject = wrapperVb.getMainModel();
		try {
			String sql = "SELECT USER_GROUP, USER_PROFILE FROM CATALOG_ACCESS_SelfBI " +
					" WHERE UPPER(CATALOG_ID) = UPPER('"+mainObject.getCatalogId()+"') " +
					" ORDER BY USER_GROUP, USER_PROFILE";
			List<LevelOfDisplayVb> profileList = getJdbcTemplate().query(sql, new RowMapper<LevelOfDisplayVb>() {
				@Override
				public LevelOfDisplayVb mapRow(ResultSet rs, int rowNum) throws SQLException {
					return new LevelOfDisplayVb(rs.getString("USER_GROUP"), rs.getString("USER_PROFILE"), null);
				}
			});
			
			wrapperVb.setLodProfileList(ValidationUtil.isValidList(profileList)?profileList:null);
			return wrapperVb;
		}catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<DesignAnalysisVb> ValidatingCatalogData(String catalogId) {
		return getJdbcTemplate().query("select VCQD_QUERY_ID,CATALOG_ID from VCQD_QUERIES where CATALOG_ID = '"+catalogId+"' ", new RowMapper<DesignAnalysisVb>() {
			@Override
			public DesignAnalysisVb mapRow(ResultSet rs, int rowNum) throws SQLException {
				DesignAnalysisVb desingnVb = new DesignAnalysisVb();
				desingnVb.setVcqdQueryId(rs.getString("VCQD_QUERY_ID"));
				desingnVb.setCatalogId(rs.getString("CATALOG_ID"));
				return desingnVb;
			}
		});
	}

}
