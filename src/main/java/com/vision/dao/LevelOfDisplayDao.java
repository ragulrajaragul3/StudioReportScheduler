package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonVb;
import com.vision.vb.LevelOfDisplayUserVb;
import com.vision.vb.LevelOfDisplayVb;
 
@Component
public class LevelOfDisplayDao<E extends CommonVb> extends AbstractCommonDao {
	
	public int doInsertRecordForCatalogAccess(List<LevelOfDisplayVb> vObjList, boolean isMain, E dynamicObject) {

		String tableName = "CATALOG_ACCESS_SelfBI_WIP";
		if (isMain)
			tableName = "CATALOG_ACCESS_SelfBI";

		String sql = "DELETE FROM " + tableName + " WHERE CATALOG_ID = '" + dynamicObject.getLocale() + "'";

		try {
			getJdbcTemplate().update(sql);
		} catch (Exception e) {
		}

		sql = "INSERT INTO " + tableName
				+ " (CATALOG_ID, USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT, USER_PROFILE, CATALOG_STATUS,  "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), To_Date(?, 'DD-MM-YYYY HH24:MI:SS'))";
		try {
			for (LevelOfDisplayVb vObject : vObjList) {
				Object[] args = { dynamicObject.getLocale(), vObject.getUserGroupAt(), vObject.getUserGroup(),
						vObject.getUserProfileAt(), vObject.getUserProfile(), "0", dynamicObject.getRecordIndicatorNt(),
						dynamicObject.getRecordIndicator(), dynamicObject.getMaker(), dynamicObject.getVerifier(),
						dynamicObject.getInternalStatus(), dynamicObject.getDateLastModified(),
						dynamicObject.getDateCreation() };
				getJdbcTemplate().update(sql, args);
			}
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Insert Error in CATALOG_ACCESS_SelfBI_WIP: " + e.getMessage());
			return Constants.ERRONEOUS_OPERATION;
		}
	}

	public int doInsertRecordForDataConnectorLOD(List<LevelOfDisplayVb> vObjList, boolean isMain, E dynamicObject) {

		String tableName = "DATACONNECTOR_LOD_WIP";
		if (isMain)
			tableName = "DATACONNECTOR_LOD";

		String sql = "DELETE FROM " + tableName + " WHERE VARIABLE_NAME = '" + dynamicObject.getLocale() + "'";

		try {
			getJdbcTemplate().update(sql);
		} catch (Exception e) {
		}

		sql = "INSERT INTO " + tableName
				+ " (VARIABLE_NAME, USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT, USER_PROFILE, DATACONNECTOR_STATUS,   "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), To_Date(?, 'DD-MM-YYYY HH24:MI:SS'))";
		try {
			for (LevelOfDisplayVb vObject : vObjList) {
				Object[] args = { dynamicObject.getLocale(), vObject.getUserGroupAt(), vObject.getUserGroup(),
						vObject.getUserProfileAt(), vObject.getUserProfile(), "0", dynamicObject.getRecordIndicatorNt(),
						dynamicObject.getRecordIndicator(), dynamicObject.getMaker(), dynamicObject.getVerifier(),
						dynamicObject.getInternalStatus(), dynamicObject.getDateLastModified(),
						dynamicObject.getDateCreation() };
				getJdbcTemplate().update(sql, args);
			}
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Insert Error in DATACONNECTOR_LOD_WIP: " + e.getMessage());
			return Constants.ERRONEOUS_OPERATION;
		}
	}

	public int doInsertRecordForManualQueries(List<LevelOfDisplayVb> vObjList, boolean isMain, E dynamicObject) {

		String tableName = "VCQD_QUERIES_ACCESS";
		if (isMain)
			tableName = "VCQD_QUERIES_ACCESS";

		String sql = "DELETE FROM " + tableName + " WHERE VCQD_QUERY_ID = '" + dynamicObject.getLocale() + "'";

		try {
			getJdbcTemplate().update(sql);
		} catch (Exception e) {
		}

		sql = "INSERT INTO " + tableName
				+ " (VCQD_QUERY_ID, USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT, USER_PROFILE, VCQDA_STATUS, QUERY_TYPE,   "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), To_Date(?, 'DD-MM-YYYY HH24:MI:SS'))";
		try {
			for (LevelOfDisplayVb vObject : vObjList) {
				Object[] args = { dynamicObject.getLocale(), vObject.getUserGroupAt(), vObject.getUserGroup(),
						vObject.getUserProfileAt(), vObject.getUserProfile(), "0", "M",
						dynamicObject.getRecordIndicatorNt(), dynamicObject.getRecordIndicator(),
						dynamicObject.getMaker(), dynamicObject.getVerifier(), dynamicObject.getInternalStatus(),
						dynamicObject.getDateLastModified(), dynamicObject.getDateCreation() };
				getJdbcTemplate().update(sql, args);
			}
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Insert Error in VCQD_QUERIES_ACCESS: " + e.getMessage());
			return Constants.ERRONEOUS_OPERATION;
		}
	}

	public int doInsertRecordForDesignQueries(List<LevelOfDisplayVb> vObjList, boolean isMain, E dynamicObject) {

		String tableName = "VCQD_QUERIES_ACCESS";
		if (isMain)
			tableName = "VCQD_QUERIES_ACCESS";

		String sql = "DELETE FROM " + tableName + " WHERE VCQD_QUERY_ID = '" + dynamicObject.getLocale() + "'";

		try {
			getJdbcTemplate().update(sql);
		} catch (Exception e) {
		}

		sql = "INSERT INTO " + tableName
				+ " (VCQD_QUERY_ID, USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT, USER_PROFILE, VCQDA_STATUS, QUERY_TYPE,   "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, To_Date(?, 'DD-MM-YYYY HH24:MI:SS'), To_Date(?, 'DD-MM-YYYY HH24:MI:SS'))";
		try {
			for (LevelOfDisplayVb vObject : vObjList) {
				Object[] args = { dynamicObject.getLocale(), vObject.getUserGroupAt(), vObject.getUserGroup(),
						vObject.getUserProfileAt(), vObject.getUserProfile(), "0", "D",
						dynamicObject.getRecordIndicatorNt(), dynamicObject.getRecordIndicator(),
						dynamicObject.getMaker(), dynamicObject.getVerifier(), dynamicObject.getInternalStatus(),
						dynamicObject.getDateLastModified(), dynamicObject.getDateCreation() };
				getJdbcTemplate().update(sql, args);
			}
			return Constants.SUCCESSFUL_OPERATION;
		} catch (Exception e) {
			e.printStackTrace();
			strErrorDesc = e.getMessage();
			logger.error("Insert Error in VCQD_QUERIES_ACCESS: " + e.getMessage());
			return Constants.ERRONEOUS_OPERATION;
		}
	}
	
	public List getUserListExcludingUserGrpProf(String usrGroupAndProfile) {
		StringBuffer sql = new StringBuffer("SELECT VISION_ID, USER_NAME, USER_LOGIN_ID, USER_GROUP, USER_PROFILE " + 
				"    FROM SSBI_USER_VIEW ");
		if(ValidationUtil.isValid(usrGroupAndProfile)) {
			sql.append(" WHERE USER_GROUP || '-' || USER_PROFILE NOT IN ("+usrGroupAndProfile+") AND USER_STATUS = 0 ");
		} else {
			sql.append(" WHERE USER_STATUS = 0 ");
		}
		sql.append(" ORDER BY VISION_ID, USER_NAME ");
		try {
			return getJdbcTemplate().query(String.valueOf(sql), new RowMapper<LevelOfDisplayUserVb>() {
				@Override
				public LevelOfDisplayUserVb mapRow(ResultSet rs, int rowNum) throws SQLException {
					return new LevelOfDisplayUserVb(rs.getInt("VISION_ID"),
							rs.getString("USER_NAME"), rs.getString("USER_LOGIN_ID"), rs.getString("USER_GROUP"),
							rs.getString("USER_PROFILE"));
				}
				
			});
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
}
