package com.vision.wb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vision.dao.AbstractDao;
import com.vision.dao.CommonDao;
import com.vision.dao.DesignAnalysisDao;
import com.vision.dao.VcConfigMainDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.DeepCopy;
import com.vision.util.ValidationUtil;
import com.vision.vb.DSConnectorVb;
import com.vision.vb.DesignAnalysisVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.VcConfigMainCRUDVb;
import com.vision.vb.VcConfigMainColumnsVb;
import com.vision.vb.VcConfigMainLODWrapperVb;
import com.vision.vb.VcConfigMainRelationVb;
import com.vision.vb.VcConfigMainTreeVb;
import com.vision.vb.VcConfigMainVb;
import com.vision.vb.VcMainDataSourceMetaDataVb;


@Component
public class VcConfigMainWb extends AbstractWorkerBean<VcConfigMainVb>{
	
	@Autowired
	private VcConfigMainDao vcConfigMainDao;
	
	@Autowired
	private DesignAnalysisDao designAnalysisDao;
	
	@Autowired
	private CommonDao commonDao;

	@Autowired
	private Environment env;
	
	@Override
	protected void setVerifReqDeleteType(VcConfigMainVb vObject){
		vObject.setStaticDelete(false);
		vObject.setVerificationRequired(false);
	}
	@Override
	protected void setAtNtValues(VcConfigMainVb vObject){
		vObject.setRecordIndicatorNt(7);
		vObject.setRecordIndicator(0);
		vObject.setVcStatusNt(2000);
	}
	
	@Override
	protected AbstractDao<VcConfigMainVb> getScreenDao() {
		return vcConfigMainDao;
	}
	
	public ExceptionCode insertRecord(VcConfigMainVb vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<VcConfigMainVb> deepCopy = new DeepCopy<VcConfigMainVb>();
		VcConfigMainVb clonedObject = null;
		try{
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			doFormateData(vObject);
			exceptionCode = doValidate(vObject);
			if(exceptionCode!=null && exceptionCode.getErrorMsg()!=""){
				return exceptionCode;
			}
			exceptionCode = vcConfigMainDao.doInsertApprRecordforVcCatalogWIP(vObject);
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Insert Exception In Vision Catalog" + rex.getCode().getErrorMsg());
			logger.error( ((vObject==null)? "vObject is Null":vObject.toString()));
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	
	public ExceptionCode modifyRecord(VcConfigMainVb vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<VcConfigMainVb> deepCopy = new DeepCopy<VcConfigMainVb>();
		VcConfigMainVb clonedObject = null;
		try{
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			exceptionCode = vcConfigMainDao.doUpdateRecordVcCatalogIntoWIP(vObject);
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Modify Exception In Vision Catalog" + rex.getCode().getErrorMsg());
			logger.error( ((vObject==null)? "vObject is Null":vObject.toString()));
			rex.printStackTrace();
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	
	public ExceptionCode deleteRecord(VcConfigMainVb vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<VcConfigMainVb> deepCopy = new DeepCopy<VcConfigMainVb>();
		VcConfigMainVb clonedObject = null;
		try{
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			exceptionCode = vcConfigMainDao.doDeleteRecordVcCatalogFromPend(vObject);
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Delete Exception In Vision Catalog" + rex.getCode().getErrorMsg());
			logger.error( ((vObject==null)? "vObject is Null":vObject.toString()));
			rex.printStackTrace();
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	
	public List<VcConfigMainVb> getQueryPopupVcConfigMain(VcConfigMainVb queryPopupObj){
		List<VcConfigMainVb> arrListLocal = new ArrayList<VcConfigMainVb>();
		try{
			setVerifReqDeleteType(queryPopupObj);
			doFormateDataForQuery(queryPopupObj);
			List<VcConfigMainVb> arrListResult = vcConfigMainDao.getQueryPopupResultsFromVisionCatalog(queryPopupObj);
			if(arrListResult == null){
				arrListLocal.add(queryPopupObj);
			}else{
				arrListLocal.addAll(arrListResult);
			}
			return arrListLocal;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception in getting the Catalog QueryPopup results.", ex);
			return null;
		}
	}
	
	public String getDbScript(String getMacroVar) throws DataAccessException{
		try {
			return commonDao.getScriptValue(getMacroVar);
		}catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	
	public ExceptionCode formListForTableTree(String macroVar, String dbScript){
		ExceptionCode exceptionCode = new ExceptionCode();
		ArrayList<ArrayList<VcMainDataSourceMetaDataVb>> arrayListMain = null;
		ArrayList<VcMainDataSourceMetaDataVb> arrayListTree = null;
		ArrayList<VcMainDataSourceMetaDataVb> arrayListView = null;
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		exceptionCode = CommonUtils.getConnection(dbScript);
		if(exceptionCode.getErrorCode()==Constants.SUCCESSFUL_OPERATION){
			con = (Connection) exceptionCode.getResponse();
		}else{
			return exceptionCode;
		}
		String dataBaseType = CommonUtils.getValue(dbScript, "DATABASE_TYPE");
		String dbSetParam1 =CommonUtils.getValue(dbScript, "DB_SET_PARAM1");
		String dbSetParam2 =CommonUtils.getValue(dbScript, "DB_SET_PARAM2");
		String dbSetParam3 =CommonUtils.getValue(dbScript, "DB_SET_PARAM3");
		try{
			String viewQuery = "";
			String tableQuery = "";
			arrayListTree = new ArrayList<VcMainDataSourceMetaDataVb>();
			arrayListView = new ArrayList<VcMainDataSourceMetaDataVb>();
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			if(ValidationUtil.isValid(dbSetParam1))
				stmt.executeUpdate(dbSetParam1);
			if(ValidationUtil.isValid(dbSetParam2))
				stmt.executeUpdate(dbSetParam2);
			if(ValidationUtil.isValid(dbSetParam3))
				stmt.executeUpdate(dbSetParam3);
			NumSubTabVb vObjNum = vcConfigMainDao.getActiveNumTab(2004, dataBaseType);
			Map<String, String> dataTypeMap = vcConfigMainDao.getDataTypeMap(dataBaseType);
			if("ORACLE".equalsIgnoreCase(dataBaseType)){
				viewQuery = "SELECT T1.VIEW_NAME, T2.COLUMN_NAME, T2.DATA_TYPE FROM USER_VIEWS T1,USER_TAB_COLUMNS T2 WHERE T1.VIEW_NAME=T2.TABLE_NAME ORDER BY T1.VIEW_NAME, T2.COLUMN_ID";
				tableQuery = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLUMNS where TABLE_NAME not in  (SELECT T1.VIEW_NAME FROM USER_VIEWS T1) "+ 
						" ORDER BY TABLE_NAME, COLUMN_ID";
			}else if("MYSQL".equalsIgnoreCase(dataBaseType)){
				String dbSchema =CommonUtils.getValue(dbScript, "DB_SCHEMA");
				if(ValidationUtil.isValid(dbSchema)){
					viewQuery = vcConfigMainDao.getQueryForTableAndView("VIEW_QUERY_FOR_MYSQL");
					viewQuery = viewQuery.replaceAll("?", dbSchema);
					tableQuery = vcConfigMainDao.getQueryForTableAndView("TABLE_QUERY_FOR_MYSQL");
					tableQuery = tableQuery.replaceAll("?", dbSchema);
				}else{
					exceptionCode.setErrorCode(9999);
					exceptionCode.setErrorMsg("Schema Maintenance");
					return exceptionCode;
				}
			}else if("MSSQL".equalsIgnoreCase(dataBaseType)){
				viewQuery = vcConfigMainDao.getQueryForTableAndView("VIEW_QUERY_FOR_MSSQL");
				tableQuery = vcConfigMainDao.getQueryForTableAndView("TABLE_QUERY_FOR_MSSQL");
			}
			if(ValidationUtil.isValid(viewQuery) && ValidationUtil.isValid(tableQuery)){
				rs = stmt.executeQuery(viewQuery);
				VcMainDataSourceMetaDataVb vObjViewDSMD = new VcMainDataSourceMetaDataVb();
				String viewName = "";
				List<VcMainDataSourceMetaDataVb> colListViewVbDSMD = new ArrayList<VcMainDataSourceMetaDataVb>();
				boolean first = true;
				while(rs.next()){
					if(first){
						viewName = rs.getString("VIEW_NAME");
						first = false;
					}
					VcMainDataSourceMetaDataVb vObjColDSMD = new VcMainDataSourceMetaDataVb();
					if(viewName.equalsIgnoreCase(rs.getString("VIEW_NAME"))){
						vObjColDSMD.setTableName(rs.getString("VIEW_NAME"));
						vObjColDSMD.setColumnName(rs.getString("COLUMN_NAME"));
						if(dataTypeMap!=null && ValidationUtil.isValid(dataTypeMap.get(rs.getString("DATA_TYPE")))){
							vObjColDSMD.setColumnType(dataTypeMap.get(rs.getString("DATA_TYPE")));
						}else{
							vObjColDSMD.setColumnType("Y");
						}
						colListViewVbDSMD.add(vObjColDSMD);
					}else{
						vObjViewDSMD.setTableName(viewName);
						vObjViewDSMD.setDatabaseConnectivityDetails(macroVar);
						vObjViewDSMD.setChildren(colListViewVbDSMD);
						arrayListView.add(vObjViewDSMD);
						vObjViewDSMD = new VcMainDataSourceMetaDataVb();
						
						colListViewVbDSMD = new ArrayList<VcMainDataSourceMetaDataVb>();
						viewName = rs.getString("VIEW_NAME");
						vObjColDSMD.setTableName(rs.getString("VIEW_NAME"));
						vObjColDSMD.setColumnName(rs.getString("COLUMN_NAME"));
						if(dataTypeMap!=null && ValidationUtil.isValid(dataTypeMap.get(rs.getString("DATA_TYPE")))){
							vObjColDSMD.setColumnType(dataTypeMap.get(rs.getString("DATA_TYPE")));
						}else{
							vObjColDSMD.setColumnType("Y");
						}
						colListViewVbDSMD.add(vObjColDSMD);
					}
					if (rs.isLast()) {
						vObjViewDSMD.setTableName(viewName);
						vObjViewDSMD.setDatabaseConnectivityDetails(macroVar);
						vObjViewDSMD.setChildren(colListViewVbDSMD);
						arrayListView.add(vObjViewDSMD);
						vObjViewDSMD = new VcMainDataSourceMetaDataVb();
					}
				}
				rs = stmt.executeQuery(tableQuery);
				VcMainDataSourceMetaDataVb vObjTableDSMD = new VcMainDataSourceMetaDataVb();
				String tableName = "";
				List<VcMainDataSourceMetaDataVb> colListTableVbDSMD = new ArrayList<VcMainDataSourceMetaDataVb>();
				first = true;
				while(rs.next()){
					if(first){
						tableName = rs.getString("TABLE_NAME");
						first = false;
					}
					VcMainDataSourceMetaDataVb vObjColDSMD = new VcMainDataSourceMetaDataVb();
					if(tableName.equalsIgnoreCase(rs.getString("TABLE_NAME"))){
						vObjColDSMD.setTableName(rs.getString("TABLE_NAME"));
						vObjColDSMD.setColumnName(rs.getString("COLUMN_NAME"));
						if(dataTypeMap!=null && ValidationUtil.isValid(dataTypeMap.get(rs.getString("DATA_TYPE")))){
							vObjColDSMD.setColumnType(dataTypeMap.get(rs.getString("DATA_TYPE")));
						}else{
							vObjColDSMD.setColumnType("Y");
						}
						colListTableVbDSMD.add(vObjColDSMD);
					}else{
						vObjTableDSMD.setTableName(tableName);
						vObjTableDSMD.setDatabaseConnectivityDetails(macroVar);
						vObjTableDSMD.setChildren(colListTableVbDSMD);
						arrayListTree.add(vObjTableDSMD);
						vObjTableDSMD = new VcMainDataSourceMetaDataVb();
						
						colListTableVbDSMD = new ArrayList<VcMainDataSourceMetaDataVb>();
						tableName = rs.getString("TABLE_NAME");
						vObjColDSMD.setTableName(rs.getString("TABLE_NAME"));
						vObjColDSMD.setColumnName(rs.getString("COLUMN_NAME"));
						if(dataTypeMap!=null && ValidationUtil.isValid(dataTypeMap.get(rs.getString("DATA_TYPE")))){
							vObjColDSMD.setColumnType(dataTypeMap.get(rs.getString("DATA_TYPE")));
						}else{
							vObjColDSMD.setColumnType("Y");
						}
						colListTableVbDSMD.add(vObjColDSMD);
					}
					if (rs.isLast()) {
						vObjTableDSMD.setTableName(tableName);
						vObjTableDSMD.setDatabaseConnectivityDetails(macroVar);
						vObjTableDSMD.setChildren(colListTableVbDSMD);
						arrayListTree.add(vObjTableDSMD);
						vObjTableDSMD = new VcMainDataSourceMetaDataVb();
					}
				}
				arrayListMain = new ArrayList<ArrayList<VcMainDataSourceMetaDataVb>>();
				arrayListTree.get(0).setJsonFormationFor("Table");
				arrayListView.get(0).setJsonFormationFor("View");
				arrayListMain.add(arrayListTree);
				arrayListMain.add(arrayListView);
			}else{
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Current database type being used is not yet supported. Contact system admin");
				return exceptionCode;
			}
			
		}catch (SQLException e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}catch(Exception e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}finally{
			try{
				if(rs!=null)
					rs.close();
				if(stmt!=null)
					stmt.close();
				if(con!=null)
					con.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(arrayListMain);
		return exceptionCode;
	}
	
	public ExceptionCode formConnectorTables(String macroVar, String dbScript, List<VcConfigMainTreeVb> treeVbList){
		
		String excludeTableListStr = null;
		if(ValidationUtil.isValidList(treeVbList)) {
			StringBuffer excludeTableList = new StringBuffer("(");
			final String QUOTE = "'";
			final String COMMA = ",";
			for(VcConfigMainTreeVb treeVb : treeVbList) {
				excludeTableList.append(QUOTE + treeVb.getTableName().toUpperCase() + QUOTE + COMMA);
			}
			excludeTableList = new StringBuffer(new String(excludeTableList.substring(0, (excludeTableList.length()-1))));
			excludeTableList.append(")");
			excludeTableListStr = String.valueOf(excludeTableList);
		}
		
		ExceptionCode exceptionCode = new ExceptionCode();
		JSONArray result = null;
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		exceptionCode = CommonUtils.getConnection(dbScript);
		DSConnectorVb connectorVb = designAnalysisDao.getQueriesForDatasource();
		if(exceptionCode.getErrorCode()==Constants.SUCCESSFUL_OPERATION){
			con = (Connection) exceptionCode.getResponse();
		}else{
			return exceptionCode;
		}
		String dataBaseType = CommonUtils.getValue(dbScript, "DATABASE_TYPE");
		String dbSetParam1 =CommonUtils.getValue(dbScript, "DB_SET_PARAM1");
		String dbSetParam2 =CommonUtils.getValue(dbScript, "DB_SET_PARAM2");
		String dbSetParam3 =CommonUtils.getValue(dbScript, "DB_SET_PARAM3");
		String parentDbUserName = env.getProperty("spring.datasource.username");
		try{
			String tableQuery = "";
//			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			stmt = con.createStatement();
			if(ValidationUtil.isValid(dbSetParam1))
				stmt.executeUpdate(dbSetParam1);
			if(ValidationUtil.isValid(dbSetParam2))
				stmt.executeUpdate(dbSetParam2);
			if(ValidationUtil.isValid(dbSetParam3))
				stmt.executeUpdate(dbSetParam3);
			if("ORACLE".equalsIgnoreCase(dataBaseType)){
				/*tableQuery = "SELECT TABLE_NAME FROM USER_TABLES where TABLE_NAME not in (SELECT T1.VIEW_NAME FROM USER_VIEWS T1) " +
						(ValidationUtil.isValid(excludeTableListStr)?" AND TABLE_NAME not in "+excludeTableListStr:"")+
						" ORDER BY TABLE_NAME";*/
				if(ValidationUtil.isValid(excludeTableListStr)){
					if(ValidationUtil.isValid(connectorVb.getTableExcludeQuery())) { 
						tableQuery=connectorVb.getTableExcludeQuery().replaceAll("#EXCLUDE_TABLES#", excludeTableListStr);
					    tableQuery=tableQuery.replaceAll("#PARENT_DB_USER#", parentDbUserName);
				}
				else
						tableQuery = " SELECT TABLE_NAME FROM ALL_TABLES WHERE TABLE_NAME NOT IN "+excludeTableListStr+" and UPPER(OWNER)= UPPER('"+parentDbUserName+"') "
								+ " UNION "
								+ " SELECT VIEW_NAME TABLE_NAME FROM ALL_VIEWS WHERE VIEW_NAME NOT IN "+excludeTableListStr + " and UPPER(OWNER)= UPPER('"+parentDbUserName+"') ";
				} else {
					if(ValidationUtil.isValid(connectorVb.getTableQuery())) 
						tableQuery=connectorVb.getTableQuery().replaceAll("#PARENT_DB_USER#", parentDbUserName);
					else	
						tableQuery = " SELECT TABLE_NAME FROM ALL_TABLES WHERE  UPPER(OWNER)= UPPER('"+parentDbUserName+"') "
								+ " UNION "
								+ " SELECT VIEW_NAME TABLE_NAME FROM ALL_VIEWS WHERE UPPER(OWNER)= UPPER('"+parentDbUserName+"') ";
				}
				
			}else if("MYSQL".equalsIgnoreCase(dataBaseType)){
				String dbSchema =CommonUtils.getValue(dbScript, "DB_SCHEMA");
				if(ValidationUtil.isValid(dbSchema)) {
					tableQuery = vcConfigMainDao.getQueryForTableAndView("TABLE_QUERY_FOR_MYSQL");
					tableQuery = tableQuery.replaceAll("?", dbSchema);
				} else {
					exceptionCode.setErrorCode(9999);
					exceptionCode.setErrorMsg("Schema Maintenance");
					return exceptionCode;
				}
			}else if("MSSQL".equalsIgnoreCase(dataBaseType)){
//				tableQuery = vcConfigMainDao.getQueryForTableAndView("TABLE_QUERY_FOR_MSSQL");
				
				if(ValidationUtil.isValid(excludeTableListStr)){
					if(ValidationUtil.isValid(connectorVb.getTableExcludeQuery())) 
						tableQuery=connectorVb.getTableExcludeQuery().replaceAll("#EXCLUDE_TABLES#", excludeTableListStr);
					else
					tableQuery = "SELECT T2.NAME AS TABLE_NAME FROM SYS.TABLES T2 WHERE T2.NAME NOT IN "+excludeTableListStr
							+ " UNION "
							+ " SELECT T2.NAME AS TABLE_NAME FROM SYS.VIEWS T2 WHERE T2.NAME NOT IN "+excludeTableListStr 
							+ " ORDER BY TABLE_NAME";
				} else {
					if(ValidationUtil.isValid(connectorVb.getTableQuery())) 
						tableQuery=connectorVb.getTableQuery();
					else	
					tableQuery = "SELECT T2.NAME AS TABLE_NAME FROM SYS.TABLES T2 "
							+ " UNION "
							+ " SELECT T2.NAME AS TABLE_NAME FROM SYS.VIEWS T2 " 
							+ " ORDER BY TABLE_NAME";
				}
			}
			if(ValidationUtil.isValid(tableQuery)){
				rs = stmt.executeQuery(tableQuery);
				int i=0;
				result = new JSONArray();
				while(rs.next()){
					JSONObject obj =new JSONObject();
					obj.put("name",rs.getString("TABLE_NAME"));
					obj.put("id",i);
					result.add( obj );
					i++;
				}
			}else{
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Current database type being used is not yet supported. Contact system admin");
				return exceptionCode;
			}
			
		}catch (SQLException e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}catch(Exception e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}finally{
			try{
				if(rs!=null)
					rs.close();
				if(stmt!=null)
					stmt.close();
				if(con!=null)
					con.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(result);
		return exceptionCode;
	}
	
	public ExceptionCode formConnectorViews(String macroVar, String dbScript){
		ExceptionCode exceptionCode = new ExceptionCode();
		JSONArray result = null;
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		exceptionCode = CommonUtils.getConnection(dbScript);
		if(exceptionCode.getErrorCode()==Constants.SUCCESSFUL_OPERATION){
			con = (Connection) exceptionCode.getResponse();
		}else{
			return exceptionCode;
		}
		String dataBaseType = CommonUtils.getValue(dbScript, "DATABASE_TYPE");
		String dbSetParam1 =CommonUtils.getValue(dbScript, "DB_SET_PARAM1");
		String dbSetParam2 =CommonUtils.getValue(dbScript, "DB_SET_PARAM2");
		String dbSetParam3 =CommonUtils.getValue(dbScript, "DB_SET_PARAM3");
		try{
			String viewQuery = "";
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			if(ValidationUtil.isValid(dbSetParam1))
				stmt.executeUpdate(dbSetParam1);
			if(ValidationUtil.isValid(dbSetParam2))
				stmt.executeUpdate(dbSetParam2);
			if(ValidationUtil.isValid(dbSetParam3))
				stmt.executeUpdate(dbSetParam3);
			if("ORACLE".equalsIgnoreCase(dataBaseType)){
				viewQuery = "SELECT T1.VIEW_NAME FROM USER_VIEWS T1 ORDER BY T1.VIEW_NAME";
			}else if("MYSQL".equalsIgnoreCase(dataBaseType)){
				String dbSchema =CommonUtils.getValue(dbScript, "DB_SCHEMA");
				if(ValidationUtil.isValid(dbSchema)){
					viewQuery = vcConfigMainDao.getQueryForTableAndView("VIEW_QUERY_FOR_MYSQL");
					viewQuery = viewQuery.replaceAll("?", dbSchema);
				}else{
					exceptionCode.setErrorCode(9999);
					exceptionCode.setErrorMsg("Schema Maintenance");
					return exceptionCode;
				}
			}else if("MSSQL".equalsIgnoreCase(dataBaseType)){
				viewQuery = vcConfigMainDao.getQueryForTableAndView("VIEW_QUERY_FOR_MSSQL");
			}
			if(ValidationUtil.isValid(viewQuery)){
				rs = stmt.executeQuery(viewQuery);
				result = new JSONArray();
				int i=0;
				while(rs.next()){
					JSONObject obj =new JSONObject();
					obj.put("name",rs.getString("VIEW_NAME"));
					obj.put("id",i);
					result.add( obj );
					i++;
				}
			}else{
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Current database type being used is not yet supported. Contact system admin");
				return exceptionCode;
			}
			
		}catch (SQLException e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}catch(Exception e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}finally{
			try{
				if(rs!=null)
					rs.close();
				if(stmt!=null)
					stmt.close();
				if(con!=null)
					con.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(result);
		return exceptionCode;
	}
	
	public ExceptionCode formConnectorTableSpecificCols(String macroVar, String dbScript, String tableName){
		ExceptionCode exceptionCode = new ExceptionCode();
		ArrayList<VcMainDataSourceMetaDataVb> arrayListTree = null;
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		exceptionCode = CommonUtils.getConnection(dbScript);
		if(exceptionCode.getErrorCode()==Constants.SUCCESSFUL_OPERATION){
			con = (Connection) exceptionCode.getResponse();
		}else{
			return exceptionCode;
		}
		String dataBaseType = CommonUtils.getValue(dbScript, "DATABASE_TYPE");
		String dbSetParam1 =CommonUtils.getValue(dbScript, "DB_SET_PARAM1");
		String dbSetParam2 =CommonUtils.getValue(dbScript, "DB_SET_PARAM2");
		String dbSetParam3 =CommonUtils.getValue(dbScript, "DB_SET_PARAM3");
		String parentDbUserName = env.getProperty("spring.datasource.username");
		try{
			String tableQuery = "";
			arrayListTree = new ArrayList<VcMainDataSourceMetaDataVb>();
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			if(ValidationUtil.isValid(dbSetParam1))
				stmt.executeUpdate(dbSetParam1);
			if(ValidationUtil.isValid(dbSetParam2))
				stmt.executeUpdate(dbSetParam2);
			if(ValidationUtil.isValid(dbSetParam3))
				stmt.executeUpdate(dbSetParam3);
			NumSubTabVb vObjNum = vcConfigMainDao.getActiveNumTab(2004, dataBaseType);
			DSConnectorVb connectorVb = designAnalysisDao.getQueriesForDatasource();
			Map<String, String> dataTypeMap = vcConfigMainDao.getDataTypeMap(dataBaseType);

			if("ORACLE".equalsIgnoreCase(dataBaseType)){
				if (ValidationUtil.isValid(connectorVb.getTableColumnQuery())) {
					tableQuery = connectorVb.getTableColumnQuery().replace("#TABLE_NAME#", tableName);
					tableQuery = tableQuery.replace("#PARENT_DB_USER#", parentDbUserName);
				}
				else
					tableQuery = "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS where UPPER(TABLE_NAME) = UPPER('"+tableName+"') ORDER BY TABLE_NAME, COLUMN_ID";
			}else if("MYSQL".equalsIgnoreCase(dataBaseType)){
				String dbSchema =CommonUtils.getValue(dbScript, "DB_SCHEMA");
				if(ValidationUtil.isValid(dbSchema)){
					tableQuery = vcConfigMainDao.getQueryForTableAndView("TABLE_QUERY_FOR_MYSQL");
					tableQuery = tableQuery.replaceAll("?", dbSchema);
				}else{
					exceptionCode.setErrorCode(9999);
					exceptionCode.setErrorMsg("Schema Maintenance");
					return exceptionCode;
				}
			}else if("MSSQL".equalsIgnoreCase(dataBaseType)){
//				tableQuery = vcConfigMainDao.getQueryForTableAndView("TABLE_QUERY_FOR_MSSQL");
				if(ValidationUtil.isValid(connectorVb.getTableColumnQuery()))
				    tableQuery= connectorVb.getTableColumnQuery().replace("#TABLE_NAME#", tableName);
				else
				tableQuery = "SELECT T1.column_id, T1.NAME AS COLUMN_NAME, T1.max_length, T1.scale, "
						+ "CASE "
						+ "	when (T1.system_type_id=108 AND T1.scale>0) then 'NUMBER' "
						+ "	when (T1.system_type_id=108 AND T1.scale=0) then 'INT' "
						+ "	else UPPER(TYPE_NAME (T1.system_type_id)) "
						+ "END DATA_TYPE "
						+ "FROM SYS.COLUMNS T1 ,SYS.TABLES T2  "
						+ "WHERE UPPER(T2.name) = UPPER('"+tableName+"') AND T1.OBJECT_ID = T2.OBJECT_ID AND T1.name != 'ROWID' "
						+ "UNION "
						+ "SELECT T1.column_id, T1.NAME AS COLUMN_NAME, T1.max_length, T1.scale, "
						+ "CASE "
						+ "	when (T1.system_type_id=108 AND T1.scale>0) then 'NUMBER' "
						+ "	when (T1.system_type_id=108 AND T1.scale=0) then 'INT' "
						+ "	else UPPER(TYPE_NAME (T1.system_type_id)) "
						+ "END DATA_TYPE "
						+ "FROM SYS.COLUMNS T1 ,SYS.VIEWS T2  "
						+ "WHERE UPPER(T2.name) = UPPER('"+tableName+"') AND T1.OBJECT_ID = T2.OBJECT_ID AND T1.name != 'ROWID' "
						+ "ORDER BY column_id";
			}
			if(ValidationUtil.isValid(tableQuery)){
				rs = stmt.executeQuery(tableQuery);
				while(rs.next()){
					VcMainDataSourceMetaDataVb vObjColDSMD = new VcMainDataSourceMetaDataVb();
					vObjColDSMD.setTableName(tableName);
					vObjColDSMD.setColumnName(rs.getString("COLUMN_NAME"));
					if(dataTypeMap!=null && ValidationUtil.isValid(dataTypeMap.get(rs.getString("DATA_TYPE")))){
						vObjColDSMD.setColumnType(dataTypeMap.get(rs.getString("DATA_TYPE")));
					}else{
						vObjColDSMD.setColumnType("Y");
					}
					arrayListTree.add(vObjColDSMD);
				}
			}
		}catch (SQLException e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}catch(Exception e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}finally{
			try{
				if(rs!=null)
					rs.close();
				if(stmt!=null)
					stmt.close();
				if(con!=null)
					con.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(arrayListTree);
		return exceptionCode;
	}

	
	public ExceptionCode formConnectorViewSpecificCols(String macroVar, String dbScript, String viewName){
		ExceptionCode exceptionCode = new ExceptionCode();
		ArrayList<VcMainDataSourceMetaDataVb> arrayListTree = null;
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		exceptionCode = CommonUtils.getConnection(dbScript);
		if(exceptionCode.getErrorCode()==Constants.SUCCESSFUL_OPERATION){
			con = (Connection) exceptionCode.getResponse();
		}else{
			return exceptionCode;
		}
		String dataBaseType = CommonUtils.getValue(dbScript, "DATABASE_TYPE");
		String dbSetParam1 =CommonUtils.getValue(dbScript, "DB_SET_PARAM1");
		String dbSetParam2 =CommonUtils.getValue(dbScript, "DB_SET_PARAM2");
		String dbSetParam3 =CommonUtils.getValue(dbScript, "DB_SET_PARAM3");
		try{
			String viewQuery = "";
			arrayListTree = new ArrayList<VcMainDataSourceMetaDataVb>();
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			if(ValidationUtil.isValid(dbSetParam1))
				stmt.executeUpdate(dbSetParam1);
			if(ValidationUtil.isValid(dbSetParam2))
				stmt.executeUpdate(dbSetParam2);
			if(ValidationUtil.isValid(dbSetParam3))
				stmt.executeUpdate(dbSetParam3);
			NumSubTabVb vObjNum = vcConfigMainDao.getActiveNumTab(2004, dataBaseType);
			Map<String, String> dataTypeMap = vcConfigMainDao.getDataTypeMap(dataBaseType);
			if("ORACLE".equalsIgnoreCase(dataBaseType)){
				viewQuery = "SELECT T1.VIEW_NAME, T2.COLUMN_NAME, T2.DATA_TYPE FROM USER_VIEWS T1,USER_TAB_COLUMNS T2 WHERE T1.VIEW_NAME = '"+viewName+"' and T1.VIEW_NAME=T2.TABLE_NAME ORDER BY T1.VIEW_NAME, T2.COLUMN_ID";
			}else if("MYSQL".equalsIgnoreCase(dataBaseType)){
				String dbSchema =CommonUtils.getValue(dbScript, "DB_SCHEMA");
				if(ValidationUtil.isValid(dbSchema)){
					viewQuery = vcConfigMainDao.getQueryForTableAndView("VIEW_QUERY_FOR_MYSQL");
					viewQuery = viewQuery.replaceAll("?", dbSchema);
				}else{
					exceptionCode.setErrorCode(9999);
					exceptionCode.setErrorMsg("Schema Maintenance");
					return exceptionCode;
				}
			}else if("MSSQL".equalsIgnoreCase(dataBaseType)){
				viewQuery = vcConfigMainDao.getQueryForTableAndView("VIEW_QUERY_FOR_MSSQL");
			}
			
			if(ValidationUtil.isValid(viewQuery)){
				rs = stmt.executeQuery(viewQuery);
				boolean first = true;
				while(rs.next()){
					if(first){
						first = false;
					}
					VcMainDataSourceMetaDataVb vObjColDSMD = new VcMainDataSourceMetaDataVb();
					vObjColDSMD.setTableName(viewName);
					vObjColDSMD.setColumnName(rs.getString("COLUMN_NAME"));
					if(dataTypeMap!=null && ValidationUtil.isValid(dataTypeMap.get(rs.getString("DATA_TYPE")))){
						vObjColDSMD.setColumnType(dataTypeMap.get(rs.getString("DATA_TYPE")));
					}else{
						vObjColDSMD.setColumnType("Y");
					}
					arrayListTree.add(vObjColDSMD);
				}
			}
		}catch (SQLException e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}catch(Exception e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}finally{
			try{
				if(rs!=null)
					rs.close();
				if(stmt!=null)
					stmt.close();
				if(con!=null)
					con.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(arrayListTree);
		return exceptionCode;
	}
	
	/*public ExceptionCode getConnection(String dbScript){
		ExceptionCode exceptionCode = new ExceptionCode();
		Connection con = null;
		String dbIP = "";
		String jdbcUrl = "";
		String dbServiceName = CommonUtils.getValue(dbScript, "SERVICE_NAME");
		String dbOracleSid = CommonUtils.getValue(dbScript, "SID");
		String dbUserName = CommonUtils.getValue(dbScript, "USER");
		if(!ValidationUtil.isValid(dbUserName))
			dbUserName = CommonUtils.getValue(dbScript, "DB_USER");
		String dbPassWord = CommonUtils.getValue(dbScript, "PWD");
		if(!ValidationUtil.isValid(dbPassWord))
			dbPassWord = CommonUtils.getValue(dbScript, "DB_PWD");
		String dbPortNumber = CommonUtils.getValue(dbScript, "DB_PORT");
		String dataBaseName = CommonUtils.getValue(dbScript, "DB_NAME");
		String dataBaseType = CommonUtils.getValue(dbScript, "DATABASE_TYPE");
		String dbInstance = CommonUtils.getValue(dbScript, "DB_INSTANCE");
		String dbIp = CommonUtils.getValue(dbScript, "DB_IP");		
		String serverName = CommonUtils.getValue(dbScript, "SERVER_NAME");
		String version = CommonUtils.getValue(dbScript, "JAR_VERSION");
		
		String hostName = dbServiceName;
		if(!ValidationUtil.isValid(hostName)){
			hostName = dbOracleSid;
		}
		if(ValidationUtil.isValid(dbIp))
			dbIP = dbIp;
		try{
			if("ORACLE".equalsIgnoreCase(dataBaseType)){
				jdbcUrl = "jdbc:oracle:thin:@"+dbIP+":"+dbPortNumber+":"+hostName;
				con = vcConfigMainDao.getDbConnection(jdbcUrl, dbUserName, dbPassWord, "ORACLE", version);
			}else if("MSSQL".equalsIgnoreCase(dataBaseType)){
				if(ValidationUtil.isValid(dbInstance) && ValidationUtil.isValid(hostName)){
					jdbcUrl = "jdbc:sqlserver://"+dbIP+":"+dbPortNumber+";instanceName="+dbInstance+";databaseName="+hostName;
				}else if(ValidationUtil.isValid(dbInstance) && !ValidationUtil.isValid(hostName)){
					jdbcUrl = "jdbc:sqlserver://"+dbIP+":"+dbPortNumber+";instanceName="+dbInstance;
				}else if(!ValidationUtil.isValid(dbInstance) && ValidationUtil.isValid(hostName)){
					jdbcUrl = "jdbc:sqlserver://"+dbIP+":"+dbPortNumber+";databaseName="+hostName;
				}else{
					jdbcUrl = "jdbc:sqlserver://"+dbIP+":"+dbPortNumber+";databaseName="+hostName;
				}
				con = vcConfigMainDao.getDbConnection(jdbcUrl, dbUserName, dbPassWord, "MSSQL", version);
				
			}else if("MYSQL".equalsIgnoreCase(dataBaseType)){
				if(ValidationUtil.isValid(dbInstance) && ValidationUtil.isValid(dataBaseName)){
					jdbcUrl = "jdbc:mysql://"+dbIp+":"+dbPortNumber+"//instanceName="+dbInstance+"//databaseName="+dataBaseName;
				}else if(ValidationUtil.isValid(dbInstance) && !ValidationUtil.isValid(dataBaseName)){
					jdbcUrl = "jdbc:mysql://"+dbIp+":"+dbPortNumber+"//instanceName="+dbInstance;
				}else if(!ValidationUtil.isValid(dbInstance) && ValidationUtil.isValid(dataBaseName)){
					jdbcUrl = "jdbc:mysql://"+dbIp+":"+dbPortNumber+"/"+dataBaseName;
				}else{
					jdbcUrl = "jdbc:mysql://"+dbIp+":"+dbPortNumber+"/"+dataBaseName;
				}	
				con = vcConfigMainDao.getDbConnection(jdbcUrl, dbUserName, dbPassWord, "MYSQL", version);
				
			}else if("POSTGRESQL".equalsIgnoreCase(dataBaseType)){
				jdbcUrl = "jdbc:postgresql://"+dbIP+":"+dbPortNumber+"/"+dataBaseName;
				con = vcConfigMainDao.getDbConnection(jdbcUrl, dbUserName, dbPassWord, "POSTGRESQL", version);
			}else if("SYBASE".equalsIgnoreCase(dataBaseType)){
				jdbcUrl="jdbc:sybase:Tds:"+dbIP+":"+dbPortNumber+"?ServiceName="+hostName;
				con = vcConfigMainDao.getDbConnection(jdbcUrl, dbUserName, dbPassWord, "SYBASE", version);
			}else if("INFORMIX".equalsIgnoreCase(dataBaseType)){
				jdbcUrl = "jdbc:informix-sqli://"+dbIP+":"+dbPortNumber+"/"+dataBaseName+":informixserver="+serverName;
				con = vcConfigMainDao.getDbConnection(jdbcUrl, dbUserName, dbPassWord, "INFORMIX", version);
			}
			if(con==null){
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Problem in gaining connection with datasource");
				return exceptionCode;
			}
		}catch(ClassNotFoundException e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		} catch (SQLException e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}catch(Exception e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setResponse(con);
		return exceptionCode;
	}*/
	
	public ExceptionCode doPublishToMain(VcConfigMainVb vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<VcConfigMainVb> deepCopy = new DeepCopy<VcConfigMainVb>();
		VcConfigMainVb clonedObject = null;
		try{
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			exceptionCode = vcConfigMainDao.doPublishToMain(vObject);
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Insert Exception In Catalog Publish" + rex.getCode().getErrorMsg());
			logger.error( ((vObject==null)? "vObject is Null":vObject.toString()));
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	
	public ExceptionCode doSaveOperationsWIP(VcConfigMainCRUDVb vcMainVb){
		ExceptionCode exceptionCode  = null;
		try{
			vcMainVb.setRecordIndicator(0);
			vcMainVb.setStaticDelete(false);
			vcMainVb.setVerificationRequired(false);
			String sysDate = vcConfigMainDao.getSystemDate();
			vcMainVb.setDateCreation(sysDate);
			vcMainVb.setDateLastModified(sysDate);
			setAtNtValuesForCRUD(vcMainVb);
			exceptionCode = vcConfigMainDao.doSaveOperationsWIP(vcMainVb);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Insert Exception In Save All" + rex.getCode().getErrorMsg());
			exceptionCode = rex.getCode();
			return exceptionCode;
		}
	}
	
	public Integer returnBaseTableId(String catalogId) {
		try{
			return vcConfigMainDao.returnBaseTableId(catalogId);
		} catch(Exception e) {
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	
	public ExceptionCode getQueryResultsVcConfigMain(VcConfigMainVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		int intStatus = 1;
		setVerifReqDeleteType(vObject);
		List<VcConfigMainVb> collTemp;
		if(0 == vObject.getVcStatus() || 9 == vObject.getVcStatus()) {
			intStatus = 0;
			collTemp = vcConfigMainDao.getQueryResultsFromVisionCatalog(vObject, intStatus, true);
		} else {
			collTemp = vcConfigMainDao.getQueryResultsFromVisionCatalog(vObject, intStatus, true);
		}
		exceptionCode.setResponse((collTemp!=null && collTemp.size()>0)?collTemp.get(0):null);
		if(collTemp.size() == 0){
			exceptionCode = CommonUtils.getResultObject("Catalog Edit", 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
		}else{
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("Success");
		}
		return exceptionCode;
	}
	
	public ExceptionCode getQueryResultsForDistinctCatalogSrc(VcConfigMainVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		setVerifReqDeleteType(vObject);
		List collTemp = vcConfigMainDao.getDistinctOfDatadourceFromVcTree(vObject);
		exceptionCode.setResponse((collTemp!=null && collTemp.size()>0)?collTemp:null);
		if(collTemp.size() == 0){
			exceptionCode = CommonUtils.getResultObject("Catalog Edit", 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
		}else{
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("Success");
		}
		return exceptionCode;
	}
	
	public ExceptionCode formConnectorTablesAndColumns(String macroVar, String dbScript){
		ExceptionCode exceptionCode = new ExceptionCode();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		List<VcMainDataSourceMetaDataVb> returnList = null;
		exceptionCode = CommonUtils.getConnection(dbScript);
		if(exceptionCode.getErrorCode()==Constants.SUCCESSFUL_OPERATION){
			con = (Connection) exceptionCode.getResponse();
		}else{
			return exceptionCode;
		}
		String dataBaseType = CommonUtils.getValue(dbScript, "DATABASE_TYPE");
		String dbSetParam1 =CommonUtils.getValue(dbScript, "DB_SET_PARAM1");
		String dbSetParam2 =CommonUtils.getValue(dbScript, "DB_SET_PARAM2");
		String dbSetParam3 =CommonUtils.getValue(dbScript, "DB_SET_PARAM3");
		try{
			String tableQuery = "";
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			if(ValidationUtil.isValid(dbSetParam1))
				stmt.executeUpdate(dbSetParam1);
			if(ValidationUtil.isValid(dbSetParam2))
				stmt.executeUpdate(dbSetParam2);
			if(ValidationUtil.isValid(dbSetParam3))
				stmt.executeUpdate(dbSetParam3);
			NumSubTabVb vObjNum = vcConfigMainDao.getActiveNumTab(2004, dataBaseType);
			Map<String, String> dataTypeMap = vcConfigMainDao.getDataTypeMap(dataBaseType);
			if("ORACLE".equalsIgnoreCase(dataBaseType)){
				/*tableQuery = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLUMNS where TABLE_NAME not in  (SELECT T1.VIEW_NAME FROM USER_VIEWS T1) AND TABLE_NAME like 'FIN%' ORDER BY TABLE_NAME, COLUMN_ID";*/
				tableQuery = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLUMNS where TABLE_NAME not in  (SELECT T1.VIEW_NAME FROM USER_VIEWS T1) ORDER BY TABLE_NAME, COLUMN_ID";
			}else if("MYSQL".equalsIgnoreCase(dataBaseType)){
				String dbSchema =CommonUtils.getValue(dbScript, "DB_SCHEMA");
				if(ValidationUtil.isValid(dbSchema)){
					tableQuery = vcConfigMainDao.getQueryForTableAndView("TABLE_QUERY_FOR_MYSQL");
					tableQuery = tableQuery.replaceAll("?", dbSchema);
				}else{
					exceptionCode.setErrorCode(9999);
					exceptionCode.setErrorMsg("Schema Maintenance");
					return exceptionCode;
				}
			}else if("MSSQL".equalsIgnoreCase(dataBaseType)){
				tableQuery = vcConfigMainDao.getQueryForTableAndView("TABLE_QUERY_FOR_MSSQL");
			}
			if(ValidationUtil.isValid(tableQuery)){
				returnList = new ArrayList<VcMainDataSourceMetaDataVb>();
				List<VcMainDataSourceMetaDataVb> childList = new ArrayList<VcMainDataSourceMetaDataVb>();
				rs = stmt.executeQuery(tableQuery);
				String chkTblName = "";
				if(rs.next()) {
					chkTblName = rs.getString("TABLE_NAME");
				}
				rs.beforeFirst();
				while(rs.next()){
					if(chkTblName.equalsIgnoreCase(rs.getString("TABLE_NAME"))) {
						childList.add(new VcMainDataSourceMetaDataVb(rs.getString("TABLE_NAME"), rs.getString("COLUMN_NAME"), null));
					} else {
						returnList.add(new VcMainDataSourceMetaDataVb(chkTblName, "", childList));
						childList = new ArrayList<VcMainDataSourceMetaDataVb>();
						childList.add(new VcMainDataSourceMetaDataVb(rs.getString("TABLE_NAME"), rs.getString("COLUMN_NAME"), null));
						chkTblName = rs.getString("TABLE_NAME");
					}
				}
			}
		}catch (SQLException e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}catch(Exception e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Problem in gaining connection - Cause:"+e.getMessage());
			return exceptionCode;
		}finally{
			try{
				if(rs!=null)
					rs.close();
				if(stmt!=null)
					stmt.close();
				if(con!=null)
					con.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setOtherInfo(returnList);
		return exceptionCode;
	}
	
	public List<VcConfigMainVb> getQuerySmartSearchFilter(VcConfigMainVb queryPopupObj){
		List<VcConfigMainVb> arrListLocal = new ArrayList<VcConfigMainVb>();
		try{
			setVerifReqDeleteType(queryPopupObj);
			doFormateDataForQuery(queryPopupObj);
			List<VcConfigMainVb> arrListResult = vcConfigMainDao.getQuerySmartSearchFilter(queryPopupObj);
			if(arrListResult == null){
				//arrListLocal.add(queryPopupObj);
			}else{
				arrListLocal.addAll(arrListResult);
			}
			return arrListLocal;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception in getting the Catalog Smart Search Filter results.", ex);
			return null;
		}
	}
	
	public VcConfigMainTreeVb getTableColumnByTableCatalogId(VcConfigMainTreeVb vcTreeVb) {
		try {
			VcConfigMainTreeVb tableVb = vcConfigMainDao.getTableColumnByTableIdCatalogId(vcTreeVb);
			return tableVb;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	@SuppressWarnings("rawtypes")
	public List getTableColumnAlias(VcConfigMainVb vcConfigMainVb) {
		try {
			List arrListResult = vcConfigMainDao.getTableColumnAliasByCatalogId(vcConfigMainVb);
			return arrListResult;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	
	public ExceptionCode getColumnMetaDataScriptForVcQueries(String macroVar) throws DataAccessException{
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String columnData = vcConfigMainDao.getColumnMetaDataScriptForVcQueries(macroVar);
			List columnVbList = new ArrayList();
			Matcher matObj = Pattern.compile("<column><name>(.*?)<\\/name><type>(.*?)<\\/type>(.*?)<\\/column>",Pattern.DOTALL).matcher(columnData);
			while(matObj.find()) {
				VcMainDataSourceMetaDataVb vObjColDSMD = new VcMainDataSourceMetaDataVb();
				vObjColDSMD.setTableName(macroVar);
				vObjColDSMD.setColumnName(matObj.group(1));
				vObjColDSMD.setColumnType(matObj.group(2));
				columnVbList.add(vObjColDSMD);
			}
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setResponse(columnVbList);
		}catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	public ExceptionCode getRelationDetailsForCatalog(VcConfigMainVb vObject){
		ExceptionCode exceptionCode = new ExceptionCode();
		setVerifReqDeleteType(vObject);
		Map<Integer, List<VcConfigMainRelationVb>> relationMap = vcConfigMainDao.getRelationDetailsForCatalog(vObject);
		exceptionCode.setResponse((relationMap!=null && relationMap.size()>0)?relationMap:null);
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setErrorMsg("Success");
		return exceptionCode;
	}
	public VcConfigMainDao getVcConfigMainDao() {
		return vcConfigMainDao;
	}
	public void setVcConfigMainDao(VcConfigMainDao vcConfigMainDao) {
		this.vcConfigMainDao = vcConfigMainDao;
	}
		
	protected void setAtNtValuesForCRUD(VcConfigMainCRUDVb vObject){
		vObject.setRecordIndicatorNt(7);
		vObject.setRecordIndicator(0);
		vObject.setVcStatusNt(2000);
		vObject.setJoinClauseNt(2001);
		if(ValidationUtil.isValidList(vObject.getAddModifyMetadata())) {
			for(VcConfigMainTreeVb treeVb:vObject.getAddModifyMetadata()) {
				treeVb.setDatabaseTypeAt(1082);
				treeVb.setVctStatusNt(1);
//				treeVb.setVctStatus(0);
				if(ValidationUtil.isValidList(treeVb.getChildren())) {
					for(VcConfigMainColumnsVb columnVb:treeVb.getChildren()) {
						columnVb.setColDisplayTypeAt(2000);
						columnVb.setColAttributeTypeAt(2002);
						columnVb.setColExperssionTypeAt(2003);
						columnVb.setFormatTypeNt(40);
						columnVb.setMagTypeNt(2002);
						columnVb.setMagSelectionTypeAt(2007);
						columnVb.setVccStatusNt(1);
						columnVb.setVccStatus(0);
						columnVb.setColTypeAt(2001);
					}
				}
			}
		}
		if(ValidationUtil.isValidList(vObject.getRelationAddModifyMetadata())) {
			for(VcConfigMainRelationVb relationVb:vObject.getRelationAddModifyMetadata()) {
				relationVb.setCatalogId(vObject.getCatalogId());
				relationVb.setJoinTypeNt(41);
			}
		}
	}
	
	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doInsertRecordForAccessControl(VcConfigMainLODWrapperVb vcConfigMainLODWrapperVb, boolean isMain) throws RuntimeCustomException {
		ExceptionCode exceptionCode = new ExceptionCode();
		int result = Constants.ERRONEOUS_OPERATION;
		try {
			String sysDate = commonDao.getSystemDate();
			vcConfigMainLODWrapperVb.getMainModel().setDateCreation(sysDate);
			vcConfigMainLODWrapperVb.getMainModel().setDateLastModified(sysDate);
			result = vcConfigMainDao.doInsertRecordForCatalogAccess(vcConfigMainLODWrapperVb, isMain);
			exceptionCode.setErrorCode(result);
			return exceptionCode;
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	
	public ExceptionCode getLODForCatalog(VcConfigMainLODWrapperVb vcConfigMainLODWrapperVb) throws RuntimeCustomException {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String sysDate = commonDao.getSystemDate();
			vcConfigMainLODWrapperVb.getMainModel().setDateCreation(sysDate);
			vcConfigMainLODWrapperVb.getMainModel().setDateLastModified(sysDate);
			exceptionCode.setResponse(vcConfigMainDao.getRecordForCatalogLOD(vcConfigMainLODWrapperVb));
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}

	public ExceptionCode deleteCatalog(VcConfigMainVb catalogVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		List<DesignAnalysisVb> designVb = vcConfigMainDao.ValidatingCatalogData(catalogVb.getCatalogId());
		if (catalogVb.getVcStatus() == 0 && !ValidationUtil.isValidList(designVb)) {
			/* Get version number to be used */
			int versionNumber = vcConfigMainDao.getMaxVersionNumber(catalogVb);
			versionNumber++;
			/* Move MAIN tables data to AD tables */
			vcConfigMainDao.moveMainDataToAD(catalogVb, versionNumber);
			vcConfigMainDao.deleteDataFromTables(catalogVb, true);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} else if (catalogVb.getVcStatus() != 0) {
			vcConfigMainDao.deleteDataFromTables(catalogVb, false);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} else {
			exceptionCode.setErrorMsg("As CATALOG is used in design Query it cannot be deleted");
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		return exceptionCode;
	}
}