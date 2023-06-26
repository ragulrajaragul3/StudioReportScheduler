package com.vision.wb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.DesignAnalysisDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.DeepCopy;
import com.vision.util.ValidationUtil;
import com.vision.vb.DCManualQueryVb;
import com.vision.vb.DesignAnalysisVb;
import com.vision.vb.DesignAndAnalysisMagnifierVb;
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

@Component
public class DesignAnalysisWb extends AbstractWorkerBean<DesignAnalysisVb> {
	public JdbcTemplate jdbcTemplate = null;

	public DesignAnalysisWb(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}

	public static Logger logger = LoggerFactory.getLogger(DesignAnalysisWb.class);
	
	@Autowired
	public DesignAnalysisDao designAnalysisDao;
	
	@Autowired
	DataSource dataSource;
	
	@Value("${app.databaseType}")
	 private String databaseType = "ORACLE";

	@Override
	protected void setVerifReqDeleteType(DesignAnalysisVb vObject) {
		vObject.setStaticDelete(false);
		vObject.setVerificationRequired(false);
	}

	@Override
	protected AbstractDao<DesignAnalysisVb> getScreenDao() {
		return designAnalysisDao;
	}
	
	@Override
	protected void setAtNtValues(DesignAnalysisVb vObject) {
		vObject.setRecordIndicatorNt(7);
		vObject.setRecordIndicator(0);
	}

	
	public List<DesignAnalysisVb> getAllDesignQuery(DesignAnalysisVb designVb) {
		try {
			List<DesignAnalysisVb> arrListResult = new DesignAnalysisDao(jdbcTemplate).getDesignQueryFromVisionCatalog(designVb);
			return arrListResult;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
	/*public List<DCManualQueryVb> getAllManualQuery(DCManualQueryVb vObj) {
		try {
			List<DCManualQueryVb> arrListResult = new DCManualQueryDao(jdbcTemplate).getAllManualQueryDetails(vObj);
			return arrListResult;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}*/
	
	public List<VcConfigMainVb> getUserCatalogs() {
		try {
			List<VcConfigMainVb> arrListResult = new DesignAnalysisDao(jdbcTemplate).getUserCatalogsDetails();
			return arrListResult;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	
	public List<VcConfigMainTreeVb> getQueryTree(VcConfigMainTreeVb treeVb) {
		try {
			List<VcConfigMainTreeVb> arrListResult = new DesignAnalysisDao(jdbcTemplate).getQueryTreeDetails(treeVb.getCatalogId());
			return arrListResult;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	public List<VcConfigMainColumnsVb> getQueryTreeColumns(VcConfigMainTreeVb treeVb) {
		try {
			List<VcConfigMainColumnsVb> arrListResult = new DesignAnalysisDao(jdbcTemplate).getQueryTreeColumnsDetails(treeVb);
			return arrListResult;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	
	public List<VcForCatalogTableRelationVb> getQueryTreeColumnsRelations(VcForCatalogTableRelationVb vcForCatalogTableRelationVb) {
		try {
			List<VcForCatalogTableRelationVb> arrListResult = new DesignAnalysisDao(jdbcTemplate).getQueryTreeColumnsRelationsDetails(vcForCatalogTableRelationVb);
			return arrListResult;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeCustomException(e.getMessage());
		}
	}
	
	/***Load Reports***/
	public ExceptionCode getReportNamesForCatalog(DesignAnalysisVb designAnalysisVb){
		ExceptionCode exceptionCode = null;
		try{
			List<VcForQueryReportVb> result = new DesignAnalysisDao(jdbcTemplate).getAvailableActiveReportsForUser(designAnalysisVb);
			if(result == null || result.isEmpty() ){
				exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceName(), Constants.NO_RECORDS_FOUND, "No Query Results", "");
				exceptionCode.setOtherInfo(designAnalysisVb);
				return exceptionCode;
			}
			exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceName(), Constants.SUCCESSFUL_OPERATION, "Query", "");
			exceptionCode.setResponse(result);
		}catch(RuntimeCustomException rex){
			logger.error("Insert Exception " + rex.getCode().getErrorMsg());
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(designAnalysisVb);
			return exceptionCode;
		}
		return exceptionCode;
	}
	public ExceptionCode saveReport(VcForQueryReportFieldsWrapperVb vObjMain){
		VcForQueryReportVb vcForQueryReportVb = vObjMain.getMainModel();
		vcForQueryReportVb.setReportFields(vObjMain.getReportFields());
		ExceptionCode exceptionCode = null;
		try{
			exceptionCode = new DesignAnalysisDao(jdbcTemplate).saveOrUpdate(vcForQueryReportVb);
			exceptionCode.setOtherInfo(vcForQueryReportVb);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Insert Exception " + rex.getCode().getErrorMsg());
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(vcForQueryReportVb);
			return exceptionCode;
		}
	}

	public String getDbScript(String macroVar){
		return new DesignAnalysisDao(jdbcTemplate).getScriptValue(macroVar);
	}
	
	public Integer returnBaseTableId(String CatalogId) {
		try {
			Integer returnBaseTableId = new DesignAnalysisDao(jdbcTemplate).returnBaseTableId(CatalogId);
			return returnBaseTableId;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	
	public ExceptionCode returnHashVariableListing(VcForQueryReportFieldsWrapperVb vcForQueryReportFieldsWrapperVb, List<String> uniqueTableIdArray) {
		DeepCopy<VcForQueryReportVb> deepCopy = new DeepCopy<VcForQueryReportVb>();
		VcForQueryReportVb clonedObject = deepCopy.copy(vcForQueryReportFieldsWrapperVb.getMainModel());
		ExceptionCode exceptionCode = new ExceptionCode();
		new DesignAnalysisDao(jdbcTemplate).setServiceDefaults();
		String[] hashArray = vcForQueryReportFieldsWrapperVb.getHashArray();
		String[] hashValueArray = vcForQueryReportFieldsWrapperVb.getHashValueArray();
		try{
			Integer joinType = new DesignAnalysisDao(jdbcTemplate).getJoinTypeInCatalog(clonedObject.getCatalogId());
			ArrayList getDataAL = new DesignAnalysisDao(jdbcTemplate).getData(clonedObject.getCatalogId(),joinType);
			HashMap<String,VcConfigMainTreeVb> tableDetailsHM = (HashMap<String,VcConfigMainTreeVb>) getDataAL.get(2);
			parseQueryWithActualAliasName(clonedObject, uniqueTableIdArray, tableDetailsHM);
			HashMap<String, Object> joinResponseMap = new HashMap<String, Object>();
			exceptionCode = returnJoinCondition(clonedObject, uniqueTableIdArray, joinType, getDataAL);
			if(exceptionCode.getErrorCode()!=Constants.SUCCESSFUL_OPERATION) {
				return exceptionCode;
			}
			joinResponseMap = (HashMap<String, Object>) exceptionCode.getResponse();
			String fromString = String.valueOf(joinResponseMap.get("FROM"));
			List<String> manualQueryList = new ArrayList<String>();
			Pattern patternObj = Pattern.compile("\\{\\{MQ_\\((.*?)\\)\\}\\}", Pattern.DOTALL);
			Matcher matchObj = patternObj.matcher(fromString);
			while(matchObj.find()) {
				manualQueryList.add(matchObj.group(1).substring(0,matchObj.group(1).lastIndexOf("_")));
			}
			/*LinkedHashSet<String> hashVarSet = new LinkedHashSet<String>();
			List<String> hashValueList = new ArrayList<String>();*/
			Map<String, String> hashVariableMap = new HashMap<String, String>();
			if(manualQueryList.size()>0) {
				for(String queryId : manualQueryList) {
					List<DCManualQueryVb> queryVbList = new DesignAnalysisDao(jdbcTemplate).getQueryIdDetails(queryId);
					if (ValidationUtil.isValidList(queryVbList)){
						String hashScript = queryVbList.get(0).getHashVariableScript();
						if(ValidationUtil.isValid(hashScript)) {
							Matcher regexMatcher = Pattern.compile("\\{(.*?):#(.*?)\\$@!(.*?)\\#}", Pattern.DOTALL).matcher(hashScript);
							while (regexMatcher.find()) {
								hashVariableMap.put(regexMatcher.group(1), regexMatcher.group(3));
								/*if(hashVarSet.add(regexMatcher.group(1))) {
									hashValueList.add(regexMatcher.group(3));
								}*/
							}							
						}
					} else {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						exceptionCode.setErrorMsg("Error in manual query maintenance for Query ID ["+queryId+"]");
						return exceptionCode;
					}
				}
			}
			if(ValidationUtil.isValid(clonedObject.getWhere())) {
				Matcher regexMatcher = Pattern.compile("#(.*?)#", Pattern.DOTALL).matcher(clonedObject.getWhere());
				while (regexMatcher.find()) {
					if (hashVariableMap.get(regexMatcher.group(1)) == null)
						hashVariableMap.put(regexMatcher.group(1), "");
					/*if(hashVarSet.add(regexMatcher.group(1))) {
						hashValueList.add(regexMatcher.group(3));
					}*/
				}							
			}
			if(hashArray!=null && hashValueArray!=null && hashArray.length == hashValueArray.length) {
				for(int index = 0;index < hashArray.length;index++) {
					if(hashVariableMap.get(hashArray[index]) != null) {
						hashVariableMap.put(hashArray[index],hashValueArray[index]);
					}
				}
			}
			exceptionCode.setResponse(hashVariableMap);
		} catch (Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	
	public ExceptionCode executeCatalogQuery(VcForQueryReportFieldsWrapperVb vcForQueryReportFieldsWrapperVb, List<String> uniqueTableIdArray, boolean isJsonResponceRequired, boolean isTableDropRequired){
		
		DeepCopy<VcForQueryReportVb> deepCopy = new DeepCopy<VcForQueryReportVb>();
		VcForQueryReportVb clonedObject = deepCopy.copy(vcForQueryReportFieldsWrapperVb.getMainModel());
		ExceptionCode exceptionCode = new ExceptionCode();
		new DesignAnalysisDao(jdbcTemplate).setServiceDefaults();
		List<String> tablesToDrop = new ArrayList<String>();
		
		String hashArray[] = vcForQueryReportFieldsWrapperVb.getHashArray();
		String hashValueArray[] = vcForQueryReportFieldsWrapperVb.getHashValueArray();
		StringBuffer restrictionCondition = new StringBuffer();
		//VisionUsersVb userVbFromSession = SessionContextHolder.getContext();
		VisionUsersVb userVbFromSession = new VisionUsersVb();
		
		String responceTableName = "";
		
		try{
			Integer joinType = new DesignAnalysisDao(jdbcTemplate).getJoinTypeInCatalog(clonedObject.getCatalogId());
			ArrayList getDataAL = new DesignAnalysisDao(jdbcTemplate).getData(clonedObject.getCatalogId(),joinType);
			
			HashMap<String,VcConfigMainTreeVb> tableDetailsHM = (HashMap<String,VcConfigMainTreeVb>) getDataAL.get(2);
			parseQueryWithActualAliasName(clonedObject, uniqueTableIdArray, tableDetailsHM);
//			vcForQueryReportFieldsWrapperVb.getMainModel().setGroupBy1(clonedObject.getGroupBy1());
			if(isJsonResponceRequired && ValidationUtil.isValid(vcForQueryReportFieldsWrapperVb.getMainModel().getTableName())) {
				exceptionCode = updateWidgetCreationStagingTable(vcForQueryReportFieldsWrapperVb.getMainModel().getTableName());
				if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
					return new DesignAnalysisDao(jdbcTemplate).formResponceJsonForExecute(vcForQueryReportFieldsWrapperVb,vcForQueryReportFieldsWrapperVb.getMainModel().getTableName());
				}
			}
			String joinFromStr = "";
			String joinWhereStr = "";
			HashMap<String, Object> joinResponseMap = new HashMap<String, Object>();
			exceptionCode = returnJoinCondition(clonedObject, uniqueTableIdArray, joinType, getDataAL);
			if(exceptionCode.getErrorCode()!=Constants.SUCCESSFUL_OPERATION) {
				return exceptionCode;
			}
			
			joinResponseMap = (HashMap<String, Object>) exceptionCode.getResponse();
			joinFromStr = String.valueOf(joinResponseMap.get("FROM"));
			joinWhereStr = String.valueOf(joinResponseMap.get("WHERE"));
			HashSet<String> tableSet = (HashSet<String>) joinResponseMap.get("TABLES");
			List<String> usedTablesList = new ArrayList<String>();
			
			if (tableSet != null)
				tableSet.stream().forEach(tabId -> usedTablesList.add(tabId));
			
			List<String> tablesUsed = (usedTablesList != null && usedTablesList.size() > 0) ? usedTablesList
					: uniqueTableIdArray;
			
			for(String tableId : tablesUsed) {
				VcConfigMainTreeVb treeVb = tableDetailsHM.get(tableId);
				List<UserRestrictionVb> accessControlScriptParsed = treeVb.getAccessControlScriptParsed();
				List<UserRestrictionVb> restrictedDataList = userVbFromSession.getRestrictionList();
				if(ValidationUtil.isValid(accessControlScriptParsed) && ValidationUtil.isValid(restrictedDataList)) {
					for(UserRestrictionVb restrictionVb : accessControlScriptParsed) {
						UserRestrictionVb filteredDataVb = restrictedDataList.stream().filter(vb -> restrictionVb.getMacrovarName().equalsIgnoreCase(vb.getMacrovarName())).findAny().orElse(null);
						if(filteredDataVb!=null) {
							StringBuffer columnNameStr = new StringBuffer();
							String columnValueStr = filteredDataVb.getRestrictedValue();
							for(UserRestrictionVb columnNameVb : restrictionVb.getChildren()) {
								columnNameStr.append(treeVb.getAliasName()+"."+columnNameVb.getTagValue()+"||'-'||");
							}
							columnNameStr = new StringBuffer(columnNameStr.substring(0, columnNameStr.length()-7));
							if(ValidationUtil.isValid(columnValueStr)) {
								if (restrictionCondition.length() > 0)
									restrictionCondition.append(" AND ");
								restrictionCondition.append(columnNameStr + " in (" + columnValueStr + ")");
							}
						}
					}
				}
			}
			
			if(restrictionCondition.length()>0) {
				if(ValidationUtil.isValid(joinWhereStr)) {
					joinWhereStr = joinWhereStr +" AND "+restrictionCondition;
				} else {
					joinWhereStr = String.valueOf(restrictionCondition);
				}
			}
			
			exceptionCode = parseFromStringWithActualTableName(tableDetailsHM, joinFromStr, hashArray, hashValueArray);
			tablesToDrop = (List<String>) exceptionCode.getOtherInfo();
			if(exceptionCode.getErrorCode()!=Constants.SUCCESSFUL_OPERATION) {
				return exceptionCode;
			}
			joinFromStr = String.valueOf(exceptionCode.getResponse());
			
			StringBuffer executionSql = new StringBuffer();
			StringBuffer whereClause = new StringBuffer();
			StringBuffer havingClause = new StringBuffer();
			int whereColumnIndex = 0;
			int havingColumnIndex = 0;
			for (VcForQueryReportFieldsVb reportsObj : vcForQueryReportFieldsWrapperVb.getReportFields()) {
				
//				Form WHERE Clause Condition
				if (ValidationUtil.isValid(reportsObj.getOperator()) && !CommonUtils.isAggrigateFunction(reportsObj.getOperator())) {
						
					if((!ValidationUtil.isValid(reportsObj.getValue1()) && !ValidationUtil.isValid(reportsObj.getValue2())) && (!reportsObj.getOperator().equalsIgnoreCase("is null") && !reportsObj.getOperator().equalsIgnoreCase("is not null"))&&(ValidationUtil.isValid(reportsObj.getDynamicStartFlag()) && !reportsObj.getDynamicStartFlag().equalsIgnoreCase("y") ) )
						continue;
						
					if (whereColumnIndex > 0) {
						whereClause.append(" "+reportsObj.getJoinCondition()+" ");
					}
					
					/*String localDateConvertSyntax = localDateConversionMap.get(reportsObj.getFormatType());
					String localDateFormatSyntax = localDdateFormatMap.get(reportsObj.getFormatType());*/
					
					String columnName = reportsObj.getTabelAliasName()+"."+reportsObj.getColName();
					if(reportsObj.getColDisplayType().equalsIgnoreCase("d")) {
//						columnName = "  CONVERT(date, "+columnName+" ,'"+reportsObj.getDynamicDateFormat()+"')";
						columnName = reportsObj.getDateConversionSyntax().replaceAll("#VALUE#", columnName);
					}
					if(reportsObj.getOperator().equalsIgnoreCase("between")) {
						String value1= ("y".equalsIgnoreCase(reportsObj.getDynamicStartFlag()))?generateDynamicDate(reportsObj.getDynamicStartDate(),reportsObj.getDynamicStartOperator(),reportsObj.getDynamicDateFormat(),reportsObj.getJavaFormatDesc()):reportsObj.getValue1();
						String value2= ("y".equalsIgnoreCase(reportsObj.getDynamicEndFlag()))?generateDynamicDate(reportsObj.getDynamicEndDate(),reportsObj.getDynamicEndOperator(),reportsObj.getDynamicDateFormat(),reportsObj.getJavaFormatDesc()):reportsObj.getValue2();
						if("D".equalsIgnoreCase(reportsObj.getColDisplayType())){
//							whereClause.append(columnName+" "+reportsObj.getOperator()+"  CONVERT(date, '"+value1+"' ,'"+reportsObj.getDynamicDateFormat()+"')  AND   CONVERT(date, '"+value2+"' ,'"+reportsObj.getDynamicDateFormat()+ "')");
							whereClause.append(columnName+" "+reportsObj.getOperator()+" "+reportsObj.getDateConversionSyntax().replaceAll("#VALUE#", "'"+value1+"'")+" AND "+  reportsObj.getDateConversionSyntax().replaceAll("#VALUE#", "'"+value2+"'"));
						} else if("N".equalsIgnoreCase(reportsObj.getColDisplayType()) || "C".equalsIgnoreCase(reportsObj.getColDisplayType())){
							whereClause.append(columnName+" "+reportsObj.getOperator()+" '"+value1+"' AND '"+value2+"' ");
						} else {
							whereClause.append("UPPER("+columnName+") "+reportsObj.getOperator()+" UPPER('"+value1+"') AND UPPER('"+value2+"') ");
			     		}
				    } else {
						String value1= ("y".equalsIgnoreCase(reportsObj.getDynamicStartFlag()))?generateDynamicDate(reportsObj.getDynamicStartDate(),reportsObj.getDynamicStartOperator(),reportsObj.getDynamicDateFormat(),reportsObj.getJavaFormatDesc()):reportsObj.getValue1();
						if (reportsObj.getOperator().equalsIgnoreCase("in") || reportsObj.getOperator().equalsIgnoreCase("not in")){
							whereClause.append("UPPER("+columnName + ") "+ reportsObj.getOperator() +" "+ getInOperatorValues(value1,reportsObj.getColDisplayType(), reportsObj.getDateConversionSyntax()));
						}else if(reportsObj.getOperator().equalsIgnoreCase("is null") || reportsObj.getOperator().equalsIgnoreCase("is not null") ) {
							whereClause.append(columnName + " "+ reportsObj.getOperator()+" ");
						} else if (reportsObj.getColDisplayType().equalsIgnoreCase("d")) {
//							whereClause.append(columnName + " "+ reportsObj.getOperator() + " CONVERT(date, '" + value1 + "' ,'"+ reportsObj.getDynamicDateFormat() + "') ");
							whereClause.append(columnName + " "+ reportsObj.getOperator() + " "+reportsObj.getDateConversionSyntax().replaceAll("#VALUE#", "'"+value1+"'"));
						} else{
							whereClause.append("UPPER("+columnName + ") "+ reportsObj.getOperator() + " UPPER('" + value1 + "')");
						}
					}
					whereColumnIndex++;
				} 
//				Form HAVING Clause Condition
				if(ValidationUtil.isValid(reportsObj.getAggFunction()) && CommonUtils.isAggrigateFunction(reportsObj.getAggFunction()) && ValidationUtil.isValid(reportsObj.getSummaryCriteria())) {
					if((!ValidationUtil.isValid(reportsObj.getSummaryValue1()) && !ValidationUtil.isValid(reportsObj.getSummaryValue2())) && (!reportsObj.getSummaryCriteria().equalsIgnoreCase("is null") && !reportsObj.getSummaryCriteria().equalsIgnoreCase("is not null"))&&(ValidationUtil.isValid(reportsObj.getSummaryDynamicStartFlag()) && !reportsObj.getSummaryDynamicStartFlag().equalsIgnoreCase("y") ) )
						continue;
						
					if (havingColumnIndex > 0) {
						havingClause.append(" AND ");
					}
					
					String columnName = reportsObj.getTabelAliasName()+"."+reportsObj.getColName();
					if(reportsObj.getColDisplayType().equalsIgnoreCase("d")) {
						if("COUNT(DISTINCT)".equalsIgnoreCase(reportsObj.getAggFunction()))
							columnName = reportsObj.getDateConversionSyntax().replaceAll("#VALUE#", "COUNT (DISTINCT "+columnName+")");
						else
							columnName = reportsObj.getDateConversionSyntax().replaceAll("#VALUE#", reportsObj.getAggFunction() + " (" +columnName+ ")");
					} else if(ValidationUtil.isValid(reportsObj.getAggFunction())) {
						if("COUNT(DISTINCT)".equalsIgnoreCase(reportsObj.getAggFunction()))
							columnName = "COUNT (DISTINCT "+columnName+")";
						else
							columnName = reportsObj.getAggFunction() + " (" +columnName+ ")";
					}
					
					if(reportsObj.getSummaryCriteria().equalsIgnoreCase("between")) {
						String value1= ("y".equalsIgnoreCase(reportsObj.getSummaryDynamicStartFlag()))?generateDynamicDate(reportsObj.getSummaryDynamicStartDate(),reportsObj.getSummaryDynamicStartOperator(),reportsObj.getDynamicDateFormat(),reportsObj.getJavaFormatDesc()):reportsObj.getSummaryValue1();
						String value2= ("y".equalsIgnoreCase(reportsObj.getSummaryDynamicEndFlag()))?generateDynamicDate(reportsObj.getSummaryDynamicEndDate(),reportsObj.getSummaryDynamicEndOperator(),reportsObj.getDynamicDateFormat(),reportsObj.getJavaFormatDesc()):reportsObj.getSummaryValue2();
						if("D".equalsIgnoreCase(reportsObj.getColDisplayType())) {
							havingClause.append(columnName+" "+reportsObj.getSummaryCriteria()+" "+reportsObj.getDateConversionSyntax().replaceAll("#VALUE#", "'"+value1+"'")+" AND "+  reportsObj.getDateConversionSyntax().replaceAll("#VALUE#", "'"+value2+"'"));
						} else if("N".equalsIgnoreCase(reportsObj.getColDisplayType()) || "C".equalsIgnoreCase(reportsObj.getColDisplayType())) {
							havingClause.append(columnName+" "+reportsObj.getSummaryCriteria()+" '"+value1+"' AND '"+value2+"' ");
						} else {
							havingClause.append(columnName+" "+reportsObj.getSummaryCriteria()+" '"+value1+"' AND '"+value2+"' ");
			     		}
				    } else {
						String value1= ("y".equalsIgnoreCase(reportsObj.getSummaryDynamicStartFlag()))?generateDynamicDate(reportsObj.getSummaryDynamicStartDate(),reportsObj.getSummaryDynamicStartOperator(),reportsObj.getDynamicDateFormat(),reportsObj.getJavaFormatDesc()):reportsObj.getSummaryValue1();
						if (reportsObj.getSummaryCriteria().equalsIgnoreCase("in") || reportsObj.getSummaryCriteria().equalsIgnoreCase("not in")) {
							havingClause.append("UPPER("+columnName + ") "+ reportsObj.getSummaryCriteria() +" "+ getInOperatorValues(value1,reportsObj.getColDisplayType(), reportsObj.getDateConversionSyntax()));
						} else if(reportsObj.getSummaryCriteria().equalsIgnoreCase("is null") || reportsObj.getSummaryCriteria().equalsIgnoreCase("is not null") ) {
							havingClause.append(columnName + " "+ reportsObj.getSummaryCriteria()+" ");
						} else if (reportsObj.getColDisplayType().equalsIgnoreCase("d")) {
							havingClause.append(columnName + " "+ reportsObj.getSummaryCriteria() + " "+reportsObj.getDateConversionSyntax().replaceAll("#VALUE#", "'"+value1+"'"));
						} else {
							havingClause.append(columnName + " "+ reportsObj.getSummaryCriteria() + " '" + value1 + "'");
						}
					}
					havingColumnIndex++;
				}
			}
			executionSql.append(clonedObject.getSelect());
			executionSql.append(" FROM "+joinFromStr);
			if(1 == joinType) {
				if(ValidationUtil.isValid(whereClause.toString())) {
					executionSql.append(" WHERE "+whereClause);
				}
			} else {
				if(ValidationUtil.isValid(joinWhereStr)) {
					executionSql.append(" WHERE "+joinWhereStr);
					if(ValidationUtil.isValid(whereClause.toString())) {
						executionSql.append(" AND "+whereClause);
					}
				} else if(ValidationUtil.isValid(whereClause.toString())) {
					executionSql.append(" WHERE "+whereClause);
				}
			}
			
			if(havingColumnIndex>0) {
				executionSql.append(" HAVING "+havingClause);
			}
			
			/*System.out.println("clonedObject.getWhere()......"+clonedObject.getWhere());
			System.out.println("whereClause.................."+whereClause);*/

			if(ValidationUtil.isValid(clonedObject.getGroupBy())) {
				executionSql.append(" GROUP BY "+clonedObject.getGroupBy());
			}
			if ("ORACLE".equalsIgnoreCase(databaseType)) {
				if (ValidationUtil.isValid(clonedObject.getOrderBy())) {
					executionSql.append(" ORDER BY " + clonedObject.getOrderBy());
				}
			}
			
			executionSql = new StringBuffer(new DesignAnalysisDao(jdbcTemplate).returnParsedStagingQuery(String.valueOf(executionSql), "sample", hashArray, hashValueArray));
			
			ExceptionCode viewExceptionCode = generateTableWithStaggingTableLog(0);
			
			if(viewExceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				responceTableName = String.valueOf(viewExceptionCode.getResponse());
			} else {
				viewExceptionCode.setErrorMsg("Problem in creating and logging Data Lake");
				return viewExceptionCode;
			}
			
//				tablesToDrop.add(tempTableName);
			exceptionCode = formTableScriptWithAliasName(vcForQueryReportFieldsWrapperVb, tableDetailsHM, responceTableName, String.valueOf(executionSql));
			if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				return exceptionCode;
			}
			
			Map<String, String> tableScriptAndInsertionString = (Map<String, String>) exceptionCode.getResponse();
			
			exceptionCode =  new DesignAnalysisDao(jdbcTemplate).executeSqlString(tableScriptAndInsertionString.get("createString")); // Create table
			if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				return exceptionCode;
			}
			
			String insertQuery = tableScriptAndInsertionString.get("insertStringWithDirectSelect");
			
			exceptionCode =  new DesignAnalysisDao(jdbcTemplate).executeSqlString(insertQuery); // Insert Records into table
			if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				return exceptionCode;
			}
			if (isJsonResponceRequired) {
				vcForQueryReportFieldsWrapperVb.getMainModel().setTableName(responceTableName);
				return new DesignAnalysisDao(jdbcTemplate).formResponceJsonForExecute(vcForQueryReportFieldsWrapperVb, responceTableName);
			}
			exceptionCode.setResponse(responceTableName);

		} catch (Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		} finally {
			
			if (isTableDropRequired && ValidationUtil.isValid(responceTableName))
				new DesignAnalysisDao(jdbcTemplate).dropTable(responceTableName);
			
			tablesToDrop.forEach(tableName -> new DesignAnalysisDao(jdbcTemplate).dropTable(tableName));
			
		}
		return exceptionCode;
	}
	
	public String generateDynamicDate(String dynamicDate, Integer dynamicOperator,String dateFormat,String javaFormat) {
		SimpleDateFormat df = new SimpleDateFormat(javaFormat);
		Calendar cal = Calendar.getInstance();
		System.out.println(df.format(cal.getTime()));

		switch (dynamicDate.toLowerCase()) {
		case ("d"): {
			cal.add(Calendar.DATE, dynamicOperator);
			break;
		}
		case ("m"): {
			cal.add(Calendar.MONTH, dynamicOperator);
			break;
		}
		case ("y"): {
			cal.add(Calendar.YEAR, dynamicOperator);
			break;
		  }
		}
		return df.format(cal.getTime());
	}

	private ExceptionCode generateTableWithStaggingTableLog(int attemptIndex) {
		ExceptionCode exceptionCode = new ExceptionCode();
		String tableName = new DesignAnalysisDao(jdbcTemplate).createRandomTableName();
		try {
			if(attemptIndex<20) {
				int result = new DesignAnalysisDao(jdbcTemplate).insertRecordIntoStaggingTableLog(tableName);
				if(result == Constants.SUCCESSFUL_OPERATION) {
					exceptionCode.setResponse(tableName);
					exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					return exceptionCode;
				} else {
					return generateTableWithStaggingTableLog(++attemptIndex);
				}
			} else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			return exceptionCode;
		}
	}
	
	public ExceptionCode formTableScriptWithAliasName(VcForQueryReportFieldsWrapperVb mainWrapperVb, HashMap<String,VcConfigMainTreeVb> tableDetailsHM, String targetTableName, String executionSql) {
		String level = "Forming table creation scripts";
		ExceptionCode exceptionCode = new ExceptionCode();
		StringBuffer createStr = new StringBuffer("CREATE TABLE "+targetTableName+" ( ");
		StringBuffer insertStr = new StringBuffer("INSERT INTO "+targetTableName+" ( ");
		StringBuffer selectStrForDirectInsert = new StringBuffer("SELECT ");
		final String COMMA = ",";
		final String SPACE = " ";
		
//		final String CHAR_STRING = " CHAR"; Commented bcz of fixed length - DD
		
		
		
		
		final String NUMBER_MAX_LENGTH = "38";
		
		final String DECIMAL_MAX_LENGTH = "38,10";
		final String CHAR_MAX_LENGTH = "2000";
		final String openBraket = "(";
		final String closeBraket = ")";
		String NUMBER_STRING = "";
		String DATE_STRING = "";
		String VARCHAR_MAX_LENGTH = "";
		String BYTE_STRING = "";
		String VARCHAR_STRING = "";
		String CHAR_STRING = "";
		String DECIMAL_STRING = "";
		if("ORACLE".equalsIgnoreCase(databaseType)) {
			NUMBER_STRING = " NUMBER";
			DATE_STRING = " DATE ";
			BYTE_STRING = " BYTE";
			VARCHAR_MAX_LENGTH = "4000";
			VARCHAR_STRING = " VARCHAR2";
			DECIMAL_STRING = " NUMBER";
			CHAR_STRING = " VARCHAR2";
		} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
			NUMBER_STRING = " NUMERIC";
			CHAR_STRING = " VARCHAR";
			DECIMAL_STRING = " NUMERIC";
			VARCHAR_STRING = " VARCHAR";
			DATE_STRING = " DATETIME2 ";
			VARCHAR_MAX_LENGTH = "8000";
		}
			
		try {
			int countIndex = 1;
			for(VcForQueryReportFieldsVb reportFieldVb : mainWrapperVb.getReportFields()) {
				
				if("Y".equalsIgnoreCase(reportFieldVb.getDisplayFlag())) {
					VcConfigMainTreeVb treeVb = tableDetailsHM.get(reportFieldVb.getTabelId());
					VcConfigMainColumnsVb columnVb = treeVb.getChildren().stream().filter(columnsVb -> reportFieldVb.getColId().equalsIgnoreCase(columnsVb.getColId())).findFirst().get();
					String length = columnVb.getColLength();
					createStr.append(reportFieldVb.getAlias()+SPACE);
					if("C".equalsIgnoreCase(columnVb.getColDisplayType())) {
						createStr.append(NUMBER_STRING+openBraket+(ValidationUtil.isValid(length)?length:NUMBER_MAX_LENGTH)+closeBraket);
					} else if("D".equalsIgnoreCase(columnVb.getColDisplayType())) {
						createStr.append(DATE_STRING);
					} else if("N".equalsIgnoreCase(columnVb.getColDisplayType())) {
						createStr.append(DECIMAL_STRING+openBraket+(ValidationUtil.isValid(length)?length:DECIMAL_MAX_LENGTH)+closeBraket);
					} else if("T".equalsIgnoreCase(columnVb.getColDisplayType())) {
						createStr.append(CHAR_STRING+openBraket+(ValidationUtil.isValid(length)?length:CHAR_MAX_LENGTH)+SPACE+BYTE_STRING+closeBraket);
					} else if("Y".equalsIgnoreCase(columnVb.getColDisplayType())) {
						createStr.append(VARCHAR_STRING+openBraket+(ValidationUtil.isValid(length)?length:VARCHAR_MAX_LENGTH)+SPACE+BYTE_STRING+closeBraket);
					} else {
						throw new RuntimeCustomException("No proper data-type maintained for the column ["+columnVb.getAliasName()+"] of table ["+treeVb.getAliasName()+"] from catalog ["+treeVb.getCatalogId()+"]");
					}
					if("ORACLE".equalsIgnoreCase(databaseType)) {
						if("T".equalsIgnoreCase(columnVb.getColType())) {
						if("D".equalsIgnoreCase(columnVb.getColDisplayType())) {
							selectStrForDirectInsert.append("TO_DATE(" + reportFieldVb.getAlias() + ", '"
									+ columnVb.getFormatTypeDesc() + "')" + SPACE + reportFieldVb.getAlias());
						} else {
							selectStrForDirectInsert.append(reportFieldVb.getAlias());
						}
					} else {
						/*if("D".equalsIgnoreCase(columnVb.getColDisplayType())) {
							selectStrForDirectInsert.append("TO_DATE("+openBraket+columnVb.getExperssionText()+closeBraket+", '"+columnVb.getFormatTypeDesc()+"')"+SPACE+reportFieldVb.getAlias());
						} else {
							selectStrForDirectInsert.append(openBraket + columnVb.getExperssionText() + closeBraket + SPACE + reportFieldVb.getAlias());
						}*/
						if("D".equalsIgnoreCase(columnVb.getColDisplayType())) {
							selectStrForDirectInsert.append("TO_DATE(" + reportFieldVb.getAlias() + ", '"
									+ columnVb.getFormatTypeDesc() + "')" + SPACE + reportFieldVb.getAlias());
						} else {
							selectStrForDirectInsert.append(reportFieldVb.getAlias());
						}
					}
					} else if ("MSSQL".equalsIgnoreCase(databaseType)) {
						if("T".equalsIgnoreCase(columnVb.getColType())) {
							if("D".equalsIgnoreCase(columnVb.getColDisplayType())) {
		//							selectStrForDirectInsert.append("CONVERT(date, "+reportFieldVb.getAlias()+", '"+columnVb.getFormatTypeDesc()+"')"+SPACE+reportFieldVb.getAlias());
								selectStrForDirectInsert.append(columnVb.getDateConversionSyntax().replaceAll("#VALUE#", reportFieldVb.getAlias())+SPACE+reportFieldVb.getAlias());
							} else {
								selectStrForDirectInsert.append(reportFieldVb.getAlias());
							}
					} else {
						/*if("D".equalsIgnoreCase(columnVb.getColDisplayType())) {
							selectStrForDirectInsert.append("TO_DATE("+openBraket+columnVb.getExperssionText()+closeBraket+", '"+columnVb.getFormatTypeDesc()+"')"+SPACE+reportFieldVb.getAlias());
						} else {
							selectStrForDirectInsert.append(openBraket + columnVb.getExperssionText() + closeBraket + SPACE + reportFieldVb.getAlias());
						}*/
						if("D".equalsIgnoreCase(columnVb.getColDisplayType())) {
//							selectStrForDirectInsert.append("CONVERT(date, "+reportFieldVb.getAlias()+", '"+columnVb.getFormatTypeDesc()+"')"+SPACE+reportFieldVb.getAlias());
							selectStrForDirectInsert.append(columnVb.getDateConversionSyntax().replaceAll("#VALUE#", reportFieldVb.getAlias())+SPACE+reportFieldVb.getAlias());
						} else {
							selectStrForDirectInsert.append(reportFieldVb.getAlias());
						}
					}

					}
					
					insertStr.append(reportFieldVb.getAlias());
					
					createStr.append(COMMA+SPACE);
					selectStrForDirectInsert.append(COMMA+SPACE);
					insertStr.append(COMMA+SPACE);
				}
				
				if(countIndex == mainWrapperVb.getReportFields().size()) {
					createStr = new StringBuffer(createStr.substring(0, createStr.lastIndexOf(",")));
					createStr.append(closeBraket);
					selectStrForDirectInsert = new StringBuffer(selectStrForDirectInsert.substring(0, selectStrForDirectInsert.lastIndexOf(",")));
					selectStrForDirectInsert.append(SPACE+"FROM"+SPACE+openBraket+executionSql+closeBraket);
					insertStr = new StringBuffer(insertStr.substring(0, insertStr.lastIndexOf(",")));
					insertStr.append(closeBraket);
				} 
				countIndex++;
			}
			
			Map<String, String> returnMap = new HashMap<String, String>(); 
			returnMap.put("createString", String.valueOf(createStr));
			returnMap.put("selectStrForDirectInsert", String.valueOf(selectStrForDirectInsert));
			returnMap.put("insertColumnString", String.valueOf(insertStr));
			if("ORACLE".equalsIgnoreCase(databaseType))
				returnMap.put("insertStringWithDirectSelect", String.valueOf(insertStr+SPACE+selectStrForDirectInsert));
			else if("MSSQL".equalsIgnoreCase(databaseType))
				returnMap.put("insertStringWithDirectSelect", String.valueOf(insertStr+SPACE+selectStrForDirectInsert+ " TEMP"));

			exceptionCode.setResponse(returnMap);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Exception during "+level+". Cause ["+e.getMessage()+"].");
		}
		exceptionCode.setOtherInfo(targetTableName);
		return exceptionCode;
	}
	
	private ExceptionCode parseFromStringWithActualTableName(HashMap<String,VcConfigMainTreeVb> tableDetailsHM, String fromString, String hashArray[], String hashValueArray[]) {
		ExceptionCode exceptionCode = new ExceptionCode();
		List<String> tablesToDrop = new ArrayList<String>();
		try {
			
			Map<String, String> tableIdMapWithReplacePattern = new HashMap<String, String>();
			tableIdMapWithReplacePattern = identifyDynamicTableId(fromString, "F");
			if(tableIdMapWithReplacePattern.size()>0) {
				exceptionCode = findAndReplaceFileToDynamicTableInFromString(tableDetailsHM, fromString, tableIdMapWithReplacePattern);
				tablesToDrop.addAll((List<String>) exceptionCode.getOtherInfo());
				if(exceptionCode.getErrorCode()!=Constants.SUCCESSFUL_OPERATION) {
					exceptionCode.setOtherInfo(tablesToDrop);
					return exceptionCode;
				} else {
					fromString = (String) exceptionCode.getResponse();
				}
			}
			
			tableIdMapWithReplacePattern = identifyDynamicTableId(fromString, "DB");
			if(tableIdMapWithReplacePattern.size()>0) {
				exceptionCode = findAndReplaceCreatedTempTableInFromString(tableDetailsHM, fromString, tableIdMapWithReplacePattern);
				tablesToDrop.addAll((List<String>) exceptionCode.getOtherInfo());
				if(exceptionCode.getErrorCode()!=Constants.SUCCESSFUL_OPERATION) {
					exceptionCode.setOtherInfo(tablesToDrop);
					return exceptionCode;
				} else {
					fromString = (String) exceptionCode.getResponse();
				}
			}
			
			tableIdMapWithReplacePattern = identifyDynamicTableId(fromString, "MQ");
			if(tableIdMapWithReplacePattern.size()>0) {
				exceptionCode = findAndReplaceCreatedTempTableInFromStringForManualQuery(tableDetailsHM, fromString, tableIdMapWithReplacePattern, hashArray, hashValueArray);
				tablesToDrop.addAll((List<String>) exceptionCode.getOtherInfo());
				if(exceptionCode.getErrorCode()!=Constants.SUCCESSFUL_OPERATION) {
					exceptionCode.setOtherInfo(tablesToDrop);
					return exceptionCode;
				} else {
					fromString = (String) exceptionCode.getResponse();
				}
			}
			
			exceptionCode.setResponse(fromString);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		exceptionCode.setOtherInfo(tablesToDrop);
		return exceptionCode;
	}
	
	private ExceptionCode findAndReplaceCreatedTempTableInFromString(HashMap<String,VcConfigMainTreeVb> tableDetailsHM, String fromString, Map<String, String> tableIdMapWithReplacePattern) {
		ExceptionCode exceptionCode = new ExceptionCode();
		List<String> tablesToDrop = new ArrayList<String>();
		for(Map.Entry<String, String> entry : tableIdMapWithReplacePattern.entrySet()) {
			
			VcConfigMainTreeVb treeVb = tableDetailsHM.get(entry.getKey());
			
			String tempTableName = new DesignAnalysisDao(jdbcTemplate).createRandomTableName();
			tablesToDrop.add(tempTableName);
			
			String connectionScript = new DesignAnalysisDao(jdbcTemplate).fetchConnectionScriptFromVisionDynamicHashVariable(treeVb.getDatabaseConnectivityDetails());
			if(!ValidationUtil.isValid(connectionScript)) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setOtherInfo(tablesToDrop);
				return exceptionCode;
			}
			
			exceptionCode = new DesignAnalysisDao(jdbcTemplate).createTempTableWithConnectionScript(connectionScript, treeVb, tempTableName);
			if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode.setOtherInfo(tablesToDrop);
				return exceptionCode;
			}
			fromString = fromString.replaceAll(entry.getValue(), tempTableName);
			exceptionCode.setResponse(fromString);
		}
		exceptionCode.setOtherInfo(tablesToDrop);
		return exceptionCode;
	}
	
	private ExceptionCode findAndReplaceCreatedTempTableInFromStringForManualQuery(
			HashMap<String, VcConfigMainTreeVb> tableDetailsHM, String fromString,
			Map<String, String> tableIdMapWithReplacePattern, String[] hashArray, String[] hashValueArray) {
		ExceptionCode exceptionCode = new ExceptionCode();
		List<String> tablesToDrop = new ArrayList<String>();
		for(Map.Entry<String, String> entry : tableIdMapWithReplacePattern.entrySet()) {
			
			VcConfigMainTreeVb treeVb = tableDetailsHM.get(entry.getKey());
			
			DCManualQueryVb mQueryVb = new DesignAnalysisDao(jdbcTemplate).fetchManualQueryDetailFromVcQueries(treeVb.getQueryId());
			if(mQueryVb == null) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("No Record found in VC_QUERIES for Query ID ["+treeVb.getQueryId()+"]");
				exceptionCode.setOtherInfo(tablesToDrop);
				return exceptionCode;
			}
			
			String tempTableName = new DesignAnalysisDao(jdbcTemplate).createRandomTableName();
			tablesToDrop.add(tempTableName);
			
			String connectionScript = new DesignAnalysisDao(jdbcTemplate).fetchConnectionScriptFromVisionDynamicHashVariable(mQueryVb.getDatabaseConnectivityDetails());
			if(!ValidationUtil.isValid(connectionScript)) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setOtherInfo(tablesToDrop);
				return exceptionCode;
			}
			exceptionCode = new DesignAnalysisDao(jdbcTemplate).createTempTableWithConnectionScriptForManualQuery(mQueryVb, connectionScript, treeVb, tempTableName, hashArray, hashValueArray);
			tablesToDrop.addAll((List<String>) exceptionCode.getOtherInfo());
			if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode.setOtherInfo(tablesToDrop);
				return exceptionCode;
			}
			fromString = fromString.replaceAll(entry.getValue(), String.valueOf(exceptionCode.getResponse()));
			exceptionCode.setResponse(fromString);
		}
		exceptionCode.setOtherInfo(tablesToDrop);
		return exceptionCode;
	}
	
	private ExceptionCode findAndReplaceFileToDynamicTableInFromString(HashMap<String,VcConfigMainTreeVb> tableDetailsHM, String fromString, Map<String, String> tableIdMapWithReplacePattern) {
		ExceptionCode exceptionCode = new ExceptionCode();
		List<String> tablesToDrop = new ArrayList<String>();
		for(Map.Entry<String, String> entry : tableIdMapWithReplacePattern.entrySet()) {
			VcConfigMainTreeVb treeVb = tableDetailsHM.get(entry.getKey());
			
			String tempTableName = new DesignAnalysisDao(jdbcTemplate).createRandomTableName();
			tablesToDrop.add(tempTableName);
			
			String dynamicTableName = new DesignAnalysisDao(jdbcTemplate).fetchRecordFromConnectorFileUploadMapper(treeVb.getDatabaseConnectivityDetails(), treeVb.getTableName());
			
			exceptionCode = new DesignAnalysisDao(jdbcTemplate).createTempTableForFile(treeVb, dynamicTableName, tempTableName);
			
			if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode.setOtherInfo(tablesToDrop);
				return exceptionCode;
			}
			
			fromString = fromString.replaceAll(entry.getValue(), tempTableName);
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setResponse(fromString);
		exceptionCode.setOtherInfo(tablesToDrop);
		return exceptionCode;
	}
	
	private Map<String, String> identifyDynamicTableId(String fromString, String switchCase) {
//		switchCase = "F" for file
//		switchCase = "DB" for other database connection
//		switchCase = "MQ" for manual query
		Map<String, String> tableIdMap = new HashMap<String, String>();
		Pattern patternObj = Pattern.compile("\\{\\{"+switchCase+"_\\((.*?)\\)\\}\\}", Pattern.DOTALL);
		Matcher matchObj = patternObj.matcher(fromString);
		while(matchObj.find()) {
			tableIdMap.put(matchObj.group(1).substring(matchObj.group(1).lastIndexOf("_")+1, matchObj.group(1).length()), "\\{\\{"+switchCase+"_\\("+matchObj.group(1)+"\\)\\}\\}") ;
		}
		return tableIdMap;
	}
	
	private void parseQueryWithActualAliasName(VcForQueryReportVb vObj, List<String> uniqueTableIdArray, HashMap<String,VcConfigMainTreeVb> tableDetailsHM) {
		
		HashMap<String,String> replaceMap = new HashMap<String,String>();
		
		uniqueTableIdArray.forEach(tableId -> replaceMap.put("\\$\\{#TABLE_ALIAS_"+tableId+"#\\}", tableDetailsHM.get(tableId).getAliasName()));
		
		for (Map.Entry<String, String> entry : replaceMap.entrySet()) {
			vObj.setSelect(ValidationUtil.isValid(vObj.getSelect())?vObj.getSelect().replaceAll(entry.getKey(), entry.getValue()):"");
			vObj.setWhere(ValidationUtil.isValid(vObj.getWhere())?vObj.getWhere().replaceAll(entry.getKey(), entry.getValue()):"");
			vObj.setGroupBy(ValidationUtil.isValid(vObj.getGroupBy())?vObj.getGroupBy().replaceAll(entry.getKey(), entry.getValue()):"");
			vObj.setOrderBy(ValidationUtil.isValid(vObj.getOrderBy())?vObj.getOrderBy().replaceAll(entry.getKey(), entry.getValue()):"");
		}
		
	}
	
	@SuppressWarnings("rawtypes")
	public ExceptionCode returnJoinCondition(VcForQueryReportVb vObj, List<String> uniqueTableIdArray, Integer joinType, ArrayList getDataAL) {
		ExceptionCode exceptionCode = new ExceptionCode();
		new DesignAnalysisDao(jdbcTemplate).setServiceDefaults();
		try {
			List<Integer> intTblIdList = uniqueTableIdArray.stream().mapToInt(e -> Integer.parseInt(e)).boxed().collect(Collectors.toList());
			Collections.sort(intTblIdList);
			uniqueTableIdArray = intTblIdList.stream().map(v -> String.valueOf(v)).collect(Collectors.toList());
			exceptionCode.setResponse(DynamicJoinNew.formDynamicJoinString(vObj.getCatalogId(), joinType, uniqueTableIdArray, getDataAL, vObj.getBaseTableId()));
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			return exceptionCode;
		} catch (Exception e) {
			exceptionCode = CommonUtils.getResultObject(new DesignAnalysisDao(jdbcTemplate).getServiceName(), Constants.ERRONEOUS_OPERATION, "", "Exception in getting dynamic join string");
			return exceptionCode;
		}
	}
	public List<VcForQueryReportVb> getAnalysisList() {
		try {
			List<VcForQueryReportVb> arrListResult= new DesignAnalysisDao(jdbcTemplate).getQueryAnalysisList();
			return arrListResult;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
		
	}
	public List<VcForQueryReportVb> getReportsList() {
		try {
			List<VcForQueryReportVb> arrListResult= new DesignAnalysisDao(jdbcTemplate).getQueryReportsList();
			return arrListResult;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
	public List<Map<String, Object>> getInteractiveDashboardList() {
		try {
			List<Map<String, Object>>  result= new DesignAnalysisDao(jdbcTemplate).getQueryInteractiveDashboardList();
			if (result.size() > 0) {
                return result;
            } else {
                return null;
            }
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
	public ExceptionCode executeManualQueryForManifierDetails(DCManualQueryVb vObj, String dbScript, String[] hashArr, String[] hashValArr, DesignAndAnalysisMagnifierVb magnefierVb){
		ExceptionCode exceptionCode = new ExceptionCode();
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		String level = "";
		exceptionCode = CommonUtils.getConnection(dbScript);
		if(exceptionCode.getErrorCode()==Constants.SUCCESSFUL_OPERATION){
			con = (Connection) exceptionCode.getResponse();
		}else{
			return exceptionCode;
		}
		String dbSetParam1 = CommonUtils.getValue(dbScript, "DB_SET_PARAM1");
		String dbSetParam2 = CommonUtils.getValue(dbScript, "DB_SET_PARAM2");
		String dbSetParam3 = CommonUtils.getValue(dbScript, "DB_SET_PARAM3");
		String stgQuery = "";
		String sessionId = String.valueOf(System.currentTimeMillis());
		String stgTableName1 = "TVC_"+sessionId+"_STG_1";
		String stgTableName2 = "TVC_"+sessionId+"_STG_2";
		String stgTableName3 = "TVC_"+sessionId+"_STG_3";
		String sqlMainQuery = "";
		try{
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			if(ValidationUtil.isValid(dbSetParam1)){
				level = "DB Param 1";
				stmt.executeUpdate(dbSetParam1);
			}
			if(ValidationUtil.isValid(dbSetParam2)){
				level = "DB Param 2";
				stmt.executeUpdate(dbSetParam2);
			}
			if(ValidationUtil.isValid(dbSetParam3)){
				level = "DB Param 3";
				stmt.executeUpdate(dbSetParam3);
			}
			Pattern pattern = Pattern.compile("#(.*?)#");
			Matcher matcher = null;
			if(ValidationUtil.isValid(vObj.getStgQuery1())){
				stgQuery = vObj.getStgQuery1();
				matcher = pattern.matcher(stgQuery);
				while(matcher.find()){
					if("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName1);
					if("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName2);
					if("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName3);
				}
				level = "Staging 1";
				stgQuery = CommonUtils.replaceHashTag(stgQuery, hashArr, hashValArr);
				stmt.executeUpdate(stgQuery);
			}
			if(ValidationUtil.isValid(vObj.getStgQuery2())){
				stgQuery = vObj.getStgQuery2();
				matcher = pattern.matcher(stgQuery);
				while(matcher.find()){
					if("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName1);
					if("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName2);
					if("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName3);
				}
				level = "Staging 2";
				stgQuery = CommonUtils.replaceHashTag(stgQuery, hashArr, hashValArr);
				stmt.executeUpdate(stgQuery);
			}
			if(ValidationUtil.isValid(vObj.getStgQuery3())){
				stgQuery = vObj.getStgQuery3();
				matcher = pattern.matcher(stgQuery);
				while(matcher.find()){
					if("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName1);
					if("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName2);
					if("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName3);
				}
				level = "Staging 3";
				stgQuery = CommonUtils.replaceHashTag(stgQuery, hashArr, hashValArr);
				stmt.executeUpdate(stgQuery);
			}
			sqlMainQuery = vObj.getSqlQuery();
			matcher = pattern.matcher(sqlMainQuery);
			while(matcher.find()){
				if("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
					sqlMainQuery = sqlMainQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName1);
				if("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
					sqlMainQuery = sqlMainQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName2);
				if("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
					sqlMainQuery = sqlMainQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName3);
				else
					sqlMainQuery = sqlMainQuery.replaceAll("#"+matcher.group(1)+"#", "#"+String.valueOf(matcher.group(1)).toUpperCase().replaceAll("\\s", "\\_")+"#");
			}
			level = "Main Query";
			
			LinkedHashMap<String,String> before2AfterChangeWordMap = new LinkedHashMap<String,String>();
			LinkedHashMap<String,String> before2AfterChangeIndexMap = new LinkedHashMap<String,String>();
			int index = 0;
			
			if(hashArr!=null && hashValArr!=null && hashArr.length==hashValArr.length){
				for(String variable:hashArr){
					int varGuessIndex = sqlMainQuery.indexOf("#"+variable+"#");
					while(varGuessIndex >= 0){
						String preString = sqlMainQuery.substring(0, varGuessIndex);
						String wholeWordBefChange = "";
						String wholeWordAfterChange = "";
						int startIndexOfMainSQL = 0;
						startIndexOfMainSQL = preString.indexOf(" ")!=-1?preString.lastIndexOf(" "):-1;
						int endIndexOfMainSQL = 0;
						endIndexOfMainSQL = sqlMainQuery.indexOf(" ",varGuessIndex+1);
						
						/* Get whole word  */
						wholeWordBefChange = (startIndexOfMainSQL != -1 && endIndexOfMainSQL != -1)
								? sqlMainQuery.substring(startIndexOfMainSQL, endIndexOfMainSQL)
								: (startIndexOfMainSQL == -1 && endIndexOfMainSQL != -1)
										? sqlMainQuery.substring(0, endIndexOfMainSQL)
										: sqlMainQuery.substring(startIndexOfMainSQL, sqlMainQuery.length());
						wholeWordBefChange = wholeWordBefChange.trim();
						wholeWordAfterChange = wholeWordBefChange.replaceFirst("#"+hashArr[index]+"#", hashValArr[index]).trim();
						
						String storingIndex = "";
						if(before2AfterChangeIndexMap.get(wholeWordAfterChange)!=null)
							storingIndex = before2AfterChangeIndexMap.get(wholeWordAfterChange);
						storingIndex = storingIndex + varGuessIndex + ",";
						before2AfterChangeIndexMap.put(wholeWordAfterChange.toUpperCase(),storingIndex);
						before2AfterChangeWordMap.put(wholeWordAfterChange.toUpperCase(), wholeWordBefChange);
						
						/* change the value in main query */
						if(startIndexOfMainSQL!=-1  && endIndexOfMainSQL!=-1)
							sqlMainQuery = sqlMainQuery.substring(0,startIndexOfMainSQL) + " " + wholeWordAfterChange + " " + sqlMainQuery.substring(endIndexOfMainSQL,sqlMainQuery.length());
						else if(startIndexOfMainSQL==-1)
							sqlMainQuery = wholeWordAfterChange + " " + sqlMainQuery.substring(endIndexOfMainSQL,sqlMainQuery.length());
						else if(endIndexOfMainSQL==-1)
							sqlMainQuery = sqlMainQuery.substring(0,startIndexOfMainSQL) + " " + wholeWordAfterChange;
						
						varGuessIndex = sqlMainQuery.indexOf("#"+variable+"#", varGuessIndex + 1);
					}
					index++;
				}
			}
			sqlMainQuery = CommonUtils.replaceHashTag(sqlMainQuery, hashArr, hashValArr);
			
			
			StringBuffer queryForExecution = new StringBuffer();
			
			queryForExecution.append("SELECT "+magnefierVb.getUseColumn()+", "+magnefierVb.getDisplayColumn());
			if("ORACLE".equalsIgnoreCase(databaseType))
				queryForExecution.append(" FROM ("+sqlMainQuery+")");
			else if("MSSQL".equalsIgnoreCase(databaseType))
				queryForExecution.append(" FROM ("+sqlMainQuery+") MANUAL_TEMP ");

			rs = stmt.executeQuery(String.valueOf(queryForExecution));
			
			List<Map<String, String>> returnList = new ArrayList<Map<String, String>>();
			while (rs.next()) {
				Map<String, String> responseMap = new HashMap<String, String>();
				responseMap.put(magnefierVb.getUseColumn(), rs.getString(magnefierVb.getUseColumn()));
				responseMap.put(magnefierVb.getDisplayColumn(), rs.getString(magnefierVb.getDisplayColumn()));
				returnList.add(responseMap);
			}
			
			exceptionCode.setResponse(returnList);
			
			if(ValidationUtil.isValid(vObj.getPostQuery())){
				stgQuery = vObj.getPostQuery();
				matcher = pattern.matcher(stgQuery);
				while(matcher.find()){
					if("TVC_SESSIONID_STG_1".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName1);
					if("TVC_SESSIONID_STG_2".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName2);
					if("TVC_SESSIONID_STG_3".equalsIgnoreCase(matcher.group(1)))
						stgQuery = stgQuery.replaceAll("#"+matcher.group(1)+"#", stgTableName3);
				}
				level = "Post Query";
				try{
					stgQuery = CommonUtils.replaceHashTag(stgQuery, hashArr, hashValArr);
					stmt.executeUpdate(stgQuery);
				}catch(Exception e){
					e.printStackTrace();
					exceptionCode.setOtherInfo(vObj);
					exceptionCode.setErrorCode(9999);
					exceptionCode.setErrorMsg("Warning - Post Query Execution Failed - Cause:"+e.getMessage());
					return exceptionCode;
				}
			}
		}catch (SQLException e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Validation failed at level - "+level+" - Cause:"+e.getMessage());
			exceptionCode.setOtherInfo(vObj);
			return exceptionCode;
		}catch(Exception e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("Validation failed at level - "+level+" - Cause:"+e.getMessage());
			exceptionCode.setOtherInfo(vObj);
			return exceptionCode;
		}finally{
			String purge = "";
			if("ORACLE".equalsIgnoreCase(databaseType))
				purge = " PURGE";
			
			try{
				if(ValidationUtil.isValid(stgTableName1))
					stmt.executeUpdate("DROP TABLE "+stgTableName1+purge);
			}catch(Exception e){}
			try{
				if(ValidationUtil.isValid(stgTableName2))
					stmt.executeUpdate("DROP TABLE "+stgTableName2+purge);
			}catch(Exception e){}
			try{
				if(ValidationUtil.isValid(stgTableName3))
					stmt.executeUpdate("DROP TABLE "+stgTableName3+purge);
			}catch(Exception e){}
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
		return exceptionCode;
	}
	
	public ExceptionCode getReportDetailFromReportDefs(VcForQueryReportVb vObject){
		System.out.println("Function Call success");
		ExceptionCode exceptionCode = new ExceptionCode();
		VcForQueryReportFieldsWrapperVb wrapperVb = new VcForQueryReportFieldsWrapperVb();
		try {
			List<VcForQueryReportVb> reportDetailList = new DesignAnalysisDao(jdbcTemplate).getReportDetailListingFromReportDefs(vObject);
			if(ValidationUtil.isValidList(reportDetailList)) {
				VcForQueryReportVb reportMainVb = reportDetailList.get(0);
				wrapperVb.setMainModel(reportMainVb);
				
				List<String> hashVar = new ArrayList<String>();
				List<String> hashValueVar = new ArrayList<String>();
				if(ValidationUtil.isValid(reportMainVb.getHashVariableScript())) {
					Matcher regexMatcher = Pattern.compile("\\{(.*?):#(.*?)\\$@!(.*?)\\#}", Pattern.DOTALL).matcher(reportMainVb.getHashVariableScript());
					while (regexMatcher.find()) {
						hashVar.add(regexMatcher.group(1));
						hashValueVar.add(regexMatcher.group(3));
					}
				}
				
				if(ValidationUtil.isValidList(hashVar)) {
					String[] hashArr = new String[hashVar.size()];
					hashVar.toArray(hashArr);
					
					String[] hashValArr = new String[hashValueVar.size()];
					hashValueVar.toArray(hashValArr);
					
					wrapperVb.setHashArray(hashArr);
					wrapperVb.setHashValueArray(hashValArr);
				}
				
				List<VcForQueryReportFieldsVb> reportFieldsList = new DesignAnalysisDao(jdbcTemplate).getReportFieldsDetailFromReportFields(vObject);
				if(ValidationUtil.isValidList(reportFieldsList)) {
					wrapperVb.setReportFields(reportFieldsList);
				} else {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					exceptionCode.setErrorMsg("No fields found for Report ID ["+vObject.getReportId()+"]");
				}
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			} else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("No records found for Report ID ["+vObject.getReportId()+"]");
			}
		} catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		exceptionCode.setResponse(wrapperVb);
		return exceptionCode;
	}
	
	public ExceptionCode getReportDetailListingFromReportDefs(VcForQueryReportVb vObj) throws DataAccessException {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			List<VcForQueryReportVb> reportInfoList = new DesignAnalysisDao(jdbcTemplate).getReportDetailListingFromReportDefs(vObj);
			if(ValidationUtil.isValidList(reportInfoList)) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setResponse(reportInfoList);
			} else {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setResponse(null);
			}
		} catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}		
		return exceptionCode;
	}

	public DesignAnalysisDao getDesignAnalysisDao() {
		return designAnalysisDao;
	}

	public void setDesignAnalysisDao(DesignAnalysisDao designAnalysisDao) {
		this.designAnalysisDao = designAnalysisDao;
	}
	
	public ExceptionCode deleteDesignQuery(DesignAnalysisVb designVb) {
		ExceptionCode exceptionCode = new ExceptionCode();
		List<WidgetDesignVb> mainWidgetsVb = new DesignAnalysisDao(jdbcTemplate).validatingMainWidgetsData(designVb.getVcqdQueryId());
		if(mainWidgetsVb.size()>0) {
			  exceptionCode.setErrorMsg("Design Query used in MainWidgets cannot delete");
			  exceptionCode.setResponse(mainWidgetsVb);
			  exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		  }else {
			  new DesignAnalysisDao(jdbcTemplate).moveMainDataToAD(designVb.getVcqdQueryId());
			  new DesignAnalysisDao(jdbcTemplate).deleteRecords("VCQD_QUERIES","VCQD_QUERY_ID",designVb.getVcqdQueryId());
			  new DesignAnalysisDao(jdbcTemplate).deleteRecords("VC_REPORT_DEFS_SELFBI","REPORT_ID",designVb.getVcqdQueryId());
			  new DesignAnalysisDao(jdbcTemplate).deleteRecords("VC_REPORT_FIELDS_SELFBI","REPORT_ID",designVb.getVcqdQueryId());
			  exceptionCode.setResponse(getAllDesignQuery(designVb));
			  exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		  }		  
		return exceptionCode;
	}

	/*public List<DesignAnalysisVb> getQuerySmartSearchFilter(DesignAnalysisVb designVb) {
		List<DesignAnalysisVb> arrListLocal = new ArrayList<DesignAnalysisVb>();
		try{
			setVerifReqDeleteType(designVb);
			doFormateDataForQuery(designVb);
			List<DesignAnalysisVb> arrListResult = new DesignAnalysisDao(jdbcTemplate).getQuerySmartSearchFilter(designVb);
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
	}*/
	
	public String getInOperatorValues(String value, String datatype, String dateConversionSyntax) {
		value = value.toUpperCase(); //To avoid enable insensitivity - DD
		String[] valueArray = value.split(",");
		StringBuffer invalues = new StringBuffer("(");
		for (int i = 0; i < valueArray.length; i++) {
			if (datatype.equalsIgnoreCase("d")) {
//				invalues.append("TO_DATE('" + valueArray[i].trim() + "' ,'" + format + "'), ");
				invalues.append(dateConversionSyntax.replaceAll("#VALUE#", "'"+valueArray[i].trim()+"'") + ", ");
			} else {
				invalues.append(valueArray[i] + ", ");
			}

		}
		return invalues.substring(0, invalues.length() - 2) + ")";
	}

}