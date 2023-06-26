package com.vision.wb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.dao.AbstractDao;
import com.vision.dao.AlphaSubTabDao;
import com.vision.dao.CommonDao;
import com.vision.dao.NumSubTabDao;
import com.vision.dao.VisionVariablesDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.DeepCopy;
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.CommonVb;
import com.vision.vb.LevelOfDisplayVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.ProfileData;
import com.vision.vb.ReviewResultVb;
import com.vision.vb.VisionVariablesVb;

@Component
public abstract class AbstractWorkerBean<E extends CommonVb> {
	
	public static Logger logger = LoggerFactory.getLogger(AbstractWorkerBean.class);
	@Autowired
	private NumSubTabDao numSubTabDao = new NumSubTabDao();
	@Autowired
	private AlphaSubTabDao alphaSubTabDao = new AlphaSubTabDao();
	@Autowired
	private CommonDao commonDao;
	
	@Autowired
	private VisionVariablesDao visionVariablesDao;
	
	public VisionVariablesDao getVisionVariablesDao() {
		return visionVariablesDao;
	}
	public void setVisionVariablesDao(VisionVariablesDao visionVariablesDao) {
		this.visionVariablesDao = visionVariablesDao;
	}
	
	public AlphaSubTabDao getAlphaSubTabDao() {
		return alphaSubTabDao;
	}
	public void setAlphaSubTabDao(AlphaSubTabDao alphaSubTabDao) {
		this.alphaSubTabDao = alphaSubTabDao;
	}
	public NumSubTabDao getNumSubTabDao() {
		return numSubTabDao;
	}
	public void setNumSubTabDao(NumSubTabDao numSubTabDao) {
		this.numSubTabDao = numSubTabDao;
	}
	public CommonDao getCommonDao() {
		return commonDao;
	}
	public void setCommonDao(CommonDao commonDao) {
		this.commonDao = commonDao;
	}
	
	protected abstract AbstractDao<E> getScreenDao(); 
	protected abstract void setAtNtValues(E vObject);
	protected abstract void setVerifReqDeleteType(E vObject);
	//Additional method for formating stuff to be implemented by child classes if required
	protected void doFormateData(E vObject){}
	protected void doFormateDataForQuery(E vObject){}
	protected List<ReviewResultVb> transformToReviewResults(List<E> approvedCollection, List<E> pendingCollection){
		return new ArrayList<ReviewResultVb>();
	}
	//Setting child or parent objects can be done here.
	protected void doSetDesctiptionsAfterQuery(E vObject) {}
	
	protected ExceptionCode doValidate(E vObject){
		ExceptionCode exceptionCode = null;
		return exceptionCode;
	}
	/**
	 * Returns results matching the criteria for the query popup window.
	 * @param queryPopupObj
	 * @return
	 */
	public List<E> getQueryPopupResults(E queryPopupObj){
		List<E> arrListLocal = new ArrayList<E>();
		try{
			setVerifReqDeleteType(queryPopupObj);
			doFormateDataForQuery(queryPopupObj);
			List<E> arrListResult = getScreenDao().getQueryPopupResults(queryPopupObj);
			if(arrListResult == null){
				arrListLocal.add(queryPopupObj);
			}else{
				arrListLocal.add(queryPopupObj);
				arrListLocal.addAll(arrListResult);
			}
			return arrListLocal;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception in getting the QueryPopup results.", ex);
			return null;
		}
	}
	/**
	 * Inserts a new record to the approved or pending tables depending on Verification Required flag of the 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode insertRecord(E vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		E clonedObject = null;
		try
		{
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			doFormateData(vObject);
			exceptionCode = doValidate(vObject);
			if(exceptionCode!=null && exceptionCode.getErrorMsg()!=""){
				return exceptionCode;
			}			
			if(!vObject.isVerificationRequired()){
				exceptionCode = getScreenDao().doInsertApprRecord(vObject);
			}else{
				exceptionCode = getScreenDao().doInsertRecord(vObject);
			}
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Insert Exception " + rex.getCode().getErrorMsg());
			logger.error( ((vObject==null)? "vObject is Null":vObject.toString()));
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	/**
	 * updates the existing record in the approved or pending tables depending on Verification Required flag of the 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode modifyRecord(E vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		E clonedObject = null;
		try{
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			doFormateData(vObject);
			exceptionCode = doValidate(vObject);
			if(exceptionCode!=null && exceptionCode.getErrorMsg()!=""){
				return exceptionCode;
			}
			if(!vObject.isVerificationRequired()){
				exceptionCode = getScreenDao().doUpdateApprRecord(vObject);
			}else{
				exceptionCode = getScreenDao().doUpdateRecord(vObject);
			}
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Modify Exception " + rex.getCode().getErrorMsg());
			logger.error( ((vObject==null)? "vObject is Null":vObject.toString()));
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	/**
	 * Delete the existing record in the approved or pending tables depending on Verification Required flag of the 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode deleteRecord(E vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		E clonedObject = null;
		try{
			setAtNtValues(vObject);
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			doFormateData(vObject);
			exceptionCode = doValidate(vObject);
			if(exceptionCode!=null && exceptionCode.getErrorMsg()!=""){
				return exceptionCode;
			}
			if(!vObject.isVerificationRequired()){
				exceptionCode = getScreenDao().doDeleteApprRecord(vObject);
			}else{
				exceptionCode = getScreenDao().doDeleteRecord(vObject);
			}
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Delete Exception " + rex.getCode().getErrorMsg());
			logger.error( ((vObject==null)? "vObject is Null":vObject.toString()));
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	/**
	 * Approve to be called from other Business classes 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode approve(E vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		E clonedObject = null;
		try{
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			exceptionCode = getScreenDao().doApproveForTransaction(vObject,vObject.isStaticDelete());
			getScreenDao().fetchMakerVerifierNames(vObject);
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Approve Exception " + rex.getCode().getErrorMsg());
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	/**
	 * Approve to be called from other Business classes 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode reject(E vObject){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		E clonedObject = null;
		try{
			setVerifReqDeleteType(vObject);
			clonedObject = deepCopy.copy(vObject);
			exceptionCode = getScreenDao().doRejectForTransaction(vObject);
			exceptionCode.setOtherInfo(vObject);
			getScreenDao().fetchMakerVerifierNames(vObject);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Reject Exception " + rex.getCode().getErrorMsg());
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObject);
			return exceptionCode;
		}
	}
	/**
	 * Bulk Approve to be called from other Business classes 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode bulkApprove(List<E> vObjects,E queryPopObj){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		List<E> clonedObjects = null;
		try{
			setVerifReqDeleteType(queryPopObj);
			clonedObjects = deepCopy.copyCollection(vObjects);
			exceptionCode =  getScreenDao().bulkApprove(vObjects,queryPopObj.isStaticDelete());
			ArrayList<E> tmpResult = (ArrayList<E>) getQueryPopupResults(queryPopObj);
			exceptionCode.setOtherInfo(tmpResult);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Bulk Approve Exception " + rex.getCode().getErrorMsg());
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObjects);
			return exceptionCode;
		}
	}
	/**
	 * Bulk Approve to be called from other Business classes 
	 * object. 
	 * @param vObject
	 * @return 
	 */
	public ExceptionCode bulkReject(List<E> vObjects,E queryPopObj){
		ExceptionCode exceptionCode  = null;
		DeepCopy<E> deepCopy = new DeepCopy<E>();
		List<E> clonedObjects = null;
		try{
			setVerifReqDeleteType(queryPopObj);
			clonedObjects = deepCopy.copyCollection(vObjects);
			exceptionCode =  getScreenDao().doBulkReject(vObjects);
			ArrayList<E> tmpResult = (ArrayList<E>) getQueryPopupResults(queryPopObj);
			exceptionCode.setOtherInfo(tmpResult);
			return exceptionCode;
		}catch(RuntimeCustomException rex){
			logger.error("Bulk Reject Exception " + rex.getCode().getErrorMsg());
			exceptionCode = rex.getCode();
			exceptionCode.setOtherInfo(clonedObjects);
			return exceptionCode;
		}
	}
	public ExceptionCode getQueryResults(E vObject){
		int intStatus = 1;
		setVerifReqDeleteType(vObject);
		List<E> collTemp = getScreenDao().getQueryResults(vObject,intStatus);
		if (collTemp.size() == 0){
			intStatus = 0;
			collTemp = getScreenDao().getQueryResults(vObject,intStatus);
		}
		if(collTemp.size() == 0){
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 16, "Query", "");
			exceptionCode.setOtherInfo(vObject);
			return exceptionCode;
		}else{
			doSetDesctiptionsAfterQuery(((ArrayList<E>)collTemp).get(0));
			ExceptionCode exceptionCode = CommonUtils.getResultObject(getScreenDao().getServiceDesc(), 1, "Query", "");
			getScreenDao().fetchMakerVerifierNames(((ArrayList<E>)collTemp).get(0));
			((ArrayList<E>)collTemp).get(0).setVerificationRequired(vObject.isVerificationRequired());
			((ArrayList<E>)collTemp).get(0).setStaticDelete(vObject.isStaticDelete());
			exceptionCode.setOtherInfo(((ArrayList<E>)collTemp).get(0));
			return exceptionCode;
		}
	}

	public List<ReviewResultVb> reviewRecord(E vObject){
		try{
			List<E> approvedCollection = getScreenDao().getQueryResults(vObject,1);
			List<E> pendingCollection = getScreenDao().getQueryResults(vObject,0);
			return transformToReviewResults(approvedCollection,pendingCollection);
		}catch(Exception ex){
			return null;
		}
	}
	protected String getVisionVariableValue(String variable){
		VisionVariablesVb lVisionVariablesVb = new VisionVariablesVb();
    	lVisionVariablesVb.setRecordIndicator(0);
    	lVisionVariablesVb.setVariableStatus(0);
    	lVisionVariablesVb.setVariable(variable);
    	List<VisionVariablesVb> lVisionVariables = getVisionVariablesDao().getQueryResults(lVisionVariablesVb, 0);
    	if(lVisionVariables != null && !lVisionVariables.isEmpty()){
    		return lVisionVariables.get(0).getValue();
    	}
    	return null;
	}
	protected String getNtDescription(List<NumSubTabVb> numSubTabs, int numSubTab){
		for(NumSubTabVb numSubTabVb:numSubTabs){
			if(numSubTabVb.getNumSubTab() == numSubTab) return numSubTabVb.getDescription();
		}
		return "Invalid";
	}
	protected String getAtDescription(List<AlphaSubTabVb> alphaSubTabs, String alphaSubTab){
		for(AlphaSubTabVb alphaSubTabVb:alphaSubTabs){
			if(alphaSubTabVb.getAlphaSubTab().equalsIgnoreCase(alphaSubTab)) return alphaSubTabVb.getDescription();
		}
		return "Invalid";
	}
	public List<LevelOfDisplayVb> getQueryUserGroupProfile() {
		List<LevelOfDisplayVb> treeStructuredProfile = new ArrayList<LevelOfDisplayVb>();
		String conditionalUserGrp = "";
		try {
			List<LevelOfDisplayVb> lodVbList = getCommonDao().getQueryUserGroupProfile();
			for (LevelOfDisplayVb lodVb : lodVbList) {
				if (!conditionalUserGrp.equalsIgnoreCase(lodVb.getUserGroup())) {
					String tempUserGrp = lodVb.getUserGroup();
					List<LevelOfDisplayVb> filteredProfile = (List<LevelOfDisplayVb>) lodVbList.stream()
							.filter(s -> s.getUserGroup().equalsIgnoreCase(tempUserGrp)).collect(Collectors.toList());
					conditionalUserGrp = tempUserGrp;
					treeStructuredProfile.add(new LevelOfDisplayVb(conditionalUserGrp, "", filteredProfile));
				}
			}
			return treeStructuredProfile;
		} catch (Exception e) {
			throw new RuntimeCustomException("Failed to retrieve user group and profile.");
		}
	}
	public List<Object> getQueryUserGroup() {
 		List < Object > collTemp = new ArrayList < Object > ();
		try {
			List<ProfileData> profileDataList = getCommonDao().getQueryUserGroup();
			if (profileDataList != null && !profileDataList.isEmpty()) {
				for (ProfileData profileData : profileDataList) {
					collTemp.add(profileData.getUserGroup());
				}
			}
			return collTemp;
		} catch (Exception e) {
			throw new RuntimeCustomException("Failed to retrieve user group.");
		}
	}
	
	public List<Object> getQueryUserGroupBasedProfile(String userGroup) {
 		List < Object > collTemp = new ArrayList < Object > ();
		try {
			List<ProfileData> profileDataList = getCommonDao().getQueryUserGroupBasedProfile(userGroup);
			if (profileDataList != null && !profileDataList.isEmpty()) {
				for (ProfileData profileData : profileDataList) {
					collTemp.add(profileData.getUserProfile());
				}
			}
			return collTemp;
		} catch (Exception e) {
			throw new RuntimeCustomException("Failed to retrieve user group based profile.");
		}
	}
	public ExceptionCode getAllQueryPopupResult(E queryPopupObj){
		ExceptionCode exceptionCode = new ExceptionCode();
		try{
			setVerifReqDeleteType(queryPopupObj);
			doFormateDataForQuery(queryPopupObj);
			List<E> arrListResult = getScreenDao().getQueryPopupResults(queryPopupObj);
			if(arrListResult!= null && arrListResult.size() > 0) {
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setResponse(arrListResult);
			}else {
				exceptionCode.setErrorCode(Constants.NO_RECORDS_FOUND);
			}
			exceptionCode.setOtherInfo(queryPopupObj);
			return exceptionCode;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Exception in getting the getAllQueryPopupResult results.", ex);
			return null;
		}
	}
	public ExceptionCode updateWidgetCreationStagingTable(String tableName) {
		ExceptionCode exceptionCode = new ExceptionCode();
		int result = Constants.ERRONEOUS_OPERATION;
		try {
			result = getCommonDao().updateWidgetCreationStagingTable(tableName);
			exceptionCode.setResponse(tableName);
		} catch (Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorMsg(e.getMessage());
		}
		exceptionCode.setErrorCode(result);
		return exceptionCode;
	}
}