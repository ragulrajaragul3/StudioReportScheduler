package com.vision.wb;

import java.util.Iterator;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vision.dao.AbstractDao;
import com.vision.dao.CommonDao;
import com.vision.dao.LevelOfDisplayDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.CommonVb;
import com.vision.vb.LevelOfDisplayUserVb;
import com.vision.vb.LevelOfDisplayVb;

@Component
public class LevelOfDisplayWb<E extends CommonVb> extends AbstractWorkerBean {

	@Autowired
	LevelOfDisplayDao levelOfDisplayDao;

	@Autowired
	CommonDao commonDao;

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doInsertRecord(List<LevelOfDisplayVb> vObjList, String switchOption, boolean isMain,
			E dynamicObject) throws RuntimeCustomException {
		ExceptionCode exceptionCode = new ExceptionCode();
		int result = Constants.ERRONEOUS_OPERATION;
		try {
			String sysDate = commonDao.getSystemDate();
			dynamicObject.setDateCreation(sysDate);
			dynamicObject.setDateLastModified(sysDate);
			switch (switchOption) {
			case "CATALOG": {
				result = levelOfDisplayDao.doInsertRecordForCatalogAccess(vObjList, isMain, dynamicObject);
				break;
			}
			case "CONNECTOR": {
				result = levelOfDisplayDao.doInsertRecordForDataConnectorLOD(vObjList, isMain, dynamicObject);
				break;
			}
			case "M_QUERY": {
				result = levelOfDisplayDao.doInsertRecordForManualQueries(vObjList, isMain, dynamicObject);
				break;
			}
			case "D_QUERY": {
				result = levelOfDisplayDao.doInsertRecordForDesignQueries(vObjList, isMain, dynamicObject);
				break;
			}
			}
			exceptionCode.setErrorCode(result);
			return exceptionCode;
		} catch (RuntimeCustomException rcException) {
			throw rcException;
		}
	}
	
	public List<Object> getUserListExcludingUserGrpProf(List<LevelOfDisplayUserVb> usersVbList) {
		try {
			StringBuffer usrGroupAndProfile = new StringBuffer();
			Iterator<LevelOfDisplayUserVb> userListItr = usersVbList.iterator(); 
			while(userListItr.hasNext()) {
				LevelOfDisplayUserVb vb = userListItr.next();
				if(ValidationUtil.isValid(vb.getUserGroup()) && ValidationUtil.isValid(vb.getUserProfile())) {
					usrGroupAndProfile.append("'"+vb.getUserGroup()+"-"+vb.getUserProfile()+"'");
					usrGroupAndProfile.append((userListItr.hasNext())?",":"");					
				}
			}
			return levelOfDisplayDao.getUserListExcludingUserGrpProf(String.valueOf(usrGroupAndProfile));
		} catch (Exception e) {
			throw new RuntimeCustomException("Failed to retrieve user group based profile.");
		}
	}

	@Override
	protected AbstractDao getScreenDao() {
		return null;
	}

	@Override
	protected void setAtNtValues(CommonVb vObject) {
	}

	@Override
	protected void setVerifReqDeleteType(CommonVb vObject) {
	}

}
