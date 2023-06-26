package com.vision.dao;

import java.util.List;

import org.springframework.stereotype.Component;

@Component
public class StaggingSchedulerDao extends AbstractCommonDao {

	public void doBulkDelete() {
		List<String> tableNameList = getJdbcTemplate().queryForList(
				"SELECT TABLE_NAME FROM VWC_STAGGING_TABLE_LOGGING WHERE  DATE_LAST_MODIFIED  < (SYSDATE -  (15/(24*60)))",
				String.class);
		getJdbcTemplate().execute("DELETE FROM VWC_STAGGING_TABLE_LOGGING WHERE  DATE_LAST_MODIFIED < (SYSDATE - (15/(24*60)))");
		for (String tableName : tableNameList) {
			try {
				getJdbcTemplate().execute("DROP TABLE " + tableName + " PURGE");
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}	
}
