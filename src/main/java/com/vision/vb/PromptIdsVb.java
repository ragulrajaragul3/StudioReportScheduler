package com.vision.vb;

public class PromptIdsVb extends CommonVb {

	private static final long serialVersionUID = 1L;
	private int promptTypeAt = 0;
	private int promptStatusNt = 0;
	private int promptStatus = -1;
	private int promptLogicAt = 1205;
	private String promptLogic = "";
	private String promptId = "";
	private String promptDesc = "";
	private String promptString = "";
	private String promptScript = "";
	private String promptType = "";
	private String dependentPromptId = "";
	private String filterStr = "";
	private String sortStr = "";
	private String scheduleScript = "";
	private String scheduleSortOrder = "";
	private String promptPosition = "";
	private PromptTreeVb selectedValue1 = null;
	private PromptTreeVb selectedValue2 = null;
	private boolean cascadePrompt = false;

	public int getPromptTypeAt() {
		return promptTypeAt;
	}
	public void setPromptTypeAt(int promptTypeAt) {
		this.promptTypeAt = promptTypeAt;
	}
	public int getPromptStatusNt() {
		return promptStatusNt;
	}
	public void setPromptStatusNt(int promptStatusNt) {
		this.promptStatusNt = promptStatusNt;
	}
	public int getPromptStatus() {
		return promptStatus;
	}
	public void setPromptStatus(int promptStatus) {
		this.promptStatus = promptStatus;
	}
	public int getPromptLogicAt() {
		return promptLogicAt;
	}
	public void setPromptLogicAt(int promptLogicAt) {
		this.promptLogicAt = promptLogicAt;
	}
	public String getPromptLogic() {
		return promptLogic;
	}
	public void setPromptLogic(String promptLogic) {
		this.promptLogic = promptLogic;
	}
	public String getPromptId() {
		return promptId;
	}
	public void setPromptId(String promptId) {
		this.promptId = promptId;
	}
	public String getPromptDesc() {
		return promptDesc;
	}
	public void setPromptDesc(String promptDesc) {
		this.promptDesc = promptDesc;
	}
	public String getPromptString() {
		return promptString;
	}
	public void setPromptString(String promptString) {
		this.promptString = promptString;
	}
	public String getPromptScript() {
		return promptScript;
	}
	public void setPromptScript(String promptScript) {
		this.promptScript = promptScript;
	}
	public String getPromptType() {
		return promptType;
	}
	public void setPromptType(String promptType) {
		this.promptType = promptType;
	}
	public String getDependentPromptId() {
		return dependentPromptId;
	}
	public void setDependentPromptId(String dependentPromptId) {
		this.dependentPromptId = dependentPromptId;
	}
	public String getFilterStr() {
		return filterStr;
	}
	public void setFilterStr(String filterStr) {
		this.filterStr = filterStr;
	}
	public String getSortStr() {
		return sortStr;
	}
	public void setSortStr(String sortStr) {
		this.sortStr = sortStr;
	}
	public String getScheduleScript() {
		return scheduleScript;
	}
	public void setScheduleScript(String scheduleScript) {
		this.scheduleScript = scheduleScript;
	}
	public String getScheduleSortOrder() {
		return scheduleSortOrder;
	}
	public void setScheduleSortOrder(String scheduleSortOrder) {
		this.scheduleSortOrder = scheduleSortOrder;
	}
	public String getPromptPosition() {
		return promptPosition;
	}
	public void setPromptPosition(String promptPosition) {
		this.promptPosition = promptPosition;
	}
	public PromptTreeVb getSelectedValue1() {
		return selectedValue1;
	}
	public void setSelectedValue1(PromptTreeVb selectedValue1) {
		this.selectedValue1 = selectedValue1;
	}
	public PromptTreeVb getSelectedValue2() {
		return selectedValue2;
	}
	public void setSelectedValue2(PromptTreeVb selectedValue2) {
		this.selectedValue2 = selectedValue2;
	}
	public boolean isCascadePrompt() {
		return cascadePrompt;
	}
	public void setCascadePrompt(boolean cascadePrompt) {
		this.cascadePrompt = cascadePrompt;
	}
	
}