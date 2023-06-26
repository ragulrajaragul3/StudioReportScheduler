package com.vision.vb;

public class LevelOfDisplayUserVb {
	
	public LevelOfDisplayUserVb() {}
	
	public LevelOfDisplayUserVb(int visionId, String userName, String userLoginID, String userGroup,
			String userProfile) {
		super();
		this.visionId = visionId;
		this.userName = userName;
		this.userLoginID = userLoginID;
		this.userGroup = userGroup;
		this.userProfile = userProfile;
	}
	
	public LevelOfDisplayUserVb(int visionId, String userName, String userLoginID, String userGroup,
			String userProfile,String writeFlag) {
		super();
		this.visionId = visionId;
		this.userName = userName;
		this.userLoginID = userLoginID;
		this.userGroup = userGroup;
		this.userProfile = userProfile;
		this.writeFlag = writeFlag;
	}
	
	public LevelOfDisplayUserVb(int visionId) {
		super();
		this.visionId = visionId;
	}
	
	private int visionId = 0;
	private String userName = "";
	private String userLoginID = "";
	private String userGroup = "";
	private String userProfile = "";
	private String writeFlag="";
	private String privateFlag="S";
	
	public int getVisionId() {
		return visionId;
	}
	public void setVisionId(int visionId) {
		this.visionId = visionId;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getUserLoginID() {
		return userLoginID;
	}
	public void setUserLoginID(String userLoginID) {
		this.userLoginID = userLoginID;
	}
	public String getUserGroup() {
		return userGroup;
	}
	public void setUserGroup(String userGroup) {
		this.userGroup = userGroup;
	}
	public String getUserProfile() {
		return userProfile;
	}
	public void setUserProfile(String userProfile) {
		this.userProfile = userProfile;
	}

	public String getWriteFlag() {
		return writeFlag;
	}

	public void setWriteFlag(String writeFlag) {
		this.writeFlag = writeFlag;
	}

	public String getPrivateFlag() {
		return privateFlag;
	}

	public void setPrivateFlag(String privateFlag) {
		this.privateFlag = privateFlag;
	}
}
