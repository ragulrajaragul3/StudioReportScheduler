package com.vision.vb;

import java.util.ArrayList;
import java.util.List;

public class LevelOfDisplayVb {

	private int userGroupAt = 1;
	private String userGroup = "";
	private int userProfileAt = 2;
	private String userProfile = "";
	private String writeFlag="";
	private List<LevelOfDisplayVb> children = new ArrayList<LevelOfDisplayVb>();

	public LevelOfDisplayVb() {
	}

	public LevelOfDisplayVb(String userGroup, String userProfile, List<LevelOfDisplayVb> children) {
		this.userGroup = userGroup;
		this.userProfile = userProfile;
		this.children = children;
	}

	public LevelOfDisplayVb(String userGroup, String userProfile, List<LevelOfDisplayVb> children,String writeFlag) {
		this.userGroup = userGroup;
		this.userProfile = userProfile;
		this.children = children;
		this.writeFlag = writeFlag;
	}
	

	public int getUserGroupAt() {
		return userGroupAt;
	}

	public void setUserGroupAt(int userGroupAt) {
		this.userGroupAt = userGroupAt;
	}

	public String getUserGroup() {
		return userGroup;
	}

	public void setUserGroup(String userGroup) {
		this.userGroup = userGroup;
	}

	public int getUserProfileAt() {
		return userProfileAt;
	}

	public void setUserProfileAt(int userProfileAt) {
		this.userProfileAt = userProfileAt;
	}

	public String getUserProfile() {
		return userProfile;
	}

	public void setUserProfile(String userProfile) {
		this.userProfile = userProfile;
	}

	public List<LevelOfDisplayVb> getChildren() {
		return children;
	}

	public void setChildren(List<LevelOfDisplayVb> children) {
		this.children = children;
	}

	public String getWriteFlag() {
		return writeFlag;
	}

	public void setWriteFlag(String writeFlag) {
		this.writeFlag = writeFlag;
	}

}