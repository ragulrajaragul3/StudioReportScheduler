package com.vision.vb;

public class MaskingVb {
	
	private String userGroup = "";
	private String userProfile = "";
	private String pattern = "";

	public MaskingVb(String userGroup, String userProfile, String pattern) {
		super();
		this.userGroup = userGroup;
		this.userProfile = userProfile;
		this.pattern = pattern;
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
	public String getPattern() {
		return pattern;
	}
	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

}