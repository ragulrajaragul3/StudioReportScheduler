package com.vision.vb;

import java.io.Serializable;

public class ProfileData extends CommonVb implements Serializable{
	
	private static final long serialVersionUID = -7365141127791003770L;
	private int	userGroupAt =  1;
	private String	userGroup = "-1";
	private int	userProfileAt =  2;
	private String	userProfile = "-1";
	private int	menuGroupNt =  176;	
	private String menuItem = "";
	private int menuGroup = -1;
	private String profileAdd  = "N";
	private String profileDelete = "N";
	private String profileInquiry = "N";
	private String profileVerification = "N"; 
	private String profileModify = "N";
	private String profileUpload = "N";
	private int	profileStatusNt =  1;
	private int	profileStatus = -1;	
	private String menuIcon = "N";
	private String excludeMenuProgramList = "";
	
	public String getMenuIcon() {
		return menuIcon;
	}
	public void setMenuIcon(String menuIcon) {
		this.menuIcon = menuIcon;
	}
	public String getMenuItem() {
		return menuItem;
	}
	public void setMenuItem(String menuItem) {
		this.menuItem = menuItem;
	}
	public int getMenuGroup() {
		return menuGroup;
	}
	public void setMenuGroup(int menuGroup) {
		this.menuGroup = menuGroup;
	}
	public String getProfileAdd() {
		return profileAdd;
	}
	public void setProfileAdd(String profileAdd) {
		this.profileAdd = profileAdd;
	}
	public String getProfileDelete() {
		return profileDelete;
	}
	public void setProfileDelete(String profileDelete) {
		this.profileDelete = profileDelete;
	}
	public String getProfileInquiry() {
		return profileInquiry;
	}
	public void setProfileInquiry(String profileInquiry) {
		this.profileInquiry = profileInquiry;
	}
	public String getProfileVerification() {
		return profileVerification;
	}
	public void setProfileVerification(String profileVerification) {
		this.profileVerification = profileVerification;
	}
	public String getProfileModify() {
		return profileModify;
	}
	public void setProfileModify(String profileModify) {
		this.profileModify = profileModify;
	}
	public String getProfileUpload() {
		return profileUpload;
	}
	public void setProfileUpload(String profileUpload) {
		this.profileUpload = profileUpload;
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
	public int getMenuGroupNt() {
		return menuGroupNt;
	}
	public void setMenuGroupNt(int menuGroupNt) {
		this.menuGroupNt = menuGroupNt;
	}
	public int getProfileStatusNt() {
		return profileStatusNt;
	}
	public void setProfileStatusNt(int profileStatusNt) {
		this.profileStatusNt = profileStatusNt;
	}
	public int getProfileStatus() {
		return profileStatus;
	}
	public void setProfileStatus(int profileStatus) {
		this.profileStatus = profileStatus;
	}
	public String getExcludeMenuProgramList() {
		return excludeMenuProgramList;
	}
	public void setExcludeMenuProgramList(String excludeMenuProgramList) {
		this.excludeMenuProgramList = excludeMenuProgramList;
	}
	
}
