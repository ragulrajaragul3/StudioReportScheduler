package com.vision.vb;

import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;

public class WidgetDesignVb extends CommonVb {
	private String widgetId = "";
	private String description = "";
	private String queryId = "";
	private String widgetContext = "";
	private String widgetContextJson = "";
	private String filterContext = "";
	private String filterContextJson = "";
	private int widgetStatusNt = 1;
	private int widgetStatus = 0;
	private String hashVariableScript = "";
	private String dashboardId="";
	private Integer subDashboardId;
	private List<WidgetFilterVb> widgetFilterList = null;
	private String orderBy="";
	
	public String getWidgetId() {
		return widgetId;
	}

	public void setWidgetId(String widgetId) {
		this.widgetId = widgetId;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public String getWidgetContext() {
		return widgetContext;
	}

	public void setWidgetContext(String widgetContext) {
		this.widgetContext = StringEscapeUtils.unescapeXml(widgetContext);
	}

	public String getWidgetContextJson() {
		return widgetContextJson;
	}

	public void setWidgetContextJson(String widgetContextJson) {
		this.widgetContextJson = widgetContextJson;
	}

	public String getFilterContext() {
		return filterContext;
	}

	public void setFilterContext(String filterContext) {
		this.filterContext = filterContext;
	}

	public String getFilterContextJson() {
		return filterContextJson;
	}

	public void setFilterContextJson(String filterContextJson) {
		this.filterContextJson = filterContextJson;
	}

	public int getWidgetStatusNt() {
		return widgetStatusNt;
	}

	public void setWidgetStatusNt(int widgetStatusNt) {
		this.widgetStatusNt = widgetStatusNt;
	}

	public int getWidgetStatus() {
		return widgetStatus;
	}

	public void setWidgetStatus(int widgetStatus) {
		this.widgetStatus = widgetStatus;
	}

	public String getHashVariableScript() {
		return hashVariableScript;
	}

	public void setHashVariableScript(String hashVariableScript) {
		this.hashVariableScript = hashVariableScript;
	}
	
	public String getDashboardId() {
		return dashboardId;
	}

	public void setDashboardId(String dashboardId) {
		this.dashboardId = dashboardId;
	}

	public Integer getSubDashboardId() {
		return subDashboardId;
	}

	public void setSubDashboardId(Integer subDashboardId) {
		this.subDashboardId = subDashboardId;
	}

	public List<WidgetFilterVb> getWidgetFilterList() {
		return widgetFilterList;
	}

	public void setWidgetFilterList(List<WidgetFilterVb> widgetFilterList) {
		this.widgetFilterList = widgetFilterList;
	}

	public String getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}
}