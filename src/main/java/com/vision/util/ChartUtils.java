package com.vision.util;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.ServletContextAware;

import com.sun.rowset.CachedRowSetImpl;
import com.vision.dao.CommonDao;
import com.vision.dao.ReportsDao;
import com.vision.exception.ExceptionCode;
import com.vision.vb.ReportStgVb;

import edu.emory.mathcs.backport.java.util.Arrays;

@Component
public class ChartUtils implements ServletContextAware{
	@Autowired
	CommonDao commonDao;
	@Autowired
	ReportsDao reportsDao;
	private ServletContext servletContext;
	public static Logger logger = LoggerFactory.getLogger(ChartUtils.class);
	public void setServletContext(ServletContext arg0) {
		servletContext = arg0;
	}
	public ExceptionCode updateReturnXmlForSingleRepeatTag(String repeatTagMain, String chartXML, String xAxisCol, String yAxisCol, String zAxisCol,
			String seriesCol, String returnChartXml, ResultSet rs, StringBuffer dataExistCheck,String ddKeyCol,String chartType,String sumLabel) throws SQLException{
		ExceptionCode exceptionCode = new ExceptionCode();
		Map<String, String> m1 = new HashMap<String, String>();
		try {
			if ("FCMap".equalsIgnoreCase(chartType)) {
				String assetFolderUrl = servletContext.getRealPath("/WEB-INF/classes/chart_xml/country_state.properties");
				BufferedReader in = new BufferedReader(new FileReader(assetFolderUrl));
				String line = "";
				while ((line = in.readLine()) != null) {
					String parts[] = line.split("=");
					m1.put(parts[0], parts[1]);
				}
				in.close();
			}
			Matcher matcherObj = Pattern.compile("\\<" + repeatTagMain + "(.*?)\\<\\/" + repeatTagMain + "\\>", Pattern.DOTALL)
					.matcher(chartXML);
			if (matcherObj.find()) {
				String tagString = "<" + repeatTagMain + matcherObj.group(1) + "</" + repeatTagMain + ">";
				ArrayList<String> patternStrAL = new ArrayList<String>();
				ArrayList<String> patternStrColNameAL = new ArrayList<String>();
				Matcher valColMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#", Pattern.DOTALL).matcher(tagString);

				while (valColMatcher.find()) {
					patternStrAL.add(valColMatcher.group(1));
					patternStrColNameAL.add(
							getColumnName(valColMatcher.group(1), xAxisCol, yAxisCol, zAxisCol, seriesCol, ddKeyCol));
					if (!ValidationUtil.isValid(ddKeyCol)) {
						patternStrColNameAL.remove(ddKeyCol);
					}
				}

				rs.beforeFirst();
				StringBuffer formedXmlSB = new StringBuffer();
				int i = 0;
				while (rs.next()) {
					if (patternStrColNameAL.size() == 1) {
						// int i =0;
						if (dataExistCheck.indexOf("," + rs.getObject(patternStrColNameAL.get(0)) + ",") == -1) {
							String value = String.valueOf(rs.getObject(patternStrColNameAL.get(0)));
							formedXmlSB.append(tagString.replaceAll("\\!\\@\\#" + patternStrAL.get(0) + "\\!\\@\\#",
									ValidationUtil.isValid(value) ? value : ""));
							dataExistCheck.append("," + rs.getObject(patternStrColNameAL.get(0)) + ",");
							String newXml = formedXmlSB.toString();
							if (newXml.contains("@#CNT@#")) {
								i = i + 10;
								newXml = newXml.replace("@#CNT@#", Integer.toString(i));
								formedXmlSB = new StringBuffer();
								formedXmlSB.append(newXml);
							}
						}
					} else if (patternStrColNameAL.size() > 1) {
						int arrListIndex = 0;
						String tempTagString = tagString;
						for (String colName : patternStrColNameAL) {
							String value = "";
							try {
								value = String.valueOf(rs.getObject(colName));
							} catch (Exception e) {
								exceptionCode.setErrorMsg(e.getCause().getMessage());
								exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
								if (!ValidationUtil.isValid(exceptionCode.getErrorMsg()))
									exceptionCode.setErrorMsg(e.getMessage());
								return exceptionCode;
							}
							tempTagString = tempTagString.replaceAll(
									"\\!\\@\\#" + patternStrAL.get(arrListIndex) + "\\!\\@\\#",
									ValidationUtil.isValid(value) ? value : "");
							if ("Waterfall2D".equalsIgnoreCase(chartType)) {
								double retVal = 0;
								boolean result = value.matches(".*[a-zA-Z]+.*");
								if (!result)
									retVal = Double.parseDouble(value);
								if (retVal >= 0)
									tempTagString = tempTagString.replaceAll("#COLOR_CODE#", "6baa01");
								else if (retVal < 0)
									tempTagString = tempTagString.replaceAll("#COLOR_CODE#", "CE3132");
								if (ValidationUtil.isValid(sumLabel) && sumLabel.equalsIgnoreCase(value))
									tempTagString = tempTagString.replaceAll("6baa01", "00BCD4");
							}
							if ("FCMap".equalsIgnoreCase(chartType)) {
								if(m1.containsKey(value)) {
									tempTagString = tempTagString.replace(value, m1.get(value));
								}
								
							}
							arrListIndex++;
						}
						formedXmlSB.append(tempTagString);
					}
				}
				/* Update to return XML */
				Matcher returnMatcher = Pattern
						.compile("^(.*?)\\<" + repeatTagMain + "(.*?)\\<\\/" + repeatTagMain + "\\>(.*?)$",
								Pattern.DOTALL)
						.matcher(returnChartXml);
				if (returnMatcher.find()) {
					returnChartXml = returnMatcher.group(1) + formedXmlSB + returnMatcher.group(3);
				}
			}
			if (ValidationUtil.isValid(returnChartXml)) {
				exceptionCode.setResponse(returnChartXml);
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setErrorMsg("sucessful operation");
			}
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		
		return exceptionCode;
	}
	public ExceptionCode updateReturnXmlForMultipleRepeatTagMultiY_NoSeries(String repeatTagMain, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol, String chartXML, ResultSet rs, StringBuffer dataExistCheck, CachedRowSetImpl rsChild, String returnChartXml,String ddKeyCol) throws SQLException{
		ExceptionCode exceptionCode = new ExceptionCode();
		String[] repeatTagArr = repeatTagMain.split(",");
		try {
			if(repeatTagArr.length==2){
				Matcher matcherObj = Pattern.compile("\\<"+repeatTagArr[0]+"(.*?)\\<\\/"+repeatTagArr[0]+"\\>",Pattern.DOTALL).matcher(chartXML);
				if(matcherObj.find()){
					String fullTagString = "<"+repeatTagArr[0]+matcherObj.group(1)+"</"+repeatTagArr[0]+">";
					String parentTagStr = "";
					String childTagString = "";
					String parentReplaceString = "";
					String childReplaceString = "";
					StringBuffer formedXmlSB = new StringBuffer();
					
					/* Form parent tag String */
					Matcher parentTagMatcher = Pattern.compile("\\<"+repeatTagArr[0]+"(.*?)\\>",Pattern.DOTALL).matcher(fullTagString);
					if(parentTagMatcher.find()){
						parentTagStr = "<"+repeatTagArr[0]+parentTagMatcher.group(1)+">";
					}
					
					
					/* Get exact pattern from parent tag to be replaced with Y-Axis column name */
					String yAxisColArr[] = yAxisCol.split(",");
					Matcher replaceMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(parentTagStr);
					if(replaceMatcher.find()){
						parentReplaceString = replaceMatcher.group(1);
					}
					
					/* Form child tag String */
					Matcher matcherChildObj = Pattern.compile("\\<"+repeatTagArr[1]+"(.*?)\\<\\/"+repeatTagArr[1]+"\\>",Pattern.DOTALL).matcher(fullTagString);
					if(matcherChildObj.find()){
						childTagString = "<"+repeatTagArr[1]+matcherChildObj.group(1)+"</"+repeatTagArr[1]+">";
					}
					
					replaceMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(childTagString);
					if(replaceMatcher.find()){
						childReplaceString = replaceMatcher.group(1);
					}
					
					/* For every Y-Axis column Name from parent tag string [dataset] */
					for(String yCol:yAxisColArr){
						formedXmlSB.append(parentTagStr.replaceAll("\\!\\@\\#"+parentReplaceString+"\\!\\@\\#", yCol));
						
						rs.beforeFirst();
						while(rs.next()){
							formedXmlSB.append(childTagString.replaceAll("\\!\\@\\#"+childReplaceString+"\\!\\@\\#", String.valueOf(rs.getObject(yCol))));
						}
						formedXmlSB.append("</"+repeatTagArr[0]+">");
					}
					
			    	/* Update return XML */
			    	Matcher returnMatcher = Pattern.compile("^(.*?)\\<"+repeatTagArr[0]+"(.*?)\\<\\/"+repeatTagArr[0]+"\\>(.*?)$",Pattern.DOTALL).matcher(returnChartXml);
			    	if(returnMatcher.find()){
			    		returnChartXml = returnMatcher.group(1)+formedXmlSB+returnMatcher.group(3);
			    	}
				}
			}
			if(ValidationUtil.isValid(returnChartXml)) {
				exceptionCode.setResponse(returnChartXml);
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				exceptionCode.setErrorMsg("sucessful operation");
			}
			return exceptionCode;
		}catch(Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
		
		
	}
	public String getColumnName(String pattern, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol,String ddKeyCol){
		switch(pattern){
			case "X_AXIS":
				return xAxisCol;
			case "Y_AXIS":
				return yAxisCol;
			case "Z_AXIS":
				return zAxisCol;
			case "SERIES_AXIS":
				return seriesCol;
			case "DRILLDOWN_KEY":
				return ddKeyCol;
			default:
				return ".";
		}
	}
	public ExceptionCode updateReturnXmlForSingleRepeatTag_OnlyMeasure(String repeatTagMain, String chartXML, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol, String returnChartXml, ResultSet rs, StringBuffer dataExistCheck,String ddKeyCol) throws SQLException{
		ExceptionCode exceptionCode = new ExceptionCode();
		Matcher matcherObj = Pattern.compile("\\<"+repeatTagMain+"(.*?)\\<\\/"+repeatTagMain+"\\>",Pattern.DOTALL).matcher(chartXML);
		if(matcherObj.find()){
			String tagString = "<"+repeatTagMain+matcherObj.group(1)+"</"+repeatTagMain+">";
			String replaceTagString = "";
			Matcher valColMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(tagString);
			while(valColMatcher.find()){
				replaceTagString = "Y_AXIS".equalsIgnoreCase(valColMatcher.group(1))?replaceTagString:valColMatcher.group(1);
			}
			rs.beforeFirst();
			StringBuffer formedXmlSB = new StringBuffer();
			while(rs.next()){
				String columnArr[] = yAxisCol.split(",");
				for(String columnName:columnArr){
					String value ="";
					try{
						value = String.valueOf(rs.getObject(columnName));
					}catch(Exception e){
						e.printStackTrace();
    					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
    					exceptionCode.setErrorMsg(e.getMessage());
						return exceptionCode;
					}
					value = tagString.replaceAll("\\!\\@\\#"+replaceTagString+"\\!\\@\\#", columnName).replaceAll("\\!\\@\\#Y_AXIS\\!\\@\\#", ValidationUtil.isValid(value)?value:"");
					if(formedXmlSB.indexOf(value)==-1)
						formedXmlSB.append(value);
				}
			}
	    	/* Update to return XML */
	    	Matcher returnMatcher = Pattern.compile("^(.*?)\\<"+repeatTagMain+"(.*?)\\<\\/"+repeatTagMain+"\\>(.*?)$",Pattern.DOTALL).matcher(returnChartXml);
	    	if(returnMatcher.find()){
	    		returnChartXml = returnMatcher.group(1)+formedXmlSB+returnMatcher.group(3);
	    	}
		}
		if(ValidationUtil.isValid(returnChartXml)) {
			exceptionCode.setResponse(returnChartXml);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		}
		return exceptionCode;
	}
	public ExceptionCode updateReturnXmlForMultipleRepeatTagMultiY(String repeatTagMain, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol, String chartXML, ResultSet rs, StringBuffer dataExistCheck, CachedRowSetImpl rsChild, String returnChartXml,String ddKeyCol) throws SQLException{
		ExceptionCode exceptionCode =new ExceptionCode();
		try {
			String[] repeatTagArr = repeatTagMain.split(",");
			if(repeatTagArr.length==2){
				Matcher matcherObj = Pattern.compile("\\<"+repeatTagArr[0]+"(.*?)\\<\\/"+repeatTagArr[0]+"\\>",Pattern.DOTALL).matcher(chartXML);
				if(matcherObj.find()){
					String fullTagString = "<"+repeatTagArr[0]+matcherObj.group(1)+"</"+repeatTagArr[0]+">";
					String parentTagStr = "";
					String colNameForParent = "";
					Matcher parentTagMatcher = Pattern.compile("\\<"+repeatTagArr[0]+"(.*?)\\>",Pattern.DOTALL).matcher(fullTagString);
					if(parentTagMatcher.find()){
						parentTagStr = "<"+repeatTagArr[0]+parentTagMatcher.group(1)+">";
					}
					Matcher valColMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(parentTagStr);
					if(valColMatcher.find()){
						colNameForParent = getColumnName(valColMatcher.group(1), xAxisCol, yAxisCol, zAxisCol, seriesCol,ddKeyCol);
					}
					rs.beforeFirst();
					StringBuffer formedXmlSB = new StringBuffer();
					while(rs.next()){
			    		if(dataExistCheck.indexOf(","+rs.getObject(colNameForParent)+",")==-1){
			    			String value = String.valueOf(rs.getObject(colNameForParent));
			    			formedXmlSB.append(parentTagStr.replaceAll("\\!\\@\\#"+valColMatcher.group(1)+"\\!\\@\\#", ValidationUtil.isValid(value)?value:"" ));
			    			
			    			Matcher matcherChildObj = Pattern.compile("\\<"+repeatTagArr[1]+"(.*?)\\<\\/"+repeatTagArr[1]+"\\>",Pattern.DOTALL).matcher(fullTagString);
							if(matcherChildObj.find()){
								String childTagString = "<"+repeatTagArr[1]+matcherChildObj.group(1)+"</"+repeatTagArr[1]+">";
								
								ArrayList<String> patternStrAL = new ArrayList<String>();
								ArrayList<String> patternStrColNameAL = new ArrayList<String>();
								Matcher valColMatcherChild = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(childTagString);
								
								while(valColMatcherChild.find()){
									patternStrAL.add(valColMatcherChild.group(1));
									patternStrColNameAL.add(getColumnName(valColMatcherChild.group(1), xAxisCol, yAxisCol, zAxisCol, seriesCol,ddKeyCol));
								}
								for(String pattern:patternStrAL){
									rsChild.beforeFirst();
									while(rsChild.next()){
										if(String.valueOf(rsChild.getObject(colNameForParent)).equalsIgnoreCase(String.valueOf(rs.getObject(colNameForParent))) ){
											for(String colName:patternStrColNameAL.get(0).split(",")){
												String valueChild = String.valueOf(rsChild.getObject(colName));
												formedXmlSB.append(childTagString.replaceAll("\\!\\@\\#"+pattern+"\\!\\@\\#", ValidationUtil.isValid(valueChild)?valueChild:""));
							    			}
										}
							    	}
								}
							}
							
							formedXmlSB.append("</"+repeatTagArr[0]+">");
				    		dataExistCheck.append(","+rs.getObject(colNameForParent)+",");
			    		}
			    	}
			    	/* Update return XML */
			    	Matcher returnMatcher = Pattern.compile("^(.*?)\\<"+repeatTagArr[0]+"(.*?)\\<\\/"+repeatTagArr[0]+"\\>(.*?)$",Pattern.DOTALL).matcher(returnChartXml);
			    	if(returnMatcher.find()){
			    		returnChartXml = returnMatcher.group(1)+formedXmlSB+returnMatcher.group(3);
			    	}
			    	if(ValidationUtil.isValid(returnChartXml)) {
						exceptionCode.setResponse(returnChartXml);
						exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
					}
				}
			}
			return exceptionCode;
		}catch(Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
		
	}
	public ExceptionCode updateReturnXmlForMultipleRepeatTag(String repeatTagMain, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol, String chartXML, ResultSet rs, StringBuffer dataExistCheck, CachedRowSetImpl rsChild, String returnChartXml,String ddKeyCol) throws SQLException{
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
		String[] repeatTagArr = repeatTagMain.split(",");
		int xCordVal = 0;
		if(repeatTagArr.length==2){
			Matcher matcherObj = Pattern.compile("\\<"+repeatTagArr[0]+"(.*?)\\<\\/"+repeatTagArr[0]+"\\>",Pattern.DOTALL).matcher(chartXML);
			if(matcherObj.find()){
				String fullTagString = "<"+repeatTagArr[0]+matcherObj.group(1)+"</"+repeatTagArr[0]+">";
				String parentTagStr = "";
				String colNameForParent = "";
				Matcher parentTagMatcher = Pattern.compile("\\<"+repeatTagArr[0]+"(.*?)\\>",Pattern.DOTALL).matcher(fullTagString);
				if(parentTagMatcher.find()){
					parentTagStr = "<"+repeatTagArr[0]+parentTagMatcher.group(1)+">";
				}
				Matcher valColMatcher = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(parentTagStr);
				if(valColMatcher.find()){
					colNameForParent = getColumnName(valColMatcher.group(1), xAxisCol, yAxisCol, zAxisCol, seriesCol,ddKeyCol);
				}
				rs.beforeFirst();
				StringBuffer formedXmlSB = new StringBuffer();
				while(rs.next()){
					xCordVal = 0;
		    		if(ValidationUtil.isValid(colNameForParent) && dataExistCheck.indexOf(","+rs.getObject(colNameForParent)+",")==-1){
		    			String value = String.valueOf(rs.getObject(colNameForParent));
		    			formedXmlSB.append(parentTagStr.replaceAll("\\!\\@\\#"+valColMatcher.group(1)+"\\!\\@\\#", ValidationUtil.isValid(value)?value:"" ));
		    			
		    			Matcher matcherChildObj = Pattern.compile("\\<"+repeatTagArr[1]+"(.*?)\\<\\/"+repeatTagArr[1]+"\\>",Pattern.DOTALL).matcher(fullTagString);
						if(matcherChildObj.find()){
							String childTagString = "<"+repeatTagArr[1]+matcherChildObj.group(1)+"</"+repeatTagArr[1]+">";
							
							ArrayList<String> patternStrAL = new ArrayList<String>();
							ArrayList<String> patternStrColNameAL = new ArrayList<String>();
							Matcher valColMatcherChild = Pattern.compile("\\!\\@\\#(.*?)\\!\\@\\#",Pattern.DOTALL).matcher(childTagString);
							
								while (valColMatcherChild.find()) {
									patternStrAL.add(valColMatcherChild.group(1));
									patternStrColNameAL.add(getColumnName(valColMatcherChild.group(1), xAxisCol,yAxisCol, zAxisCol, seriesCol, ddKeyCol));
									if (!ValidationUtil.isValid(ddKeyCol)) {
										patternStrColNameAL.remove(ddKeyCol);
									}
								}
							
							rsChild.beforeFirst();
							while(rsChild.next()){
								String tempTagString = childTagString;
								if(String.valueOf(rsChild.getObject(colNameForParent)).equalsIgnoreCase(String.valueOf(rs.getObject(colNameForParent))) ){
									xCordVal = xCordVal+10;
									int arrListIndex = 0;
									for(String colName:patternStrColNameAL){
										String valueChild = String.valueOf(rsChild.getObject(colName));
					    				tempTagString = tempTagString.replaceAll("\\!\\@\\#"+patternStrAL.get(arrListIndex)+"\\!\\@\\#", ValidationUtil.isValid(valueChild)?valueChild:"");
					    				if(tempTagString.contains("@#CNT@#")) {
					    					tempTagString = tempTagString.replace("@#CNT@#", Integer.toString(xCordVal));
					    				}
					    				arrListIndex++;
					    			}
									formedXmlSB.append(tempTagString);
								}
					    	}
						}
						
						formedXmlSB.append("</"+repeatTagArr[0]+">");
			    		dataExistCheck.append(","+rs.getObject(colNameForParent)+",");
		    		}
		    	}
		    	/* Update return XML */
		    	Matcher returnMatcher = Pattern.compile("^(.*?)\\<"+repeatTagArr[0]+"(.*?)\\<\\/"+repeatTagArr[0]+"\\>(.*?)$",Pattern.DOTALL).matcher(returnChartXml);
		    	if(returnMatcher.find()){
		    		returnChartXml = returnMatcher.group(1)+formedXmlSB+returnMatcher.group(3);
		    	}
			}
		}
		if(ValidationUtil.isValid(returnChartXml)) {
			exceptionCode.setResponse(returnChartXml);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorMsg("sucessful operation");
		}
		return exceptionCode;
		}catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
	}
	public List<String> returnColorListBasedOnColorCount(int colorCount,String singleColor){
		Color randomColor = null;
		List<String> colorList = new ArrayList<String>();
		final int maxLimit = 255;
		colorList.add(singleColor);
		int difference = calculateDifferenceCountBase(colorCount);
		int breakMark = difference;
		if("ff0000".equalsIgnoreCase(singleColor)){
			do{
				randomColor = new Color(maxLimit, breakMark, breakMark);
				colorList.add(CommonUtils.rgb2Hex(randomColor));
				breakMark+=difference;
			}while(breakMark<maxLimit);
		}else if("00ff00".equalsIgnoreCase(singleColor)){
			do{
				randomColor = new Color(breakMark, maxLimit, breakMark);
				colorList.add(CommonUtils.rgb2Hex(randomColor));
				breakMark+=difference;
			}while(breakMark<maxLimit);
		}else if("0000ff".equalsIgnoreCase(singleColor)){
			do{
				randomColor = new Color(breakMark, breakMark, maxLimit);
				colorList.add(CommonUtils.rgb2Hex(randomColor));
				breakMark+=difference;
			}while(breakMark<maxLimit);
		}else{
			colorList = new ArrayList<String>();
			colorList.add("000000");
			do{
				randomColor = new Color(breakMark, breakMark, breakMark);
				colorList.add(CommonUtils.rgb2Hex(randomColor));
				breakMark+=difference;
			}while(breakMark<maxLimit);
		}
		return colorList;
	}
	public  int calculateDifferenceCountBase(int count){
		if(count<5)return 50; if(count<10)return 25; if(count<12)return 20; if(count<15)return 15;
		if(count<25)return 10; if(count<50)return 5; if(count<85)return 3; if(count<125)return 2;
		return  1;
	}
	public ExceptionCode getChartXML(String chartType, String  chartXml,ResultSet rs,CachedRowSetImpl rsChild,String widgetTheme) {
		ExceptionCode exceptionCode = new ExceptionCode();
		boolean multiY_NoSeries = false;
		boolean onlyX_NoSeries = false;
		boolean onlyY_onlyMeasure = false;
		boolean onlyY_WithSeries = false;
		String xAxisCol = CommonUtils.getValueForXmlTag(chartXml, "X-AXIS");
		String yAxisCol = CommonUtils.getValueForXmlTag(chartXml, "Y-AXIS");
		if(!ValidationUtil.isValid(xAxisCol) || !ValidationUtil.isValid(yAxisCol)) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg("X-Axis/Y-Axis value not maintained");
			return exceptionCode;
		}
		String zAxisCol = CommonUtils.getValueForXmlTag(chartXml, "Z-AXIS");
		String seriesCol = CommonUtils.getValueForXmlTag(chartXml, "SERIES");
		String measureProp = CommonUtils.getValueForXmlTag(chartXml, "MEASURE_PROP");
		String isCustomColor = CommonUtils.getValueForXmlTag(chartXml, "isCustomColor");
	    String isRadiantColor = CommonUtils.getValueForXmlTag(chartXml, "isRadiantColor");
		String enableColorPalette = CommonUtils.getValueForXmlTag(chartXml, "EnableColorPalette");
		String userDefinedColorCode = CommonUtils.getValueForXmlTag(chartXml, "ColorCode");
		String userDefinedPalette = CommonUtils.getValueForXmlTag(chartXml, "ColorPalette");
		String genricChartProperties = CommonUtils.getValueForXmlTag(chartXml,"GenericAttributes");
		String drillDownKey = CommonUtils.getValueForXmlTag(chartXml,"DRILLDOWN_KEY");
		String sumLabel = CommonUtils.getValueForXmlTag(chartXml,"sumlabel");
		
		
		String defaultTheme = commonDao.findVisionVariableValue("PRD_WIDGET_BLUE");
		String selectedTheme = "";
		if(ValidationUtil.isValid(widgetTheme))
			selectedTheme = commonDao.findVisionVariableValue("PRD_WIDGET_"+widgetTheme+"");
		else
			selectedTheme = defaultTheme;
		
		//BLUE
		if(!ValidationUtil.isValid(userDefinedPalette))
			userDefinedPalette = selectedTheme;
		
		
		String[] selectedThemeArr = selectedTheme.split(","); 
		if(!ValidationUtil.isValid(userDefinedColorCode))
			userDefinedColorCode = selectedThemeArr[0]; 
		
		try {
			if (ValidationUtil.isValid(xAxisCol) && !ValidationUtil.isValid(yAxisCol)
					&& !ValidationUtil.isValid(seriesCol))
				onlyX_NoSeries = true;
			else if (ValidationUtil.isValid(xAxisCol) && ValidationUtil.isValid(yAxisCol) && yAxisCol.indexOf(",") != -1
					&& !ValidationUtil.isValid(seriesCol))
				multiY_NoSeries = true;
			else if (!ValidationUtil.isValid(xAxisCol) && ValidationUtil.isValid(yAxisCol)
					&& yAxisCol.indexOf(",") != -1 && !ValidationUtil.isValid(seriesCol))
				onlyY_onlyMeasure = true;
			else if (!ValidationUtil.isValid(xAxisCol) && ValidationUtil.isValid(yAxisCol)
					&& yAxisCol.indexOf(",") != -1 && ValidationUtil.isValid(seriesCol))
				onlyY_WithSeries = true;
			String chartXmlPath = servletContext.getRealPath("/WEB-INF/classes/chart_xml/");
			//String chartXmlPath = commonDao.findVisionVariableValue("PRD_CHARTXML_PATH");
			String chartXML="";
			if(!ValidationUtil.isValid(chartXmlPath)) {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Chart Xml Path not maintained in Vision_Variables[PRD_CHARTXML_PATH]");
				return exceptionCode;
			}
			if(chartType.contains("FCMap"))
				chartType = "FCMap";
			
			File file = new File(chartXmlPath+chartType+".xml");
			if(!file.exists()) {
	        	exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg(chartXmlPath+chartType+ ".xml file not found!!");
				return exceptionCode;
		    }
			BufferedReader br = new BufferedReader(new FileReader(chartXmlPath+chartType+".xml"));
			StringBuilder chartXmlB = new StringBuilder();
	        String line = br.readLine();
	        while (line != null) {
	        	chartXmlB.append(line);
	        	chartXmlB.append("\n");
	            line = br.readLine();
	        }
	        br.close();
	        String chartMataDataXml = chartXmlB.toString();
			List<String> repeatTagList = new ArrayList<String>();
			Matcher matcherObj = Pattern.compile("\\<REPEATING_TAG\\>(.*?)\\<\\/REPEATING_TAG\\>", Pattern.DOTALL)
					.matcher(chartMataDataXml);
			while (matcherObj.find()) {
				String repeatTags = matcherObj.group(1).replaceAll("\n", "").replaceAll("\r", "")
						.replaceAll("\\s+", " ").trim();
				repeatTagList.add(repeatTags);
			}
			matcherObj = Pattern.compile("\\<chart(.*?)\\<\\/chart\\>", Pattern.DOTALL).matcher(chartMataDataXml);
			if (matcherObj.find()) {
	    		String tempChartXml = matcherObj.group(1);
	    		String postChartXml = tempChartXml.substring(tempChartXml.indexOf('>'),tempChartXml.length());
	    		tempChartXml = tempChartXml.substring(0, tempChartXml.indexOf('>'));
	    		if(ValidationUtil.isValid(genricChartProperties)){
		    		Matcher attributeMatcherObj = Pattern.compile("</(.*?)>",Pattern.DOTALL).matcher(genricChartProperties);
					while(attributeMatcherObj.find()){
						if(tempChartXml.indexOf(" "+attributeMatcherObj.group(1))==-1)
							if("exportFileName".equalsIgnoreCase(attributeMatcherObj.group(1))){
								String value = CommonUtils.getValueForXmlTag(genricChartProperties, attributeMatcherObj.group(1));
								if(!ValidationUtil.isValid(value)){
									value = ValidationUtil.isValid(CommonUtils.getValueForXmlTag(genricChartProperties, "caption"))
													? CommonUtils.getValueForXmlTag(genricChartProperties,"caption")
													: ValidationUtil.isValid(CommonUtils.getValueForXmlTag(genricChartProperties, "subcaption"))
																	? CommonUtils.getValueForXmlTag(genricChartProperties, "subcaption")
																	: "VisionCharts";
								}
								tempChartXml = tempChartXml + " exportFileName=\"" + value + "\"";
							}else
								tempChartXml = tempChartXml + " " + attributeMatcherObj.group(1) + "=\"" +CommonUtils.getValueForXmlTag(genricChartProperties, attributeMatcherObj.group(1)) + "\"";
					}
	    		}
				tempChartXml = tempChartXml + postChartXml;
	    		
	    		chartXML = "<chart"+tempChartXml+"</chart>";
	    	}
			String returnChartXml = chartXML;
			if (ValidationUtil.isValid(chartXML)) {
				if("AngularGauge".equalsIgnoreCase(chartType)) {
					exceptionCode  =updateReturnXmlForGaugeAndWidgets(chartXML, xAxisCol,yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, drillDownKey);
					if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION)
							returnChartXml = (String) exceptionCode.getResponse();
						else
							return exceptionCode;
				}else if("HLinearGauge".equalsIgnoreCase(chartType)) {
					exceptionCode  = getWidgetGrowthRatios(chartXML, xAxisCol,yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, drillDownKey);
					if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION)
							returnChartXml = (String) exceptionCode.getResponse();
						else
							return exceptionCode;
				}else {
					for (String repeatTagMain : repeatTagList) {
						StringBuffer dataExistCheck = new StringBuffer();
						if (onlyX_NoSeries && repeatTagList.size() == 1) {
							exceptionCode = updateReturnXmlForSingleRepeatTag(repeatTagMain, chartXML, xAxisCol, yAxisCol,
									zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck,drillDownKey,chartType,"");
						} else if (multiY_NoSeries && repeatTagList.size() == 2) {
							if (repeatTagMain.indexOf(",") == -1) {
								exceptionCode = updateReturnXmlForSingleRepeatTag(repeatTagMain, chartXML, xAxisCol,
										yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck,drillDownKey,chartType,"");
							} else {
								exceptionCode = updateReturnXmlForMultipleRepeatTagMultiY_NoSeries(repeatTagMain, xAxisCol,
										yAxisCol, zAxisCol, seriesCol, chartXML, rs, dataExistCheck, rsChild,
										returnChartXml,drillDownKey);
							}
						} else if (onlyY_onlyMeasure) {
							exceptionCode = updateReturnXmlForSingleRepeatTag_OnlyMeasure(repeatTagMain, chartXML,
									xAxisCol, yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck,drillDownKey);
						} else if (onlyY_WithSeries) {
							if (repeatTagMain.indexOf(",") == -1) {
								exceptionCode = updateReturnXmlForSingleRepeatTag_OnlyMeasure(repeatTagMain, chartXML,
										xAxisCol, yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck,drillDownKey);
							} else {
								exceptionCode = updateReturnXmlForMultipleRepeatTagMultiY(repeatTagMain, xAxisCol,
										yAxisCol, zAxisCol, seriesCol, chartXML, rs, dataExistCheck, rsChild,
										returnChartXml,drillDownKey);
							}
						} else {
							if (repeatTagMain.indexOf(",") == -1) {
								exceptionCode = updateReturnXmlForSingleRepeatTag(repeatTagMain, chartXML, xAxisCol,
										yAxisCol, zAxisCol, seriesCol, returnChartXml, rsChild, dataExistCheck,drillDownKey,chartType,sumLabel);
								
							} else {
								exceptionCode = updateReturnXmlForMultipleRepeatTag(repeatTagMain, xAxisCol, yAxisCol,
										zAxisCol, seriesCol, chartXML, rs, dataExistCheck, rsChild, returnChartXml,drillDownKey);
							}
						}
						if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION)
							returnChartXml = (String) exceptionCode.getResponse();
						else
							return exceptionCode;
						
					}
				}
			}
			
			if(chartType.contains("FCMap")) {
				ArrayList valueslst = new ArrayList();
				rs.beforeFirst();
				while(rs.next()){
					valueslst.add(rs.getInt(yAxisCol));
				}
				int minval =(int) valueslst.stream()
							.min(Comparator.naturalOrder())
							.get();
				
				int maxval =(int) valueslst.stream()
							.max(Comparator.naturalOrder())
							.get();
				
				int avgVal = maxval/2;
				
				returnChartXml = returnChartXml.replaceAll("!@#MIN!@#", ""+minval);
				returnChartXml = returnChartXml.replaceAll("!@#MAX!@#", ""+maxval+1);
				returnChartXml = returnChartXml.replaceAll("!@#AVG!@#", ""+avgVal);
				returnChartXml = returnChartXml.replaceAll("></set>", " />");
			}
				
			if (ValidationUtil.isValid(returnChartXml)) {
				returnChartXml = returnChartXml.replaceAll("\n", "").replaceAll("\r", "").replaceAll("\\s+", " ");
			}
			List<String> colorAL = new ArrayList<String>();
			String systemPalette = "426FC0,59B697,F57E56,FFD700,DE7CB6,F8BB00,2B9C2B,ba40c3,b95c65,bac883,b96f13,bb5509,b9e5bb,bae653,ba36a1,ba5e94,bb425b,b965ef,ba8e7a,baddc8,b9048c,ba7242,b948b7,bafa01,ba989d,baf076,bb9a35,baca1b,b9abb3,baac4b,ba06bb,b9a090,bb69b6,babff7,b98b4b,bbde60,b9d10e,b90d17,ba7bcc,b9be61,b9789d,b92ae7,bb7c64,b9efde,b9ee47,ba8f12,b9215c,bab5d5,ba671f,b9b43e,baa228,b99706,b93e95,ba0524,ba494f,bb8687,b9bdc9,b86359,ba1a69,ba4be6,b94720,b9da99,bbade1,b98de2,ba0eae,b920c4,bb11dd,ba23f3,bb248b,ba0f46,b9966e,bae7ea,b982c0,b8a884,bad3a5,b902f4,bac18f,b9a91c,ba85ef,bab66d,ba2c7f,ba53d9,ba5471,bb3739,ba5dfc,b9c7eb,b96687,b9350a,bad43d,bbfc30,b97935,b93372,ba3f2d,ba2d17,baa3bf,bbcbb2,ba18d1,bafb98,b95143,ba8458,b9dc31,b9173a,b8edaf,ba70aa,b81f2d,bbf20e";
			if (ValidationUtil.isValid(userDefinedPalette)) {
				systemPalette = userDefinedPalette;
			}
			String color[] = systemPalette.split(",");
			colorAL = Arrays.asList(color[1].split(","));
			if (ValidationUtil.isValid(enableColorPalette) && "Y".equalsIgnoreCase(enableColorPalette)) {
				int colorReplaceIndex = 0;
				matcherObj = Pattern.compile("\\#COLOR\\_CODE\\#", Pattern.DOTALL).matcher(returnChartXml);
				while (matcherObj.find()) {
					colorReplaceIndex++;
				}
				String singleColor = "ba40c3";
				colorAL = returnColorListBasedOnColorCount(colorReplaceIndex, singleColor);
				colorReplaceIndex = 0;
				matcherObj = Pattern.compile("\\#COLOR\\_CODE\\#", Pattern.DOTALL).matcher(returnChartXml);
				while (matcherObj.find()) {
					try {
						returnChartXml = returnChartXml.replaceFirst("\\#COLOR\\_CODE\\#", color[colorReplaceIndex]);
					} catch (Exception e) {
						returnChartXml = returnChartXml.replaceFirst("\\#COLOR\\_CODE\\#", color[colorReplaceIndex]);
						colorReplaceIndex = 0;
					}
					colorReplaceIndex++;
				}
			} else {
				matcherObj = Pattern.compile("\\#COLOR\\_CODE\\#", Pattern.DOTALL).matcher(returnChartXml);
				while (matcherObj.find()) {
					try {
						returnChartXml = returnChartXml.replaceFirst("\\#COLOR\\_CODE\\#",
								ValidationUtil.isValid(userDefinedColorCode) ? userDefinedColorCode : color[0]);
					} catch (Exception e) {
						returnChartXml = returnChartXml.replaceFirst("\\#COLOR\\_CODE\\#",
								ValidationUtil.isValid(userDefinedColorCode) ? userDefinedColorCode : color[0]);
					}
				}
			}
			String temp = "ddkey=";
			String temp1 = "!@#DRILLDOWN_KEY!@#";
			temp = temp+"\""+temp1+"\"";
			if(returnChartXml.contains(temp))
				returnChartXml=returnChartXml.replaceAll(temp, "");
			if(ValidationUtil.isValid(returnChartXml)) {
				exceptionCode.setResponse(returnChartXml);
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
				return exceptionCode;
			} else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("error while getting chartxml");
				return exceptionCode;
			}
		}catch(Exception e) {
			e.printStackTrace();
			logger.error("Exception while getting the Chart xml");
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
			return exceptionCode;
		}
	}
	public ExceptionCode updateReturnXmlForGaugeAndWidgets(String chartXML, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol, String returnChartXml, ResultSet rs, String ddKeyCol) {
	ExceptionCode exceptionCode = new ExceptionCode();
		try {
			rs.beforeFirst();
			int xAxisColVal = 0;
			int yAxisColVal = 0;
			while(rs.next()){
				xAxisColVal = rs.getInt(xAxisCol);
				yAxisColVal = rs.getInt(yAxisCol);
			}
			chartXML = chartXML.replaceAll("!@#X_AXIS!@#", ""+xAxisColVal);
			chartXML = chartXML.replaceAll("!@#Y_AXIS!@#", ""+yAxisColVal);
			int divVal = yAxisColVal/3;
			int range[]= new int[3];
			int value1 = 0;
			for(int i =0;i < 3;i++) {
				chartXML = chartXML.replaceAll("!@#R_VALUE_"+(i+1)+"!@#", ""+value1);
				range[i] = value1;
				value1 = value1+divVal;
				
			}
			returnChartXml = chartXML;
			exceptionCode.setResponse(returnChartXml);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		}
		catch(Exception e) {
				exceptionCode.setErrorMsg(e.getMessage());
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		return exceptionCode;
	}
	private static String transformDOMtoString(org.w3c.dom.Document document) throws TransformerException{
		DOMSource source = new DOMSource(document);
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		StringWriter sw = new StringWriter();
		StreamResult result = new StreamResult(sw);
		transformer.transform(source, result);
		String output = sw.toString().replaceAll("\"", "'");
		output = output.replace("<?xml version='1.0' encoding='UTF-8' standalone='no'?>", "");
		return output;
	}
	
	public static String createReturnAssetRatioWidgetChartData(List<ReportStgVb> result,String minRange,String maxRange,String ratio,String Rev,String margin) throws ParserConfigurationException, TransformerException {
		
		/*if(result == null || result.size() <= 0 || reportsWriterVb==null){
			return "";
		}*/
		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		org.w3c.dom.Document document = db.newDocument();
		org.w3c.dom.Element rootElement = document.createElement("chart");
		document.appendChild(rootElement);
		//rootElement.setAttribute("caption", reportsWriterVb.getReportTitle());
		
		org.w3c.dom.Element styles = document.createElement("styles"); 	
		org.w3c.dom.Element definition = document.createElement("definition");
		org.w3c.dom.Element style = document.createElement("style");
		style.setAttribute("name", "myCaptionFont");
		style.setAttribute("type", "font");
		style.setAttribute("font", "Arial");
		style.setAttribute("size", "10");
		style.setAttribute("bold", "1");
		style.setAttribute("color", "000000");
		style.setAttribute("showToolTip", "1");

		definition.appendChild(style);
		org.w3c.dom.Element application = document.createElement("application");
		org.w3c.dom.Element apply = document.createElement("apply");
		apply.setAttribute("toObject", "Caption");
		apply.setAttribute("styles", "myCaptionFont");
		application.appendChild(apply);
		styles.appendChild(definition);
		styles.appendChild(application);
		rootElement.appendChild(styles);
		
		rootElement.setAttribute("manageResize","1");
		//rootElement.setAttribute("paletteColors", colorPallet1);
		rootElement.setAttribute("bgcolor", "FFFFFF");
		rootElement.setAttribute("origW","420");
		rootElement.setAttribute("origH","90");
		rootElement.setAttribute("numberSuffix","%");
		rootElement.setAttribute("showBorder","0");
		rootElement.setAttribute("bgColor","FFFFFF");
		rootElement.setAttribute("ticksBelowGauge","1");
		rootElement.setAttribute("valuePadding","0");
		rootElement.setAttribute("showToolTip", "1");

		rootElement.setAttribute("gaugeFillMix","");
		rootElement.setAttribute("showGaugeBorder","0");
		rootElement.setAttribute("pointerOnTop","0");
		rootElement.setAttribute("pointerRadius","5");
		rootElement.setAttribute("pointerBorderColor","000000");
		rootElement.setAttribute("pointerBgColor","000000");
		rootElement.setAttribute("annRenderDelay","0");
		rootElement.setAttribute("showShadow","0");
		rootElement.setAttribute("minorTMNumber","0");
		rootElement.setAttribute("baseFontColor","000000");
		rootElement.setAttribute("animation","0");
		
		org.w3c.dom.Element categ = document.createElement("annotations");

		//rootElement.setAttribute("yAxisName",resultCatagory);

		for(ReportStgVb vObject: result){
			
			rootElement.setAttribute("lowerLimit",minRange);
			rootElement.setAttribute("upperLimit",maxRange);
		
			
			org.w3c.dom.Element colorRange = document.createElement("colorRange");
			org.w3c.dom.Element color = document.createElement("color");
			color.setAttribute("minValue","0");
			color.setAttribute("maxValue","100");
			color.setAttribute("alpha", "0");
			colorRange.appendChild(color);
			rootElement.appendChild(colorRange);

			org.w3c.dom.Element pointers = document.createElement("pointers");
			org.w3c.dom.Element pointer = document.createElement("pointer");
			pointer.setAttribute("value",ratio.trim());
			pointers.appendChild(pointer);
			rootElement.appendChild(pointers);
			org.w3c.dom.Element categories = document.createElement("annotationGroup");
			org.w3c.dom.Element categories1 = document.createElement("annotation");
			categories.setAttribute("showBelow","0");
			categories.setAttribute("x", "210");
			categories.setAttribute("y", "-765");
			categories1.setAttribute("type", "circle");
			categories1.setAttribute("radius", "800");
			categories1.setAttribute("color", "FFFFFF");
			categories.appendChild(categories1);
			categ.appendChild(categories);
				
			org.w3c.dom.Element categories21 = document.createElement("annotationGroup");
			org.w3c.dom.Element categories2 = document.createElement("annotation");
			categories21.setAttribute("showBelow","1");
			categories2.setAttribute("type", "rectangle");
			categories2.setAttribute("x", "$gaugeStartX");
			categories2.setAttribute("y", "$gaugeStartY");
			categories2.setAttribute("toX", "$gaugeEndX");
			categories2.setAttribute("toY", "$gaugeEndY");
			if("margin".equalsIgnoreCase(margin)){
				categories2.setAttribute("fillColor", "E00000,FCEF27,678000");
			}else{
				if(Rev==""){
					categories2.setAttribute("fillColor", "678000,FCEF27,E00000");
				}else{
					categories2.setAttribute("fillColor", "E00000,FCEF27,678000");
				}

			}
			categories21.appendChild(categories2);
			categ.appendChild(categories21);

			org.w3c.dom.Element categories31 = document.createElement("annotationGroup");
			org.w3c.dom.Element categories3 = document.createElement("annotation");
			org.w3c.dom.Element categories4 = document.createElement("annotation");
			
			categories31.setAttribute("showBelow","0");
			categories3.setAttribute("type", "text");
			categories3.setAttribute("x", "$gaugeStartX+25");
			categories3.setAttribute("y", "40");
			categories3.setAttribute("size", "10");
			categories3.setAttribute("color", "FFFFFF");
			categories3.setAttribute("bold", "1");
			if("margin".equalsIgnoreCase(margin)){
				categories3.setAttribute("label", "Bad");
			}else{
				String disp="Bad";
				if(Rev==""){
					disp="Good";
				}
				categories3.setAttribute("label", disp);
			}
			categories4.setAttribute("type", "text");
			categories4.setAttribute("x", "$gaugeEndX-25");
			categories4.setAttribute("y", "40");
			categories4.setAttribute("size", "10");
			categories4.setAttribute("color", "FFFFFF");
			categories4.setAttribute("bold", "1");
			if("margin".equalsIgnoreCase(margin)){
				categories4.setAttribute("label", "Good");
			}else{
				String disp1="Good";
				if(Rev==""){
					disp1="Bad";
				}
				categories4.setAttribute("label", disp1);
			}
		
			categories31.appendChild(categories3);
			categories31.appendChild(categories4);
			categ.appendChild(categories31);
			
			rootElement.appendChild(categ);

		}
		return transformDOMtoString(document);
	}
	public ExceptionCode getWidgetGrowthRatios(String chartXML, String xAxisCol, String yAxisCol, String zAxisCol, String seriesCol, String returnChartXml, ResultSet rs, String ddKeyCol){
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			String chartData = "";
			List<ReportStgVb> arrayList = null;
			rs.beforeFirst();
			int minVal = 0;
			int maxVal = 0;
			float ratio = 0;
			String margin = "";
			// String[] chartDataarr = null;
			StringBuffer resChartData = new StringBuffer();
			int i = 1;
			while (rs.next()) {
				chartData = "";
				minVal = rs.getInt(xAxisCol);
				maxVal = rs.getInt(yAxisCol);
				ratio = rs.getInt(zAxisCol);
				margin = rs.getString(seriesCol);
				chartData = chartXML.replaceAll("!@#X_AXIS!@#", "" + minVal);
				chartData = chartData.replaceAll("!@#Y_AXIS!@#", "" + maxVal);
				chartData = chartData.replaceAll("!@#Z_AXIS!@#", "" + ratio);
				resChartData.append(chartData);
				if (i == 3)
					break;
				i++;
				// resChartData.append("@-:@");
			}

			exceptionCode.setResponse(resChartData.toString());
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		}catch(Exception ex){
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ex.getMessage());
		}
		return exceptionCode;
	}
}
