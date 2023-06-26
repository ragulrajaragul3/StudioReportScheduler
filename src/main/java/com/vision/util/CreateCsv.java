package com.vision.util;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactoryConfigurationError;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.vision.vb.ColumnHeadersVb;
import com.vision.vb.ReportStgVb;
import com.vision.vb.ReportsVb;
import com.vision.wb.ReportsWb;
@Component
public class CreateCsv {
	public static final String CAP_COL = "captionColumn";
	public static final String DATA_COL = "dataColumn";
	@Autowired
	ReportsWb reportsWb;
	
	
	private String findValue(ReportStgVb reportsStgs, String type, int index){
		if(CAP_COL.equalsIgnoreCase(type)){
			switch(index){
				case 1: return reportsStgs.getCaptionColumn1() == null ? "" : reportsStgs.getCaptionColumn1().replaceAll(",", "");
				case 2: return reportsStgs.getCaptionColumn2() == null ? "" : reportsStgs.getCaptionColumn2().replaceAll(",", "");
				case 3: return reportsStgs.getCaptionColumn3() == null ? "" : reportsStgs.getCaptionColumn3().replaceAll(",", "");
				case 4: return reportsStgs.getCaptionColumn4() == null ? "" : reportsStgs.getCaptionColumn4().replaceAll(",", "");
				case 5: return reportsStgs.getCaptionColumn5() == null ? "" : reportsStgs.getCaptionColumn5().replaceAll(",", "");
			}
		}else if (DATA_COL.equalsIgnoreCase(type)){
			switch(index){
				case 1 : return reportsStgs.getDataColumn1()  == null? "": reportsStgs.getDataColumn1() ;
				case 2 : return reportsStgs.getDataColumn2()  == null? "": reportsStgs.getDataColumn2() ;
				case 3 : return reportsStgs.getDataColumn3()  == null? "": reportsStgs.getDataColumn3() ;
				case 4 : return reportsStgs.getDataColumn4()  == null? "": reportsStgs.getDataColumn4() ;
				case 5 : return reportsStgs.getDataColumn5()  == null? "": reportsStgs.getDataColumn5() ;
				case 6 : return reportsStgs.getDataColumn6()  == null? "": reportsStgs.getDataColumn6() ;
				case 7 : return reportsStgs.getDataColumn7()  == null? "": reportsStgs.getDataColumn7() ;
				case 8 : return reportsStgs.getDataColumn8()  == null? "": reportsStgs.getDataColumn8() ;
				case 9 : return reportsStgs.getDataColumn9()  == null? "": reportsStgs.getDataColumn9() ;
				case 10: return reportsStgs.getDataColumn10() == null? "": reportsStgs.getDataColumn10(); 
				case 11: return reportsStgs.getDataColumn11() == null? "": reportsStgs.getDataColumn11(); 
				case 12: return reportsStgs.getDataColumn12() == null? "": reportsStgs.getDataColumn12(); 
				case 13: return reportsStgs.getDataColumn13() == null? "": reportsStgs.getDataColumn13(); 
				case 14: return reportsStgs.getDataColumn14() == null? "": reportsStgs.getDataColumn14(); 
				case 15: return reportsStgs.getDataColumn15() == null? "": reportsStgs.getDataColumn15(); 
				case 16: return reportsStgs.getDataColumn16() == null? "": reportsStgs.getDataColumn16(); 
				case 17: return reportsStgs.getDataColumn17() == null? "": reportsStgs.getDataColumn17(); 
				case 18: return reportsStgs.getDataColumn18() == null? "": reportsStgs.getDataColumn18(); 
				case 19: return reportsStgs.getDataColumn19() == null? "": reportsStgs.getDataColumn19(); 
				case 20: return reportsStgs.getDataColumn20() == null? "": reportsStgs.getDataColumn20(); 
				case 21: return reportsStgs.getDataColumn21() == null? "": reportsStgs.getDataColumn21(); 
				case 22: return reportsStgs.getDataColumn22() == null? "": reportsStgs.getDataColumn22(); 
				case 23: return reportsStgs.getDataColumn23() == null? "": reportsStgs.getDataColumn23(); 
				case 24: return reportsStgs.getDataColumn24() == null? "": reportsStgs.getDataColumn24(); 
				case 25: return reportsStgs.getDataColumn25() == null? "": reportsStgs.getDataColumn25(); 
				case 26: return reportsStgs.getDataColumn26() == null? "": reportsStgs.getDataColumn26(); 
				case 27: return reportsStgs.getDataColumn27() == null? "": reportsStgs.getDataColumn27(); 
				case 28: return reportsStgs.getDataColumn28() == null? "": reportsStgs.getDataColumn28(); 
				case 29: return reportsStgs.getDataColumn29() == null? "": reportsStgs.getDataColumn29(); 
				case 30: return reportsStgs.getDataColumn30() == null? "": reportsStgs.getDataColumn30(); 
				case 31: return reportsStgs.getDataColumn31() == null? "": reportsStgs.getDataColumn31(); 
				case 32: return reportsStgs.getDataColumn32() == null? "": reportsStgs.getDataColumn32(); 
				case 33: return reportsStgs.getDataColumn33() == null? "": reportsStgs.getDataColumn33(); 
				case 34: return reportsStgs.getDataColumn34() == null? "": reportsStgs.getDataColumn34(); 
				case 35: return reportsStgs.getDataColumn35() == null? "": reportsStgs.getDataColumn35(); 
				case 36: return reportsStgs.getDataColumn36() == null? "": reportsStgs.getDataColumn36(); 
				case 37: return reportsStgs.getDataColumn37() == null? "": reportsStgs.getDataColumn37(); 
				case 38: return reportsStgs.getDataColumn38() == null? "": reportsStgs.getDataColumn38(); 
				case 39: return reportsStgs.getDataColumn39() == null? "": reportsStgs.getDataColumn39(); 
				case 40: return reportsStgs.getDataColumn40() == null? "": reportsStgs.getDataColumn40();
			}
		}
		return "";
	}
	
	
	
	public void writeDataToCSV(String xmlData, PrintWriter out, String csvSeparator, String lineSeparator, List<String> colTyps ){
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			NumberFormat fromat = new DecimalFormat("0000000000000000.000000");
			//Using factory get an instance of document builder
			DocumentBuilder db = dbf.newDocumentBuilder();
			//InputStream is = new ByteArrayInputStream(xmlData.getBytes()); 
			InputStream is = new ByteArrayInputStream(xmlData.getBytes("UTF-8"));
			//parse using builder to get DOM representation of the XML file
			org.w3c.dom.Document dom = db.parse(is);
			org.w3c.dom.Element docEle = dom.getDocumentElement();
			NodeList nl = docEle.getElementsByTagName("tableRow");
			if(nl != null && nl.getLength() > 0) {
				for(int i = 0 ; i < nl.getLength();i++) {
					Element el = (Element)nl.item(i);
					NodeList ncl  = el.getChildNodes();
					for(int loopCount=0;loopCount<ncl.getLength();loopCount++){
						Element el1= (Element)ncl.item(loopCount);
						if("formatType".equalsIgnoreCase(el1.getNodeName())){
							continue;
						}
						if(!"N".equalsIgnoreCase(colTyps.get(loopCount))){
							if(el1.getFirstChild()!=null){
								if(el1.getFirstChild().getNodeValue() != null && el1.getFirstChild().getNodeValue().contains(",")){
									out.print("\""+el1.getFirstChild().getNodeValue()+"\"");
								}else{
									out.print(el1.getFirstChild().getNodeValue());
								}
								if(ncl.getLength() != (loopCount+1))
								   out.print(csvSeparator);
							}else{
								out.print("");
								if(ncl.getLength() != (loopCount+1))
								    out.print(csvSeparator);
							}
						}else{
							String value = "";
							if(el1.getFirstChild()!=null){
								value = el1.getFirstChild().getNodeValue();
								value = value.replaceAll(",", "");
								if(value!=""){
									Double value1 = Double.parseDouble(value);
									out.print(fromat.format(value1));
									if(ncl.getLength() != (loopCount+1))
									   out.print(csvSeparator);
								}
							}else{
								out.print("");
								if(ncl.getLength() != (loopCount+1))
								    out.print(csvSeparator);
							}
						}
					}
					out.print(lineSeparator);
				}
			}
		} catch (TransformerFactoryConfigurationError e) {
			System.out.println("catch - TransformerFactoryConfigurationError  : "+e);
			e.printStackTrace();
		}catch(ParserConfigurationException pce) {
			System.out.println("catch - ParserConfigurationException  : "+pce);
			pce.printStackTrace();
		}catch(SAXException se) {
			System.out.println("catch - SAXException  : "+se);
			se.printStackTrace();
		}catch(IOException ioe) {
			System.out.println("catch - IOException  : "+ioe);
			ioe.printStackTrace();
		}
	}
	public List<ColumnHeadersVb> sortColumnHeaders(List<ColumnHeadersVb> columnHeaders){
		List<ColumnHeadersVb> columnHeadersTemp = new ArrayList<ColumnHeadersVb>();
		for(int loopCount = 0; loopCount < columnHeaders.size(); loopCount++){
			for(ColumnHeadersVb columnHeadersVb1 : columnHeaders){
				if(columnHeadersVb1.getLabelColNum() == (loopCount+1) /*&& columnHeadersVb1.getColSpanNum() == 0*/)
					columnHeadersTemp.add(columnHeadersVb1);
			}
		}
		return columnHeadersTemp;
	}

	public int writeHeadersToCsv(List<ColumnHeadersVb> columnHeaders, ReportsVb reportsVb, FileWriter fw, int rowNum,PrintWriter out,String csvSeparator) {
		int colNum = 0;
		try {
			int maxHeaderRow = columnHeaders.stream().mapToInt(ColumnHeadersVb::getLabelRowNum).max().orElse(0);
			if (maxHeaderRow >= 2) {
				rowNum++;
				for (int loopCount = 0; loopCount < columnHeaders.size(); loopCount++) {
					ColumnHeadersVb columnHeadersVb1 = columnHeaders.get(loopCount);
					if (columnHeadersVb1.getCaption().contains("<br/>")) {
						String value = columnHeadersVb1.getCaption().replaceAll("<br/>", " ");
						columnHeadersVb1.setCaption(value);
					}
					out.print(columnHeadersVb1.getCaption());
					out.print(csvSeparator);
					//data[loopCount] = columnHeadersVb1.getCaption();
					++colNum;

				}
				/*List<String[]> dataLst = new ArrayList<String[]>();
				dataLst.add(data);
				writer.writeAll(dataLst);*/

			} else {
				for (ColumnHeadersVb columnHeadersVb : columnHeaders) {
					if (columnHeadersVb.getCaption().contains("<br/>")) {
						String value = columnHeadersVb.getCaption().replaceAll("<br/>", " ");
						columnHeadersVb.setCaption(value);
					}
					out.print(columnHeadersVb.getCaption());
					out.print(csvSeparator);
					//data[colNum] = columnHeadersVb.getCaption();
					++colNum;
				}
				/*List<String[]> dataLst = new ArrayList<String[]>();
				dataLst.add(data);
				writer.writeAll(dataLst);*/
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rowNum;
	}
	public int writeDataToCsv(List<ColumnHeadersVb> columnHeaders, List<HashMap<String, String>> dataLst,
			ReportsVb reportsVb, FileWriter fw, int rowNum,PrintWriter out,String csvSeparator) {
		int colNum = 0;
		String lineSeparator = "\n";
		try {
			colNum = 0;
			int ctr = 1;
			List<String> colTypes = new ArrayList<String>();
				Map<Integer,Integer> columnWidths = new HashMap<Integer,Integer>(columnHeaders.size());
				for(int loopCnt= 0; loopCnt < columnHeaders.size(); loopCnt++){
					columnWidths.put(Integer.valueOf(loopCnt), Integer.valueOf(-1));
					ColumnHeadersVb colHVb = columnHeaders.get(loopCnt);
					if(colHVb.getColspan() <= 1 && colHVb.getNumericColumnNo() != 99) {
						colTypes.add(colHVb.getColType());
					}
				}
			for (HashMap dataMap : dataLst) {
				out.print(lineSeparator);
				for (int loopCount = 0; loopCount < columnHeaders.size(); loopCount++) {
					ColumnHeadersVb colHeadersVb = columnHeaders.get(loopCount);
					if(colHeadersVb.getColspan() > 1 || colHeadersVb.getNumericColumnNo() == 99)
						continue;
					String type = "";
					String colType = colHeadersVb.getColType();
					if ("T".equalsIgnoreCase(colHeadersVb.getColType())) {
						type = CAP_COL;
					} else {
						type = DATA_COL;
					}
					if (type.equalsIgnoreCase(CAP_COL)) {
						String value = "";
						if (dataMap.containsKey(colHeadersVb.getDbColumnName())) {
							value = ((dataMap.get(colHeadersVb.getDbColumnName())) != null
									|| dataMap.get(colHeadersVb.getDbColumnName()) == "")
											? dataMap.get(colHeadersVb.getDbColumnName()).toString()
											: "";
						}
						if(ValidationUtil.isValid(value)) {
							out.print(value.toString());
						}else {
							out.print("");
						}
						out.print(csvSeparator);
					} else {
						String value = "";
						if (dataMap.containsKey(colHeadersVb.getDbColumnName())) {
							value = ((dataMap.get(colHeadersVb.getDbColumnName())) != null
									|| dataMap.get(colHeadersVb.getDbColumnName()) == "")
											? dataMap.get(colHeadersVb.getDbColumnName()).toString()
											: "";
						}
						value = value.replaceAll(",", "");
						if(ValidationUtil.isValid(value)) {
							out.print(value.toString());
						}else {
							out.print("");
						}
						
						out.print(csvSeparator);
					}
				}
				rowNum++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rowNum;
	}
	}