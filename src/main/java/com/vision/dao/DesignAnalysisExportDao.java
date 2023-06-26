package com.vision.dao;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.vision.util.ValidationUtil;
import com.vision.vb.WidgetDesignVb;

@Component
public class DesignAnalysisExportDao extends AbstractDao<WidgetDesignVb>{
	public JdbcTemplate jdbcTemplate = null;

	public DesignAnalysisExportDao(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	
	@SuppressWarnings("unchecked")
	public String formXMLDataForGidWithQuery(String query,ArrayList colTypes, int columnCount) {
		return jdbcTemplate.query(String.valueOf(query), new ResultSetExtractor<String>() {
			@Override
			public String extractData(ResultSet rs) throws SQLException, DataAccessException {
				StringBuffer resultStrBuf = new StringBuffer();
				resultStrBuf.append("<tableData>");
				while(rs.next()) {
					resultStrBuf.append("<tableRow>");
					for(int colIndex = 0;colIndex<=(columnCount-1);colIndex++) {
						String value = rs.getString(colIndex+1);
						if(value == null) value="";
						value =value.replaceAll("\\\\","@-@");
						resultStrBuf.append("<c"+colIndex+">"+value+"</c"+colIndex+">");
					}
					resultStrBuf.append("</tableRow>");
				}
				resultStrBuf.append("</tableData>");
				String str2 = resultStrBuf.toString().replaceAll("@-@","/");
				StringBuffer str3 = new StringBuffer(str2);
				return StringEscapeUtils.unescapeJava(str3.toString());
			}
		});
	}
	
	@SuppressWarnings("unchecked")
	public void wrideDataForGidWithQuery(Sheet sheet, XSSFWorkbook wb, String xmlData,ArrayList colTypes, int columnCount, int rowStartIndex,Map<Integer,Integer> columnWidths) throws SAXException, IOException, ParserConfigurationException {
		
		int rowIndex = rowStartIndex+1;
		Map<String, CellStyle> styleMap = getStyleMapBasedOnCeelType(wb);
		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		//Using factory get an instance of document builder
		DocumentBuilder db = dbf.newDocumentBuilder();
		xmlData = xmlData.replaceAll("&", "&amp;");
		InputStream is = new ByteArrayInputStream(xmlData.getBytes(Charset.forName("UTF-8")));
		//parse using builder to get DOM representation of the XML file
		Document dom = db.parse(is);
		Element docEle = dom.getDocumentElement();
		NodeList nl = docEle.getElementsByTagName("tableRow");
		if(nl != null && nl.getLength() > 0) {
			for(int loopCnt = 0 ; loopCnt < nl.getLength();loopCnt++) {
				Row row = sheet.createRow(rowIndex);
				//get the employee element
				Element el = (Element)nl.item(loopCnt);
				NodeList ncl  = el.getChildNodes();
				for(int loopCount=0;loopCount<ncl.getLength();loopCount++){
					String cellType = colTypes.get(loopCount).toString();
					CellStyle style = (cellType.equalsIgnoreCase("N")||cellType.equalsIgnoreCase("C"))?styleMap.get("N"):styleMap.get("O");
					Element el1= (Element)ncl.item(loopCount);
					String cellValue = el1 != null && el1.getFirstChild() != null? ValidationUtil.replaceComma(el1.getFirstChild().getNodeValue().trim()):"";
					Cell cell = row.createCell(loopCount);
					cell.setCellStyle(style);
					cell.setCellValue(cellValue);
//					CellUtil.createCell(row, loopCount, cellValue, style);
				}
				rowIndex++;
			}
		}
	
	}
	private static CellStyle setDataCellStyle(Workbook workbook,String colType) {
		/* create font */
		Font font = workbook.createFont();
		font.setColor(IndexedColors.BLACK.index);
		font.setBoldweight(Font.BOLDWEIGHT_NORMAL);
		font.setFontName("Calibri");
		font.setFontHeightInPoints((short)10);
		font.setItalic(false);
		/*byte[] pinkClr = { (byte) 230, (byte) 184, (byte) 183 };
		XSSFColor pinkXClor = new XSSFColor(pinkClr);*/
		
		/* Create cell style */
		XSSFCellStyle style = (XSSFCellStyle)workbook.createCellStyle();
		if("N".equalsIgnoreCase(colType) || "C".equalsIgnoreCase(colType)) {
			style.setAlignment(CellStyle.ALIGN_RIGHT);
		}else {
			style.setAlignment(CellStyle.ALIGN_LEFT);
		}
		
		style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		/* Setting font to style */
		style.setFont(font);
		// style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		//Cell Border Color for Data Cell
		byte[] pinkClr = { (byte) 230, (byte) 184, (byte) 183 };
		XSSFColor pinkXClor = new XSSFColor(pinkClr);
		//Header Color
		byte[] sunoidaPinkClr = { (byte) 177, (byte) 24, (byte) 124 };
		XSSFColor sunoidaPinkXClr = new XSSFColor(sunoidaPinkClr);
		style.setFillForegroundColor(IndexedColors.WHITE.index);
		style.setBorderBottom(CellStyle.BORDER_THIN);
	    style.setBorderRight(CellStyle.BORDER_THIN);
	    style.setBottomBorderColor(pinkXClor);
	    style.setRightBorderColor(pinkXClor);
		return style;
	}
	
	private static Map<String, CellStyle> getStyleMapBasedOnCeelType(Workbook workbook) {
		
		Map<String, CellStyle> styleMap = new HashMap<String, CellStyle>();
		
		/* create font */
		Font font = workbook.createFont();
		font.setColor(IndexedColors.BLACK.index);
		font.setBoldweight(Font.BOLDWEIGHT_NORMAL);
		font.setFontName("Calibri");
		font.setFontHeightInPoints((short)10);
		font.setItalic(false);
		
		//	Cell Border Color for Data Cell
		byte[] pinkClr = { (byte) 230, (byte) 184, (byte) 183 };
		XSSFColor pinkXClor = new XSSFColor(pinkClr);
		//	Header Color
		byte[] sunoidaPinkClr = { (byte) 177, (byte) 24, (byte) 124 };
		XSSFColor sunoidaPinkXClr = new XSSFColor(sunoidaPinkClr);
		
		
		/* Cell style for Number */
		XSSFCellStyle numberStyle = (XSSFCellStyle)workbook.createCellStyle();
		numberStyle.setAlignment(CellStyle.ALIGN_RIGHT);
		numberStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		/* Setting font to style */
		numberStyle.setFont(font);
		numberStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);
		numberStyle.setFillForegroundColor(IndexedColors.WHITE.index);
		numberStyle.setBorderBottom(CellStyle.BORDER_THIN);
	    numberStyle.setBorderRight(CellStyle.BORDER_THIN);
	    numberStyle.setBottomBorderColor(pinkXClor);
	    numberStyle.setRightBorderColor(pinkXClor);
	    
	    styleMap.put("N", numberStyle);	    
	    
	    
	    /* Cell style for Text */
	    XSSFCellStyle textStyle = (XSSFCellStyle)workbook.createCellStyle();
		textStyle.setAlignment(CellStyle.ALIGN_LEFT);
		textStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		/* Setting font to style */
		textStyle.setFont(font);
		textStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);
		textStyle.setFillForegroundColor(IndexedColors.WHITE.index);
		textStyle.setBorderBottom(CellStyle.BORDER_THIN);
	    textStyle.setBorderRight(CellStyle.BORDER_THIN);
	    textStyle.setBottomBorderColor(pinkXClor);
	    textStyle.setRightBorderColor(pinkXClor);
	    
	    styleMap.put("O", textStyle);
	    
		return styleMap;
	}
	
	public int returnRecordCount(String query) {
		String countSQL = "select COUNT(1) COUNT from ("+query+") TEMP";
		return jdbcTemplate.queryForObject(countSQL,Integer.class);
	}
}
