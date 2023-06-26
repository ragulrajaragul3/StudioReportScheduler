package com.vision.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.RegionUtil;
import org.apache.poi.ss.util.SheetUtil;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;

import com.vision.vb.ColumnHeadersVb;
import com.vision.vb.ReportsVb;
import com.vision.vb.VcForQueryReportFieldsWrapperVb;
import com.vision.wb.ScheduledReportWb;

public class ExcelExportUtil {
	public static int MAX_FETCH_RECORDS = 3000;
	public static int CELL_STYLE_HEADER_CAP_COL = 0; //csHeaderCaptionCol
	public static int CELL_STYLE_MID_HEADER_CAP_COL = 1; //csMidHeaderCaptionCol
	public static int CELL_STYLE_HEADER_DATA_COL = 2; //csHeaderDataCol
	public static int CELL_STYLE_DETAILS_CAP_COL = 3; //csDataAlt1 - White back ground
	public static int CELL_STYLE_DETAILS_CAP_COL_ALT = 4; //csDataAlt2 -Cream back Ground
	public static int CELL_STYLE_DETAILS_DATA_COL = 5; //csDataAlt1Data - White back ground - For Numeric Fields
	public static int CELL_STYLE_DETAILS_DATA_COL_ALT = 6; //csDataAlt2Data -Cream back Ground - For Numeric Fields
	public static int CELL_STYLE_SUMMERY_CAP_COL = 7; //csSumary 
	public static int CELL_STYLE_SUMMERY_DATA_COL = 8; //csSumaryData - For Numeric Fields
	public static int CELL_STYLE_SUMMERY_CAP_COL_ALT = 9; //csSumaryAlt2 - Cream back Ground 
	public static int CELL_STYLE_SUMMERY_DATA_COL_ALT = 10; //csSumaryData - Cream back Ground  - For Numeric Fields
	public static int CELL_STYLE_TITLE_CAP = 11; // csTitleCaption - require for Scheduler
	public static int CELL_STYLE_PROMPTS = 12; // csPrompt - require for Scheduler
	public static int CELL_STYLE_HEADER_CAP_COL_TOP = 13; //csHeaderCaptionColTop
	

	public static int CELL_STYLE_DETAILS_DATA_COL_COUNT = 14; //csDataAlt1Data - White back ground - For Numeric Fields (Non Decimals)
	public static int CELL_STYLE_DETAILS_DATA_COL_COUNT_ALT = 15; //csDataAlt2Data -Cream back Ground - For Numeric Fields (Non Decimals)
	public static int CELL_STYLE_SUMMERY_DATA_COL_COUNT = 16; //csSumaryData - For Numeric Fields (Non Decimals)
	public static int CELL_STYLE_SUMMERY_DATA_COL_COUNT_ALT = 17; //csSumaryData - Cream back Ground  - For Numeric Fields  (Non Decimals)
	public static int CELL_STYLE_TITLES = 18; //csSumaryData 
	public static int CELL_STYLE_REPORT_PROMPTS =19;
	public static int CELL_STYLE_REPORT_PROMPTS_CAP = 20;
	public static int  CELL_STYLE__END_CAPTION  = 21;
	public static final String CAP_COL = "captionColumn";
	public static final String DATA_COL = "dataColumn";
	
	@Autowired
	static
    ResourceLoader resourceLoader;
	
	private static XSSFCellStyle createStyle(Workbook workBook, XSSFColor color, short allign, Font font, 
			String formatString, short borderBottom, XSSFColor borderBottomColor){
		XSSFCellStyle style = (XSSFCellStyle)workBook.createCellStyle();
		style.setFillForegroundColor(color);
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
	    style.setAlignment(allign);
	    style.setFont(font);
	    if(formatString != null){
	    	style.setDataFormat((short)BuiltinFormats.getBuiltinFormat(formatString));	
	    }
	    if(borderBottom != CellStyle.BORDER_NONE){
	    	style.setBorderBottom(borderBottom);
	    	style.setBorderRight(borderBottom);
	    	style.setBottomBorderColor(borderBottomColor);
	    	style.setRightBorderColor(borderBottomColor);
	    }
	    return style;
	}
	private static Font createFont(Workbook workBook, short colorIdx, short fontWeight, String fontName, int heightInPoints){
		Font font = workBook.createFont();
		font.setColor(colorIdx);
		font.setBoldweight(fontWeight);
		font.setFontName(fontName);
		font.setFontHeightInPoints((short)heightInPoints);
		return font;
	}
	private static Font createNewFont(Workbook workBook, short fontWeight, String fontName, int heightInPoints){
		Font font = workBook.createFont();
		byte[] greenClr = {(byte) 177, (byte) 24, (byte) 124};
		XSSFColor rowClr = new XSSFColor(greenClr);
		font.setColor(rowClr.getIndexed());
		font.setBoldweight(fontWeight);
		font.setFontName(fontName);
		font.setFontHeightInPoints((short)heightInPoints);
		return font;
	}
	public static Map<Integer, XSSFCellStyle> createStyles(Workbook workBook,String applicationTheme){
        Map<Integer, XSSFCellStyle> styles = new HashMap<Integer, XSSFCellStyle>();
		Font fontHeader = createFont(workBook, IndexedColors.WHITE.index, Font.BOLDWEIGHT_BOLD, "Calibri", 10);
		Font fontData  = createFont(workBook, IndexedColors.BLACK.index, Font.BOLDWEIGHT_NORMAL, "Calibri", 10);
		Font fontSummary = createFont(workBook, IndexedColors.BLACK.index, Font.BOLDWEIGHT_BOLD, "Calibri", 10);
		Font fontHeaderTitle = createFont(workBook, IndexedColors.GREEN.index, Font.BOLDWEIGHT_BOLD, "Calibri", 10);
		Font fontDetails  = createFont(workBook, IndexedColors.WHITE.index, Font.BOLDWEIGHT_BOLD, "Calibri", 10);
		Font fontPrompts  = createNewFont(workBook, Font.BOLDWEIGHT_BOLD, "Calibri", 10);
		
		/*byte[] greenClr = {(byte) 79, (byte) 98, (byte) 40};*/
	//	byte[] greenClr = {(byte) 3, (byte) 80, (byte) 122};
		byte[] greenClr = {(byte) 0, (byte) 92, (byte) 140};
		XSSFColor greenXClor = new XSSFColor(greenClr);
		byte[] pinkClr = {(byte) 230, (byte) 184, (byte) 183};
		XSSFColor pinkXClor = new XSSFColor(pinkClr);
		XSSFColor whiteClr = new XSSFColor();
	    whiteClr.setIndexed(IndexedColors.WHITE.index);
	    byte[] creemClr = {(byte) 205, (byte) 226, (byte) 236};
		XSSFColor creemXClor = new XSSFColor(creemClr);
		//Dark pink for Summary 
		byte[] darkPinkClr = {(byte) 177, (byte) 19, (byte) 27};
		XSSFColor blackClr = new XSSFColor(darkPinkClr);
		byte[] DgreenClr = {(byte) 54, (byte) 67, (byte) 27};
		XSSFColor DgreenXClor = new XSSFColor(DgreenClr);
		
		byte[] sunoidaPinkClr =  getRGB(applicationTheme); //{(byte) 177, (byte) 24, (byte) 124};
		//byte[] sunoidaPinkClr = {(byte) 177, (byte) 24, (byte) 124};
		XSSFColor sunoidaPinkXClr = new XSSFColor(sunoidaPinkClr);
		//grey color
		byte[] lightGrey = {(byte) 242, (byte) 244, (byte) 242};
		XSSFColor lightGreyXColor = new XSSFColor(lightGrey);
		
		XSSFColor blackcolor = new XSSFColor();
		blackcolor.setIndexed(IndexedColors.BLACK.index);
		XSSFCellStyle csHeaderCaptionColTop = createStyle(workBook, sunoidaPinkXClr, CellStyle.ALIGN_CENTER_SELECTION, fontHeader, null, CellStyle.BORDER_THIN , whiteClr); //For Multi headers reports
		XSSFCellStyle csHeaderCaptionCol =  createStyle(workBook, greenXClor, CellStyle.ALIGN_LEFT, fontHeader, null, CellStyle.BORDER_NONE , null);// For header
		XSSFCellStyle csMidHeaderCaptionCol =  createStyle(workBook, DgreenXClor, CellStyle.ALIGN_LEFT, fontHeader, null, CellStyle.BORDER_NONE , null);// For Mid header
	    XSSFCellStyle csDataAlt1 = createStyle(workBook, whiteClr, CellStyle.ALIGN_GENERAL,fontData, BuiltinFormats.getBuiltinFormat(0), CellStyle.BORDER_THIN, pinkXClor); //For Caption With background
	    XSSFCellStyle csDataAlt1Data =  createStyle(workBook, whiteClr, CellStyle.ALIGN_RIGHT, fontData, BuiltinFormats.getBuiltinFormat(4), CellStyle.BORDER_THIN, pinkXClor); //For Data With background
	    XSSFCellStyle csDataAlt2 =  createStyle(workBook, creemXClor, CellStyle.ALIGN_GENERAL, fontData, BuiltinFormats.getBuiltinFormat(0), CellStyle.BORDER_THIN, pinkXClor);//For Caption With out background
	    XSSFCellStyle csDataAlt2Data =  createStyle(workBook, creemXClor, CellStyle.ALIGN_RIGHT, fontData, BuiltinFormats.getBuiltinFormat(4), CellStyle.BORDER_THIN, pinkXClor);//For Data With out background
		XSSFCellStyle csHeaderDataCol = createStyle(workBook, greenXClor, CellStyle.ALIGN_RIGHT, fontHeader, null, CellStyle.BORDER_NONE , null);// For header Right Align
		XSSFCellStyle csReportTitle =  createStyle(workBook, whiteClr, CellStyle.ALIGN_CENTER_SELECTION, fontHeaderTitle, null, CellStyle.BORDER_NONE , greenXClor); //For Headings
		
		XSSFCellStyle csSumary = createStyle(workBook, whiteClr, CellStyle.ALIGN_GENERAL, fontSummary, null, CellStyle.BORDER_THIN, pinkXClor);
		csSumary.setBorderTop(CellStyle.BORDER_THIN);
		csSumary.setTopBorderColor(pinkXClor);
		
		XSSFCellStyle csSumaryData = createStyle(workBook, whiteClr, CellStyle.ALIGN_RIGHT, fontSummary, BuiltinFormats.getBuiltinFormat(4), CellStyle.BORDER_THIN, pinkXClor); //For Data Summary
		csSumaryData.setBorderTop(CellStyle.BORDER_THIN);
		csSumaryData.setTopBorderColor(pinkXClor);
		
		/*XSSFCellStyle csSumaryAlt2 = createStyle(workBook, creemXClor, CellStyle.ALIGN_GENERAL, fontSummary, null,CellStyle.BORDER_THIN, pinkXClor);
		csSumaryAlt2.setBorderTop(CellStyle.BORDER_THIN);
		csSumaryAlt2.setTopBorderColor(pinkXClor);*/
		
		XSSFCellStyle csSumaryAlt2 = createStyle(workBook, lightGreyXColor, CellStyle.ALIGN_GENERAL, fontPrompts, null,CellStyle.BORDER_THIN, pinkXClor);
		csSumaryAlt2.setBorderTop(CellStyle.BORDER_THIN);
		csSumaryAlt2.setTopBorderColor(pinkXClor);
	
		XSSFCellStyle csSumaryDataAlt2 = createStyle(workBook, lightGreyXColor, CellStyle.ALIGN_RIGHT, fontPrompts, BuiltinFormats.getBuiltinFormat(4), CellStyle.BORDER_THIN, pinkXClor); //For Data Summary with Alt clr
		csSumaryDataAlt2.setBorderTop(CellStyle.BORDER_THIN);
		csSumaryDataAlt2.setTopBorderColor(pinkXClor);
		
		XSSFCellStyle csPromptsCaption = createStyle(workBook, sunoidaPinkXClr, CellStyle.VERTICAL_CENTER, fontDetails, null, CellStyle.BORDER_THIN, whiteClr);
		csPromptsCaption.setBorderBottom(CellStyle.BORDER_THIN);
		//csSumary.setBorderRight(CellStyle.BORDER_THICK);
		
		XSSFCellStyle csReportPrompt = createStyle(workBook, lightGreyXColor, CellStyle.VERTICAL_CENTER, fontPrompts, null, CellStyle.BORDER_THIN, blackcolor);
		csReportPrompt.setBorderBottom(CellStyle.BORDER_THIN);
		
		XSSFCellStyle csDataAlt1DataForCount =  createStyle(workBook, whiteClr, CellStyle.ALIGN_RIGHT, fontData, BuiltinFormats.getBuiltinFormat(3), CellStyle.BORDER_THIN, pinkXClor); //For Data With background
		XSSFCellStyle csDataAlt2DataForCount =  createStyle(workBook, lightGreyXColor, CellStyle.ALIGN_RIGHT, fontPrompts, BuiltinFormats.getBuiltinFormat(3), CellStyle.BORDER_MEDIUM, pinkXClor);//For Data With out background
		XSSFCellStyle csSumaryDataForCount = createStyle(workBook, whiteClr, CellStyle.ALIGN_RIGHT, fontSummary, BuiltinFormats.getBuiltinFormat(3), CellStyle.BORDER_THIN, pinkXClor); //For Data Summary
		csSumaryDataForCount.setBorderTop(CellStyle.BORDER_THIN);
		csSumaryDataForCount.setTopBorderColor(pinkXClor);
		XSSFCellStyle csSumaryDataAlt2ForCount = createStyle(workBook, lightGreyXColor, CellStyle.ALIGN_RIGHT, fontPrompts, BuiltinFormats.getBuiltinFormat(3), CellStyle.BORDER_THIN, pinkXClor); //For Data Summary with Alt clr
		csSumaryDataAlt2ForCount.setBorderTop(CellStyle.BORDER_THIN);
		csSumaryDataAlt2ForCount.setTopBorderColor(pinkXClor);
		
	
		
		styles.put(CELL_STYLE_HEADER_CAP_COL_TOP, csHeaderCaptionColTop);
		styles.put(CELL_STYLE_HEADER_CAP_COL,csHeaderCaptionCol);
		styles.put(CELL_STYLE_MID_HEADER_CAP_COL,csMidHeaderCaptionCol);
		styles.put(CELL_STYLE_HEADER_DATA_COL, csHeaderDataCol);
		styles.put(CELL_STYLE_DETAILS_CAP_COL,csDataAlt1);
		styles.put(CELL_STYLE_DETAILS_CAP_COL_ALT,csDataAlt2);
		styles.put(CELL_STYLE_DETAILS_DATA_COL,csDataAlt1Data);
		styles.put(CELL_STYLE_DETAILS_DATA_COL_ALT,csDataAlt2Data);
		styles.put(CELL_STYLE_SUMMERY_CAP_COL,csSumary);
		styles.put(CELL_STYLE_SUMMERY_DATA_COL,csSumaryData);
		styles.put(CELL_STYLE_SUMMERY_CAP_COL_ALT,csSumaryAlt2);
		styles.put(CELL_STYLE_SUMMERY_DATA_COL_ALT,csSumaryDataAlt2);
		
		styles.put(CELL_STYLE_DETAILS_DATA_COL_COUNT,csDataAlt1DataForCount);
		styles.put(CELL_STYLE_DETAILS_DATA_COL_COUNT_ALT,csDataAlt2DataForCount);
		styles.put(CELL_STYLE_SUMMERY_DATA_COL_COUNT,csSumaryDataForCount);
		styles.put(CELL_STYLE_SUMMERY_DATA_COL_COUNT_ALT,csSumaryDataAlt2ForCount);
		styles.put(CELL_STYLE_TITLES,csReportTitle);
		styles.put(CELL_STYLE_REPORT_PROMPTS_CAP,csPromptsCaption);
		styles.put(CELL_STYLE_REPORT_PROMPTS,csReportPrompt);
		
        return styles;
	}

	public static void createTemplateFile(File lFile){
		FileOutputStream  fileOS = null;
		try{
			if(lFile.exists()){
				lFile.delete();
			}
			lFile.createNewFile();
			//Create a template file and save it on to disk
			XSSFWorkbook workBook = new XSSFWorkbook();
			workBook.createSheet();
			fileOS = new FileOutputStream(lFile);
			workBook.write(fileOS);
			fileOS.flush();
			fileOS.close();
			fileOS = null;
			workBook = null;
		}catch(Exception e){
			if(fileOS != null){
				try{fileOS.close();}catch(Exception ex){}
			}
		}
	}
	private static double getColumnWidth(Cell cell, boolean useMergedCells, double width){
        DataFormatter formatter = new DataFormatter();
        int defaultCharWidth = 5 ;//(int)layout.getAdvance();
       /* double cellWidth = SheetUtil.getCellWidth(cell, defaultCharWidth, formatter, useMergedCells);
        if (cellWidth != -1) {
        	cellWidth *= 256;
		    int maxColumnWidth = 255*256; // The maximum column width for an individual cell is 255 characters
		    if (cellWidth > maxColumnWidth) {
		    	cellWidth = maxColumnWidth;
		    }
		    width = Math.max(width, cellWidth);
		}*/
        return width;
    }
	private static double getColumnWidth(Cell cell, Font font, boolean useMergedCells, double width){
        DataFormatter formatter = new DataFormatter();
        int defaultCharWidth = 5 ;//(int)layout.getAdvance();
        double cellWidth = SheetUtil.getCellWidth(cell, defaultCharWidth, formatter, useMergedCells);
        if (cellWidth != -1) {
        	cellWidth *= 256;
		    int maxColumnWidth = 255*256; // The maximum column width for an individual cell is 255 characters
		    if (cellWidth > maxColumnWidth) {
		    	cellWidth = maxColumnWidth;
		    }
		    width = Math.max(width, cellWidth);
		}
        return width;
    }	
	private static int getColumnNoForWidth(Sheet sheet, int headerCnt) {
		float width = 0;
		for(int loopCnt =0 ; loopCnt<headerCnt; loopCnt++){
			width += sheet.getColumnWidth(loopCnt);
			if(width >=32000){
				return loopCnt;
			}
		}
		return headerCnt;
	}
	private static void createStylesForPrompts(Workbook workBook,  Map<Integer, XSSFCellStyle> styles){
		
		XSSFCellStyle csTitleCaption = (XSSFCellStyle)  workBook.createCellStyle();
		XSSFCellStyle csPrompt = (XSSFCellStyle)  workBook.createCellStyle();
		XSSFCellStyle csReportPrompt2 = (XSSFCellStyle)  workBook.createCellStyle();
		
		Font fontHeader1 = workBook.createFont();
		fontHeader1.setColor(IndexedColors.ROSE.index);
		fontHeader1.setBoldweight(Font.BOLDWEIGHT_BOLD);
		fontHeader1.setFontName("Calibri");
		fontHeader1.setFontHeightInPoints((short)8);
		
		Font fontHeader = workBook.createFont();
		fontHeader.setColor(IndexedColors.BLACK.index);
		fontHeader.setFontName("Calibri");
		fontHeader.setFontHeightInPoints((short)8);
		
/*		XSSFColor creemXClor = new XSSFColor();
		creemXClor.setIndexed(IndexedColors.WHITE.index);*/
		//byte[] creemClr = {(byte) 235, (byte) 241, (byte) 222};	
		/*byte[] creemClr = {(byte) 205, (byte) 226, (byte) 236};
		XSSFColor creemXClor = new XSSFColor(creemClr);*/
		byte[] lightGrey = {(byte) 242, (byte) 244, (byte) 242};
		XSSFColor lightGreyXColor = new XSSFColor(lightGrey);
	    csTitleCaption.setFillForegroundColor(lightGreyXColor);
	    csTitleCaption.setFillPattern(CellStyle.SOLID_FOREGROUND);
	    csTitleCaption.setAlignment(CellStyle.ALIGN_CENTER);
	    csTitleCaption.setFont(fontHeader1);
	    
	    
	    csPrompt.setFillForegroundColor(lightGreyXColor);
	    csPrompt.setFillPattern(CellStyle.SOLID_FOREGROUND);
	    csPrompt.setAlignment(CellStyle.ALIGN_CENTER);
	    csPrompt.setFont(fontHeader);
	    
	    /*byte[] lightGrey = {(byte) 224, (byte) 244, (byte) 242};	
		XSSFColor lightGreyXColor = new XSSFColor(lightGrey);		
		csReportPrompt2.setFillForegroundColor(lightGreyXColor);
		csReportPrompt2.setFillPattern(CellStyle.SOLID_FOREGROUND);
		csReportPrompt2.setAlignment(CellStyle.ALIGN_LEFT);
		csReportPrompt2.setFont(fontHeader1);*/
	    
	    styles.put(CELL_STYLE_TITLE_CAP, csTitleCaption);
	    styles.put(CELL_STYLE_PROMPTS, csPrompt);
	   //styles.put(CELL_STYLE__END_CAPTION, csReportPrompt2);
	    
	}
	public static void setBorderForCell(Cell cell, CellRangeAddress cellRangeAddress, Sheet sheet, Workbook workbook){
		try{
			RegionUtil.setBorderTop(cell.getCellStyle().getBorderTop(), cellRangeAddress, sheet, workbook);
			RegionUtil.setBorderLeft(cell.getCellStyle().getBorderLeft(), cellRangeAddress, sheet, workbook);
			RegionUtil.setBorderRight(cell.getCellStyle().getBorderRight(), cellRangeAddress, sheet, workbook);
			RegionUtil.setBorderBottom(cell.getCellStyle().getBorderBottom(), cellRangeAddress, sheet, workbook);
		}catch(Exception e){e.printStackTrace();}
	}
	public static int writeHeadersRA(ReportsVb reportsVb, List<ColumnHeadersVb> columnHeaders,
			int rowNum, SXSSFSheet sheet, Map<Integer, XSSFCellStyle>  styls, List<String> colTypes,
			Map<Integer,Integer> columnWidths){
		Row row = null;
		Row row1 = null;
		Row row2 = null;
		Row row3 = null;
		Row row4 = null;
		Row row5 = null;
		Row row6 = null;
		Row row7 = null;
		Cell cell = null;
		Row rowH = sheet.createRow(rowNum);
		Cell cellH = rowH.createCell(0);
		Cell cellH1 = rowH.createCell(1);
		Cell cellH2 = rowH.createCell(2);
		//cellH.setCellValue(reportStgVb.getDataColumn3()+" : "+reportStgVb.getDataColumn1()+" - "+reportStgVb.getDataColumn2());
		cellH.setCellStyle(styls.get(CELL_STYLE_SUMMERY_CAP_COL));
		cellH1.setCellStyle(styls.get(CELL_STYLE_SUMMERY_CAP_COL));
		cellH2.setCellStyle(styls.get(CELL_STYLE_SUMMERY_CAP_COL));
		if(ValidationUtil.isValid(reportsVb.getScreenName())) {
		   row = sheet.createRow(rowNum);
	       cell = row.createCell(0);
	       cell.setCellValue(reportsVb.getScreenName());
	       cell.setCellStyle(styls.get(CELL_STYLE_SUMMERY_CAP_COL));
	       ArrayList<ColumnHeadersVb> columnHeadersFinallst = new ArrayList<ColumnHeadersVb>();
	       columnHeaders.forEach(colHeadersVb -> {
				if(colHeadersVb.getColspan() <= 1) {
					columnHeadersFinallst.add(colHeadersVb);
				}
			});
	       //cell.setCellStyle(styls.get(CELL_STYLE_SUMMERY_CAP_COL_ALT));
	       //sheet.setColumnWidth(5 + 1, 6000);
	       int colSize = columnHeadersFinallst.size();
	       if(columnHeadersFinallst.size() == 0)
	    	   colSize = 6;
	       sheet.addMergedRegion(new CellRangeAddress(rowNum, rowNum, 0, (colSize-1)));
	       rowNum++;
	       reportsVb.setScreenName("");
		}
		int firstHeaderRowno = rowNum;
		int maxHeaderRow = columnHeaders.stream()
							.mapToInt(ColumnHeadersVb::getLabelRowNum)
							.max()
							.orElse(0);
		int maxHeaderCol = columnHeaders.stream()
				.mapToInt(ColumnHeadersVb::getLabelColNum)
				.max()
				.orElse(0);
		
		for(int rowIndex = 1; rowIndex <= maxHeaderRow; rowIndex++){
			if(rowIndex == 1){
				row1 = sheet.createRow(rowNum);
			}else if(rowIndex == 2){
				row2 = sheet.createRow(rowNum);
			}else if(rowIndex == 3){
				row3 = sheet.createRow(rowNum);
			}else if(rowIndex == 4){
				row4 = sheet.createRow(rowNum);
			}else if(rowIndex == 5){
				row5 = sheet.createRow(rowNum);
			}else if(rowIndex == 6){
				row6 = sheet.createRow(rowNum);
			}else if(rowIndex == 7){
				row7 = sheet.createRow(rowNum);
			}			
			for(int colIndex = 0; colIndex < maxHeaderCol; colIndex++){
				if(rowIndex == 1){
					cell = row1.createCell(colIndex);
				}else if(rowIndex == 2){
					cell = row2.createCell(colIndex);
				}else if(rowIndex == 3){
					cell = row3.createCell(colIndex);
				}else if(rowIndex == 4){
					cell = row4.createCell(colIndex);
				}else if(rowIndex == 5){
					cell = row5.createCell(colIndex);
				}else if(rowIndex == 6){
					cell = row6.createCell(colIndex);
				}else if(rowIndex == 7){
					cell = row7.createCell(colIndex);
				}
				cell.setCellStyle(styls.get(CELL_STYLE_HEADER_CAP_COL_TOP));
			}
			rowNum++;
		}
		int colNum=0;
		for(ColumnHeadersVb columnHeadersVb:columnHeaders){
			int rowStart = firstHeaderRowno+columnHeadersVb.getLabelRowNum()-1;
			int rowEnd = 0;
			if(columnHeadersVb.getRowspan() != 0 && columnHeadersVb.getRowspan() != 1) {
				rowEnd = rowStart+(columnHeadersVb.getRowspan()-1);
			} else {
				rowEnd = rowStart+0;
				/*if(columnHeadersVb.getRowspan() == 0) {
					
				}else if(columnHeadersVb.getRowspan() == 1){
					rowEnd = rowStart+1;
				}*/
			}
			//int rowEnd = rowStart+((columnHeadersVb.getRowspan() != 0 || columnHeadersVb.getRowspan() != 1) ? columnHeadersVb.getRowspan()-1:if(columnHeadersVb.getRowspan() == 0)? ));
			int columnStart = columnHeadersVb.getLabelColNum()-1;
			int columnEnd = 0;
			if(columnHeadersVb.getColspan() != 0 && columnHeadersVb.getColspan() != 1) {
				columnEnd = columnHeadersVb.getColspan()-1;
			} else {
				columnEnd = 0;
				/*if(columnHeadersVb.getColspan() == 0) {
					columnEnd = 0;
				}else if(columnHeadersVb.getColspan() == 1){
					columnEnd = 1;
				}*/
			}
			//int columnEnd = ((columnHeadersVb.getColspan() != 0 || columnHeadersVb.getColspan() != 1) ?columnHeadersVb.getColspan()-1:0);
			//int columnEnd = (columnHeadersVb.getColSpanNum() == 0 ? columnHeadersVb.getLabelColNum() : columnHeadersVb.getColSpanNum())-1;
			if(columnHeadersVb.getLabelRowNum() == 1){
				row = row1;
			}else if(columnHeadersVb.getLabelRowNum() == 2){
				row = row2;
			}else if(columnHeadersVb.getLabelRowNum() == 3){
				row = row3;
			}else if(columnHeadersVb.getLabelRowNum() == 4){
				row = row4;
			}else if(columnHeadersVb.getLabelRowNum() == 5){
				row = row5;
			}else if(columnHeadersVb.getLabelRowNum() == 6){
				row = row6;
			}else if(columnHeadersVb.getLabelRowNum() == 7){
				row = row7;
			}			
			cell = row.getCell(columnStart);
			if((columnHeadersVb.getColspan() != 0 && columnHeadersVb.getColspan() != 1) || (columnHeadersVb.getRowspan() != 0 && columnHeadersVb.getRowspan() != 1)){
				int end = columnStart+columnEnd;
				sheet.addMergedRegion(new CellRangeAddress(rowStart, rowEnd, columnStart, end));
			}
			cell.setCellStyle(styls.get(CELL_STYLE_HEADER_CAP_COL_TOP));
			cell.setCellValue(columnHeadersVb.getCaption());
			if(columnHeadersVb.getCaption().contains("<br/>")){
				columnHeadersVb.setCaption(columnHeadersVb.getCaption().replaceAll("<br/>","\n"));
				cell.setCellValue(columnHeadersVb.getCaption().replaceAll("_", " "));
		        cell.getCellStyle().setWrapText(true);
			}
			columnWidths.put(colNum,(int)getColumnWidth(cell, (styls.get(CELL_STYLE_HEADER_CAP_COL)).getFont(), true, columnWidths.get(colNum)));
			colNum++;
		}
		return rowNum;
	}
	
	public static int writeReportDataRA(Workbook workBook,ReportsVb reportVb,List<ColumnHeadersVb> colHeaderslst,List<HashMap<String, String>> dataLst,SXSSFSheet sheet,
			int rowNum,Map<Integer, XSSFCellStyle>  styls, List<String> columnTypes, Map<Integer,Integer> columnWidths,Boolean totalRow,String assetFolderUrl){
		Row row = null;
		Cell cell = null;
		String[] capGrpCols = null; 
		ArrayList<String> groupingCols = new ArrayList<String>();
		String screenSortColumn = reportVb.getScreenSortColumn();
		ArrayList<ColumnHeadersVb> columnHeadersFinallst = new ArrayList<ColumnHeadersVb>();
		colHeaderslst.forEach(colHeadersVb -> {
			if(colHeadersVb.getColspan() <= 1 && colHeadersVb.getNumericColumnNo() != 99) {
				columnHeadersFinallst.add(colHeadersVb);
			}
		});
		if(ValidationUtil.isValid(reportVb.getScreenGroupColumn())) {
			reportVb.setPdfGroupColumn(reportVb.getScreenGroupColumn());
		}
		if(ValidationUtil.isValid(reportVb.getPdfGroupColumn()))
			capGrpCols = reportVb.getPdfGroupColumn().split("!@#");
		
		if(reportVb.getTotalRows() <= reportVb.getMaxRecords() && !totalRow && capGrpCols != null && capGrpCols.length > 0) {
			for(String grpStr : capGrpCols) {
				for(ColumnHeadersVb colHeader : columnHeadersFinallst) {
					if(grpStr.equalsIgnoreCase(colHeader.getCaption().toUpperCase())) {
						groupingCols.add(colHeader.getDbColumnName());
						break;
					}
				}
			}
		}
		final String[] grpColNames =  capGrpCols;
		Map<String, List < HashMap<String, String> >> groupingMap = new HashMap<String, List < HashMap<String, String> >>();
		
		if(reportVb.getTotalRows() <= reportVb.getMaxRecords() && !totalRow 
				&& (groupingCols != null && groupingCols.size() > 0)  && dataLst != null && dataLst.size() > 0) {
			switch(groupingCols.size()) {
				case 1:
					groupingMap = dataLst.stream().collect(
							Collectors.groupingBy(
							m -> (m.get(groupingCols.get(0))) == null ? 
									"" :grpColNames[0]+": " + m.get(groupingCols.get(0))
							));
					break;
				case 2:
					groupingMap = dataLst.stream().collect(
							Collectors.groupingBy(
							m -> (m.get(groupingCols.get(0))
									+" >> "+m.get(groupingCols.get(1))) == null ? 
									"" :grpColNames[0]+": " + m.get(groupingCols.get(0))
									+" >> "+grpColNames[1]+": " +m.get(groupingCols.get(1))
							));
					break;
				case 3:
					groupingMap = dataLst.stream().collect(
							Collectors.groupingBy(
							m -> (m.get(groupingCols.get(0))
									+" >> "+m.get(groupingCols.get(1))
									+" >> "+m.get(groupingCols.get(2))) == null ? 
									"" :grpColNames[0]+": " + m.get(groupingCols.get(0))
									+" >> "+grpColNames[1]+": " +m.get(groupingCols.get(1))
									+" >> "+grpColNames[2]+": " +m.get(groupingCols.get(2))
							));
					break;
				case 4:
					groupingMap = dataLst.stream().collect(
							Collectors.groupingBy(
							m -> (m.get(groupingCols.get(0))
									+" >> "+m.get(groupingCols.get(1))
									+" >> "+m.get(groupingCols.get(2))
									+" >> "+m.get(groupingCols.get(3))) == null ? 
									"" :grpColNames[0]+": " + m.get(groupingCols.get(0))
									+" >> "+grpColNames[1]+": " +m.get(groupingCols.get(1))
									+" >> "+grpColNames[2]+": " +m.get(groupingCols.get(2))
									+" >> "+grpColNames[2]+": " +m.get(groupingCols.get(3))
							));
					break;
				case 5:
					groupingMap = dataLst.stream().collect(
							Collectors.groupingBy(
							m -> (m.get(groupingCols.get(0))
									+" >> "+m.get(groupingCols.get(1))
									+" >> "+m.get(groupingCols.get(2))
									+" >> "+m.get(groupingCols.get(3))
									+" >> "+m.get(groupingCols.get(4))) == null ? 
									"" :grpColNames[0]+": " + m.get(groupingCols.get(0))
									+" >> "+grpColNames[1]+": " +m.get(groupingCols.get(1))
									+" >> "+grpColNames[2]+": " +m.get(groupingCols.get(2))
									+" >> "+grpColNames[2]+": " +m.get(groupingCols.get(3))
									+" >> "+grpColNames[2]+": " +m.get(groupingCols.get(4))
							));
					break;
			}
			Map<String, List < HashMap<String, String> >> sortedMap = new TreeMap<String, List < HashMap<String, String> >>();
			if (ValidationUtil.isValid(screenSortColumn)) {
				if (screenSortColumn.contains(groupingCols.get(0))) {
					String value = screenSortColumn.substring(9, screenSortColumn.length()).toUpperCase();
					String[] col = value.split(",");
					for (int i = 0; i < col.length; i++) {
						if (col[i].contains(groupingCols.get(0))) {
							String val = col[i];
							if (val.contains("DESC")) {
								sortedMap = new TreeMap<String, List<HashMap<String, String>>>(Collections.reverseOrder());
								sortedMap.putAll(groupingMap);
							} else {
								sortedMap = new TreeMap<String, List<HashMap<String, String>>>(groupingMap);
							}
						}
					}
				} else {
					sortedMap = new TreeMap<String, List<HashMap<String, String>>>(groupingMap);
				}
			} else {
				sortedMap = new TreeMap<String, List<HashMap<String, String>>>(groupingMap);
			}
			//Map<String, List < HashMap<String, String> >> sortedMap = new TreeMap<String, List < HashMap<String, String> >>(groupingMap);
			Set entrySet = sortedMap.entrySet();
			Iterator it = entrySet.iterator();
			while(it.hasNext()){
		       Map.Entry dataVal = (Map.Entry)it.next();
		       List<HashMap<String, String>> grpDataLst= (List)dataVal.getValue();
		       String key = (String) dataVal.getKey();
		       if(key.isEmpty())
		    	   continue;
		       row = sheet.createRow(rowNum);
		       cell = row.createCell(0);
		       cell.setCellValue(dataVal.getKey().toString());
		       cell.setCellStyle(styls.get(CELL_STYLE_SUMMERY_CAP_COL_ALT));
		       sheet.addMergedRegion(new CellRangeAddress(rowNum, rowNum, 0, columnHeadersFinallst.size()-1));
		       rowNum++;
		       rowNum = writeDatatoExcel (workBook,reportVb, colHeaderslst, grpDataLst, sheet, rowNum, styls, columnTypes, columnWidths,columnHeadersFinallst,row,cell,true,assetFolderUrl);
			}
		}else {
			rowNum = writeDatatoExcel (workBook,reportVb, colHeaderslst, dataLst, sheet, rowNum, styls, columnTypes, columnWidths,columnHeadersFinallst,row,cell,false,assetFolderUrl);
		}
		return rowNum;
	}
	private static int writeDatatoExcel(Workbook workBook,ReportsVb reportVb,List<ColumnHeadersVb> colHeaderslst,List<HashMap<String, String>> dataLst,SXSSFSheet sheet,
			int rowNum,Map<Integer, XSSFCellStyle>  styls, List<String> columnTypes, Map<Integer,Integer> columnWidths,
			ArrayList<ColumnHeadersVb> columnHeadersFinallst,Row row,Cell cell,Boolean calculateTotal,String assetFolderUrl) {
		int rowSpanNumStart = 0;
		int rowSpanNumEnd = 0;
		String spanValue = "";
		String formatType = "D";
		HashMap<String,String> sumMap = new HashMap<String,String>();
		List<HashMap<String, String>> summationLst = new ArrayList();
		Double sumVal = 0.0;
		String firstColHeaderName =  columnHeadersFinallst.get(0).getDbColumnName();
		int imgCol = 0;
		Boolean hideColumn = false;
		if(dataLst != null) {
			for(HashMap dataMap:dataLst){
				if(dataMap.containsKey("FORMAT_TYPE"))
					formatType = dataMap.get("FORMAT_TYPE").toString();
				row = sheet.createRow(rowNum);
				for(int loopCount =0; loopCount < columnHeadersFinallst.size(); loopCount++){
					
					ColumnHeadersVb colHeadersVb = columnHeadersFinallst.get(loopCount);
					if(colHeadersVb.getColspan() > 1 || colHeadersVb.getNumericColumnNo() == 99)
						continue;
					String cellValue = "";
					cell = row.createCell(loopCount);
					String colType= colHeadersVb.getColType();
					String colDiffer = colHeadersVb.getColorDiff();
					String type = "";
					if("T".equalsIgnoreCase(colHeadersVb.getColType())) {
						type = CAP_COL;
					} else if ("P".equalsIgnoreCase(colHeadersVb.getColType())) {
						type = "GROWTH_IMG";
					} else {
						type = DATA_COL;
					}
					if(CAP_COL.equalsIgnoreCase(type)){
						String orgValue = "";
						if(dataMap.containsKey(colHeadersVb.getDbColumnName())) {
							orgValue = ((dataMap.get(colHeadersVb.getDbColumnName())) != null ||  dataMap.get(colHeadersVb.getDbColumnName()) == "") ? dataMap.get(colHeadersVb.getDbColumnName()).toString() : "";
						}
						
						if("S".equalsIgnoreCase(formatType)) {
							cell.setCellStyle(styls.get(CELL_STYLE_SUMMERY_CAP_COL_ALT));
							if(ValidationUtil.isValid(reportVb.getGrandTotalCaption()) && loopCount == 0) {
								orgValue= reportVb.getGrandTotalCaption();
							}
						}else if("ST".equalsIgnoreCase(formatType) || "FT".equalsIgnoreCase(formatType)){
							cell.setCellStyle(styls.get(CELL_STYLE_SUMMERY_CAP_COL));
						}else {
							cell.setCellStyle(styls.get(CELL_STYLE_DETAILS_CAP_COL));
						}
						XSSFRichTextString string = new XSSFRichTextString(orgValue);
						cellValue = string.toString();
						cell.setCellValue(string);
						columnWidths.put(loopCount,(int)getColumnWidth(cell, null, true, columnWidths.get(loopCount)));
					}else if("GROWTH_IMG".equalsIgnoreCase(type)) {
						String orgValue = "";
						imgCol = loopCount;
						hideColumn = true;
					}else{
						if("S".equalsIgnoreCase(formatType)) {
							if("I".equalsIgnoreCase(colType) || "S".equalsIgnoreCase(colType)) {
								cell.setCellStyle(styls.get(CELL_STYLE_SUMMERY_DATA_COL_COUNT_ALT));
							}else {
								cell.setCellStyle(styls.get(CELL_STYLE_SUMMERY_DATA_COL_ALT));
							}
						}else if("ST".equalsIgnoreCase(formatType) || "FT".equalsIgnoreCase(formatType)) {
							if("I".equalsIgnoreCase(colType) || "S".equalsIgnoreCase(colType)) {
								cell.setCellStyle(styls.get(CELL_STYLE_SUMMERY_DATA_COL_COUNT));
							}else {
								cell.setCellStyle(styls.get(CELL_STYLE_SUMMERY_DATA_COL));
							}
						}else {
							if("I".equalsIgnoreCase(colType) || "S".equalsIgnoreCase(colType) || "NR".equalsIgnoreCase(colType)) {
								cell.setCellStyle(styls.get(CELL_STYLE_DETAILS_DATA_COL_COUNT));
							}else {
								cell.setCellStyle(styls.get(CELL_STYLE_DETAILS_DATA_COL));
							}
						}
						String orgValue = "";
						if(dataMap.containsKey(colHeadersVb.getDbColumnName())) 
							orgValue = ((dataMap.get(colHeadersVb.getDbColumnName())) != null ||  dataMap.get(colHeadersVb.getDbColumnName()) == "") ? dataMap.get(colHeadersVb.getDbColumnName()).toString() : "";
						
						if(ValidationUtil.isValid(orgValue)) {
							if("NR".equalsIgnoreCase(colType) || "TR".equalsIgnoreCase(colType)) {
								cellValue = orgValue;
							}else {
								cellValue = ValidationUtil.replaceComma(orgValue.trim());
							}

							if (ValidationUtil.isNumericDecimal(cellValue)) {
								cell.setCellType(Cell.CELL_TYPE_NUMERIC);
								cell.setCellValue(Double.valueOf(cellValue));
							} else {
								if (!"NR".equalsIgnoreCase(colType) && !"TR".equalsIgnoreCase(colType)) {
									Double val = Double.parseDouble(cellValue);
									DecimalFormat formatValue = new DecimalFormat("0.00");
									cell.setCellValue(formatValue.format(val));
								} else {
									cell.setCellValue(cellValue);
								}
							}
						}else {
							cell.setCellValue(cellValue);
						}
						columnWidths.put(loopCount,(int)getColumnWidth(cell, null, true, columnWidths.get(loopCount)));
						if("NR".equalsIgnoreCase(colType) || "TR".equalsIgnoreCase(colType))
							continue;
						if(calculateTotal) {
							String prevValstr = sumMap.containsKey(colHeadersVb.getDbColumnName())?sumMap.get(colHeadersVb.getDbColumnName()):"0";
							if(!ValidationUtil.isValid(prevValstr))
								prevValstr = "0";
							if(!ValidationUtil.isValid(cellValue))
								cellValue = "0";
							Double strVal = Double.parseDouble(prevValstr);
							sumVal = strVal+Double.parseDouble(cellValue);
							DecimalFormat formatter = new DecimalFormat("####.00");
							String cellText = formatter.format(sumVal);
							sumMap.put(colHeadersVb.getDbColumnName(), cellText);
						}
					}
				}
				rowNum++;
			}
		}else {
			row = sheet.createRow(rowNum);
			cell = row.createCell(0);
			cell.setCellStyle(styls.get(CELL_STYLE_DETAILS_CAP_COL));
			cell.setCellValue("No Data Found");
			rowNum++;
		}
		// This is calculate the Groupwise total only when total row < max record
		if(calculateTotal) {
			if(!sumMap.isEmpty()) {
				sumMap.put("FORMAT_TYPE", "ST");
				sumMap.put(firstColHeaderName, "Sub Total");
				summationLst.add(sumMap);
				rowNum = writeDatatoExcel (workBook,reportVb, colHeaderslst, summationLst, sheet, rowNum, styls, columnTypes, columnWidths,columnHeadersFinallst,row,cell,false,assetFolderUrl);
			}
		}
		if(hideColumn)
			sheet.setColumnHidden(imgCol, true);
		return rowNum;
	}
	protected static int getStyle(ReportsVb reportStgVb, int rowNum, String colType){
		if((rowNum %2) == 0 && ("S".equalsIgnoreCase(reportStgVb.getFormatType()) || "G".equalsIgnoreCase(reportStgVb.getFormatType()))){
			if("C".equalsIgnoreCase(colType))return 5;
			return 1; //Summary with Background 
		}else if((rowNum %2) == 0){
			if("C".equalsIgnoreCase(colType))return 6;
			return 2; //Non Summary with background.
		}else if("S".equalsIgnoreCase(reportStgVb.getFormatType()) || "G".equalsIgnoreCase(reportStgVb.getFormatType())){
			if("C".equalsIgnoreCase(colType))return 7;
			return 3;//Summary with out Background
		}else{
			if("C".equalsIgnoreCase(colType))return 8;
			return 4;//Non Summary With out background
		}
	}
	public static int createPromptsPage(ReportsVb reportsVb, Sheet sheet,
			Workbook workBook, String assetFolderUrl, Map<Integer, XSSFCellStyle> styles, int headerCnt) {
		int intRow = 0;
		Row row = null;
		Cell cell = null;
		createStylesForPrompts(workBook, styles);
		byte[] tClr= getRGB(reportsVb.getApplicationTheme());
		try {
			XSSFColor sPinkclr = new XSSFColor();
			//byte[] tClr = {(byte) 177, (byte) 24, (byte) 124};
			sPinkclr = new XSSFColor(tClr);
			
			XSSFColor greyClr = new XSSFColor();
			byte[] greyXclr = {(byte) 242, (byte) 244, (byte) 242};
			greyClr = new XSSFColor(greyXclr);
			
			String promptLabel[] = null;
			if(ValidationUtil.isValid(reportsVb.getPromptLabel())) {
				promptLabel = reportsVb.getPromptLabel().split("!@#");
			}
			int intCol = 0;
			float rowheight = 19.5f;
			int loopCount = 2;
			row = sheet.createRow(intRow);
			for(int i=0; i<=loopCount; i++) {
				cell = row.createCell(i);
				cell.setCellStyle(styles.get(CELL_STYLE_TITLE_CAP));
			}
			
			intCol++;

			sheet.setDisplayGridlines(false);
			Row row1 = sheet.createRow(0);
			drawImageToSheet(workBook, sheet, "Product_Logo.png", 0, 1, 0, 2, true,assetFolderUrl);
			drawImageToSheet(workBook, sheet, "Bank_Logo.png", 2, 3, 0, 2, true,assetFolderUrl);
			
			int cnt = 1;
			intRow = 4;
			row = sheet.createRow(intRow);
			row.setHeightInPoints(rowheight);
			cell = row.createCell(0);
			cell.setCellValue("Report Title");
			styles.get(CELL_STYLE_REPORT_PROMPTS_CAP).setVerticalAlignment(VerticalAlignment.CENTER);
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS_CAP));
			cell = row.createCell(1);
			cell.setCellValue(reportsVb.getReportTitle());
			styles.get(CELL_STYLE_REPORT_PROMPTS_CAP).setVerticalAlignment(VerticalAlignment.CENTER);
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS_CAP));
			intRow++;
			if(promptLabel != null && promptLabel.length>0) {
				for(int i=0; i<promptLabel.length; i++) {
					String[] promptArr = null;
					String[] val = null;
					promptArr = promptLabel[i].split(" : ");
					val = promptArr[1].split(",");
					row = sheet.createRow(intRow);
					row.setHeightInPoints(rowheight);
					cell = row.createCell(0);
					if(val.length>2) {
						promptArr[1] = val[0]+","+val[1]+" (+) "+(val.length-2);
					}else if(val.length == 2){
						promptArr[1] = val[0]+","+val[1];
					}
					cell.setCellValue(promptArr[0]);
					cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS_CAP));
					cell = row.createCell(1);
					cell.setCellValue(promptArr[1]);
					styles.get(CELL_STYLE_REPORT_PROMPTS).getFont().setColor(sPinkclr);
					styles.get(CELL_STYLE_REPORT_PROMPTS).setVerticalAlignment(VerticalAlignment.CENTER);
					cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS));
					intRow++;
				}
			}
			row = sheet.createRow(intRow);
			row.setHeightInPoints(rowheight);
			cell = row.createCell(0);
			cell.setCellValue("Generated on");
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS_CAP));
			cell = row.createCell(1);
			SimpleDateFormat dateFormat = new SimpleDateFormat("MMM dd, yyyy - hh:mm:ss a");
			cell.setCellValue(dateFormat.format(new Date()));
			styles.get(CELL_STYLE_REPORT_PROMPTS).getFont().setColor(sPinkclr);
			styles.get(CELL_STYLE_REPORT_PROMPTS).setAlignment(CellStyle.VERTICAL_CENTER);
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS));
			intRow++;
			row = sheet.createRow(intRow);
			row.setHeightInPoints(rowheight);
			cell = row.createCell(0);
			cell.setCellValue("Generated By");
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS_CAP));
			cell = row.createCell(1);
			cell.setCellValue(reportsVb.getMakerName());
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS));
			intRow = intRow+2;
			row = sheet.createRow(intRow);
			row.setHeightInPoints(rowheight);
			for(int i=0; i<=loopCount; i++) {
				cell = row.createCell(i);
				if(i==1) {
					cell.setCellValue("Powered by Sunoida");
				}
				styles.get(CELL_STYLE_TITLE_CAP).getFont().setColor(sPinkclr);
				styles.get(CELL_STYLE_TITLE_CAP).setVerticalAlignment(VerticalAlignment.CENTER);
			    cell.setCellStyle(styles.get(CELL_STYLE_TITLE_CAP));
			    styles.get(CELL_STYLE_TITLE_CAP).setBorderBottom(HSSFCellStyle.BORDER_THIN);
			    styles.get(CELL_STYLE_TITLE_CAP).setBorderRight(HSSFCellStyle.BORDER_NONE);
			    if(i==loopCount) {
			   	//styles.get(CELL_STYLE_TITLE_CAP).setBorderRight(HSSFCellStyle.BORDER_THIN);
			    	cell.setCellStyle(styles.get(CELL_STYLE_TITLE_CAP));
			    	XSSFCellStyle style = (XSSFCellStyle) workBook.createCellStyle();
		            style.setBorderRight(BorderStyle.THIN);
		            style.setBorderBottom(BorderStyle.THIN);
		            style.setFillForegroundColor(greyClr);
		            style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		            cell.setCellStyle(style);
				}
			}
			CellRangeAddress cellRangeAddress1 = new CellRangeAddress(0, 2, 0, loopCount);
			sheet.addMergedRegion(cellRangeAddress1);
			RegionUtil.setBorderRight(CellStyle.BORDER_THIN, cellRangeAddress1, sheet, workBook);
			
			CellRangeAddress cellRangeAddress2 = new CellRangeAddress(3, intRow-1, loopCount, loopCount);
			sheet.addMergedRegion(cellRangeAddress2);
			RegionUtil.setBorderRight(CellStyle.BORDER_THIN, cellRangeAddress2, sheet, workBook);
			
			
			CellRangeAddress cellRangeAddress3 = new CellRangeAddress(3, 3, 0, loopCount-1);
			sheet.addMergedRegion(cellRangeAddress3);
			RegionUtil.setBorderBottom(CellStyle.BORDER_NONE, cellRangeAddress3, sheet, workBook);

			CellRangeAddress cellRangeAddress9 = new CellRangeAddress(intRow-1, intRow-1, 0, 1);
			sheet.addMergedRegion(cellRangeAddress9);
			RegionUtil.setBorderBottom(CellStyle.BORDER_NONE, cellRangeAddress9, sheet, workBook);

			sheet.setColumnWidth(0, 20 * 256);
			sheet.setColumnWidth(1, 56 * 256);
			sheet.setColumnWidth(2, 20 * 256);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return intRow;
	}
	public static int createCatalogIndex(VcForQueryReportFieldsWrapperVb vObject, Sheet sheet, XSSFWorkbook workBook,String assetFolderUrl) {
		int intRow = 0;
		Row row = null;
		Cell cell = null;
		Map<Integer, XSSFCellStyle> styles = ExcelExportUtil.createStyles(workBook,"");
		createStylesForPrompts(workBook, styles);
		byte[] tClr= getRGB(vObject.getApplicationTheme());
		try {
			XSSFColor sPinkclr = new XSSFColor();
			//byte[] tClr = { (byte) 177, (byte) 24, (byte) 124 };
			sPinkclr = new XSSFColor(tClr);
			XSSFColor greyClr = new XSSFColor();
			byte[] greyXclr = { (byte) 242, (byte) 244, (byte) 242 };
			greyClr = new XSSFColor(greyXclr);
			
			
			int intCol = 0;
			float rowheight = 19.5f;
			int loopCount = 2;
			row = sheet.createRow(intRow);
			for (int i = 0; i <= loopCount; i++) {
				cell = row.createCell(i);
				cell.setCellStyle(styles.get(CELL_STYLE_TITLE_CAP));
			}
			intCol++;
			sheet.setDisplayGridlines(false);
			Row row1 = sheet.createRow(0);
			drawImageToSheet(workBook, sheet, "Product_Logo.png", 0, 1, 0, 2, true,assetFolderUrl);
			drawImageToSheet(workBook, sheet, "Bank_Logo.png", 2, 3, 0, 2, true,assetFolderUrl);
		
			int cnt = 1;
			intRow = 4;
			row = sheet.createRow(intRow);
			row.setHeightInPoints(rowheight);
			cell = row.createCell(0);
			cell.setCellValue("Report Title");
			styles.get(CELL_STYLE_REPORT_PROMPTS_CAP).setVerticalAlignment(VerticalAlignment.CENTER);
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS_CAP));
			cell = row.createCell(1);
			cell.setCellValue("Catalog : "+vObject.getMainModel().getCatalogId()); // HardCoded
			styles.get(CELL_STYLE_REPORT_PROMPTS_CAP).setVerticalAlignment(VerticalAlignment.CENTER);
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS_CAP));
			intRow++;
			row = sheet.createRow(intRow);
			row.setHeightInPoints(rowheight);
			cell = row.createCell(0);
			cell.setCellValue("Generated on");
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS_CAP));
			cell = row.createCell(1);
			SimpleDateFormat dateFormat = new SimpleDateFormat("MMM dd, yyyy - hh:mm:ss a");
			cell.setCellValue(dateFormat.format(new Date()));
			styles.get(CELL_STYLE_REPORT_PROMPTS).getFont().setColor(sPinkclr);
			styles.get(CELL_STYLE_REPORT_PROMPTS).setAlignment(CellStyle.VERTICAL_CENTER);
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS));
			intRow++;
			row = sheet.createRow(intRow);
			row.setHeightInPoints(rowheight);
			cell = row.createCell(0);
			cell.setCellValue("Generated By");
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS_CAP));
			cell = row.createCell(1);
			//VisionUsersVb visionUsersVb = SessionContextHolder.getContext();
			cell.setCellValue(ScheduledReportWb.userName);
			cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS));
			intRow = intRow + 2;
			row = sheet.createRow(intRow);
			row.setHeightInPoints(rowheight);
			for (int i = 0; i <= loopCount; i++) {
				cell = row.createCell(i);
				if (i == 1) {
					cell.setCellValue("Powered by Sunoida");
					// cell.setCellStyle(styles.get(CELL_STYLE_REPORT_PROMPTS));
				}
				styles.get(CELL_STYLE_TITLE_CAP).getFont().setColor(sPinkclr);
				styles.get(CELL_STYLE_TITLE_CAP).setVerticalAlignment(VerticalAlignment.CENTER);
				cell.setCellStyle(styles.get(CELL_STYLE_TITLE_CAP));
				styles.get(CELL_STYLE_TITLE_CAP).setBorderBottom(HSSFCellStyle.BORDER_THIN);
				styles.get(CELL_STYLE_TITLE_CAP).setBorderRight(HSSFCellStyle.BORDER_NONE);
				if (i == loopCount) {
					// styles.get(CELL_STYLE_TITLE_CAP).setBorderRight(HSSFCellStyle.BORDER_THIN);
					cell.setCellStyle(styles.get(CELL_STYLE_TITLE_CAP));
					XSSFCellStyle style = workBook.createCellStyle();
					style.setBorderRight(BorderStyle.THIN);
					style.setBorderBottom(BorderStyle.THIN);
					style.setFillForegroundColor(greyClr);
					style.setFillPattern(CellStyle.SOLID_FOREGROUND);
					cell.setCellStyle(style);
				}
			}
			CellRangeAddress cellRangeAddress1 = new CellRangeAddress(0, 2, 0, loopCount);
			sheet.addMergedRegion(cellRangeAddress1);
			RegionUtil.setBorderRight(CellStyle.BORDER_THIN, cellRangeAddress1, sheet, workBook);

			CellRangeAddress cellRangeAddress2 = new CellRangeAddress(3, intRow - 1, loopCount, loopCount);
			sheet.addMergedRegion(cellRangeAddress2);
			RegionUtil.setBorderRight(CellStyle.BORDER_THIN, cellRangeAddress2, sheet, workBook);

			CellRangeAddress cellRangeAddress3 = new CellRangeAddress(3, 3, 0, loopCount - 1);
			sheet.addMergedRegion(cellRangeAddress3);
			RegionUtil.setBorderBottom(CellStyle.BORDER_NONE, cellRangeAddress3, sheet, workBook);

			CellRangeAddress cellRangeAddress9 = new CellRangeAddress(intRow - 1, intRow - 1, 0, 1);
			sheet.addMergedRegion(cellRangeAddress9);
			RegionUtil.setBorderBottom(CellStyle.BORDER_NONE, cellRangeAddress9, sheet, workBook);

			sheet.setColumnWidth(0, 20 * 256);
			sheet.setColumnWidth(1, 56 * 256);
			sheet.setColumnWidth(2, 20 * 256);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return intRow;
	}
	private static void drawImageToSheet(Workbook workbook, Sheet sheet, String imageName, int startCol, int endCol, int startRow, int endRow, boolean applicationImage,String assetFolderUrl) throws IOException {
		InputStream imageInputStream = null;
//		applicationImage =false;
		
		imageInputStream = new FileInputStream(assetFolderUrl+File.separator+ imageName);
		
		byte[] logoBytes = IOUtils.toByteArray(imageInputStream);
		int logoPictureIdx = workbook.addPicture(logoBytes, Workbook.PICTURE_TYPE_PNG);
		imageInputStream.close();
		CreationHelper logoHelper = workbook.getCreationHelper();
		Drawing logoDrawing = sheet.createDrawingPatriarch();
		ClientAnchor logoAnchor = logoHelper.createClientAnchor();
		logoAnchor.setCol1(startCol);
		logoAnchor.setRow1(startRow);
		logoAnchor.setCol2(endCol);
		logoAnchor.setRow2(endRow);
		Picture logoPict = logoDrawing.createPicture(logoAnchor, logoPictureIdx);
	}

	public static byte[] getRGB(final String rgb) {
		byte[] sunoidaClr = null;
		if (!ValidationUtil.isValid(rgb)) {
			sunoidaClr = new byte[] { (byte) 66, (byte) 133, (byte) 244 };
		} else {
			final int[] ret = new int[3];
			for (int i = 0; i < 3; i++) {
				ret[i] = Integer.parseInt(rgb.substring(i * 2, i * 2 + 2), 16);
			}
			sunoidaClr = new byte[] { (byte) ret[0], (byte) ret[1], (byte) ret[2] };
		}
		return sunoidaClr;
	}
}