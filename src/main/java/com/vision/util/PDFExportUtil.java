package com.vision.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.Image;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.ColumnText;
import com.itextpdf.text.pdf.PdfContentByte;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPRow;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfTemplate;
import com.itextpdf.text.pdf.PdfWriter;
import com.vision.authentication.SessionContextHolder;
import com.vision.dao.CommonDao;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.vb.ColumnHeadersVb;
import com.vision.vb.ExportVb;
import com.vision.vb.PromptIdsVb;
import com.vision.vb.ReportsVb;
import com.vision.vb.VisionUsersVb;
import com.vision.wb.ReportsWb;
@Component
public class PDFExportUtil {
	public static final String CAP_COL = "captionColumn";
	public static final String DATA_COL = "dataColumn";
	public static Logger logger = LoggerFactory.getLogger(PDFExportUtil.class);
	@Autowired
	ReportsWb reportsWb;
	@Autowired
	CommonDao commonDao;
	
	public JdbcTemplate jdbcTemplate = null;

	public PDFExportUtil(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	
	private static float[] mergeArrays(float[] source1,float[] source2){
		float[] destination = new float[source1.length+source2.length];
		int i=0;
		for(i = 0;i<source1.length;i++){
			destination[i] = source1[i];
		}
		for(int j = 0;j<source2.length;j++){
			destination[j+i] = source2[j];
		}
		return destination;
	} 
	
	private static float getSumOfWidths(Map<Integer, Float> colSizes, int colStart, int size, int labelColCount) {
		float totalWidth = 0f;
		for(int i=0;i<labelColCount;i++){
			totalWidth += colSizes.get(i);
		}
		for(int i=colStart;i<size;i++){
			totalWidth += colSizes.get(i);
		}
		return totalWidth;
	}
	private static PdfPTable createTable(PdfPTable sourceTable,List<PdfPRow> pdfpRows, int colStart, int colEnd, int currentPageCount, 
			int totalPages, int labelColCount) {
		// below comment by Praksah 
		//PdfPTable tempTable = new PdfPTable((colEnd-colStart)+labelColCount);
		PdfPTable tempTable = new PdfPTable(labelColCount);
		tempTable.setWidthPercentage(99);
		tempTable.setHeaderRows(sourceTable.getHeaderRows());
		tempTable.setSpacingAfter(10);
		tempTable.setSpacingBefore(10);
		tempTable.setSplitRows(false);
		tempTable.setSplitLate(false);
		//tempTable.getDefaultCell().setBackgroundColor(new BaseColor(79, 98, 40));
		tempTable.getDefaultCell().setBackgroundColor(new BaseColor(0, 92, 140));
		if(currentPageCount != totalPages){
			//Write the headers for other than 1st pages.
			List<PdfPRow> pdfpHRows = sourceTable.getRows(0, sourceTable.getHeaderRows());
			for(int loopCount=0;loopCount <pdfpHRows.size(); loopCount++){
				PdfPRow pdfpRow = pdfpHRows.get(loopCount);
				PdfPCell[] cells = pdfpRow.getCells();
				for(int j=0;j<labelColCount;j++){
					if(cells[j]!=null){
						tempTable.addCell(cells[j]);
					}
				}
				/*for(int j=colStart;j<colEnd;j++){
					if(cells[j]!=null)
						tempTable.addCell(cells[j]);
					else if(loopCount == 0  && colStart >1 && pdfpHRows.size() > 1){
						PdfPCell cell = new PdfPCell();
						cell.setBackgroundColor(new BaseColor(79, 98, 40));
						cell.setBorder(0|0&1|1);
						cell.setUseVariableBorders(true);
						cell.setBorderWidthTop(1);
						cell.setBorderWidthBottom(0.5f);
						cell.setNoWrap(true);
						tempTable.addCell(cell);
					}
				}*/
			}
			tempTable.completeRow();
		}
		for(PdfPRow pdfpRow :pdfpRows){
			PdfPCell[] cells = pdfpRow.getCells();
			for(int j=0;j<labelColCount;j++){
				if(cells[j]!=null)
					tempTable.addCell(cells[j]);
			}
			/*for(int j=colStart;j<colEnd;j++){
				if(cells[j]!=null)
					tempTable.addCell(cells[j]);
			}*/
			tempTable.completeRow();
		}
		return tempTable;
	}
	private static PdfPTable createTableOld(PdfPTable sourceTable,List<PdfPRow> pdfpRows, int colStart, int colEnd, int currentPageCount, 
			int totalPages, int labelColCount) {
		PdfPTable tempTable = new PdfPTable((colEnd-colStart)+labelColCount);
		tempTable.setWidthPercentage(99);
		tempTable.setHeaderRows(sourceTable.getHeaderRows());
		tempTable.setSpacingAfter(10);
		tempTable.setSpacingBefore(10);
		tempTable.setSplitRows(false);
		tempTable.setSplitLate(false);
		//tempTable.getDefaultCell().setBackgroundColor(new BaseColor(79, 98, 40));
		tempTable.getDefaultCell().setBackgroundColor(new BaseColor(0, 92, 140));
		
		if(currentPageCount != totalPages){
			//Write the headers for other than 1st pages.
			List<PdfPRow> pdfpHRows = sourceTable.getRows(0, sourceTable.getHeaderRows());
			for(int loopCount=0;loopCount <pdfpHRows.size(); loopCount++){
				PdfPRow pdfpRow = pdfpHRows.get(loopCount);
				PdfPCell[] cells = pdfpRow.getCells();
				for(int j=0;j<labelColCount;j++){
					if(cells[j]!=null)
						tempTable.addCell(cells[j]);
				}
				for(int j=colStart;j<colEnd;j++){
					if(cells[j]!=null)
						tempTable.addCell(cells[j]);
					else if(loopCount == 0  && colStart >1 && pdfpHRows.size() > 1){
						PdfPCell cell = new PdfPCell();
						cell.setBackgroundColor(new BaseColor(0, 92, 140));
						cell.setBorder(0|0&1|1);
						cell.setUseVariableBorders(true);
						cell.setBorderWidthTop(1);
						cell.setBorderWidthBottom(0.5f);
						cell.setNoWrap(true);
						tempTable.addCell(cell);
					}
				}
			}
			tempTable.completeRow();
		}
		for(PdfPRow pdfpRow :pdfpRows){
			PdfPCell[] cells = pdfpRow.getCells();
			for(int j=0;j<labelColCount;j++){
				if(cells[j]!=null)
					tempTable.addCell(cells[j]);
			}
			for(int j=colStart;j<colEnd;j++){
				if(cells[j]!=null)
					tempTable.addCell(cells[j]);
			}
			tempTable.completeRow();
		}
		return tempTable;
	}
	private static int getRowsPerPage(PdfPTable table, int rowStart, float pageHeight) {
		float totalH = rowStart != 0 ? table.getHeaderHeight(): 0f;
		for(int loopCount = rowStart; loopCount < table.getRows().size(); loopCount++){
			totalH = totalH + table.getRowHeight(loopCount);
			if(totalH > pageHeight) return loopCount-1;
			if(totalH == pageHeight) return loopCount;
		}
		return table.getRows().size();
	}
	static class InvPdfPageEventHelper extends com.itextpdf.text.pdf.PdfPageEventHelper{
		PdfTemplate total;
		PdfTemplate totalPages;
		BaseFont baseFont = null;
		int footerTextSize =7;
		int pageNumberAlignment = Element.ALIGN_LEFT;
		String footerText = "";
		String headerText = "";
		String assetFolderUrl = "";
		List<PromptIdsVb> prompts = null;
		float headerHeight = 0f;
		ReportsVb reportsVb;
		ExportVb exportVb;
		public InvPdfPageEventHelper(String headerText, String footerText, String assetFolderUrl,
				List<PromptIdsVb> prompts,ReportsVb reportsVb) throws DocumentException, IOException{
			this.headerText = headerText;
			this.footerText = footerText;
			this.assetFolderUrl = assetFolderUrl;
			this.prompts = prompts;
			this.reportsVb = reportsVb;
			baseFont = BaseFont.createFont(assetFolderUrl+"/CALIBRI.TTF", BaseFont.WINANSI, BaseFont.EMBEDDED);
		}
		public InvPdfPageEventHelper(String headerText, String footerText, String assetFolderUrl,
				List<PromptIdsVb> prompts,ExportVb exportVb) throws DocumentException, IOException{
			this.headerText = headerText;
			this.footerText = footerText;
			this.assetFolderUrl = assetFolderUrl;
			this.prompts = prompts;
			this.exportVb = exportVb;
			baseFont = BaseFont.createFont(assetFolderUrl+"/CALIBRI.TTF", BaseFont.WINANSI, BaseFont.EMBEDDED);
		}
		@Override
		public void onOpenDocument(PdfWriter writer, Document document) {
		    totalPages = writer.getDirectContent().createTemplate(100, 100);
		    totalPages.setBoundingBox(new Rectangle(-20, -20, 100, 100));
		}

		/*@Override
		public void onStartPage(PdfWriter writer, Document document) {
			try {
				headerHeight = drawHeaderAndFooters(document, reportWriterVb, prompts, assetFolderUrl);
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (DocumentException e) {
				e.printStackTrace();
			}
		}*/
		@Override
		public void onStartPage(PdfWriter writer, Document document) {
			try {
				headerHeight = drawHeaderAndFootersRA(document, reportsVb, prompts, assetFolderUrl);
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (DocumentException e) {
				e.printStackTrace();
			}
		}
		@Override
		public void onEndPage(PdfWriter writer, Document document) {
		    PdfContentByte cb = writer.getDirectContent();
		    Rectangle outLine = document.getPageSize();
			Rectangle footer = new Rectangle(outLine.getLeft(20) , outLine.getTop(20), outLine.getRight(20), outLine.getBottom(20));
			footer.setBorderWidth(1f);
			footer.setBorderColor(BaseColor.BLACK);
			footer.setBorder(Rectangle.BOX);
			cb.rectangle(footer);
		    cb.saveState();
		    String text = "";
		    float textBase = document.bottom() - 10;
		    cb.beginText();
		    cb.setFontAndSize(baseFont, footerTextSize);
	        cb.setTextMatrix((document.right() / 2), textBase);
	        cb.showText(footerText);
	        float textSize = baseFont.getWidthPoint(text, footerTextSize);
	    	SimpleDateFormat dateFormat = new SimpleDateFormat("MMM dd, yyyy - hh:mm:ss a");
	    	text = dateFormat.format(new Date());
	        float adjust = baseFont.getWidthPoint("0", footerTextSize);
	        textSize = baseFont.getWidthPoint(text, footerTextSize);
	        cb.setTextMatrix(document.right() - textSize - adjust, textBase);
	        cb.showText(text);
	        
	        text = String.format("Page %s of ", writer.getPageNumber());
	    	textSize = baseFont.getWidthPoint(text, footerTextSize);
	        cb.setTextMatrix(document.left()+2, textBase);
	        cb.showText(text);
	        cb.endText();
	        cb.addTemplate(totalPages, document.left() + 2 + textSize, textBase);
	        cb.restoreState();
		}

		@Override
		public void onCloseDocument(PdfWriter writer, Document document) {
		    totalPages.beginText();
		    totalPages.setFontAndSize(baseFont, footerTextSize);
		    totalPages.setTextMatrix(0, 0);
		    totalPages.showText(String.valueOf(writer.getPageNumber() - 1));
		    totalPages.endText();
		}
	}
	private static String getChartType(int defChartType){
		switch(defChartType){
			case 1:return "Pie2D";
			case 2:return "Pie3D";
			case 6:return "Column3D";
			case 7:return "StackedColumn3D";
			case 9:return "Area2D";
			case 19:return "MSColumn3D";
			case 20:return "Bubble";
			case 21:return "MSColumn3DLineDY";
			case 22:return "Radar";
			case 23:return "HeatMap";
			case 24:return "MSLine";
			default:return "G";
		}
	}
	private static int getStyle(String formatType, int rowNum){
		if((rowNum %2) == 0 && ("S".equalsIgnoreCase(formatType) || "G".equalsIgnoreCase(formatType))){
			return 1; //Summary with Background
		}else if((rowNum %2) == 0){
			return 2; //Non Summary with background.
		}else if("S".equalsIgnoreCase(formatType) || "G".equalsIgnoreCase(formatType)){
			return 3;//Summary with out Background
		}else{
			return 4;//Non Summary With out background
		}
	}
	int colheaderFontSize = 9;
	int dataFontSize = 8;
	int summaryDataFontSize = 8;
	public  ExceptionCode exportToPdfRAWithGroup(List<ColumnHeadersVb> columnHeaders, List<HashMap<String, String>> dataLst, 
			 ReportsVb reportsVb, String assetFolderUrl,List<String> columnTypes,List<HashMap<String, String>> totalLst,
			 Map < String, List < HashMap<String, String> >> groupingMap,ArrayList<ColumnHeadersVb> columnHeadersFinallst,
					 String processId,String versionNo){

		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			HashMap<String,String> sumMap = new HashMap<String,String>();
			Double sumVal = 0.0;
			Document document =  null;
			PdfPTable table = null;
			PdfPTable tempTable = null;
			
			float pageWidthSize = 0f;
			float pageHeightSize = 0f;
			int pdfWidthGrwth = reportsVb.getPdfGrwthPercent();
			if("P".equalsIgnoreCase(reportsVb.getReportOrientation())) {
				pageWidthSize = PageSize.A4.getWidth();
				pageHeightSize= PageSize.A4.getHeight();
			}else{
				pageWidthSize = PageSize.A4.getHeight();
				pageHeightSize= PageSize.A4.getWidth();
			}
			float widthAfterMargin = pageWidthSize - 40;
			
			File lFile = null;
			PdfWriter writer = null;
			String filePath = "";
			try {
				
				BaseFont baseFont = BaseFont.createFont(assetFolderUrl+"/CALIBRI.TTF", BaseFont.WINANSI, BaseFont.EMBEDDED);
				
				int maxDataSize =  groupingMap.size();
				int itCounter = 0;
				Set entrySet = groupingMap.entrySet();
				Iterator it = entrySet.iterator();
				Boolean subTotalFlag = false;
				Boolean firstTimeFalg = true;
				while (it.hasNext()) {
					itCounter++;
					Map.Entry dataVal = (Map.Entry) it.next();
					List<HashMap<String, String>> grpDataLst = (List) dataVal.getValue();
					String key = (String) dataVal.getKey();
				       if(key.isEmpty())
				    	   continue;
					dataLst = grpDataLst;
					subTotalFlag = false;
					sumMap = new HashMap<String, String>();
	
					PdfPTable groupingTable = new PdfPTable(1);
					groupingTable.setWidthPercentage(100);
					PdfPCell cellG;
					Font groupingFont = new Font(baseFont);
					groupingFont.setSize(7);
					groupingFont.setStyle(Font.BOLD);
					Phrase ph = new Phrase(dataVal.getKey().toString(), groupingFont);
					Paragraph p = new Paragraph(ph);
					cellG = new PdfPCell(p);
					cellG.setHorizontalAlignment(Element.ALIGN_LEFT);
					cellG.setBorder(0);
					cellG.setPaddingTop(4f);
					// cell.setBackgroundColor(new BaseColor(205, 226, 236));
					groupingTable.addCell(cellG);
	
					int colNum = 0;
					int rowNum = 1;
					int loopCnt = 0;
					int subColumnCnt = 0;
					Boolean totalRowFlag = false;
					boolean isgroupColumns = false;
					Map<Integer, Float> colSizes = new HashMap<Integer, Float>();
					int maxHeaderRow = columnHeaders.stream().mapToInt(ColumnHeadersVb::getLabelRowNum).max().orElse(0);
					int maxHeaderCol = columnHeaders.stream().mapToInt(ColumnHeadersVb::getLabelColNum).max().orElse(0);
	
					List<ColumnHeadersVb> colHeadersCol = columnHeaders.stream()
							.sorted(Comparator.comparingInt(ColumnHeadersVb::getLabelRowNum)).collect(Collectors.toList());
	
					if (columnHeaders != null) {
						table = new PdfPTable(maxHeaderCol);
						table.setWidthPercentage(100);
						table.setHeaderRows(maxHeaderRow);
						table.setSpacingAfter(12);
						table.setSpacingBefore(12);
						Font font = new Font(baseFont);
						font.setColor(BaseColor.WHITE);
						font.setStyle(Font.BOLD);
						font.setSize(colheaderFontSize);
						if (maxHeaderRow != 1) {
							table.setSplitRows(false);
							table.setSplitLate(false);
						}
						table.getDefaultCell().setBackgroundColor(new BaseColor(79, 98, 40));
						table.getDefaultCell().setNoWrap(true);
						loopCnt = 0;
						boolean isNextRow = false;
						for (ColumnHeadersVb columnHeadersVb : colHeadersCol) {
							if (columnHeadersVb.getCaption().contains("<br/>")) {
								String value = columnHeadersVb.getCaption().replaceAll("<br/>", "\n");
								columnHeadersVb.setCaption(value);
							}
							PdfPCell cell = new PdfPCell(new Phrase(columnHeadersVb.getCaption(), font));
							//cell.setBackgroundColor(new BaseColor(0, 92, 140));
							int[] sunoidaPinkClr =  getRGB(reportsVb.getApplicationTheme());
							cell.setBackgroundColor(new BaseColor(sunoidaPinkClr[0], sunoidaPinkClr[1], sunoidaPinkClr[2]));
							//cell.setBackgroundColor(new BaseColor(177, 24, 124));
							cell.setUseVariableBorders(true);
							cell.setBorderWidthTop(0.5f);
							cell.setBorderWidthBottom(0.5f);
							cell.setBorderColor(new BaseColor(222, 226, 230));
							cell.setNoWrap(false);
							cell.setPaddingBottom(4f);
							cell.setPaddingTop(4f);
							cell.setPaddingLeft(2f);
							cell.setPaddingRight(2f);
	
							String type = "";
							String colType = columnHeadersVb.getColType();
							if ("T".equalsIgnoreCase(colType)) {
								type = CAP_COL;
							} else {
								type = DATA_COL;
							}
							cell.setHorizontalAlignment(Element.ALIGN_CENTER);
	
							if (columnHeadersVb.getRowspan() != 0) {
								cell.setRowspan(columnHeadersVb.getRowspan());
								cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
							}
							if (columnHeadersVb.getColspan() != 0)
								cell.setColspan(columnHeadersVb.getColspan());
							cell.setNoWrap(true);
							if (columnHeadersVb.getLabelRowNum() > 1 && !isNextRow) {
								table.completeRow();
								table.getDefaultCell().setBackgroundColor(null);
								isNextRow = true;
							}
							table.addCell(cell);
							++colNum;
							if (columnHeadersVb.getColspan()  <= 1 && columnHeadersVb.getNumericColumnNo() != 99) {
								/*float columnWidth = Float.parseFloat(columnHeadersVb.getColumnWidth());
								float pdfColWdth = widthAfterMargin * columnWidth / 100;
								colSizes.put(columnHeadersVb.getLabelColNum() - 1, pdfColWdth);*/
								colSizes.put(columnHeadersVb.getLabelColNum()-1, ColumnText.getWidth(cell.getPhrase()));
								loopCnt++;
							}
						}
					}
					// Writing The Header
					table.completeRow();
					table.getDefaultCell().setBackgroundColor(null);
					PdfPCell cell = null;
					int recCount = 0;
					int ctr = 1;
	
					// Writing the data
					do {
						for (HashMap dataLstMap : dataLst) {
							boolean highlight = false;
							String formatType = "D";
							if(dataLstMap.containsKey("FORMAT_TYPE")) {
								formatType = dataLstMap.get("FORMAT_TYPE").toString();
							}
							for (int loopCount = 0; loopCount < columnHeadersFinallst.size(); loopCount++) {
								ColumnHeadersVb colHeadersVb = columnHeadersFinallst.get(loopCount);
								if (colHeadersVb.getColspan() > 1 || colHeadersVb.getNumericColumnNo() == 99)
									continue;
								int index = 0;
								String type = "";
								String colType = columnTypes.get(loopCount);
								if ("T".equalsIgnoreCase(colType)) {
									type = CAP_COL;
								} else {
									type = DATA_COL;
								}
								String cellText = "";
								if (type.equalsIgnoreCase(CAP_COL)) {
									index = (loopCount + 1);
									String orgValue = "";
									if (dataLstMap.containsKey(colHeadersVb.getDbColumnName())) {
										orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null
												|| dataLstMap.get(colHeadersVb.getDbColumnName()) == "")
														? dataLstMap.get(colHeadersVb.getDbColumnName()).toString()
														: "";
									}
									cellText = orgValue;
									
									if (totalRowFlag || subTotalFlag) {
										ExceptionCode excepCode = writeTotalRowCaptionColDataToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,summaryDataFontSize,cell);
										if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
											table = (PdfPTable)excepCode.getResponse();
											cell = (PdfPCell)excepCode.getOtherInfo();	
										}else {
											exceptionCode.setErrorCode(excepCode.getErrorCode());
											exceptionCode.setErrorMsg(excepCode.getErrorMsg());
											return exceptionCode;
										}
									} else {
										ExceptionCode excepCode = writeCaptionColDataToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,formatType,dataFontSize,cell);
										if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
											table = (PdfPTable)excepCode.getResponse();
											cell = (PdfPCell)excepCode.getOtherInfo();	
										}else {
											exceptionCode.setErrorCode(excepCode.getErrorCode());
											exceptionCode.setErrorMsg(excepCode.getErrorMsg());
											return exceptionCode;
										}
									}
								} else {
									String orgValue = "";
									// index = ((loopCount+1) - reportsWriterVb.getCaptionLabelColCount());
									if (dataLstMap.containsKey(colHeadersVb.getDbColumnName())) {
										orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null
												|| dataLstMap.get(colHeadersVb.getDbColumnName()) == "")
														? dataLstMap.get(colHeadersVb.getDbColumnName()).toString()
														: "";
									}
									// String orgValue = dataLstMap.containsKey(colHeadersVb.getDbColumnName())?
									// dataLstMap.get(colHeadersVb.getDbColumnName()).toString(): "";
									if (ValidationUtil.isValid(orgValue)) {
											if ("I".equalsIgnoreCase(colType) || "S".equalsIgnoreCase(colType)) {
												if (ValidationUtil.isNumericDecimal(orgValue)) {
													double amount = Double.parseDouble(orgValue);
													DecimalFormat formatter = new DecimalFormat("#,###");
													cellText = formatter.format(amount);
												} else {
													cellText = orgValue;
												}
										} else {
											if (ValidationUtil.isNumericDecimal(orgValue)) {
												if (!"NR".equalsIgnoreCase(colType) && !"TR".equalsIgnoreCase(colType)) {
													double amount = Double.parseDouble(orgValue);
													DecimalFormat formatter = new DecimalFormat("#,##0.00");
													cellText = formatter.format(amount);
												} else {
													cellText = orgValue;
												}
											} else {
												cellText = orgValue;
											}
										}
									} else {
										cellText = orgValue;
									}
									if (ValidationUtil.isValid(cellText) && "-0.00".equalsIgnoreCase(cellText)) {
										cellText = "0.00";
									}
									if (totalRowFlag || subTotalFlag) {
										ExceptionCode excepCode = writeTotalRowDataColToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,summaryDataFontSize,cell);
										if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
											table = (PdfPTable)excepCode.getResponse();
											cell = (PdfPCell)excepCode.getOtherInfo();	
										}else {
											exceptionCode.setErrorCode(excepCode.getErrorCode());
											exceptionCode.setErrorMsg(excepCode.getErrorMsg());
											return exceptionCode;
										}
									} else {
										ExceptionCode excepCode = writeRowDataColToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,dataFontSize,formatType,cellText,colHeadersVb,cell);
										if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
											table = (PdfPTable)excepCode.getResponse();
											cell = (PdfPCell)excepCode.getOtherInfo();	
										}else {
											exceptionCode.setErrorCode(excepCode.getErrorCode());
											exceptionCode.setErrorMsg(excepCode.getErrorMsg());
											return exceptionCode;
										}
										
										String cellValue = orgValue.replaceAll(",", "");
										if("Y".equalsIgnoreCase(colHeadersVb.getSumFlag())) {
											String prevValstr = sumMap.containsKey(colHeadersVb.getDbColumnName())
													? sumMap.get(colHeadersVb.getDbColumnName())
													: "0";
											Double strVal = Double.parseDouble(prevValstr);
											sumVal = strVal + Double.parseDouble(cellValue);
											DecimalFormat formatter = new DecimalFormat("####.00");
											String cellText1 = formatter.format(sumVal);
											sumMap.put(colHeadersVb.getDbColumnName(), cellText1);
										}
									}
								}
								if (cellText == null || cellText.trim().isEmpty()) {
									// colSizes.put(loopCount, Math.max(20, colSizes.get(loopCount)));
								} else {
	
									/*
									 * float width = Math.max(ColumnText.getWidth(cell.getPhrase()),
									 * colSizes.get(loopCount)); if (width < 20) width = 20; colSizes.put(loopCount,
									 * width);
									 */
								}
							}
							recCount++;
						}
						ctr++;
						reportsVb.setCurrentPage(ctr);
						dataLst = new ArrayList();
						if (itCounter == maxDataSize) {
							if (!subTotalFlag) {
								if(!sumMap.isEmpty()) {
									sumMap.put("FORMAT_TYPE", "ST");
									dataLst.add(sumMap);
									subTotalFlag = true;
								}
								totalRowFlag = false;
							} else if (!totalRowFlag) {
								dataLst = totalLst;
								totalRowFlag = true;
								subTotalFlag = true;
							}
						} else {
							if (!subTotalFlag) {
								if(!sumMap.isEmpty()) {
									sumMap.put("FORMAT_TYPE", "ST");
									dataLst.add(sumMap);
									subTotalFlag = true;
								}
								totalRowFlag = false;
							}
						}
					} while (dataLst != null && !dataLst.isEmpty());
	
					float totalWidth = 0f;
					float sumOfLabelColWidth = 0f;
					float[] colsArry = new float[colSizes.size()];
					for (int i = 0; i < colSizes.size(); i++) {
						totalWidth += colSizes.get(i);
						colsArry[i] = colSizes.get(i);
						if (i < maxHeaderCol) {
							sumOfLabelColWidth += colSizes.get(i);
						}
					}
				
				if (totalWidth > pageWidthSize) {
					float newWidth= totalWidth+100;
					float aspectRatio = 0f;
					aspectRatio = pageHeightSize/pageWidthSize;
					float finalPageHeight = aspectRatio*newWidth;
					pageWidthSize = newWidth;
					pageHeightSize = finalPageHeight;
				}
				InvPdfPageEventHelper invPdfEvt = new InvPdfPageEventHelper(null, reportsVb.getMakerName(),assetFolderUrl, new ArrayList(), reportsVb);
				if (firstTimeFalg) {
					Rectangle rectangle = new Rectangle(pageWidthSize, pageHeightSize);
					document = new Document(rectangle, 20, 20, 20, 20);
					filePath = System.getProperty("java.io.tmpdir");
					if (!ValidationUtil.isValid(filePath)) {
						filePath = System.getenv("TMP");
					}
					if (ValidationUtil.isValid(filePath)) {
						filePath = filePath + File.separator;
					}

					lFile = new File(filePath + ValidationUtil.encode(reportsVb.getReportTitle())+"_"+processId+"_"+ versionNo +".pdf");
					if (lFile.exists()) {
						lFile.delete();
					}
					lFile.createNewFile();
					FileOutputStream fips = new FileOutputStream(lFile);
					writer = PdfWriter.getInstance(document, fips);
					writer.setPageEvent(invPdfEvt);
					document.open();
					//creating prompts
					PdfPTable tableT = null;
					tableT = createPromptsPage(document,reportsVb);
				    document.add(tableT);
				    document.newPage();
				    firstTimeFalg = false;
				}
				document.add(groupingTable);
				float pageWidth = pageWidthSize;
			    float pageHeight =  (pageHeightSize) - (document.topMargin() + document.bottomMargin() + invPdfEvt.headerHeight+50);
					// float pageWidth = reportsVb.getPdfWidth();
					// pageWidth = (pageWidth - (document.leftMargin() + document.rightMargin()));
					/* if (totalWidth < pageWidth) { */
					float percentWidth = (float) ((totalWidth * 100) / (pageWidth - 200));
					// if (percentWidth > 99)
					percentWidth = 99;
					int rowStart = 0;
					int rowEnd = table.getRows().size();
					table.setTotalWidth(totalWidth);
					table.setWidths(colsArry);
					float totalHeight = table.calculateHeights(true);
					// float pageHeight = reportsVb.getPdfHeight() - (document.topMargin() +
					// document.bottomMargin()+ invPdfEvt.headerHeight + table.spacingAfter());
					int pages = 1;
					if (totalHeight > pageHeight) {
						pages = Math.round(totalHeight / pageHeight) < (totalHeight / pageHeight)
								? Math.round(totalHeight / pageHeight) + 1
								: Math.round(totalHeight / pageHeight);
						totalHeight += pages * table.getHeaderHeight();
						pages = Math.round(totalHeight / pageHeight) < (totalHeight / pageHeight)
								? Math.round(totalHeight / pageHeight) + 1
								: Math.round(totalHeight / pageHeight);
					}
	
					int totalPages = pages;
					rowEnd = getRowsPerPage(table, rowStart, pageHeight);
					while (pages > 0) {
						List<PdfPRow> pdfpRows = table.getRows(rowStart, rowEnd);
						tempTable = createTable(table, pdfpRows, maxHeaderCol, table.getNumberOfColumns(), pages,
								totalPages, maxHeaderCol);
						tempTable.setTotalWidth(totalWidth);
						tempTable.setWidthPercentage(percentWidth);
						tempTable.setWidths(colsArry);
						// tempTable.setLockedWidth(true);
						document.add(tempTable);
						rowStart = rowEnd;
						rowEnd = getRowsPerPage(table, rowStart, pageHeight);
						pages--;
						if (pages - 1 != 0)
							rowEnd = rowEnd - table.getHeaderRows();
						if (pages > 0)
							document.newPage();
					}
					if (itCounter < maxDataSize)
						document.newPage();
				}
				
				document.close();
				writer.close();
				exceptionCode.setResponse(filePath);
				exceptionCode.setOtherInfo(reportsVb.getReportTitle() + "_"  +processId+"_"+versionNo);
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			} catch (RuntimeCustomException e) {
				e.printStackTrace();
				exceptionCode.setErrorMsg(e.getMessage());
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				throw new RuntimeCustomException(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				exceptionCode.setErrorMsg(e.getMessage());
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
		}catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	
	public  ExceptionCode exportToPdfRA(List<ColumnHeadersVb> columnHeaders, List<HashMap<String, String>> dataLst, ReportsVb reportsVb, 
			String assetFolderUrl,List<String> columnTypes,List<HashMap<String, String>> totalLst,ArrayList<ColumnHeadersVb> columnHeadersFinallst,
					String processId,String versionId){

		
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			Document document =  null;
			PdfPTable table = null;
			PdfPTable tempTable = null;
			
			float pageWidthSize = 0f;
			float pageHeightSize = 0f;
			int pdfWidthGrwth = reportsVb.getPdfGrwthPercent();
			if("P".equalsIgnoreCase(reportsVb.getReportOrientation())) {
				pageWidthSize = PageSize.A4.getWidth();
				pageHeightSize= PageSize.A4.getHeight();
			}else{
				pageWidthSize = PageSize.A4.getHeight();
				pageHeightSize= PageSize.A4.getWidth();
			}
			
			
			//float widthAfterMargin = pageWidthSize - 40;
			
			try {
				BaseFont baseFont = BaseFont.createFont(assetFolderUrl + "/CALIBRI.TTF", BaseFont.WINANSI,
						BaseFont.EMBEDDED);
				
				
				int colNum = 0;
				int rowNum = 1;
				int loopCnt = 0;
				int subColumnCnt = 0;
				Boolean totalRowFlag = false;
				boolean isgroupColumns = false;
				Map<Integer, Float> colSizes = new HashMap<Integer, Float>();
				int maxHeaderRow = columnHeaders.stream().mapToInt(ColumnHeadersVb::getLabelRowNum).max().orElse(0);
				int maxHeaderCol = columnHeaders.stream().mapToInt(ColumnHeadersVb::getLabelColNum).max().orElse(0);
	
				List<ColumnHeadersVb> colHeadersCol = columnHeaders.stream()
						.sorted(Comparator.comparingInt(ColumnHeadersVb::getLabelRowNum)).collect(Collectors.toList());
				
				// The Header Table creation
				if (columnHeaders != null) {
					table = new PdfPTable(maxHeaderCol);
					table.setWidthPercentage(100);
					table.setHeaderRows(maxHeaderRow);
					table.setSpacingAfter(12);
					table.setSpacingBefore(12);
					Font font = new Font(baseFont);
					font.setColor(BaseColor.WHITE);
					//font.setStyle(Font.BOLD);
					font.setSize(colheaderFontSize);
					if (maxHeaderRow != 1) {
						table.setSplitRows(false);
						table.setSplitLate(false);
					}
					table.getDefaultCell().setBackgroundColor(new BaseColor(79, 98, 40)); // Default Color blue
					table.getDefaultCell().setNoWrap(true);
					loopCnt = 0;
					boolean isNextRow = false;
					for (ColumnHeadersVb columnHeadersVb : colHeadersCol) {
						if (columnHeadersVb.getCaption().contains("<br/>")) {
							String value = columnHeadersVb.getCaption().replaceAll("<br/>", "\n");
							columnHeadersVb.setCaption(value);
						}
						PdfPCell cell = new PdfPCell(new Phrase(columnHeadersVb.getCaption(), font));
						//cell.setBackgroundColor(new BaseColor(0, 92, 140)); // Default Color blue
						int[] sunoidaPinkClr =  getRGB(reportsVb.getApplicationTheme());
						cell.setBackgroundColor(new BaseColor(sunoidaPinkClr[0],sunoidaPinkClr[1],sunoidaPinkClr[2])); // Sunoida Latest Pink
						cell.setUseVariableBorders(true);
						cell.setBorderWidthTop(0.5f);
						cell.setBorderWidthBottom(0.5f);
						cell.setBorderColor(new BaseColor(222,226,230));
						cell.setNoWrap(true);
						cell.setPaddingBottom(4f);
						cell.setPaddingTop(4f);
						cell.setPaddingLeft(2f);
						cell.setPaddingRight(2f);
	
						String type = "";
						String colType = columnHeadersVb.getColType();
						if ("T".equalsIgnoreCase(colType)) {
							type = CAP_COL;
						} else {
							type = DATA_COL;
						}
						cell.setHorizontalAlignment(Element.ALIGN_CENTER);
						
						if (columnHeadersVb.getRowspan() != 0) {
							cell.setRowspan(columnHeadersVb.getRowspan());
							cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
						}
						if (columnHeadersVb.getColspan() != 0)
							cell.setColspan(columnHeadersVb.getColspan());
						cell.setNoWrap(true);
						if (columnHeadersVb.getLabelRowNum() > 1 && !isNextRow) {
							table.completeRow();
							table.getDefaultCell().setBackgroundColor(null);
							isNextRow = true;
						}
						table.addCell(cell);
						++colNum;
						if (columnHeadersVb.getColspan() <= 1) {
							if(columnHeadersVb.getNumericColumnNo() != 99) {
								//float columnWidth = Float.parseFloat(columnHeadersVb.getColumnWidth());
								//float pdfColWdth = widthAfterMargin*columnWidth/100;
								colSizes.put(columnHeadersVb.getLabelColNum()-1, ColumnText.getWidth(cell.getPhrase()));
								//colSizes.put(columnHeadersVb.getLabelColNum() - 1, pdfColWdth);
								loopCnt++;
							}
						}
					}
				}
				// Writing The Header
				table.completeRow();
				table.getDefaultCell().setBackgroundColor(null);
				PdfPCell cell = null;
				int recCount = 0;
				int ctr = 1;
				// Writingthe data
				do {
					if(dataLst != null) {
						for (HashMap dataLstMap : dataLst) {
							boolean highlight = false;
							String formatType = "D";
							if(dataLstMap.containsKey("FORMAT_TYPE")) {
								formatType = dataLstMap.get("FORMAT_TYPE").toString();
							}
							for (int loopCount = 0; loopCount < columnHeadersFinallst.size(); loopCount++) {
								ColumnHeadersVb colHeadersVb = columnHeadersFinallst.get(loopCount);
								if (colHeadersVb.getColspan() > 1 || colHeadersVb.getNumericColumnNo() == 99)
									continue;
								int index = 0;
								String type = "";
								String colType = columnTypes.get(loopCount);
								if ("T".equalsIgnoreCase(colType)) {
									type = CAP_COL;
								} else if ("P".equalsIgnoreCase(colType)) {
									type = "GROWTH_IMG";
								} else  {
									type = DATA_COL;
								}
								String cellText = "";
								String orgValue = "";
								if (type.equalsIgnoreCase(CAP_COL)) {
									index = (loopCount + 1);
									if(dataLstMap.containsKey(colHeadersVb.getDbColumnName())) {
										orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null ||  dataLstMap.get(colHeadersVb.getDbColumnName()) == "") ? dataLstMap.get(colHeadersVb.getDbColumnName()).toString() : "";
									}
									
									cellText = orgValue;
									if (totalRowFlag) {
										ExceptionCode excepCode = writeTotalRowCaptionColDataToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,summaryDataFontSize,cell);
										if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
											table = (PdfPTable)excepCode.getResponse();
											cell = (PdfPCell)excepCode.getOtherInfo();	
										}else {
											exceptionCode.setErrorCode(excepCode.getErrorCode());
											exceptionCode.setErrorMsg(excepCode.getErrorMsg());
											return exceptionCode;
										}
									} else {
										ExceptionCode excepCode = writeCaptionColDataToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,formatType,dataFontSize,cell);
										if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
											table = (PdfPTable)excepCode.getResponse();
											cell = (PdfPCell)excepCode.getOtherInfo();	
										}else {
											exceptionCode.setErrorCode(excepCode.getErrorCode());
											exceptionCode.setErrorMsg(excepCode.getErrorMsg());
											return exceptionCode;
										}
									}
								} else if("GROWTH_IMG".equalsIgnoreCase(type)){
									cellText="ARROW";
									if(dataLstMap.containsKey(colHeadersVb.getDbColumnName())) {
										orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null ||  dataLstMap.get(colHeadersVb.getDbColumnName()) == "") ? dataLstMap.get(colHeadersVb.getDbColumnName()).toString() : "";
									}
									table = writeGrowthColDataToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,formatType,dataFontSize,assetFolderUrl);
								} else {
									// index = ((loopCount+1) - reportsWriterVb.getCaptionLabelColCount());
									if(dataLstMap.containsKey(colHeadersVb.getDbColumnName())) 
										orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null ||  dataLstMap.get(colHeadersVb.getDbColumnName()) == "") ? dataLstMap.get(colHeadersVb.getDbColumnName()).toString() : "";
									else 
										orgValue = "";
									//String orgValue = dataLstMap.containsKey(colHeadersVb.getDbColumnName())? dataLstMap.get(colHeadersVb.getDbColumnName()).toString(): "";
									if(ValidationUtil.isValid(orgValue)) {
										if ("I".equalsIgnoreCase(colType) || "S".equalsIgnoreCase(colType)) {
											if (ValidationUtil.isNumericDecimal(orgValue)) {
													double amount = Double.parseDouble(orgValue);
													DecimalFormat formatter = new DecimalFormat("#,###");
													cellText = formatter.format(amount);
												} else {
													cellText = orgValue;
												}
										} else {
											if (ValidationUtil.isNumericDecimal(orgValue)) {
												if (!"NR".equalsIgnoreCase(colType) && !"TR".equalsIgnoreCase(colType)) {
													double amount = Double.parseDouble(orgValue);
													DecimalFormat formatter = new DecimalFormat("#,##0.00");
													cellText = formatter.format(amount);
												} else {
													cellText = orgValue;
												}
											} else {
												Double val = Double.parseDouble(orgValue);
												DecimalFormat formatValue = new DecimalFormat("0.00");
												cellText = formatValue.format(val);
											}
										}
									}else {
										cellText = orgValue;
									}
									if(ValidationUtil.isValid(cellText) && "-0.00".equalsIgnoreCase(cellText)) {
										cellText = "0.00";
									}
									if (totalRowFlag) {
										ExceptionCode excepCode = writeTotalRowDataColToTable(reportsVb,loopCount,cellText,baseFont,table,dataLst,recCount,maxHeaderCol,summaryDataFontSize,cell);
										if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
											table = (PdfPTable)excepCode.getResponse();
											cell = (PdfPCell)excepCode.getOtherInfo();	
										}else {
											exceptionCode.setErrorCode(excepCode.getErrorCode());
											exceptionCode.setErrorMsg(excepCode.getErrorMsg());
											return exceptionCode;
										}
									} else {
										ExceptionCode excepCode = writeRowDataColToTable(reportsVb,loopCount,cellText,baseFont,table,dataLst,recCount,maxHeaderCol,dataFontSize,formatType,cellText,colHeadersVb,cell);
										if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
											table = (PdfPTable)excepCode.getResponse();
											cell = (PdfPCell)excepCode.getOtherInfo();	
										}else {
											exceptionCode.setErrorCode(excepCode.getErrorCode());
											exceptionCode.setErrorMsg(excepCode.getErrorMsg());
											return exceptionCode;
										}
									}
								}
								if("ARROW".equalsIgnoreCase(cellText)) {
									float width = 10;
									colSizes.put(loopCount, width);
								}else if (ValidationUtil.isValid(cellText)) {
									float width = Math.max(ColumnText.getWidth(cell.getPhrase()), colSizes.get(loopCount));
									if (width < 20)
										width = 20;
									colSizes.put(loopCount, width);
								}
							}
							recCount++;
						}
					}
					ctr++;
					reportsVb.setCurrentPage(ctr);
					dataLst = new ArrayList();
					if (!totalRowFlag) {
						exceptionCode = new ReportsWb(jdbcTemplate).getResultData(reportsVb);
						if (ValidationUtil.isValid(exceptionCode.getResponse())) {
							ReportsVb resultVb = (ReportsVb) exceptionCode.getResponse();
							dataLst = resultVb.getGridDataSet();
							if (dataLst == null || dataLst.isEmpty()) {
								if(totalLst != null && !totalLst.isEmpty()) {
									dataLst.add(totalLst.get(0));
									totalRowFlag = true;
								}
							}
						}
					}
				} while (dataLst != null && !dataLst.isEmpty());
	
				float totalWidth = 0f;
				float sumOfLabelColWidth = 0f;
				float[] colsArry = new float[colSizes.size()];
				for (int i = 0; i < colSizes.size(); i++) {
					totalWidth += colSizes.get(i);
					colsArry[i] = colSizes.get(i);
					if (i < maxHeaderCol) {
						sumOfLabelColWidth += colSizes.get(i);
					}
				}
				if (totalWidth > pageWidthSize) {
					float newWidth= totalWidth+100;
					float aspectRatio = 0f;
					aspectRatio = pageHeightSize/pageWidthSize;
					float finalPageHeight = aspectRatio*newWidth;
					pageWidthSize = newWidth;
					pageHeightSize = finalPageHeight;
				}
				Rectangle rectangle = new Rectangle(pageWidthSize, pageHeightSize);
				document = new Document(rectangle, 20, 20, 20, 20);
				String filePath = System.getProperty("java.io.tmpdir");
				if (!ValidationUtil.isValid(filePath)) {
					filePath = System.getenv("TMP");
				}
				if (ValidationUtil.isValid(filePath)) {
					filePath = filePath + File.separator;
				}
				File lFile = null;
				lFile = new File(filePath + ValidationUtil.encode(reportsVb.getReportTitle())+"_"+processId+"_"+ versionId +".pdf");
				if (lFile.exists()) {
					lFile.delete();
				}
				
				lFile.createNewFile();
				FileOutputStream fips = new FileOutputStream(lFile);
				PdfWriter writer = PdfWriter.getInstance(document, fips);
				InvPdfPageEventHelper invPdfEvt = new InvPdfPageEventHelper(null, reportsVb.getMakerName(), assetFolderUrl,
						new ArrayList(), reportsVb);
				writer.setPageEvent(invPdfEvt);
				document.open();
				
				float pageWidth = pageWidthSize;
			    float pageHeight =  (pageHeightSize) - (document.topMargin() + document.bottomMargin() + invPdfEvt.headerHeight+50);
			    
			    //float pageWidth = reportsVb.getPdfWidth();
				pageWidth = (pageWidth - (document.leftMargin() + document.rightMargin()));
				/*if (totalWidth < pageWidth) {*/
					float percentWidth = (float) ((totalWidth * 100) / (pageWidth - 200));
					//if (percentWidth > 99)
					percentWidth = 99;
					int rowStart = 0;
					int rowEnd = table.getRows().size();
					table.setTotalWidth(totalWidth);
					table.setWidths(colsArry);
					float totalHeight = table.calculateHeights(true);
					//float pageHeight = reportsVb.getPdfHeight() - (document.topMargin() + document.bottomMargin()+ invPdfEvt.headerHeight + table.spacingAfter());
	
					int pages = 1;
					if (totalHeight > pageHeight) {
						pages = Math.round(totalHeight / pageHeight) < (totalHeight / pageHeight)? Math.round(totalHeight / pageHeight) + 1
								: Math.round(totalHeight / pageHeight);
						totalHeight += pages * table.getHeaderHeight();
						pages = Math.round(totalHeight / pageHeight) < (totalHeight / pageHeight)? Math.round(totalHeight / pageHeight) + 1
								: Math.round(totalHeight / pageHeight);
					}
					
					int totalPages = pages;
					rowEnd = getRowsPerPage(table, rowStart, pageHeight);

					PdfPTable tableT = null;
					tableT = createPromptsPage(document,reportsVb);
				    document.add(tableT);
					document.newPage();
					while (pages > 0) {
						List<PdfPRow> pdfpRows = table.getRows(rowStart, rowEnd);
						tempTable = createTable(table, pdfpRows, maxHeaderCol, table.getNumberOfColumns(), pages,totalPages, maxHeaderCol);
						tempTable.setTotalWidth(totalWidth);
						tempTable.setWidthPercentage(percentWidth);
						tempTable.setWidths(colsArry);
						// tempTable.setLockedWidth(true);
						document.add(tempTable);
						if(rowEnd != table.getRows().size()) {
							rowStart = rowEnd;
							rowEnd = getRowsPerPage(table, rowStart, pageHeight);
							pages--;
							/*if (pages - 1 != 0)
								rowEnd = rowEnd - table.getHeaderRows();*/
							if (pages > 0)
								document.newPage();
						}else {
							pages = 0;
						}
					}
				document.close();
				writer.close();
				exceptionCode.setResponse(filePath);
				exceptionCode.setOtherInfo(reportsVb.getReportTitle() + "_" +processId+"_"+versionId);
				exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			} catch (RuntimeCustomException e) {
				e.printStackTrace();
				exceptionCode.setErrorMsg(e.getMessage());
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				throw new RuntimeCustomException(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				exceptionCode.setErrorMsg(e.getMessage());
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				return exceptionCode;
			}
		}catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	private static float drawHeaderAndFootersRA(Document document, ReportsVb reportsVb, 
			List<PromptIdsVb> prompts, String assetFolderUrl) throws MalformedURLException, IOException, DocumentException{
		int height = 30;
		BaseFont baseFont = BaseFont.createFont(assetFolderUrl+"/CALIBRI.TTF", BaseFont.WINANSI, BaseFont.EMBEDDED);
		PdfPTable borderTable = new PdfPTable(1);
		borderTable.setWidthPercentage(100);
		PdfPCell cellMain = new PdfPCell();
		cellMain.setBorder(0&0&0&1);
		cellMain.setBorderWidth(5);
		cellMain.setFixedHeight(150);
		cellMain.setBackgroundColor(new BaseColor(249,249,249)); // Light Grey
		PdfPTable headerTable = new PdfPTable(3);
		headerTable.setWidthPercentage(100);
		headerTable.setSpacingAfter(2);
		int widths[] = {1,5,1};
		headerTable.setWidths(widths);
		Image visionLogoUrl = Image.getInstance(assetFolderUrl+"/Product_Logo.png"); //Vision_rep_pdf.png
		PdfPCell cell1 = new PdfPCell();
		cell1.addElement(visionLogoUrl);
		cell1.setHorizontalAlignment(Element.ALIGN_LEFT);
		cell1.setVerticalAlignment(Element.ALIGN_MIDDLE);
		cell1.setBorder(0&0&0&0);
		
		headerTable.addCell(cell1);
		List<PdfPCell> cells= new ArrayList<PdfPCell>();
			
		PdfPTable promptsTable = new PdfPTable(1);
		promptsTable.setWidthPercentage(100);
		
		PdfPCell cell3 = new PdfPCell();
		Font fontheading = new Font(baseFont);
		fontheading.setSize(8);
		fontheading.setStyle(Font.BOLD);
		Phrase ph= new Phrase(reportsVb.getReportTitle(),fontheading);
		cell3.setPhrase(ph);
		cell3.setHorizontalAlignment(Element.ALIGN_CENTER);
		cell3.setVerticalAlignment(Element.ALIGN_MIDDLE);
		cell3.setBorder(0&0&0&0);

		promptsTable.addCell(cell3);
		for(PdfPCell cell:cells){
			promptsTable.addCell(cell);
		}
					
		PdfPCell nestedCell = new PdfPCell();
		nestedCell.addElement(promptsTable);
		nestedCell.setPadding(0);
		nestedCell.setBorder(0);
		nestedCell.setHorizontalAlignment(Element.ALIGN_CENTER);
		nestedCell.setVerticalAlignment(Element.ALIGN_MIDDLE);
		//nestedCell.setBackgroundColor(new BaseColor(205, 226, 236));
		headerTable.addCell(nestedCell);
		
		Image sunoidaLogoUrl = Image.getInstance(assetFolderUrl+"/Bank_Logo.png");
		PdfPCell cell2 = new PdfPCell();
		cell2.setImage(sunoidaLogoUrl);
		cell2.setHorizontalAlignment(Element.ALIGN_RIGHT);
		cell2.setVerticalAlignment(Element.ALIGN_MIDDLE);
		cell2.setBorder(0&0&0&0);
		cell2.setFixedHeight(30);
		if(prompts != null && prompts.size()>2){
			cell2.setPaddingTop(10);
		}		
		headerTable.addCell(cell2);
		
		cellMain.setFixedHeight(height);
		cellMain.setBorderWidth(1f);
		cellMain.setBorder(0&0&0&1);
		cellMain.addElement(headerTable);
		borderTable.addCell(cellMain);
		document.add(borderTable);
		return borderTable.calculateHeights(true);
	}
	public  ExceptionCode exportMultiReportPdf(ReportsVb reportsVb, HashMap<String,ExceptionCode> resultMap,String assetFolderUrl){
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
		VisionUsersVb visionUsersVb =SessionContextHolder.getContext();
		int currentUserId = visionUsersVb.getVisionId();
		
		HashMap<String,String> sumMap = new HashMap<String,String>();
		ReportsVb vObject = new ReportsVb();
		Double sumVal = 0.0;
		Document document =  null;
		PdfPTable table = null;
		PdfPTable tempTable = null;
		
		float pageWidthSize = 0f;
		float pageHeightSize = 0f;
		List<ColumnHeadersVb> colHeaderslst = new ArrayList<>();
		List<HashMap<String, String>> dataLst = new ArrayList<>();
		List<HashMap<String, String>> totalLst = new ArrayList<>();
	
		pageWidthSize = PageSize.A4.getHeight();
		pageHeightSize= PageSize.A4.getWidth();
		
		colheaderFontSize = (int)6;
		dataFontSize = (int)5;
		summaryDataFontSize = (int)5;
		//String assetFolderUrl = commonDao.findVisionVariableValue("MDM_IMAGE_PATH");
		Rectangle rectangle = new Rectangle(pageWidthSize, pageHeightSize);
		document = new Document(rectangle, 20, 20, 20, 20);
		float widthAfterMargin = pageWidthSize - 40;
		String filePath = System.getProperty("java.io.tmpdir");

		File lFile = null;
		try {
			InvPdfPageEventHelper invPdfEvt = new InvPdfPageEventHelper(null, reportsVb.getMakerName(), assetFolderUrl,new ArrayList(), reportsVb);
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			lFile = new File(
					filePath + ValidationUtil.encode(reportsVb.getReportTitle()) + "_" + currentUserId + ".pdf");
			if (lFile.exists()) {
				lFile.delete();
			}
			lFile.createNewFile();
			FileOutputStream fips =  new FileOutputStream(lFile);
			PdfWriter writer = PdfWriter.getInstance(document,fips);
			writer.setPageEvent(invPdfEvt);
			BaseFont baseFont = BaseFont.createFont(assetFolderUrl+"/CALIBRI.TTF", BaseFont.WINANSI, BaseFont.EMBEDDED);
			document.open();
			
			int maxDataSize =  resultMap.size();
			int itCounter = 0;
			Set entrySet = resultMap.entrySet();
			Iterator it = entrySet.iterator();
			Boolean subTotalFlag = false;
			while (it.hasNext()) {
				//Map.Entry<String, ExceptionCode> entry = resultMap.entrySet();
				//ExceptionCode resultDataException = (ExceptionCode) entry.getValue();
				Map.Entry dataVal = (Map.Entry) it.next();
				ExceptionCode resultDataException  = (ExceptionCode) dataVal.getValue();
				vObject = (ReportsVb) resultDataException.getResponse();
				String reportTitle = (String)resultDataException.getRequest();
				String reportErrorMsg = resultDataException.getErrorMsg();
				//vObject.setScreenName(reportTitle);
				if(vObject != null)	{
					dataLst = vObject.getGridDataSet();
					totalLst = vObject.getTotal();
					colHeaderslst = ((ReportsVb) vObject).getColumnHeaderslst();
				} else {
					dataLst = new ArrayList<>();
					totalLst = new ArrayList<>();
					colHeaderslst = new ArrayList<>();
				}
				itCounter++;
				
				//dataLst = grpDataLst;
				subTotalFlag = false;
				sumMap = new HashMap<String, String>();
				ArrayList<ColumnHeadersVb> columnHeadersFinallst = new ArrayList<ColumnHeadersVb>();
				colHeaderslst.forEach(colHeadersVb -> {
					if(colHeadersVb.getColspan() <= 1) {
						columnHeadersFinallst.add(colHeadersVb);
					}
				});
				List<String> colTypes = new ArrayList<String>();
				Map<Integer,Integer> columnWidths = new HashMap<Integer,Integer>(colHeaderslst.size());
				for(int loopCnt= 0; loopCnt < colHeaderslst.size(); loopCnt++){
					columnWidths.put(Integer.valueOf(loopCnt), Integer.valueOf(-1));
					ColumnHeadersVb colHVb = colHeaderslst.get(loopCnt);
					if(colHVb.getColspan() <= 1) {
						colTypes.add(colHVb.getColType());
					}
				}
				PdfPTable groupingTable = new PdfPTable(1);
				groupingTable.setWidthPercentage(100);
				PdfPCell cellG;
				Font groupingFont = new Font(baseFont);
				groupingFont.setSize(8);
				groupingFont.setStyle(Font.BOLD);
				Phrase ph = new Phrase(reportTitle, groupingFont);
				Paragraph p = new Paragraph(ph);
				cellG = new PdfPCell(p);
				cellG.setHorizontalAlignment(Element.ALIGN_LEFT);
				cellG.setBorder(0);
				cellG.setPaddingTop(4f);
				// cell.setBackgroundColor(new BaseColor(205, 226, 236));
				groupingTable.addCell(cellG);
				document.add(groupingTable);

				int colNum = 0;
				int rowNum = 1;
				int loopCnt = 0;
				int subColumnCnt = 0;
				Boolean totalRowFlag = false;
				boolean isgroupColumns = false;
				Map<Integer, Float> colSizes = new HashMap<Integer, Float>();
				int maxHeaderRow = colHeaderslst.stream().mapToInt(ColumnHeadersVb::getLabelRowNum).max().orElse(0);
				int maxHeaderCol = colHeaderslst.stream().mapToInt(ColumnHeadersVb::getLabelColNum).max().orElse(0);

				List<ColumnHeadersVb> colHeadersCol = colHeaderslst.stream()
						.sorted(Comparator.comparingInt(ColumnHeadersVb::getLabelRowNum)).collect(Collectors.toList());

				if (colHeaderslst != null) {
					table = new PdfPTable(maxHeaderCol);
					table.setWidthPercentage(100);
					table.setHeaderRows(maxHeaderRow);
					table.setSpacingAfter(12);
					table.setSpacingBefore(12);
					Font font = new Font(baseFont);
					font.setColor(BaseColor.WHITE);
					font.setStyle(Font.BOLD);
					font.setSize(colheaderFontSize);
					if (maxHeaderRow != 1) {
						table.setSplitRows(false);
						table.setSplitLate(false);
					}
					table.getDefaultCell().setBackgroundColor(new BaseColor(79, 98, 40));
					table.getDefaultCell().setNoWrap(true);
					loopCnt = 0;
					boolean isNextRow = false;
					for (ColumnHeadersVb columnHeadersVb : colHeadersCol) {
						if (columnHeadersVb.getCaption().contains("<br/>")) {
							String value = columnHeadersVb.getCaption().replaceAll("<br/>", "\n");
							columnHeadersVb.setCaption(value);
						}
						PdfPCell cell = new PdfPCell(new Phrase(columnHeadersVb.getCaption(), font));
						cell.setBackgroundColor(new BaseColor(0, 92, 140)); //std blue color
						//cell.setBackgroundColor(new BaseColor(105,105,105)); //Dark Grey
						cell.setUseVariableBorders(true);
						cell.setBorderWidthTop(0.5f);
						cell.setBorderWidthBottom(0.5f);
						cell.setBorderColor(new BaseColor(222, 226, 230));
						cell.setNoWrap(false);
						cell.setPaddingBottom(4f);
						cell.setPaddingTop(4f);
						cell.setPaddingLeft(2f);
						cell.setPaddingRight(2f);

						String type = "";
						String colType = columnHeadersVb.getColType();
						if ("T".equalsIgnoreCase(colType)) {
							type = CAP_COL;
						} else {
							type = DATA_COL;
						}
						cell.setHorizontalAlignment(Element.ALIGN_CENTER);

						if (columnHeadersVb.getRowspan() != 0) {
							cell.setRowspan(columnHeadersVb.getRowspan());
							cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
						}
						if (columnHeadersVb.getColspan() != 0)
							cell.setColspan(columnHeadersVb.getColspan());
						cell.setNoWrap(true);
						if (columnHeadersVb.getLabelRowNum() > 1 && !isNextRow) {
							table.completeRow();
							table.getDefaultCell().setBackgroundColor(null);
							isNextRow = true;
						}
						table.addCell(cell);
						++colNum;
						if (columnHeadersVb.getColspan() == 0 || columnHeadersVb.getColspan() == 1) {
							float columnWidth = Float.parseFloat(columnHeadersVb.getColumnWidth());
							float pdfColWdth = widthAfterMargin * columnWidth / 100;
							colSizes.put(columnHeadersVb.getLabelColNum() - 1, pdfColWdth);
							loopCnt++;
						}
					}
				}
				// Writing The Header
				table.completeRow();
				table.getDefaultCell().setBackgroundColor(null);
				PdfPCell cell = null;
				int recCount = 0;
				int ctr = 1;

				// Writing the data
				do {
					if(dataLst != null) {
						for (HashMap dataLstMap : dataLst) {
							boolean highlight = false;
							String formatType = "D";
							if(dataLstMap.containsKey("FORMAT_TYPE")) {
								formatType = dataLstMap.get("FORMAT_TYPE").toString();
							}
							for (int loopCount = 0; loopCount < columnHeadersFinallst.size(); loopCount++) {
								ColumnHeadersVb colHeadersVb = columnHeadersFinallst.get(loopCount);
								if (colHeadersVb.getColspan() > 1)
									continue;
								int index = 0;
								String type = "";
								String colType = colTypes.get(loopCount);
								if ("T".equalsIgnoreCase(colType)) {
									type = CAP_COL;
								} else {
									type = DATA_COL;
								}
								String cellText = "";
								if (type.equalsIgnoreCase(CAP_COL)) {
									index = (loopCount + 1);
									String orgValue = "";
									if (dataLstMap.containsKey(colHeadersVb.getDbColumnName())) {
										orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null
												|| dataLstMap.get(colHeadersVb.getDbColumnName()) == "")
														? dataLstMap.get(colHeadersVb.getDbColumnName()).toString()
														: "";
									}
									cellText = orgValue;
									// cellText = findValue(reportsVb, DATA_COL, index);
									if (totalRowFlag || subTotalFlag) {
										if (!ValidationUtil.isValid(orgValue) && loopCount == 0) {
											orgValue = "Total ";
										}
										cellText = orgValue;
										if (loopCount == 0) {
											if (subTotalFlag)
												cellText = "Sub Total";
											if (totalRowFlag)
												cellText = "Total";
										} else {
											cellText = "";
										}
									}
									Font font = new Font(baseFont);
									font.setSize(dataFontSize);
									if (ValidationUtil.isValid(cellText)) {
										if ("FT".equalsIgnoreCase(formatType) || "S".equalsIgnoreCase(formatType)
												|| "ST".equalsIgnoreCase(formatType)) {
											font.setStyle(Font.BOLD);
											cell.setBackgroundColor(new BaseColor(205, 226, 236));
											if ("S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType)) {
												font.setSize(summaryDataFontSize);
											}
										}
									}
									cell = new PdfPCell(new Phrase(cellText, font));
									cell.setBorder(0 & 0 & 0 & 1);
									cell.setUseVariableBorders(true);
									cell.setBorderWidthBottom(0.5f);
									cell.setBorderColorBottom(new BaseColor(222, 226, 230));
									cell.setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
									cell.setPaddingBottom(4f);
									cell.setPaddingTop(4f);
									cell.setPaddingLeft(2f);
									cell.setPaddingRight(2f);
									if("S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType)) {
										cell.setBackgroundColor(new BaseColor(205, 226, 236));
									}
									if (recCount == dataLst.size() - 1) {
										cell.setBorderWidthBottom(0.5f);
										cell.setBorderColorBottom(new BaseColor(222, 226, 230));
									}
									if (loopCount == 0) {
										cell.setBorderWidthLeft(0.5f);
										cell.setBorderColorLeft(new BaseColor(222, 226, 230));
										cell.setBorderWidthRight(0.1f);
										cell.setBorderColorRight(new BaseColor(222, 226, 230));
									} else if (loopCount == maxHeaderCol - 1) {
										cell.setBorderWidthRight(0.1f);
										cell.setBorderColorRight(new BaseColor(222, 226, 230));
									} else {
										cell.setBorderWidthRight(0.1f);
										cell.setBorderColorRight(new BaseColor(222, 226, 230));
									}
									cell.setNoWrap(true);
									table.addCell(cell);
								} else {
									String orgValue = "";
									if (dataLstMap.containsKey(colHeadersVb.getDbColumnName())) {
										orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null
												|| dataLstMap.get(colHeadersVb.getDbColumnName()) == "")
														? dataLstMap.get(colHeadersVb.getDbColumnName()).toString()
														: "";
									}
									if (ValidationUtil.isValid(orgValue)) {
										if ("I".equalsIgnoreCase(colType) || "S".equalsIgnoreCase(colType)) {
											if (ValidationUtil.isNumericDecimal(orgValue)) {
												double amount = Double.parseDouble(orgValue);
												DecimalFormat formatter = new DecimalFormat("#,###");
												cellText = formatter.format(amount);
											} else {
												cellText = orgValue;
											}
										} else {
											if (ValidationUtil.isNumericDecimal(orgValue)) {
												double amount = Double.parseDouble(orgValue);
												DecimalFormat formatter = new DecimalFormat("#,##0.00");
												cellText = formatter.format(amount);
											} else {
												cellText = orgValue;
											}
										}
									} else {
										cellText = orgValue;
									}
									if (ValidationUtil.isValidId(cellText) && "-0.00".equalsIgnoreCase(cellText)) {
										cellText = "0.00";
									}
									
									Font font = new Font(baseFont);
									font.setSize(dataFontSize);
									cell = new PdfPCell(new Phrase(cellText, font));
									if("FT".equalsIgnoreCase(formatType) || "S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType)) {
										font.setStyle(Font.BOLD);
										if("S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType)) {
											font.setSize(summaryDataFontSize);
											cell.setBackgroundColor(new BaseColor(205, 226, 236));
										}
									}
									
									cell.setBorder(0 & 0 & 0 & 1);
									cell.setUseVariableBorders(true);
									cell.setBorderWidthBottom(0.5f);
									cell.setBorderColorBottom(new BaseColor(222, 226, 230));
									cell.setBorderColor(new BaseColor(222, 226, 230));
									cell.setHorizontalAlignment(PdfPCell.ALIGN_RIGHT);
									cell.setVerticalAlignment(PdfPCell.ALIGN_CENTER);
									cell.setPaddingBottom(4f);
									cell.setPaddingTop(4f);
									cell.setPaddingLeft(4f);
									cell.setPaddingRight(4f);
									if("S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType)) {
										cell.setBackgroundColor(new BaseColor(205, 226, 236));
									}
									if (recCount == dataLst.size() - 1) {
										cell.setBorderWidthBottom(0.5f);
										cell.setBorderColorBottom(new BaseColor(222, 226, 230));
									}
									if (loopCount == 0) {
										cell.setBorderWidthLeft(0.5f);
										cell.setBorderColorLeft(new BaseColor(222, 226, 230));
										cell.setBorderWidthRight(0.1f);
										cell.setBorderColorRight(new BaseColor(222, 226, 230));
									} else if (loopCount == maxHeaderCol - 1) {
										cell.setBorderWidthRight(0.1f);
										cell.setBorderColorRight(new BaseColor(222, 226, 230));
									} else {
										cell.setBorderWidthRight(0.1f);
										cell.setBorderColorRight(new BaseColor(222, 226, 230));
									}
									cell.setNoWrap(true);
									table.addCell(cell);
	
									String cellValue = orgValue.replaceAll(",", "");
									
									if ("I".equalsIgnoreCase(colType) || "S".equalsIgnoreCase(colType)) {
										if (ValidationUtil.isNumericDecimal(orgValue)) {
											double amount = Double.parseDouble(orgValue);
											DecimalFormat formatter = new DecimalFormat("#,###");
											cellText = formatter.format(amount);
										} else {
											cellText = orgValue;
										}
									} else {
										if (ValidationUtil.isNumericDecimal(orgValue)) {
											double amount = Double.parseDouble(orgValue);
											DecimalFormat formatter = new DecimalFormat("#,##0.00");
											cellText = formatter.format(amount);
										} else {
											cellText = orgValue;
										}
									}
									
									
									if("Y".equalsIgnoreCase(colHeadersVb.getSumFlag())) {
										String prevValstr = sumMap.containsKey(colHeadersVb.getDbColumnName())
												? sumMap.get(colHeadersVb.getDbColumnName())
												: "0";
										Double strVal = Double.parseDouble(prevValstr);
										sumVal = strVal + Double.parseDouble(cellValue);
										DecimalFormat formatter = new DecimalFormat("#,##0.00");
										if ("I".equalsIgnoreCase(colType) || "S".equalsIgnoreCase(colType)) {
											formatter = new DecimalFormat("#,###");
										}
										String cellText1 = formatter.format(sumVal);
										sumMap.put(colHeadersVb.getDbColumnName(), cellText1);
									}
								}
								if (cellText == null || cellText.trim().isEmpty()) {
									// colSizes.put(loopCount, Math.max(20, colSizes.get(loopCount)));
								} else {
	
									/*
									 * float width = Math.max(ColumnText.getWidth(cell.getPhrase()),
									 * colSizes.get(loopCount)); if (width < 20) width = 20; colSizes.put(loopCount,
									 * width);
									 */
								}
							}
							recCount++;
						}
					}else {
						PdfPTable noDataTab = new PdfPTable(1);
						noDataTab.setWidthPercentage(100);
						PdfPCell cellN;
						Font noDataFont = new Font(baseFont);
						noDataFont.setSize(7);
						noDataFont.setColor(new BaseColor(115,136,120));
						//groupingFont.setStyle(Font.BOLD);
						Phrase ph1 = new Phrase(reportErrorMsg, noDataFont);
						Paragraph p1 = new Paragraph(ph1);
						cellN = new PdfPCell(p1);
						cellN.setHorizontalAlignment(Element.ALIGN_CENTER);
						cellN.setBorder(0);
						cellN.setPaddingTop(4f);
						noDataTab.addCell(cellN);
						document.add(table);
						document.add(noDataTab);
					}
					ctr++;
					reportsVb.setCurrentPage(ctr);
					dataLst = new ArrayList();
					if (itCounter == maxDataSize) {
						if (!subTotalFlag) {
							if(!sumMap.isEmpty()) {
								sumMap.put("FORMAT_TYPE", "ST");
								dataLst.add(sumMap);
								subTotalFlag = true;
							}
							totalRowFlag = false;
						} else if (!totalRowFlag) {
							dataLst = totalLst;
							totalRowFlag = true;
							subTotalFlag = true;
						}
					} else {
						if (!subTotalFlag) {
							if(!sumMap.isEmpty()) {
								sumMap.put("FORMAT_TYPE", "ST");
								dataLst.add(sumMap);
								subTotalFlag = true;
							}
							totalRowFlag = false;
						}
					}
				} while (dataLst != null && !dataLst.isEmpty());

				float totalWidth = 0f;
				float sumOfLabelColWidth = 0f;
				float[] colsArry = new float[colSizes.size()];
				for (int i = 0; i < colSizes.size(); i++) {
					totalWidth += colSizes.get(i);
					colsArry[i] = colSizes.get(i);
					if (i < maxHeaderCol) {
						sumOfLabelColWidth += colSizes.get(i);
					}
				}
				float pageWidth = "L".equalsIgnoreCase(reportsVb.getReportOrientation()) ? PageSize.A4.getHeight()
						: PageSize.A4.getWidth();
				float pageHeight = ("L".equalsIgnoreCase(reportsVb.getReportOrientation()) ? PageSize.A4.getWidth()
						: PageSize.A4.getHeight())
						- (document.topMargin() + document.bottomMargin() + invPdfEvt.headerHeight + 50);
				// float pageWidth = reportsVb.getPdfWidth();
				// pageWidth = (pageWidth - (document.leftMargin() + document.rightMargin()));
				/* if (totalWidth < pageWidth) { */
				float percentWidth = (float) ((totalWidth * 100) / (pageWidth - 200));
				// if (percentWidth > 99)
				percentWidth = 99;
				int rowStart = 0;
				int rowEnd = table.getRows().size();
				table.setTotalWidth(totalWidth);
				table.setWidths(colsArry);
				float totalHeight = table.calculateHeights(true);
				// float pageHeight = reportsVb.getPdfHeight() - (document.topMargin() +
				// document.bottomMargin()+ invPdfEvt.headerHeight + table.spacingAfter());
				int pages = 1;
				if (totalHeight > pageHeight) {
					pages = Math.round(totalHeight / pageHeight) < (totalHeight / pageHeight)
							? Math.round(totalHeight / pageHeight) + 1
							: Math.round(totalHeight / pageHeight);
					totalHeight += pages * table.getHeaderHeight();
					pages = Math.round(totalHeight / pageHeight) < (totalHeight / pageHeight)
							? Math.round(totalHeight / pageHeight) + 1
							: Math.round(totalHeight / pageHeight);
				}

				int totalPages = pages;
				rowEnd = getRowsPerPage(table, rowStart, pageHeight);
				while (pages > 0) {
					List<PdfPRow> pdfpRows = table.getRows(rowStart, rowEnd);
					tempTable = createTable(table, pdfpRows, maxHeaderCol, table.getNumberOfColumns(), pages,
							totalPages, maxHeaderCol);
					tempTable.setTotalWidth(totalWidth);
					tempTable.setWidthPercentage(percentWidth);
					tempTable.setWidths(colsArry);
					// tempTable.setLockedWidth(true);
					document.add(tempTable);
					rowStart = rowEnd;
					rowEnd = getRowsPerPage(table, rowStart, pageHeight);
					pages--;
					if (pages - 1 != 0)
						rowEnd = rowEnd - table.getHeaderRows();
					if (pages > 0)
						document.newPage();
				}
				if (itCounter < maxDataSize)
					document.newPage();
			}
			document.close();
			writer.close();
			exceptionCode.setResponse(filePath);
			exceptionCode.setOtherInfo(reportsVb.getReportTitle() + "_" + currentUserId);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (RuntimeCustomException e) {
			e.printStackTrace();
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			throw new RuntimeCustomException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			return exceptionCode;
		}
		}catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	public PdfPTable  createPromptsPage(Document document,ReportsVb reportsVb) throws MalformedURLException, IOException, DocumentException {
		String promptLabel[] = null;
		//reportsVb.setPromptLabel("Vision SBU : 'ALL'!@#Account Officer : 'ALL'!@#Confidence Level : 'ALL'!@#From Date : '1-Jan-2016'!@#To Date : '1-Jan-2021'!@#scalingFactor : 'No Scaling");
		if(ValidationUtil.isValid(reportsVb.getPromptLabel())) {
			promptLabel = reportsVb.getPromptLabel().split("!@#");
		}
		PdfPTable tableT = new PdfPTable(2);
		tableT.setSpacingBefore(20f);
		Font fontLeft = new Font();
		fontLeft.setSize(8);
		fontLeft.setColor(BaseColor.WHITE);
		fontLeft.setStyle(Font.BOLD);
		Font fontRight = new Font();
		fontRight.setSize(8);
		int[] sunoidaPinkClr =  getRGB(reportsVb.getApplicationTheme());
		fontRight.setColor(new BaseColor(sunoidaPinkClr[0],sunoidaPinkClr[1],sunoidaPinkClr[2]));
		//fontRight.setColor(new BaseColor(177, 24, 124));
		fontRight.setStyle(Font.BOLD);
		PdfPCell cell = null;
		Paragraph pL = null;
		pL = new Paragraph(new Phrase("Report Title",fontLeft));
		cell = new PdfPCell(pL);
		cell.setHorizontalAlignment(Element.ALIGN_LEFT);
		cell.setPaddingBottom(6f);
		cell.setPaddingTop(6f);
		cell.setBackgroundColor(new BaseColor(sunoidaPinkClr[0],sunoidaPinkClr[1],sunoidaPinkClr[2]));
		//cell.setBackgroundColor(new BaseColor(177, 24, 124));
		cell.setUseVariableBorders(true);
		cell.disableBorderSide(2);
		cell.setColspan(0);
		tableT.addCell(cell);
		pL = new Paragraph(new Phrase(reportsVb.getReportTitle(),fontLeft));
		cell = new PdfPCell(pL);
		cell.setHorizontalAlignment(Element.ALIGN_LEFT);
		cell.setPaddingBottom(6f);
		cell.setPaddingTop(6f);
		cell.setBackgroundColor(new BaseColor(sunoidaPinkClr[0],sunoidaPinkClr[1],sunoidaPinkClr[2]));
		//cell.setBackgroundColor(new BaseColor(177, 24, 124));
		cell.setUseVariableBorders(true);
		cell.disableBorderSide(2);
		cell.disableBorderSide(4);
		cell.setColspan(0);
		tableT.addCell(cell);
		/*insertCell(tableT, "Report Title", Element.ALIGN_LEFT, 0,fontLeft,"L");
		insertCell(tableT, reportsVb.getReportTitle(), Element.ALIGN_LEFT, 0,fontLeft,"L");*/
		
		if(promptLabel != null && promptLabel.length>0) {
			for(int i=0; i<promptLabel.length; i++) {
				String[] promptArr = null;
				String[] val = null;
				promptArr = promptLabel[i].split(" : ");
				val = promptArr[1].split(",");
				if(val.length>2) {
					promptArr[1] = val[0]+","+val[1]+" (+) "+(val.length-2);
				}else if(val.length == 2){
					promptArr[1] = val[0]+","+val[1];
				}
				pL = new Paragraph(new Phrase(promptArr[0],fontLeft));
				cell = new PdfPCell(pL);
				cell.setHorizontalAlignment(Element.ALIGN_LEFT);
				cell.setPaddingBottom(6f);
				cell.setPaddingTop(6f);
				cell.setBackgroundColor(new BaseColor(sunoidaPinkClr[0],sunoidaPinkClr[1],sunoidaPinkClr[2]));
				//cell.setBackgroundColor(new BaseColor(177, 24, 124));
				cell.setUseVariableBorders(true);
				cell.disableBorderSide(2);
				cell.setColspan(0);
				tableT.addCell(cell);
				pL = new Paragraph(new Phrase(promptArr[1],fontRight));
				cell = new PdfPCell(pL);
				cell.setHorizontalAlignment(Element.ALIGN_LEFT);
				cell.setPaddingBottom(6f);
				cell.setPaddingTop(6f);
				cell.setBackgroundColor(new BaseColor(242, 244, 242));
				cell.setUseVariableBorders(true);
				cell.disableBorderSide(2);
				cell.disableBorderSide(4);
				cell.setColspan(0);
				tableT.addCell(cell);
				//insertCell(tableT, promptArr[0], Element.ALIGN_LEFT,0, fontLeft,"L");
				//insertCell(tableT, promptArr[1], Element.ALIGN_LEFT,0, fontRight,"R");
			}
		}
		pL = new Paragraph(new Phrase("Generated on",fontLeft));
		cell = new PdfPCell(pL);
		cell.setHorizontalAlignment(Element.ALIGN_LEFT);
		cell.setPaddingBottom(6f);
		cell.setPaddingTop(6f);
		cell.setBackgroundColor(new BaseColor(sunoidaPinkClr[0],sunoidaPinkClr[1],sunoidaPinkClr[2]));
		//cell.setBackgroundColor(new BaseColor(177, 24, 124));
		cell.setUseVariableBorders(true);
		cell.disableBorderSide(2);
		cell.setColspan(0);
		tableT.addCell(cell);
		SimpleDateFormat dateFormat = new SimpleDateFormat("MMM dd, yyyy - hh:mm:ss a");
		pL = new Paragraph(new Phrase(dateFormat.format(new Date()),fontRight));
		cell = new PdfPCell(pL);
		cell.setHorizontalAlignment(Element.ALIGN_LEFT);
		cell.setPaddingBottom(6f);
		cell.setPaddingTop(6f);
		cell.setBackgroundColor(new BaseColor(242, 244, 242));
		cell.setUseVariableBorders(true);
		cell.disableBorderSide(2);
		cell.disableBorderSide(4);
		cell.setColspan(0);
		tableT.addCell(cell);
		pL = new Paragraph(new Phrase("Generated By",fontLeft));
		cell = new PdfPCell(pL);
		cell.setHorizontalAlignment(Element.ALIGN_LEFT);
		cell.setPaddingBottom(6f);
		cell.setPaddingTop(6f);
		cell.setBackgroundColor(new BaseColor(sunoidaPinkClr[0],sunoidaPinkClr[1],sunoidaPinkClr[2]));
		//cell.setBackgroundColor(new BaseColor(177, 24, 124));
		cell.setUseVariableBorders(true);
		//cell.disableBorderSide(4);
		cell.setColspan(0);
		//cell.setBorderColorBottom(BaseColor.BLACK);
		tableT.addCell(cell);
		pL = new Paragraph(new Phrase(reportsVb.getMakerName(),fontRight));
		cell = new PdfPCell(pL);
		cell.setHorizontalAlignment(Element.ALIGN_LEFT);
		cell.setPaddingBottom(6f);
		cell.setPaddingTop(6f);
		cell.setBackgroundColor(new BaseColor(242, 244, 242));
		cell.setUseVariableBorders(true);
		cell.disableBorderSide(4);
		//cell.setBorderColorBottom(BaseColor.BLACK);
		cell.setColspan(0);
		tableT.addCell(cell);
		//insertCell(tableT, "Generated on", Element.ALIGN_LEFT,0, fontLeft,"L");
		//SimpleDateFormat dateFormat = new SimpleDateFormat("MMM dd, yyyy - hh:mm:ss a");
		//insertCell(tableT, dateFormat.format(new Date()), Element.ALIGN_LEFT,0,fontRight,"R");
		//insertCell(tableT, "Generated By", Element.ALIGN_LEFT,0, fontLeft,"L");
		//insertCell(tableT, reportsVb.getMakerName(), Element.ALIGN_LEFT,0, fontRight,"R");
		//insertCell(tableT, "\n", Element.ALIGN_LEFT,0, fontScF);
		//insertCell(tableT, "Powered By Sunoida", Element.ALIGN_CENTER,2, fontRight,"R");
	    return tableT;
		
	}
	private void insertCell(PdfPTable table, String text, int align,int colSpan, Font font,String side){
		   
		  PdfPCell cell = null;
		  Paragraph pL = new Paragraph(new Phrase(text,font));
		  cell = new PdfPCell(pL);
		  cell.setHorizontalAlignment(align);
		  cell.setPaddingBottom(6f);
		  cell.setPaddingTop(6f);
		  if (side == "L") {
			cell.setBackgroundColor(new BaseColor(177, 24, 124));
			cell.setUseVariableBorders(true);
//			cell.disableBorderSide(2);
			//cell.setBorderColorBottom(BaseColor.WHITE);
		  } else {
			cell.setBackgroundColor(new BaseColor(242, 244, 242));
		  }
		  cell.disableBorderSide(2);
		  cell.setColspan(colSpan);
		  if(text.trim().equalsIgnoreCase("")){
		   cell.setMinimumHeight(10f);
		  }
		  table.addCell(cell);
		   
	}
	public ExceptionCode dashboardExportToPdf(String screenCapturedImage, ReportsVb dObj, String assetFolderUrl,String fileName,ArrayList<ReportsVb> reportslst) {
		ExceptionCode exceptionCode = new ExceptionCode();
		Document document = null;
		PdfWriter writer = null;
		Rectangle rectangle = null;
		PdfPTable table = null;
		float pageWidthSize = 0f;
		float pageHeightSize = 0f;
		pageWidthSize = PageSize.A4.getHeight();
		pageHeightSize= PageSize.A4.getWidth();
		rectangle = new Rectangle(pageWidthSize, pageHeightSize);
		document = new Document(rectangle, 20, 20, 20, 20);
		PdfPTable tempTable = null;
		int colheaderFontSize = 9;
		int dataFontSize = 9;
		int summaryDataFontSize = 9;
		
		float widthAfterMargin = pageWidthSize - 40;
		try {
			String filePath = System.getProperty("java.io.tmpdir");
			if (!ValidationUtil.isValid(filePath)) {
				filePath = System.getenv("TMP");
			}
			if (ValidationUtil.isValid(filePath)) {
				filePath = filePath + File.separator;
			}
			File lFile = null;
			lFile = new File(filePath+fileName+".pdf");
			if (lFile.exists()) {
				lFile.delete();
			}
			BaseFont baseFont = BaseFont.createFont(assetFolderUrl + "/CALIBRI.TTF", BaseFont.WINANSI,
					BaseFont.EMBEDDED);
			lFile.createNewFile();
			FileOutputStream fips = new FileOutputStream(lFile);
			writer = PdfWriter.getInstance(document, fips);
			InvPdfPageEventHelper invPdfEvt = new InvPdfPageEventHelper(null, dObj.getMakerName(), assetFolderUrl,
					new ArrayList(), dObj);
			writer.setPageEvent(invPdfEvt);
			document.open();
			PdfPTable tableT = null;
			tableT = createPromptsPage(document, dObj);
			document.add(tableT);
			
			//Add captured Image to PDF
			document.newPage();
			table = new PdfPTable(1);
			PdfPCell cellImage = new PdfPCell();
			Image image = Image.getInstance(screenCapturedImage);
			image.setScaleToFitLineWhenOverflow(true);
			table.setWidthPercentage(100);
			float imgWidth = document.getPageSize().getWidth()-document.leftMargin()-document.rightMargin();
			float imgHeight = document.getPageSize().getHeight()-document.topMargin()-document.bottomMargin();
			
			image.scaleToFit(imgWidth, imgHeight);
			image.scaleAbsolute(imgWidth, imgHeight);
			/*image.setAlignment(Image.MIDDLE);*/
			/*cellImage.setFixedHeight(image.getHeight()-50);*/
			cellImage.addElement(image);
			cellImage.setBorderWidthLeft(0.1f);
			cellImage.setBorderColorLeft(new BaseColor(222,226,230));
			cellImage.setBorderWidthRight(0.1f);
			cellImage.setBorderColorRight(new BaseColor(222,226,230));
			cellImage.setBorderWidthTop(0.1f);
			cellImage.setBorderColorTop(new BaseColor(222,226,230));
			cellImage.setBorderWidthBottom(0.1f);
			cellImage.setBorderColorBottom(new BaseColor(222,226,230));
			System.out.println("Height after "+cellImage.getHeight());
			table.addCell(cellImage);
			document.add(table);
			//page++;
			List<ColumnHeadersVb> colHeaderslst = new ArrayList<>();
			List<HashMap<String, String>> dataLst = null;
			Boolean totalRowFlag = false;
			Map<Integer, Float> colSizes = new HashMap<Integer, Float>();
			if (reportslst != null && reportslst.size() > 0) {
				for(int ctr=0;ctr<reportslst.size();ctr++) {
					ReportsVb reportsVb = (ReportsVb)reportslst.get(ctr);
					colHeaderslst = reportsVb.getColumnHeaderslst();
					/*for(ColumnHeadersVb colHeader: colHeaderslst) {
						if("P".equalsIgnoreCase(colHeader.getColType()))
							colHeader.setNumericColumnNo(99);
					}*/
					dataLst = reportsVb.getGridDataSet();
					document.newPage();
					PdfPTable titleTable = new PdfPTable(1);
					titleTable.setWidthPercentage(100);
					PdfPCell cellG;
					Font groupingFont = new Font(baseFont);
					groupingFont.setSize(7);
					groupingFont.setStyle(Font.BOLD);
					Phrase ph = new Phrase(reportsVb.getReportTitle(), groupingFont);
					Paragraph p = new Paragraph(ph);
					cellG = new PdfPCell(p);
					cellG.setHorizontalAlignment(Element.ALIGN_LEFT);
					cellG.setBorder(0);
					cellG.setPaddingTop(4f);
					// cell.setBackgroundColor(new BaseColor(205, 226, 236));
					titleTable.addCell(cellG);
					document.add(titleTable);
					
					int colNum = 0;
					int rowNum = 1;
					int loopCnt = 0;
					List<String> colTypes = new ArrayList<String>();
					Map<Integer,Integer> columnWidths = new HashMap<Integer,Integer>(colHeaderslst.size());
					for(int loopCount= 0; loopCount < colHeaderslst.size(); loopCount++){
						columnWidths.put(Integer.valueOf(loopCount), Integer.valueOf(-1));
						ColumnHeadersVb colHVb = colHeaderslst.get(loopCount);
						if(colHVb.getColspan() <= 1 && colHVb.getNumericColumnNo() != 99) {
							colTypes.add(colHVb.getColType());
						}
					}
					colSizes = new HashMap<Integer, Float>();
					int maxHeaderRow = colHeaderslst.stream().mapToInt(ColumnHeadersVb::getLabelRowNum).max().orElse(0);
					int maxHeaderCol = colHeaderslst.stream().mapToInt(ColumnHeadersVb::getLabelColNum).max().orElse(0);
					
					List<ColumnHeadersVb> colHeadersCol = colHeaderslst.stream()
							.sorted(Comparator.comparingInt(ColumnHeadersVb::getLabelRowNum)).collect(Collectors.toList());
					
					ArrayList<ColumnHeadersVb> columnHeadersFinallst = new ArrayList<ColumnHeadersVb>();
					colHeaderslst.forEach(colHeadersVb -> {
						if(colHeadersVb.getColspan() <= 1 && colHeadersVb.getNumericColumnNo() != 99) {
							columnHeadersFinallst.add(colHeadersVb);
						}
					});
					
					// The Header Table creation
					if (colHeaderslst != null) {
						table = new PdfPTable(maxHeaderCol);
						table.setWidthPercentage(100);
						table.setHeaderRows(maxHeaderRow);
						table.setSpacingAfter(12);
						table.setSpacingBefore(12);
						Font font = new Font(baseFont);
						font.setColor(BaseColor.WHITE);
						font.setStyle(Font.BOLD);
						font.setSize(colheaderFontSize);
						if (maxHeaderRow != 1) {
							table.setSplitRows(false);
							table.setSplitLate(false);
						}
						table.getDefaultCell().setBackgroundColor(new BaseColor(79, 98, 40)); // Default Color blue
						table.getDefaultCell().setNoWrap(true);
						loopCnt = 0;
						boolean isNextRow = false;
						for (ColumnHeadersVb columnHeadersVb : colHeadersCol) {
							if (columnHeadersVb.getCaption().contains("<br/>")) {
								String value = columnHeadersVb.getCaption().replaceAll("<br/>", "\n");
								columnHeadersVb.setCaption(value);
							}
							PdfPCell cell = new PdfPCell(new Phrase(columnHeadersVb.getCaption(), font));
							//cell.setBackgroundColor(new BaseColor(0, 92, 140)); // Default Color blue
							int[] sunoidaPinkClr =  getRGB(reportsVb.getApplicationTheme());
							cell.setBackgroundColor(new BaseColor(sunoidaPinkClr[0],sunoidaPinkClr[1],sunoidaPinkClr[2]));
							//cell.setBackgroundColor(new BaseColor(177, 24, 124)); // Sunoida Latest Pink
							cell.setUseVariableBorders(true);
							cell.setBorderWidthTop(0.5f);
							cell.setBorderWidthBottom(0.5f);
							cell.setBorderColor(new BaseColor(222,226,230));
							cell.setNoWrap(true);
							cell.setPaddingBottom(4f);
							cell.setPaddingTop(4f);
							cell.setPaddingLeft(2f);
							cell.setPaddingRight(2f);
		
							String type = "";
							String colType = columnHeadersVb.getColType();
							if ("T".equalsIgnoreCase(colType)) {
								type = CAP_COL;
							} else {
								type = DATA_COL;
							}
							cell.setHorizontalAlignment(Element.ALIGN_CENTER);
							
							if (columnHeadersVb.getRowspan() != 0) {
								cell.setRowspan(columnHeadersVb.getRowspan());
								cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
							}
							if (columnHeadersVb.getColspan() != 0)
								cell.setColspan(columnHeadersVb.getColspan());
							cell.setNoWrap(true);
							if (columnHeadersVb.getLabelRowNum() > 1 && !isNextRow) {
								table.completeRow();
								table.getDefaultCell().setBackgroundColor(null);
								isNextRow = true;
							}
							table.addCell(cell);
							++colNum;
							if (columnHeadersVb.getColspan() <= 1) {
								if(columnHeadersVb.getNumericColumnNo() != 99) {
									float columnWidth = Float.parseFloat(columnHeadersVb.getColumnWidth());
									float pdfColWdth = widthAfterMargin*columnWidth/100;
									colSizes.put(columnHeadersVb.getLabelColNum() - 1, pdfColWdth);
									loopCnt++;
								}
							}
						}
					}
					table.completeRow();
					table.getDefaultCell().setBackgroundColor(null);
					PdfPCell cell = null;
					int recCount = 0;
					//writing data
					for(HashMap dataLstMap : dataLst) {
						boolean highlight = false;
						String formatType = "D";
						if(dataLstMap.containsKey("FORMAT_TYPE")) {
							formatType = dataLstMap.get("FORMAT_TYPE").toString();
						}
						for (int loopCount = 0; loopCount < columnHeadersFinallst.size(); loopCount++) {
							ColumnHeadersVb colHeadersVb = columnHeadersFinallst.get(loopCount);
							if (colHeadersVb.getColspan() > 1 || colHeadersVb.getNumericColumnNo() == 99)
								continue;
							int index = 0;
							String type = "";
							String colType = colTypes.get(loopCount);
							if ("T".equalsIgnoreCase(colType)) {
								type = CAP_COL;
							} else if ("P".equalsIgnoreCase(colType)) {
								type = "GROWTH_IMG";
							} else  {
								type = DATA_COL;
							}
							String cellText = "";
							String orgValue = "";
							if (type.equalsIgnoreCase(CAP_COL)) {
								index = (loopCount + 1);
								if(dataLstMap.containsKey(colHeadersVb.getDbColumnName())) {
									orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null ||  dataLstMap.get(colHeadersVb.getDbColumnName()) == "") ? dataLstMap.get(colHeadersVb.getDbColumnName()).toString() : "";
								}
								
								cellText = orgValue;
								if (totalRowFlag) {
									ExceptionCode excepCode = writeTotalRowCaptionColDataToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,summaryDataFontSize,cell);
									if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
										table = (PdfPTable)excepCode.getResponse();
										cell = (PdfPCell)excepCode.getOtherInfo();	
									}else {
										exceptionCode.setErrorCode(excepCode.getErrorCode());
										exceptionCode.setErrorMsg(excepCode.getErrorMsg());
										return exceptionCode;
									}
								} else {
									ExceptionCode excepCode = writeCaptionColDataToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,formatType,dataFontSize,cell);
									if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
										table = (PdfPTable)excepCode.getResponse();
										cell = (PdfPCell)excepCode.getOtherInfo();	
									}else {
										exceptionCode.setErrorCode(excepCode.getErrorCode());
										exceptionCode.setErrorMsg(excepCode.getErrorMsg());
										return exceptionCode;
									}
								}
							} else if("GROWTH_IMG".equalsIgnoreCase(type)){
								cellText="ARROW";
								if(dataLstMap.containsKey(colHeadersVb.getDbColumnName())) {
									orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null ||  dataLstMap.get(colHeadersVb.getDbColumnName()) == "") ? dataLstMap.get(colHeadersVb.getDbColumnName()).toString() : "";
								}
								table = writeGrowthColDataToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,formatType,dataFontSize,assetFolderUrl);
							}else {
								// index = ((loopCount+1) - reportsWriterVb.getCaptionLabelColCount());
								if(dataLstMap.containsKey(colHeadersVb.getDbColumnName())) 
									orgValue = ((dataLstMap.get(colHeadersVb.getDbColumnName())) != null ||  dataLstMap.get(colHeadersVb.getDbColumnName()) == "") ? dataLstMap.get(colHeadersVb.getDbColumnName()).toString() : "";
								else 
									orgValue = "";
								//String orgValue = dataLstMap.containsKey(colHeadersVb.getDbColumnName())? dataLstMap.get(colHeadersVb.getDbColumnName()).toString(): "";
								if(ValidationUtil.isValid(orgValue)) {
									if ("I".equalsIgnoreCase(colType) || "S".equalsIgnoreCase(colType)) {
										if (ValidationUtil.isNumericDecimal(orgValue)) {
												double amount = Double.parseDouble(orgValue);
												DecimalFormat formatter = new DecimalFormat("#,###");
												cellText = formatter.format(amount);
											} else {
												cellText = orgValue;
											}
									} else {
										if (ValidationUtil.isNumericDecimal(orgValue)) {
											if (!"NR".equalsIgnoreCase(colType) && !"TR".equalsIgnoreCase(colType)) {
												double amount = Double.parseDouble(orgValue);
												DecimalFormat formatter = new DecimalFormat("#,##0.00");
												cellText = formatter.format(amount);
											} else {
												cellText = orgValue;
											}
										} else {
										cellText = orgValue;
										}
									}
								}else {
									cellText = orgValue;
								}
								if(ValidationUtil.isValid(cellText) && "-0.00".equalsIgnoreCase(cellText)) {
									cellText = "0.00";
								}
								if (totalRowFlag) {
									ExceptionCode excepCode = writeTotalRowDataColToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,summaryDataFontSize,cell);
									if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
										table = (PdfPTable)excepCode.getResponse();
										cell = (PdfPCell)excepCode.getOtherInfo();	
									}else {
										exceptionCode.setErrorCode(excepCode.getErrorCode());
										exceptionCode.setErrorMsg(excepCode.getErrorMsg());
										return exceptionCode;
									}
								} else {
									ExceptionCode excepCode = writeRowDataColToTable(reportsVb,loopCount,orgValue,baseFont,table,dataLst,recCount,maxHeaderCol,dataFontSize,formatType,cellText,colHeadersVb,cell);
									if(excepCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
										table = (PdfPTable)excepCode.getResponse();
										cell = (PdfPCell)excepCode.getOtherInfo();	
									}else {
										exceptionCode.setErrorCode(excepCode.getErrorCode());
										exceptionCode.setErrorMsg(excepCode.getErrorMsg());
										return exceptionCode;
									}
									
								}
							}
							if("ARROW".equalsIgnoreCase(cellText)) {
								float width = 10;
								colSizes.put(loopCount, width);
							}else if (ValidationUtil.isValid(cellText)) {
								float width = Math.max(ColumnText.getWidth(cell.getPhrase()), colSizes.get(loopCount));
								if (width < 20)
									width = 20;
								colSizes.put(loopCount, width);
							}
						}
						recCount++;
					
					}
					float totalWidth = 0f;
					float sumOfLabelColWidth = 0f;
					float[] colsArry = new float[colSizes.size()];
					for (int i = 0; i < colSizes.size(); i++) {
						totalWidth += colSizes.get(i);
						colsArry[i] = colSizes.get(i);
						if (i < maxHeaderCol) {
							sumOfLabelColWidth += colSizes.get(i);
						}
					}
					float pageWidth = "L".equalsIgnoreCase(reportsVb.getReportOrientation()) ?  PageSize.A4.getHeight(): PageSize.A4.getWidth();
				    float pageHeight =  ("L".equalsIgnoreCase(reportsVb.getReportOrientation()) ?  PageSize.A4.getWidth(): PageSize.A4.getHeight()) 
							- (document.topMargin() + document.bottomMargin() + invPdfEvt.headerHeight+50);
				    //float pageWidth = reportsVb.getPdfWidth();
					pageWidth = (pageWidth - (document.leftMargin() + document.rightMargin()));
					if (totalWidth < pageWidth) {
						float percentWidth = (float) ((totalWidth * 100) / (pageWidth - 200));
						//if (percentWidth > 99)
						percentWidth = 99;
						int rowStart = 0;
						int rowEnd = table.getRows().size();
						table.setTotalWidth(totalWidth);
						table.setWidths(colsArry);
						float totalHeight = table.calculateHeights(true);
						//float pageHeight = reportsVb.getPdfHeight() - (document.topMargin() + document.bottomMargin()+ invPdfEvt.headerHeight + table.spacingAfter());

						int pages = 1;
						if (totalHeight > pageHeight) {
							pages = Math.round(totalHeight / pageHeight) < (totalHeight / pageHeight)? Math.round(totalHeight / pageHeight) + 1
									: Math.round(totalHeight / pageHeight);
							totalHeight += pages * table.getHeaderHeight();
							pages = Math.round(totalHeight / pageHeight) < (totalHeight / pageHeight)? Math.round(totalHeight / pageHeight) + 1
									: Math.round(totalHeight / pageHeight);
						}
						
						int totalPages = pages;
						rowEnd = getRowsPerPage(table, rowStart, pageHeight);
						while (pages > 0) {
							List<PdfPRow> pdfpRows = table.getRows(rowStart, rowEnd);
							tempTable = createTable(table, pdfpRows, maxHeaderCol, table.getNumberOfColumns(), pages,totalPages, maxHeaderCol);
							tempTable.setTotalWidth(totalWidth);
							tempTable.setWidthPercentage(percentWidth);
							tempTable.setWidths(colsArry);
							// tempTable.setLockedWidth(true);
							document.add(tempTable);
							if(rowEnd != table.getRows().size()) {
								rowStart = rowEnd;
								rowEnd = getRowsPerPage(table, rowStart, pageHeight);
								pages--;
								if (pages - 1 != 0)
									rowEnd = rowEnd - table.getHeaderRows();
								if (pages > 0)
									document.newPage();
							}else {
								pages = 0;
							}
						}
					}
				}
			}
			
			document.close();
			writer.close();
			exceptionCode.setResponse(filePath);
			exceptionCode.setOtherInfo(fileName);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch (Exception e) {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	private ExceptionCode writeTotalRowCaptionColDataToTable(ReportsVb reportsVb,int loopCount,String orgValue,BaseFont baseFont,PdfPTable table,List<HashMap<String, String>> dataLst,
			int recCount,int maxHeaderCol,int summaryDataFontSize,PdfPCell cell) {
		String cellText = orgValue;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			if(ValidationUtil.isValid(reportsVb.getGrandTotalCaption())  && loopCount == 0) {
				orgValue = reportsVb.getGrandTotalCaption();
			}
			cellText = orgValue;
			
			Font font = new Font(baseFont);// Summary with Background
			font.setStyle(Font.BOLD);
			//font.setColor(177, 24, 124);//Sunoida Light pink color
			int[] sunoidaPinkClr =  getRGB(reportsVb.getApplicationTheme());
			font.setColor(sunoidaPinkClr[0], sunoidaPinkClr[1], sunoidaPinkClr[2]);
			font.setSize(summaryDataFontSize);
			cell = new PdfPCell(new Phrase(cellText, font));
			cell.setBorder(0 & 0 & 0 & 1);
			cell.setUseVariableBorders(true);
			cell.setBorderWidthBottom(0.5f);
			// cell.setBorderWidthTop(1);
			cell.setBorderColor(new BaseColor(222,226,230));
			cell.setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			cell.setPaddingBottom(4f);
			cell.setPaddingTop(4f);
			cell.setPaddingLeft(2f);
			cell.setPaddingRight(2f);
			cell.setBackgroundColor(new BaseColor(242,244,242));//Sunoida Light Grey
			//cell.setBackgroundColor(new BaseColor(205, 226, 236));//light blue

			if (recCount == dataLst.size() - 1) {
				cell.setBorderWidthBottom(0.5f);
				cell.setBorderColorBottom(new BaseColor(222,226,230));
			}
			if (loopCount == 0) {
				cell.setBorderWidthLeft(0.5f);
				cell.setBorderColorLeft(new BaseColor(222,226,230));
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			} else if (loopCount == maxHeaderCol - 1) {
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			} else {
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			}
			cell.setNoWrap(false);
			table.addCell(cell);
		}catch(Exception e) {
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setResponse(table);
		exceptionCode.setOtherInfo(cell);
		return exceptionCode;
	}
	private ExceptionCode writeCaptionColDataToTable(ReportsVb reportsVb,int loopCount,String orgValue,BaseFont baseFont,PdfPTable table,List<HashMap<String, String>> dataLst,
			int recCount,int maxHeaderCol,String formatType,int dataFontSize,PdfPCell cell) {
		String cellText = orgValue;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			Font font = new Font(baseFont);
			font.setSize(dataFontSize);
			// FT - Text with BOLD
			//S OR ST - Text BOLD and with Background Color
			if (ValidationUtil.isValid(cellText)) {
				if ("FT".equalsIgnoreCase(formatType) || "S".equalsIgnoreCase(formatType)
						|| "ST".equalsIgnoreCase(formatType)) {
					font.setStyle(Font.BOLD);
					if ("S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType)) {
						font.setSize(summaryDataFontSize);
					}
				}
			}
			
			cell = new PdfPCell(new Phrase(cellText, font));
			cell.setBorder(0 & 0 & 0 & 1);
			cell.setUseVariableBorders(true);
			cell.setBorderWidthBottom(0.5f);
			cell.setBorderColorBottom(new BaseColor(222,226,230));
			cell.setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			cell.setPaddingBottom(4f);
			cell.setPaddingTop(4f);
			cell.setPaddingLeft(2f);
			cell.setPaddingRight(2f);
			if("S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType)) {
				cell.setBackgroundColor(new BaseColor(242,244,242));//light grey
				//cell.setBackgroundColor(new BaseColor(205, 226, 236));
			}
			if (recCount == dataLst.size() - 1) {
				cell.setBorderWidthBottom(0.5f);
				cell.setBorderColorBottom(new BaseColor(222,226,230));
			}
			if (loopCount == 0) {
				cell.setBorderWidthLeft(0.5f);
				cell.setBorderColorLeft(new BaseColor(222,226,230));
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			} else if (loopCount == maxHeaderCol - 1) {
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			} else {
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			}
			cell.setNoWrap(false);
			table.addCell(cell);
		}catch(Exception e) {
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setResponse(table);
		exceptionCode.setOtherInfo(cell);
		return exceptionCode;
	}
	private PdfPTable writeGrowthColDataToTable(ReportsVb reportsVb,int loopCount,String orgValue,BaseFont baseFont,PdfPTable table,List<HashMap<String, String>> dataLst,
			int recCount,int maxHeaderCol,String formatType,int dataFontSize,String assetFolderUrl) {
		PdfPCell cell = null;
		String cellText = "";
		try {
			cellText = "ARROW"; 
			cell = new PdfPCell();
			Image grwthImage = null;
			if("GREEN1".equalsIgnoreCase(orgValue))
				grwthImage = Image.getInstance(assetFolderUrl+File.separator+"GreenArrow.png");
			else
				grwthImage = Image.getInstance(assetFolderUrl+File.separator+"RedArrow.png");
			grwthImage.setAlignment(Image.MIDDLE);
			grwthImage.scaleToFit(10f,10f);
			cell.setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);
			cell.setHorizontalAlignment(PdfPCell.ALIGN_MIDDLE);
			cell.setBorderWidthLeft(0.1f);
			cell.setBorderColorLeft(new BaseColor(222,226,230));
			cell.setBorderWidthRight(0.1f);
			cell.setBorderColorRight(new BaseColor(222,226,230));
			cell.setBorderWidthTop(0.1f);
			cell.setBorderColorTop(new BaseColor(222,226,230));
			cell.setBorderWidthBottom(0.1f);
			cell.setBorderColorBottom(new BaseColor(222,226,230));
			cell.addElement(grwthImage);
			table.addCell(cell);
		}catch(Exception e) {
			e.printStackTrace();
		}
		return table;
	}	
	private ExceptionCode writeTotalRowDataColToTable(ReportsVb reportsVb,int loopCount,String orgValue,BaseFont baseFont,PdfPTable table,List<HashMap<String, String>> dataLst,
			int recCount,int maxHeaderCol,int summaryDataFontSize,PdfPCell cell) {
		String cellText = orgValue;
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			Font font = new Font(baseFont);
			font.setStyle(Font.BOLD);
			font.setSize(summaryDataFontSize);
			//font.setColor(177, 24, 124); //Sunoida Light pink color
			int[] sunoidaPinkClr =  getRGB(reportsVb.getApplicationTheme());
			font.setColor(sunoidaPinkClr[0], sunoidaPinkClr[1], sunoidaPinkClr[2]);
			cell = new PdfPCell(new Phrase(cellText, font));
			cell.setBorder(0 & 0 & 0 & 1);
			cell.setUseVariableBorders(true);
			cell.setBorderWidthBottom(0.5f);
			cell.setHorizontalAlignment(PdfPCell.ALIGN_RIGHT);
			cell.setVerticalAlignment(PdfPCell.ALIGN_CENTER);
			cell.setBackgroundColor(new BaseColor(242,244,242));//light grey
			//cell.setBackgroundColor(new BaseColor(205, 226, 236));//light blue
			cell.setBorderColor(new BaseColor(222,226,230));
			cell.setPaddingBottom(4f);
			cell.setPaddingTop(4f);
			cell.setPaddingLeft(2f);
			cell.setPaddingRight(2f);
		
			if (recCount == dataLst.size() - 1) {
				cell.setBorderWidthBottom(0.5f);
				cell.setBorderColorBottom(new BaseColor(222,226,230));
			}
			if (loopCount == 0) {
				cell.setBorderWidthLeft(0.5f);
				cell.setBorderColorLeft(new BaseColor(222,226,230));
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			} else if (loopCount == maxHeaderCol - 1) {
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			} else {
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			}
			cell.setNoWrap(true);
			table.addCell(cell);
		}catch(Exception e) {
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setResponse(table);
		exceptionCode.setOtherInfo(cell);
		return exceptionCode;
	}
	private ExceptionCode writeRowDataColToTable(ReportsVb reportsVb,int loopCount,String orgValue,BaseFont baseFont,PdfPTable table,List<HashMap<String, String>> dataLst,
			int recCount,int maxHeaderCol,int dataFontSize,String formatType,String cellText,ColumnHeadersVb colHeadersVb,PdfPCell cell) {
		ExceptionCode exceptionCode = new ExceptionCode();
		try {
			Font font = new Font(baseFont);
			font.setSize(dataFontSize);
			cell = new PdfPCell(new Phrase(cellText, font));
			if("FT".equalsIgnoreCase(formatType) || "S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType) || "Y".equalsIgnoreCase(colHeadersVb.getColorDiff())) {
				font.setStyle(Font.BOLD);
				if("S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType)) {
					font.setSize(summaryDataFontSize);
					cell.setBackgroundColor(new BaseColor(242,244,242));//light grey
					//cell.setBackgroundColor(new BaseColor(205, 226, 236));//light blue
				}
				if("Y".equalsIgnoreCase(colHeadersVb.getColorDiff())) {
					if(cellText.substring(0, 1).contains("-"))
						font.setColor(new BaseColor(255,0,0));
					else
						font.setColor(new BaseColor(7,158,18));
				}
			}
			cell = new PdfPCell(new Phrase(cellText, font));
			cell.setBorder(0 & 0 & 0 & 1);
			cell.setUseVariableBorders(true);
			cell.setBorderWidthBottom(0.5f);
			cell.setBorderColorBottom(new BaseColor(222,226,230));
			cell.setBorderColor(new BaseColor(222,226,230));
			cell.setHorizontalAlignment(PdfPCell.ALIGN_RIGHT);
			cell.setVerticalAlignment(PdfPCell.ALIGN_CENTER);
			cell.setPaddingBottom(4f);
			cell.setPaddingTop(4f);
			cell.setPaddingLeft(2f);
			cell.setPaddingRight(2f);
			if("S".equalsIgnoreCase(formatType) || "ST".equalsIgnoreCase(formatType)) {
				cell.setBackgroundColor(new BaseColor(205, 226, 236));
			}
			if (recCount == dataLst.size() - 1) {
				cell.setBorderWidthBottom(0.5f);
				cell.setBorderColorBottom(new BaseColor(222,226,230));
			}
			if (loopCount == 0) {
				cell.setBorderWidthLeft(0.5f);
				cell.setBorderColorLeft(new BaseColor(222,226,230));
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			} else if (loopCount == maxHeaderCol - 1) {
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			} else {
				cell.setBorderWidthRight(0.1f);
				cell.setBorderColorRight(new BaseColor(222,226,230));
			}
			cell.setNoWrap(true);
			table.addCell(cell);
		}catch(Exception e) {
			exceptionCode.setErrorMsg(e.getMessage());
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
		}
		exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		exceptionCode.setResponse(table);
		exceptionCode.setOtherInfo(cell);
		return exceptionCode;
	}

	public int[] getRGB(String rgb) {
		final int[] ret = new int[3];
		if (!ValidationUtil.isValid(rgb))
			rgb = "4285f4";//"b1187c";
		for (int i = 0; i < 3; i++) {
			ret[i] = Integer.parseInt(rgb.substring(i * 2, i * 2 + 2), 16);
		}
		return ret;
	}
	
}
