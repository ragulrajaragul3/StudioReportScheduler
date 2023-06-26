package com.vision.wb;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellUtil;
import org.apache.poi.ss.util.RegionUtil;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.context.ServletContextAware;

import com.vision.dao.DesignAnalysisExportDao;
import com.vision.exception.ExceptionCode;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ExcelExportUtil;
import com.vision.util.ValidationUtil;
import com.vision.vb.VcForQueryReportFieldsVb;
import com.vision.vb.VcForQueryReportFieldsWrapperVb;
import com.vision.vb.XmlJsonUploadVb;

@Component
public class DesignAnalysisExportWb implements ServletContextAware {
	public JdbcTemplate jdbcTemplate = null;

	public DesignAnalysisExportWb(JdbcTemplate jdbcTemplateArg) {
		jdbcTemplate = jdbcTemplateArg;
	}
	@Autowired
    ResourceLoader resourceLoader;
	
	@Autowired
	DesignAnalysisWb designAnalysisWb;
	
	@Autowired
	DesignAnalysisExportDao designAnalysisExportDao;
	
	private ServletContext servletContext;
	
	@Override
	public void setServletContext(ServletContext arg0) {
		servletContext = arg0;
	}
	/*@Autowired
	ExcelExportUtil excelExportUtil;*/
	
	//private final String EXCEL_FILE_PATH = commonDao.findVisionVariableValue("SSBI_EXPORT_EXCEL_PATH");
	private final String EXCEL_FILE_PATH = System.getProperty("java.io.tmpdir");
	private static final String IMG_PATH = System.getenv("SSBI_EXPORT_IMAGE_PATH");
	
	public ExceptionCode generateExcel(VcForQueryReportFieldsWrapperVb vObject) throws FileNotFoundException, IOException {
		ExceptionCode exceptionCode = new ExceptionCode();
		BufferedInputStream in= null;
		FileOutputStream fos = null;
		try {
			
			String title = ValidationUtil.isValid(vObject.getMainModel().getReportDescription())?vObject.getMainModel().getReportDescription():"Exported Data";
			String dataViewName; //temporary table name that contains data to write into the excel
			//String dataSheetName = ValidationUtil.isValid(vObject.getMainModel().getReportId())?vObject.getMainModel().getReportId():"Data";
			String dataSheetName = ValidationUtil.encode(vObject.getMainModel().getReportName())+"_"+ScheduledReportWb.processId+"_"+ScheduledReportWb.versionNo;
			XSSFWorkbook wb = new XSSFWorkbook();
			Sheet indexSheet = wb.createSheet("Index");
			Sheet dataSheet = wb.createSheet(dataSheetName);
			/*InputStream is = null;
			Resource resource = resourceLoader
					.getResource("classpath:" + File.separator + "images" + File.separator + "Product_Logo.png");
			is = resource.getInputStream();
			InputStream is1 = null;
			resource = resourceLoader
					.getResource("classpath:" + File.separator + "images" + File.separator + "Bank_logo.PNG");
			is1 = resource.getInputStream();*/
			//String assetFolderUrl = servletContext.getRealPath("/WEB-INF/classes/images");
			String assetFolderUrl = ScheduledReportWb.assetFolderUrl;
			ExcelExportUtil.createCatalogIndex(vObject, indexSheet, wb,assetFolderUrl);

			Set<String> uniqueTableIdSet = new HashSet<String>();
			
			for(VcForQueryReportFieldsVb rCReportFieldsVb: vObject.getReportFields()) {
				if(ValidationUtil.isValid(rCReportFieldsVb.getTabelId())){
					uniqueTableIdSet.add(rCReportFieldsVb.getTabelId());
				}
			}
			
			vObject.getMainModel().setBaseTableId(new DesignAnalysisWb(jdbcTemplate).returnBaseTableId(vObject.getMainModel().getCatalogId()));
			exceptionCode = new DesignAnalysisWb(jdbcTemplate).executeCatalogQuery(vObject, uniqueTableIdSet.stream().collect(Collectors.toList()), false, false);
			if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
				dataViewName = String.valueOf(exceptionCode.getResponse());
			} else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				exceptionCode.setErrorMsg("Problem in fetching data");
				return exceptionCode;
			}
//			Get a Table with data to write into sheet - End
			
//			Get query with display format included (Scaling, Number Format, Date Format...) & Header row with scaling labels - Start
			exceptionCode = formQueryToExecute(vObject, dataViewName);
			List returnList = (List) exceptionCode.getResponse();
			List<XmlJsonUploadVb> headerNameList = (List<XmlJsonUploadVb>) returnList.get(0);
			List<XmlJsonUploadVb> columnNameList = (List<XmlJsonUploadVb>) returnList.get(1);
			String executableQuery = (String) returnList.get(2);
			String orderByStr = (String) returnList.get(3);
//			Get query with display format included (Scaling, Number Format, Date Format...) & Header row with scaling labels - End
			/* 
			 * Column autowidth is done at Column header loop 
			 * So data must be set before the header loop
			 * */
			ArrayList colTypes = new ArrayList();
			Map<Integer,Integer> columnWidths = new HashMap<Integer,Integer>(colTypes.size());
			int i = 0;
			for(VcForQueryReportFieldsVb obj : vObject.getReportFields()) {
				colTypes.add(obj.getColDisplayType());
				columnWidths.put(Integer.valueOf(i), Integer.valueOf(-1));
				i++;
			}
			
			String query = executableQuery+((ValidationUtil.isValid(orderByStr))?" ORDER BY "+orderByStr:"");
			int columnCount = headerNameList.size();
			
			int recordCount = new DesignAnalysisExportDao(jdbcTemplate).returnRecordCount(query);
			int batchSize = 1000;
			int batchCount = (recordCount/batchSize)+1;
			
			for(int batchIndex = 1;batchIndex<=batchCount;batchIndex++) {
				SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
				Date now = new Date();
				System.out.println("Batch:"+batchIndex+"-Start:"+sdf.format(now.getTime()));
				int startIndex = (batchIndex==1)?0:((batchIndex-1)*batchSize);//0//1000
				int lastIndex = startIndex+batchSize;//1000//2000
				StringBuffer paginationSQL = new StringBuffer("SELECT * FROM (");
				paginationSQL.append("SELECT temp.*, ROWNUM num FROM (");
				paginationSQL.append(query);
				paginationSQL.append(") temp where ROWNUM <= " + (lastIndex));
				paginationSQL.append(") WHERE num > " + startIndex);
				
				String xmlData = new DesignAnalysisExportDao(jdbcTemplate).formXMLDataForGidWithQuery(String.valueOf(paginationSQL), colTypes, columnCount);
				
				new DesignAnalysisExportDao(jdbcTemplate).wrideDataForGidWithQuery(dataSheet, wb, xmlData, colTypes, columnCount, startIndex,columnWidths);
				now = new Date();
				System.out.println("Batch:"+batchIndex+"-End:"+sdf.format(now.getTime()));
			}
			
			Row row = dataSheet.createRow(0);
			int colCount = 0;
			for(XmlJsonUploadVb headerVb:headerNameList) {
				if(headerVb.getProperty()!=null &&  headerVb.getProperty().get("colScaleFormatLabel")!=null) {
					String colScaleFormatLabel = String.valueOf(headerVb.getProperty().get("colScaleFormatLabel"));
					if(ValidationUtil.isValid(colScaleFormatLabel)) {
						CellUtil.createCell(row, colCount, headerVb.getData()+" "+colScaleFormatLabel, setHeaderCellStyle(wb));
					} else {
						CellUtil.createCell(row, colCount, headerVb.getData(), setHeaderCellStyle(wb));
					}
				} else {
					CellUtil.createCell(row, colCount, headerVb.getData(), setHeaderCellStyle(wb));
				}
				dataSheet.autoSizeColumn(colCount);
				colCount++;
			}
			
			
			//Write the Excel file
			FileOutputStream fileOut = null;
			fileOut = new FileOutputStream(EXCEL_FILE_PATH+dataSheetName+".xlsx");
			wb.write(fileOut);
			fileOut.close();
			exceptionCode.setResponse(EXCEL_FILE_PATH + dataSheetName+".xlsx");
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
		} catch(IOException ioex){
			ioex.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(ioex.getMessage());
		} catch(Exception e){
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	
	
	// Common Method Start
	
	private void drawImageToSheet(XSSFWorkbook workbook, Sheet sheet, String imageName, int startCol, int endCol, int startRow, int endRow, boolean applicationImage) throws IOException {
		InputStream imageInputStream = null;
//		applicationImage =false;
		if (applicationImage) {
			Resource resource = resourceLoader
					.getResource("classpath:" + File.separator + "images" + File.separator + imageName);
			imageInputStream = resource.getInputStream();
		} else {
			imageInputStream = new FileInputStream(IMG_PATH + imageName);
		}
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
	
	private void applyRegionBorder(CellRangeAddress cellRangeAddress, XSSFWorkbook workbook, Sheet sheet) {
		RegionUtil.setBorderTop(CellStyle.BORDER_THIN, cellRangeAddress, sheet, workbook);
		RegionUtil.setBorderLeft(CellStyle.BORDER_THIN, cellRangeAddress, sheet, workbook);
		RegionUtil.setBorderRight(CellStyle.BORDER_THIN, cellRangeAddress, sheet, workbook);
		RegionUtil.setBorderBottom(CellStyle.BORDER_THIN, cellRangeAddress, sheet, workbook);
	}
	
	private CellStyle setTitleCellStyle(XSSFWorkbook workbook) {
		/* create font */
		XSSFFont font = workbook.createFont();
		font.setFontHeightInPoints((short) 15);
		font.setFontName("Calibri");
		font.setColor(IndexedColors.DARK_BLUE.getIndex());
		font.setBold(true);
		font.setItalic(false);
		/* Create cell style */
		CellStyle style = workbook.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		/* Setting font to style */
		style.setFont(font);
		style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		return style;
	}
	
	private static CellStyle setHeaderCellStyle(Workbook workbook) {
		/* create font */
		Font font = workbook.createFont();
		font.setColor(IndexedColors.WHITE.index);
		font.setBoldweight(Font.BOLDWEIGHT_BOLD);
		font.setFontName("Calibri");
		font.setFontHeightInPoints((short)10);
		font.setItalic(false);
		/*byte[] pinkClr = { (byte) 230, (byte) 184, (byte) 183 };
		XSSFColor pinkXClor = new XSSFColor(pinkClr);*/
		
		/* Create cell style */
		XSSFCellStyle style = (XSSFCellStyle)workbook.createCellStyle();
		style.setAlignment(CellStyle.ALIGN_CENTER);
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
		style.setFillForegroundColor(sunoidaPinkXClr);
		style.setBorderBottom(CellStyle.BORDER_THIN);
	    style.setBorderRight(CellStyle.BORDER_THIN);
	    style.setBottomBorderColor(IndexedColors.WHITE.index);
	    style.setRightBorderColor(IndexedColors.WHITE.index);
		return style;
	}
	// Common Method End
	
	// Form a query to extract data from temporary "dataview" with number format, scaling format and date format conditions to write into the file
	// Also return the headers with scaling labels to write to the file ("headerNameList")
	public ExceptionCode formQueryToExecute(VcForQueryReportFieldsWrapperVb vcForQueryReportFieldsWrapperVb, String dataViewName) {

		ExceptionCode exceptionCode = new ExceptionCode();
		List<XmlJsonUploadVb> columnNameList = new ArrayList<XmlJsonUploadVb>();
		List<XmlJsonUploadVb> headerNameList = new ArrayList<XmlJsonUploadVb>();
		try {

			StringBuffer selectStr = new StringBuffer(" ");
			for(VcForQueryReportFieldsVb reportsObj : vcForQueryReportFieldsWrapperVb.getReportFields()) {
			
				if(ValidationUtil.isValid(reportsObj.getDisplayFlag()) && reportsObj.getDisplayFlag().equals("N")) {
					continue;
				}

				String colDisplayType = reportsObj.getColDisplayType();
				String scalingFlag = reportsObj.getScalingFlag();
				String scalingFormat = String.valueOf(reportsObj.getScalingFormat());
				String numberFormat = reportsObj.getNumberFormat();
				String decimalFlag = reportsObj.getDecimalFlag();
				String decimalCount = String.valueOf(reportsObj.getDecimalCount());
				
				String columnAlias = reportsObj.getAlias();
								
				Map<String, Object> propertyMap = new HashMap<String, Object>();
				propertyMap.put("colDisplayType",(colDisplayType.equalsIgnoreCase("C") || colDisplayType.equalsIgnoreCase("N"))?"M":"D");
				String colScaleFormatLabel = "";
				if(("N".equalsIgnoreCase(colDisplayType) || "C".equalsIgnoreCase(colDisplayType)) && "y".equalsIgnoreCase(scalingFlag) && ValidationUtil.isValid(scalingFormat)) {
					switch(scalingFormat) {
						case "1000":
							colScaleFormatLabel = "(t)";
							break;
						case "1000000":
							colScaleFormatLabel = "(m)";
							break;
						case "1000000000":
							colScaleFormatLabel = "(b)";
							break;
						case "1000000000000":
							colScaleFormatLabel = "(tr)";
							break;
					}
				}
				propertyMap.put("colScaleFormatLabel",colScaleFormatLabel);
				propertyMap.put("colDisplayType",colDisplayType);
				columnNameList.add(new XmlJsonUploadVb(columnAlias, propertyMap));
				headerNameList.add(new XmlJsonUploadVb(reportsObj.getColName(), propertyMap));
				
				
				String tempColumn = "T"+"."+columnAlias;
				if ("y".equalsIgnoreCase(scalingFlag)) {
					tempColumn = tempColumn+"/"+scalingFormat;
				}
				if("y".equalsIgnoreCase(numberFormat)) {
					if("y".equalsIgnoreCase(decimalFlag)) {
						String decimalFormat = CommonUtils.returnDecimalFormat(decimalCount);
						tempColumn = "trim(TO_CHAR("+tempColumn+", '999,999,999,990."+decimalFormat+"'))";
					} else {
						tempColumn = "trim(TO_CHAR("+tempColumn+", '999,999,999,990'))";
					}
				}
				if((ValidationUtil.isValid(reportsObj.getColDisplayType()) && ( "d".equalsIgnoreCase(reportsObj.getColDisplayType())) )){
					//TO_DATE("+filterVb.getColumnName()+",'DD-MON-RRRR')
//							selectStr.append("FORMAT("+tempColumn+",'"+reportsObj.getDynamicDateFormat()+"')" +columnAlias+", ");
					selectStr.append(reportsObj.getDateFormattingSyntax().replaceAll("#VALUE#", tempColumn)+" "+columnAlias+", ");
				} else {	
					selectStr.append(tempColumn+" "+columnAlias+", ");
				}
			}
			String selectvar = selectStr.substring(0, (selectStr.length()-2));
			

			String orderBy = vcForQueryReportFieldsWrapperVb.getMainModel().getOrderBy();
			
			String executableQuery = "SELECT "+selectvar+" FROM  "+dataViewName+" T";
			
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			exceptionCode.setErrorCode(Constants.SUCCESSFUL_OPERATION);
			List returnList = new ArrayList();
			returnList.add(headerNameList);
			returnList.add(columnNameList);
			returnList.add(String.valueOf(executableQuery));
			returnList.add(String.valueOf((ValidationUtil.isValid(orderBy) && orderBy.length()>0)?orderBy:""));
			exceptionCode.setResponse(returnList);
		} catch(Exception e) {
			e.printStackTrace();
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			exceptionCode.setErrorMsg(e.getMessage());
		}
		return exceptionCode;
	}
	public HttpServletResponse setFileResponse(ExceptionCode exceptionCode, String fileName, HttpServletResponse response) throws IOException {
		if(exceptionCode.getErrorCode() == Constants.SUCCESSFUL_OPERATION) {
			String excelPath = (String) exceptionCode.getResponse();
			File file = new File(excelPath);
			if (file.exists()) {
				/*	Identify file type with name
				 * 
				 * String mimeType = URLConnection.guessContentTypeFromName(file.getName());
				if (mimeType == null) {
					mimeType = "application/octet-stream";
				}*/
				String mimeType = "application/octet-stream";
				response.setContentType(mimeType);
				response.setHeader("Content-Disposition", String.format("inline; filename=\"" + fileName + "\""));
				response.setContentLength((int) file.length());
				InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
				OutputStream outputStream = response.getOutputStream();
				FileCopyUtils.copy(inputStream, outputStream);
				
				outputStream.flush();
				outputStream.close();
				inputStream.close();
				file.delete();
				System.out.println("DD100: controller success End");
			} else {
				response.setStatus(Constants.ERRONEOUS_OPERATION, "File Not Found");
				System.out.println("DD100: controller error End1");
			}
		} else {
			response.setStatus(Constants.ERRONEOUS_OPERATION, "File Not Found");
			System.out.println("DD100: controller error End2");
		}
		return response;
	}
}
