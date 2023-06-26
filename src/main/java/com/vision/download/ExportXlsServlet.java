package com.vision.download;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Component;

import com.vision.authentication.SessionContextHolder;
import com.vision.util.ValidationUtil;


@Component
public class ExportXlsServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		File lFile = null;
		File lFileNoEncode = null;
		try {
			int currentUser =  SessionContextHolder.getContext().getVisionId();

			String fileName = (String) request.getAttribute("fileName");
			String fileType=(String) request.getAttribute("fileExtension");
			fileType="."+fileType.toLowerCase();
			String filePath =(String) request.getAttribute("filePath");
			if(!ValidationUtil.isValid(fileName)){
				response.setStatus(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			if(!ValidationUtil.isValid(filePath)){
				filePath = System.getProperty("java.io.tmpdir");
				if(!ValidationUtil.isValid(filePath)){
					filePath = System.getenv("TMP");
				}
			}
			if(ValidationUtil.isValid(filePath)){
				filePath = filePath + File.separator;
			}
			if(".PDF".equalsIgnoreCase(fileType)){
				response.addHeader("Content-Type", "application/pdf"); 
				response.setContentType("application/pdf");
				if(fileName.indexOf(".pdf") != -1) {
					fileName = fileName.substring(0, fileName.indexOf(".pdf"));
				}
				lFile = new File(filePath+ValidationUtil.encode(fileName)+".pdf");
				lFileNoEncode = new File(filePath+fileName+fileType);
				fileName = fileName+".pdf";
			}else if(".xlsx".equalsIgnoreCase(fileType)){
				response.addHeader("Content-Type", "application/excel"); 
				response.setContentType("application/vnd.ms-excel");
				if(fileName.indexOf(".xlsx") != -1) {
					fileName = fileName.substring(0, fileName.indexOf(".xlsx"));	
				}
				lFile = new File(filePath+ValidationUtil.encode(fileName)+".xlsx");
				lFileNoEncode = new File(filePath+fileName+fileType);
				fileName = fileName+".xlsx";
				System.out.println("File Name : "+fileName);
			}else if(".txt".equalsIgnoreCase(fileType) || ".log".equalsIgnoreCase(fileType) || ".err".equalsIgnoreCase(fileType)){
				response.addHeader("Content-Type", "application/txt");
				response.setContentType("application/txt");
				lFile = new File(filePath+ValidationUtil.encode(fileName)+fileType);
				lFileNoEncode = new File(filePath+fileName+fileType);
				fileName = fileName+fileType;
			}else if(".csv".equalsIgnoreCase(fileType)){
				response.addHeader("Content-Type", "application/txt");
				response.setContentType("application/txt");
				if(fileName.indexOf(".csv") != -1) {
					fileName = fileName.substring(0, fileName.indexOf(".csv"));
				}
				lFile = new File(filePath+ValidationUtil.encode(fileName)+".csv");
				lFileNoEncode = new File(filePath+fileName+fileType);
				fileName = fileName+fileType;
			}else if(fileName.endsWith("zip")){
				response.addHeader("Content-Type", "application/zip");
				response.setContentType("application/zip");
				lFile = new File(filePath+fileName);
			}else if(fileName.endsWith("tar")){
				response.addHeader("Content-Type", "application/x-tar");
				response.setContentType("application/x-tar");
				lFile = new File(filePath+fileName);
			}else if(".xml".equalsIgnoreCase(fileType)){
			    response.addHeader("Content-Type", "application/xml"); 
			    response.setContentType("application/xml");
				lFile = new File(filePath+ValidationUtil.encode(fileName));
				lFileNoEncode = new File(filePath+fileName+fileType);
				fileName = fileName+fileType;
			}else if(".json".equalsIgnoreCase(fileType)){
			    response.addHeader("Content-Type", "application/json"); 
			    response.setContentType("application/json");
				lFile = new File(filePath+ValidationUtil.encode(fileName));
				lFileNoEncode = new File(filePath+fileName+fileType);
				fileName = fileName+fileType;
			}else if(".docx".equalsIgnoreCase(fileType)){
			    response.addHeader("Content-Type", "application/docx"); 
			    response.setContentType("application/docx");
			    lFile = new File(filePath+ValidationUtil.encode(fileName)+".docx");
				lFileNoEncode = new File(filePath+fileName+fileType);
				fileName = fileName+fileType;
			}else{
				response.addHeader("Content-Type", "application/txt");
				response.setContentType("application/txt");
				lFile = new File(filePath+fileName+fileType);
			}
			response.setHeader("Content-Disposition","attachment; filename=\"" + fileName + "\"");
			if(lFile == null || !lFile.exists()){
				lFile = lFileNoEncode;
				if(lFile == null || !lFile.exists()){
					response.setStatus(HttpServletResponse.SC_NOT_FOUND);
					return;	
				}
			}
			FileInputStream fis = new FileInputStream(lFile);
			// Write the output
			OutputStream out = response.getOutputStream();
			final int MAX = 4096;
			byte[] buf = new byte[MAX];
			for (int bytesRead = fis.read(buf, 0, MAX); bytesRead != -1; bytesRead = fis.read(buf, 0, MAX)) {
				out.write(buf, 0, bytesRead);
			}
			/*
			FileUtil.copyStream(fis, out);*/
			
			out.flush();
			out.close();
			fis.close();
			lFile.delete();
		}catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();
			throw new ServletException("Exception in File download Servlet", e);
		}finally{
			
		}
	}
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}
	
}
