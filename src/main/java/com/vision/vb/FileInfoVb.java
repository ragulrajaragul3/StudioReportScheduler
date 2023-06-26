package com.vision.vb;

import java.util.List;

import org.springframework.web.multipart.MultipartFile;

public class FileInfoVb {
	private String name = "";
	private String size = "";
	private String description = "";
	private String date= "";
	private String groupBy= "D";
	private int downloadType = 1;
	private MultipartFile file;
	private String extension = "";
	private String delimiter = "";
	private String sheetName = "";
	private Integer tableSize=0; 
	private char headerCheck;

	public char getHeaderCheck() {
		return headerCheck;
	}

	public void setHeaderCheck(char headerCheck) {
		this.headerCheck = headerCheck;
	}

	private String macroVar ="";
	public String getMacroVar() {
		return macroVar;
	}

	public void setMacroVar(String macroVar) {
		this.macroVar = macroVar;
	}

	public Integer getTableSize() {
		return tableSize;
	}

	public void setTableSize(Integer tableSize) {
		this.tableSize = tableSize;
	}

	private List<FileInfoVb> children;

    public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public MultipartFile getFile() {
        return file;
    }

    public void setFile(MultipartFile file) {
        this.file = file;
    }	
	public int getDownloadType() {
		return downloadType;
	}

	public void setDownloadType(int downloadType) {
		this.downloadType = downloadType;
	}

	public String getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	private byte data[];
	public FileInfoVb(){
		
	}
	
	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

	public FileInfoVb(String name, String size, String description, String extension) {
		this.name = name;
		this.size = size;
		this.description = description;
		this.extension = extension;
	}
	
	public FileInfoVb(String name, String size, byte data[], String description, List<FileInfoVb> children, String extension) {
		this.name = name;
		this.size = size;
		this.data = data;
		this.description = description;
		this.children = children;
		this.extension = extension;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSize() {
		return size;
	}
	public void setSize(String size) {
		this.size = size;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}

	public List<FileInfoVb> getChildren() {
		return children;
	}

	public void setChildren(List<FileInfoVb> children) {
		this.children = children;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

}