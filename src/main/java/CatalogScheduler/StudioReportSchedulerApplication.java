package CatalogScheduler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.wb.RSSProcessorProcess;
import com.vision.wb.ScheduledReportWb;
/*
 * Detail : This Jar is used to Schedule both catalog and Report
 * Author : Pavithra
 * Date   :
 * */
@SpringBootApplication
@Configurable
public class StudioReportSchedulerApplication {

	@Autowired
	DataSource datasource;
	
	@Autowired
	public static ApplicationContext appContext;
    												
    static final String SECRET = "Spiral Architect";
	public static void main(String[] args) {
		JdbcTemplate template = new JdbcTemplate();
		template.setDataSource(getDataSource());
		String emailProcess = args[0];  
		String processId = "";
		String debugMode = "";
		String versionNo = "";
		String logFilePath = "";
		int status = 1;
		if(emailProcess.equals("Y")){
			if(args.length != 4) {
				//java -jar RSSProcessor.jar [emailProcess] [ProcessId] [DebugMode] [version No]
				//java -jar RSSProcessor.jar Y 13 Y 0
				System.out.println("Invalid number of arguments passed");
				System.exit(1);	
			}
			processId = args[1];
			debugMode = args[2];
			versionNo = args[3];
			System.out.println("Process Id: "+processId+ " Debug Mode : "+debugMode+ " VersionNo : "+versionNo);
			status =new ScheduledReportWb(template).initializeData(processId,debugMode,versionNo);
		}else {
			if(args.length != 3) {
				//java -jar RSSProcessor.jar [emailProcess DebugMode] [LogfilePath]
				//java -jar RSSProcessor.jar N Y c:RA_EXECS\Logs.txt
				System.out.println("Invalid number of arguments passed");
				System.exit(1);	
			}
			debugMode = args[1];
			logFilePath = args[2];
			System.out.println("Debug Mode : "+debugMode+ " logFilePath : "+logFilePath);
			logWriter("Build Started...",logFilePath);
			logWriter("DebugMode -> args[1] : "+  debugMode,logFilePath);
			logWriter("LogFilePath -> args[2] : "+ logFilePath,logFilePath);
			status = new RSSProcessorProcess(template).rssProcessMain(logFilePath);
			String finalStatus = "";
			if(status == Constants.SUCCESS_OPERATION)
				finalStatus= "Success";
			else
				finalStatus= "Failed";
			logWriter("Build Completed..."+finalStatus,logFilePath);
		}
		
		doFinishingStep(template);
		System.out.println("Status : "+status);
		System.exit(status);
	}
	public static void doFinishingStep(JdbcTemplate jdbcTemplate) {
		try {
			if(jdbcTemplate.getDataSource().getConnection() != null)
				jdbcTemplate.getDataSource().getConnection().close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static BasicDataSource getDataSource() {
		String driverClassName = "";
		if("ORACLE".equalsIgnoreCase(Constants.DATABASE_TYPE))
			driverClassName = "oracle.jdbc.OracleDriver";
		else
			driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		
		if (!ValidationUtil.isValid(Constants.JDBC_URL_MAIN) || !ValidationUtil.isValid(Constants.USER_NAME)
				|| !ValidationUtil.isValid(Constants.PASSWORD_ENCRYPT_FLAG) || !ValidationUtil.isValid(Constants.PASSWORD)) {
			System.out.println("Environment Variable is not configured properly");
			System.out.println("URL[" + Constants.JDBC_URL_MAIN + "]Constants.USER_NAME[" + Constants.USER_NAME + "]" + "Constants.PASSWORD_ENCRYPT_FLAG[" + Constants.PASSWORD_ENCRYPT_FLAG + "]");

		}
		//DriverManagerDataSource dataSource = new DriverManagerDataSource();
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(driverClassName);
		dataSource.setUrl(Constants.JDBC_URL_MAIN);
		dataSource.setUsername(Constants.USER_NAME);
		dataSource.setPassword(Constants.PASSWORD);
		dataSource.setInitialSize(20);
		dataSource.setMinIdle(1);
		dataSource.setMaxIdle(5);
		dataSource.setMaxTotal(20);
		dataSource.setTestOnBorrow(true);
		dataSource.setPoolPreparedStatements(false);
		dataSource.setMaxOpenPreparedStatements(5000);
		return dataSource;
	}
	static FileWriter logfile = null;
	static FileWriter mainLogfile = null;
	static BufferedWriter bufferedWriter = null;
	static String uploadLogFilePath = "";
	public static void logWriter(String logString,String uploadLogFilePath){
		try{
			logfile = new FileWriter(uploadLogFilePath,true);
			bufferedWriter = new BufferedWriter(logfile);
			bufferedWriter.newLine();
			bufferedWriter.write(getCurrentDateTime() + " : " + logString);
			//bufferedWriter.write(""+logString);
			bufferedWriter.close();
			logfile.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	public static String getCurrentDateTime() {
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date);
	}
}
