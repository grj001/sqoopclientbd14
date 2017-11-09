package com.zhiyou.sqoopclientbd14.homework_20171108;

import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.validation.Status;

public class CreateMySQLToHDFSJob {

	private final String URL = 
			"http://master:12000/sqoop/";
	
	private SqoopClient client = 
			new SqoopClient(URL);
	
	public void showConfig(MConfigList configList){
		List<MConfig> configs = configList.getConfigs();
			
		for(MConfig mConfig : configs){
			List<MInput<?>> inputs = mConfig.getInputs();
			for(MInput input : inputs){
				System.out.println("\t"+input);
			}
		}
	}
	
	
	
	public MJob createMySQLToHDFSJob(String fromLinkName
			, String toLinkName){
//		client.deleteJob("mySQLToHDFSJob");
		
		
		MJob job = client.createJob(fromLinkName, toLinkName);
		job.setName("mySQLToHDFSJob");
		
		MDriverConfig driverConfig = job.getDriverConfig();
		MFromConfig jdbcConfig = job.getFromJobConfig();
		MToConfig HDFSConfig = job.getToJobConfig();
		
		CreateMySQLToHDFSJob createMySQLToHDFSJob = new
				CreateMySQLToHDFSJob();
		System.out.println("driverConfig");
		createMySQLToHDFSJob.showConfig(driverConfig);
		System.out.println("jdbcConfig");
		createMySQLToHDFSJob.showConfig(jdbcConfig);
		System.out.println("HDFSConfig");
		createMySQLToHDFSJob.showConfig(HDFSConfig);
		
		
		jdbcConfig.getStringInput("fromJobConfig.schemaName").setValue("from_sqoop");
		jdbcConfig.getStringInput("fromJobConfig.tableName").setValue("users");
		jdbcConfig.getStringInput("fromJobConfig.partitionColumn").setValue("id");
		
		HDFSConfig.getEnumInput("toJobConfig.outputFormat").setValue("TEXT_FILE");
		HDFSConfig.getEnumInput("toJobConfig.compression").setValue("NONE");
		HDFSConfig.getStringInput("toJobConfig.outputDirectory").setValue("/user/fromMysqlAndJava");
		HDFSConfig.getBooleanInput("toJobConfig.appendMode").setValue(true);
		
		
		
		Status status = client.saveJob(job);
		if(status.canProceed()){
			System.out.println("创建link"+job.getName()+"成功");
		}else{
			System.out.println("创建link"+job.getName()+"失败");
		}
		
		return job;
	}
	
	public void startJob(MJob job){
//		System.out.println(job.getName());
		client.startJob(job.getName());
	}
	
	
	
	
	public static void main(String[] args) {
		CreateMySQLToHDFSJob mySQLToHDFSJob = new CreateMySQLToHDFSJob();
		MJob job = mySQLToHDFSJob.createMySQLToHDFSJob("window_mysql", "window_hdfs");
		mySQLToHDFSJob.startJob(job);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
