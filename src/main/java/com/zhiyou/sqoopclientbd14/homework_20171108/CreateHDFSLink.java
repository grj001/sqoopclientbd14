package com.zhiyou.sqoopclientbd14.homework_20171108;

import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.validation.Status;


public class CreateHDFSLink {
	
	private static final String URL = 
			"http://master:12000/sqoop/";
	
	private static SqoopClient client = 
			new SqoopClient(URL);
	
	public void createHDFSLink(){
		
		MLink link = client.createLink("hdfs-connector");
		link.setName("window_hdfs");
		MLinkConfig linkConfig = link.getConnectorLinkConfig();
		
		List<MConfig> configs = linkConfig.getConfigs();
		for(MConfig config : configs){
			List<MInput<?>> inputs = config.getInputs();
			for(MInput input : inputs){
				System.out.println(input);
			}
		}
		
		linkConfig.getStringInput("linkConfig.uri")
		.setValue("hdfs://master:9000/");
		linkConfig.getStringInput("linkConfig.confDir")
		.setValue("/opt/Software/Hadoop/hadoop-2.7.3/etc/hadoop");
		
		Status status = client.saveLink(link);
		if(status.canProceed()){
			System.out.println("创建link"+link.getName()+"成功");
		}else{
			System.out.println("创建link"+link.getName()+"失败");
		}
	}
	
	
	public static void main(String[] args) {
		CreateHDFSLink hdfsLink = new CreateHDFSLink();
		hdfsLink.createHDFSLink();
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
