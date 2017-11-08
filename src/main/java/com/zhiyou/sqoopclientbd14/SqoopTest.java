package com.zhiyou.sqoopclientbd14;

import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;

public class SqoopTest {

	//服务端url
	private final String URL = 
			"http://master:12000/sqoop";
	//client
	private SqoopClient client = 
			new SqoopClient(URL);
	
	
	
	//link
	public void createLink(){
		
		MLink link = client.createLink("generic-jdbc-connector");
		//获取connector的link的 配置信息
		link.setName("window_mysql");
		MLinkConfig linkConfig = link.getConnectorLinkConfig();
		
		//通过配置项名称获取配置项
//		linkConfig.getStringInput("");
		
		List<MConfig> configs = linkConfig.getConfigs();
		for(MConfig config : configs){
			System.out.println("所有配置项"+config.toString());
		}
		
	}
	
	
	public static void main(String[] args) {
		SqoopTest sqoopTest = new SqoopTest();
		sqoopTest.createLink();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
