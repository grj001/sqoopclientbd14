package com.zhiyou.sqoopclientbd14.homework_20171108;

import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.validation.Status;

public class CreateJDBCLink {

	//服务端url, 需要加个"/"
	private final String URL = 
			"http://master:12000/sqoop/";
	//client
	private SqoopClient client = 
			new SqoopClient(URL);
	
	
	
	//link
	public void createJDBCLink(){
		client.deleteLink("window_mysql");
		MLink link = client.createLink("generic-jdbc-connector");
		//获取connector的link的 配置信息
		link.setName("window_mysql");
		MLinkConfig linkConfig = link.getConnectorLinkConfig();
		
		//通过配置项名称获取配置项
//				linkConfig.getStringInput("");
		
		List<MConfig> configs = linkConfig.getConfigs();
		for(MConfig config : configs){
//					System.out.println("所有配置项"+config.toString());
			
			List<MInput<?>> inputs = config.getInputs();
			for(MInput input : inputs){
				System.out.println(input);
			}
		}
		//MlinkConfig相关配置项
		linkConfig.getStringInput("linkConfig.jdbcDriver")
		.setValue("com.mysql.jdbc.Driver");
		
		linkConfig.getStringInput("linkConfig.connectionString")
		.setValue("jdbc:mysql://192.168.58.128:3306/test");
		
		linkConfig.getStringInput("linkConfig.username")
		.setValue("root");
		
		linkConfig.getStringInput("linkConfig.password")
		.setValue("root");
		
		linkConfig.getStringInput("dialect.identifierEnclose")
		.setValue("`");
		Status status = client.saveLink(link);
		if(status.canProceed()){
			System.out.println("创建link"+link.getName()+"成功");
		}else{
			System.out.println("创建link"+link.getName()+"失败");
		}
	}
	
	
	public static void main(String[] args) {
		CreateJDBCLink jdbcLink = new CreateJDBCLink();
		jdbcLink.createJDBCLink();
	}
	
	
	
}
