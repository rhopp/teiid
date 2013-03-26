package org.teiid.resource.adpter.simpledb;


import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;


public class TestAPIClass {
	
	private AmazonSimpleDBClient client;
	
	public TestAPIClass(String key1, String key2) {
		client = new AmazonSimpleDBClient(new BasicAWSCredentials(key1, key2));
	}

	public List<String> getDomains(){
		List<String> list = new ArrayList<String>();
		list.add(client.listDomains().toString());
		return list;
	}

}
