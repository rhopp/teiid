package org.teiid.resource.adpter.simpledb;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;


public class SimpleDbAPICLass {
	
	private AmazonSimpleDBClient client;
	
	public SimpleDbAPICLass(String key1, String key2) {
		client = new AmazonSimpleDBClient(new BasicAWSCredentials(key1, key2));
	}

	public List<String> getDomains(){
		return client.listDomains().getDomainNames();
	}
	
	public Set<String> getAttributeNames(String domainName){
		Set<String> attributes = new HashSet<String>();
		DomainMetadataResult metadataResult = client.domainMetadata(new DomainMetadataRequest(domainName));
		int attributesCount = metadataResult.getAttributeNameCount();
		SelectResult selectResult = client.select(new SelectRequest("SELECT * FROM "+domainName));
		Iterator<Item> itemsIterator = selectResult.getItems().iterator();
		while (attributes.size() < attributesCount){
			Item item = itemsIterator.next();
			for (Attribute attribute : item.getAttributes()) {
				attributes.add(attribute.getName());
			}
		}
		return attributes;
		
	}
	
	public void performDelete(String domainName, String itemName){
		DeleteAttributesRequest req = new DeleteAttributesRequest(domainName, itemName);
		client.deleteAttributes(req);
	}
	
	
	public List<List<String>> performSelect(String selectExpression, List<String> columns){
		List<List<String>> list = new ArrayList<List<String>>();
		System.out.println("Performing select: "+selectExpression);
		SelectRequest selectRequest = new SelectRequest(selectExpression);
		SelectResult result = client.select(selectRequest);
		for (Item item : result.getItems()) {
			List<String> row = new ArrayList<String>();
			Map<String, List<Attribute>> attributeMap = createAttributeMap(item.getAttributes());
			for (String column : columns) {
				if (column.equals("itemName()")){
					row.add(item.getName());
				}else if (attributeMap.containsKey(column)){
					row.add(getMultivalueAttributeString(attributeMap.get(column)));
				}else{
					//korektni chovani?
					row.add("null");
					//throw new RuntimeException("There is no attribute with name "+column);
				}
			}
			list.add(row);
		}
		return list;
	}
	
	public void performUpdate(String domainName, Map<String, Map<String, String>> items){
		List<ReplaceableItem> itemsList = new ArrayList<ReplaceableItem>();
		for (Map.Entry<String, Map<String, String>> item : items.entrySet()){
			ReplaceableItem it = new ReplaceableItem(item.getKey());
			List<ReplaceableAttribute> attributesList = new ArrayList<ReplaceableAttribute>();
			for (Map.Entry<String, String> attribute : item.getValue().entrySet()){
				attributesList.add(new ReplaceableAttribute(attribute.getKey(), attribute.getValue(), true));
			}
			it.setAttributes(attributesList);
			itemsList.add(it);
		}
		BatchPutAttributesRequest req = new BatchPutAttributesRequest(domainName, itemsList);
		client.batchPutAttributes(req);
	}
	
	public int performInsert(String domainName, String itemName, Map<String, String> columnsMap){
		List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
		for (Map.Entry<String, String> column : columnsMap.entrySet()) {
			if (!column.getKey().equals("itemName()")){
				if (column.getValue().matches("^\\[.+\\]$")){
					String trimmedAttributes = column.getValue().substring(1, column.getValue().length()-1);
					Pattern p = Pattern.compile("[^\\\\];");
					Matcher match = p.matcher(trimmedAttributes);
					int lastMatch = 0;
					while (match.find()){
						ReplaceableAttribute attribute = new ReplaceableAttribute();
						attribute.setName(column.getKey());
						String value = trimmedAttributes.substring(lastMatch, match.start()+1);
						attribute.setValue(value.replaceAll("\\;", ";"));
						lastMatch = match.end();
						attribute.setReplace(false);
						attributes.add(attribute);
					}
					ReplaceableAttribute attribute = new ReplaceableAttribute();
					attribute.setName(column.getKey());
					String value = trimmedAttributes.substring(lastMatch, trimmedAttributes.length());
					attribute.setValue(value.replaceAll("\\\\;", ";"));
					attribute.setReplace(false);
					attributes.add(attribute);
				}else{
					attributes.add(new ReplaceableAttribute(column.getKey(), column.getValue(), false));
				}
			}
		}
		PutAttributesRequest putAttributesRequest = new PutAttributesRequest(domainName, itemName, attributes);
		client.putAttributes(putAttributesRequest);
		return 1;
	}

	private String getMultivalueAttributeString(List<Attribute> list) {
		if (list.size()==1){
			return list.get(0).getValue();
		}
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		String escapedValue = list.get(0).getValue().replace(";", "\\;");
		sb.append(escapedValue);
		for (int i=1; i<list.size(); i++){
			escapedValue = list.get(i).getValue().replace(";", "\\;");
			sb.append(";");
			sb.append(escapedValue);
		}
		sb.append("]");
		return sb.toString();
	}

	private Map<String, List<Attribute>> createAttributeMap(List<Attribute> attributes) {
		Map<String, List<Attribute>> map = new HashMap<String, List<Attribute>>();
		for (Attribute attribute : attributes) {
			if (map.get(attribute.getName()) == null){
				List<Attribute> list = new ArrayList<Attribute>();
				list.add(attribute);
				map.put(attribute.getName(), list);
			}else{
				map.get(attribute.getName()).add(attribute);
			}
		}
		return map;
	}
}
