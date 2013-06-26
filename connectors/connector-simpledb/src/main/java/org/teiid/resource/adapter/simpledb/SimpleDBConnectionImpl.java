package org.teiid.resource.adapter.simpledb;

import javax.resource.ResourceException;

import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.resource.adpter.simpledb.SimpleDbAPICLass;
import org.teiid.resource.spi.BasicConnection;

public class SimpleDBConnectionImpl extends BasicConnection implements
		SimpleDBConnection {

	private SimpleDbAPICLass apiClass;

	public SimpleDBConnectionImpl(String accessKey, String secretAccessKey) {
		apiClass = new SimpleDbAPICLass(accessKey, secretAccessKey);
	}

	public void close() throws ResourceException {

	}

	public SimpleDbAPICLass getAPIClass() {
		return apiClass;
	}
}
