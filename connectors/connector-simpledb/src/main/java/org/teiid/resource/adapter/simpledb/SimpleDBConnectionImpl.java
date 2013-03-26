package org.teiid.resource.adapter.simpledb;

import javax.resource.ResourceException;

import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.resource.adpter.simpledb.TestAPIClass;
import org.teiid.resource.spi.BasicConnection;

public class SimpleDBConnectionImpl extends BasicConnection implements
		SimpleDBConnection {

	private TestAPIClass apiClass;

	public SimpleDBConnectionImpl(String accessKey, String secretAccessKey) {
		apiClass = new TestAPIClass(accessKey, secretAccessKey);
	}

	public void close() throws ResourceException {

	}

	public TestAPIClass getAPIClass() {
		return apiClass;
	}
}
