package org.teiid.translator.simpledb;

import java.util.ArrayList;
import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.QueryExpression;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;

@Translator(name="simpledb", description="Translator for SimpleDB")
public class SimpleDBExecutionFactory extends ExecutionFactory<ConnectionFactory, SimpleDBConnection> {

	public SimpleDBExecutionFactory() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void start() throws TranslatorException {
		// TODO Auto-generated method stub
		super.start();
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			final SimpleDBConnection connection) throws TranslatorException {
		// TODO Auto-generated method stub
		return new ResultSetExecution() {
			boolean test=true;
			
			@Override
			public void execute() throws TranslatorException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void close() {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void cancel() throws TranslatorException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public List<?> next() throws TranslatorException, DataNotAvailableException {
				if (test){
					List<String> list = new ArrayList<String>();
					list = connection.getAPIClass().getDomains();
					test = false;
					return list;
				}else{
					return null;
				}
			}
		};
	}
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory,
			SimpleDBConnection conn) throws TranslatorException {
		Table table = metadataFactory.addTable("Test");
		Column column = new Column();
		column.setName("TestColumn");
		table.addColumn(column);
	}
	
}
