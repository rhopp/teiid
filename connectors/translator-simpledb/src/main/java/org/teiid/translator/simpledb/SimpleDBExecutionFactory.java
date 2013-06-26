package org.teiid.translator.simpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Insert;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
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
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.simpledb.SimpleDBSQLVisitor;

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
	public UpdateExecution createUpdateExecution(final Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			final SimpleDBConnection connection) throws TranslatorException {
		// TODO Auto-generated method stub
		return new UpdateExecution() {
			int updatedCount = 0;
			
			@Override
			public void execute() throws TranslatorException {
				if (command instanceof Insert){
					Insert insert = (Insert) command;
					Map<String, String> columnsMap = SimpleDBInsertVisitor.getColumnsValuesMap(insert);
					updatedCount = connection.getAPIClass().performInsert(SimpleDBInsertVisitor.getDomainName(insert), columnsMap.get("itemName()"), columnsMap);
				}else if (command instanceof Delete){
					Delete delete = (Delete) command;
					SimpleDBDeleteVisitor visitor = new SimpleDBDeleteVisitor(delete, connection.getAPIClass());
					if (visitor.hasWhere()){
						if (visitor.isSimpleDelete()){
							connection.getAPIClass().performDelete(visitor.getTableName(), visitor.getItemName());
							updatedCount = 1;
						}else{
							for (String itemName : visitor.getItemNames()) {
								connection.getAPIClass().performDelete(visitor.getTableName(), itemName);
							}
							updatedCount = visitor.getItemNames().size();
						}
					}else{
						List<List<String>> result = connection.getAPIClass().performSelect("SELECT itemName() FROM "+visitor.getTableName(), Arrays.asList("itemName()"));
						updatedCount = result.size();
						for (List<String> list : result) {
							String itemName = list.get(0);
							connection.getAPIClass().performDelete(visitor.getTableName(), itemName);
						}
					}
				}else if (command instanceof Update){
					Update update = (Update) command;
					SimpleDBUpdateVisitor updateVisitor = new SimpleDBUpdateVisitor(update, connection.getAPIClass());
					Map<String, Map<String,String>> items = new HashMap<String, Map<String,String>>();
					for(String itemName : updateVisitor.getItemNames()){
						updatedCount++;
						items.put(itemName, updateVisitor.getAttributes());
					}
					connection.getAPIClass().performUpdate(updateVisitor.getTableName(), items);
				}
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
			public int[] getUpdateCounts() throws DataNotAvailableException,
					TranslatorException {
				// TODO Auto-generated method stub
				return new int[] { updatedCount };
			}
		};
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(final QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			final SimpleDBConnection connection) throws TranslatorException {
		// TODO Auto-generated method stub
		return new ResultSetExecution() {
			List<List<String>> list;
			Iterator<List<String>> listIterator;
			
			
			@Override
			public void execute() throws TranslatorException {
				List<String> columns = new ArrayList<String>();
				for (DerivedColumn column : ((Select)command).getDerivedColumns()){
					columns.add(SimpleDBSQLVisitor.getSQLString(column));
				}
				list = connection.getAPIClass().performSelect(SimpleDBSQLVisitor.getSQLString((Select)command), columns);
				listIterator = list.iterator();
				
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
			public List<?> next(){
				try{
					return listIterator.next();
				}catch (NoSuchElementException ex){
					return null;
				}
			}
		};
	}
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory,
			SimpleDBConnection conn) throws TranslatorException {
		List<String> domains = conn.getAPIClass().getDomains();
		for (String domain : domains) {
			Table table = metadataFactory.addTable(domain);
			table.setSupportsUpdate(true);
			Column itemName = new Column();
			itemName.setName("itemName()");
			itemName.setUpdatable(true);
			itemName.setNullType(NullType.No_Nulls);
			Map<String, Datatype> datatypes = metadataFactory.getDataTypes();
			itemName.setDatatype(datatypes.get("String"));
			table.addColumn(itemName);
			for (String attributeName : conn.getAPIClass().getAttributeNames(domain)) {
				Column column = new Column();
				column.setUpdatable(true);
				column.setName(attributeName);
				column.setNullType(NullType.Nullable);
				column.setDatatype(datatypes.get("String"));
				table.addColumn(column);
			}
		}
	}
	
	@Override
	public boolean supportsCompareCriteriaEquals() {
		return true;
	}
	
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}
	
	@Override
	public boolean supportsInCriteria() {
		return true;
	}
	
	@Override
	public boolean supportsIsNullCriteria() {
		return true;
	}
	
	@Override
	public boolean supportsRowLimit() {
		return true;
	}
	
	//byl problem s <>
	@Override
	public boolean supportsNotCriteria() {
		return true;
	}
	
	@Override
	public boolean supportsOrCriteria() {
		return true;
	}
	
	//Protoze vsechny itemy musi mit definovany atribut podle ktereho se radi. (Coz neni zaruceno vzdy)
	/*@Override
	public boolean supportsOrderBy() {
		return true;
	}*/
	
	@Override
	public boolean supportsLikeCriteria() {
		return true;
	}
}
