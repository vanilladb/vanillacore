package org.vanilladb.core.query.parse;

import org.junit.Test;
import org.vanilladb.core.storage.index.IndexType;

import junit.framework.Assert;

public class ParserTest {
	
	@Test
	public void testIndexCreation() {
		Parser parser = new Parser("CREATE INDEX idx1 ON tbl1 (col1)");
		Object obj = parser.updateCommand();
		
		Assert.assertEquals("ParserTest: Wrong type of output", obj.getClass(), CreateIndexData.class);
		CreateIndexData cid = (CreateIndexData) obj;
		
		// Check each field
		Assert.assertEquals("ParserTest: Wrong index name", cid.indexName(), "idx1");
		Assert.assertEquals("ParserTest: Wrong table name", cid.tableName(), "tbl1");
		Assert.assertEquals("ParserTest: Wrong size of the field list", cid.fieldNames().size(), 1);
		Assert.assertTrue("ParserTest: 'col1' is missing", cid.fieldNames().contains("col1"));
	}
	
	@Test
	public void testMultiKeysIndexCreation() {
		Parser parser = new Parser("CREATE INDEX idx1 ON tbl1 (col1, col2, col3)");
		Object obj = parser.updateCommand();
		
		Assert.assertEquals("ParserTest: Wrong type of output", obj.getClass(), CreateIndexData.class);
		CreateIndexData cid = (CreateIndexData) obj;
		
		// Check each field
		Assert.assertEquals("ParserTest: Wrong index name", cid.indexName(), "idx1");
		Assert.assertEquals("ParserTest: Wrong table name", cid.tableName(), "tbl1");
		Assert.assertEquals("ParserTest: Wrong size of the field list", cid.fieldNames().size(), 3);
		Assert.assertTrue("ParserTest: 'col1' is missing", cid.fieldNames().contains("col1"));
		Assert.assertTrue("ParserTest: 'col2' is missing", cid.fieldNames().contains("col2"));
		Assert.assertTrue("ParserTest: 'col3' is missing", cid.fieldNames().contains("col3"));
	}
	
	@Test
	public void testIndexCreationWithGivenType() {
		Parser parser = new Parser("CREATE INDEX idx1 ON tbl1 (col1) USING HASH");
		Object obj = parser.updateCommand();
		
		Assert.assertEquals("ParserTest: Wrong type of output", obj.getClass(), CreateIndexData.class);
		CreateIndexData cid = (CreateIndexData) obj;
		
		// Check each field
		Assert.assertEquals("ParserTest: Wrong index name", cid.indexName(), "idx1");
		Assert.assertEquals("ParserTest: Wrong table name", cid.tableName(), "tbl1");
		Assert.assertEquals("ParserTest: Wrong size of the field list", cid.fieldNames().size(), 1);
		Assert.assertTrue("ParserTest: 'col1' is missing", cid.fieldNames().contains("col1"));
		Assert.assertEquals("ParserTest: Wrong type of the index", cid.indexType(), IndexType.HASH);
	}
}
