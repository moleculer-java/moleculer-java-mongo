/**
 * THIS SOFTWARE IS LICENSED UNDER MIT LICENSE.<br>
 * <br>
 * Copyright 2019 Andras Berkes [andras.berkes@programmer.net]<br>
 * Based on Moleculer Framework for NodeJS [https://moleculer.services].
 * <br><br>
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:<br>
 * <br>
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.<br>
 * <br>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package services.moleculer.mongo;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;

import io.datatree.Promise;
import io.datatree.Tree;
import junit.framework.TestCase;

public class MongoTest extends TestCase {

	protected MongoConnectionPool pool;
	protected TestDAO testDAO;
	protected MongoFilters filters = new MongoFilters();

	@Override
	protected void setUp() throws Exception {
		pool = new MongoConnectionPool();
		pool.started();
		testDAO = new TestDAO();
		testDAO.setMongoConnectionPool(pool);

		testDAO.drop().waitFor(2000);

		TestDAO t = new TestDAO();
		t.setMongoConnectionPool(pool);
		t.collection = pool.getMongoDatabase().getCollection("newcollection");
		t.drop();
	}

	@Override
	protected void tearDown() throws Exception {
		if (pool != null) {
			pool.stopped();
			pool = null;
		}
	}

	// ---------------- TESTS ----------------

	@Test
	public void testMongoAPI() throws Exception {

		// Get size of an empty Collection
		assertSize(0);

		// Insert a record
		Tree rsp = testDAO.insertUser("Tom", "Smith").waitFor(2000);
		String id = rsp.asString();
		assertNotNull(id);
		assertTrue(id.length() > 0);
		assertSize(1);

		// Modify record
		boolean ok = testDAO.modifyUserName(id, "Tom", "White").waitFor(2000).asBoolean();
		assertSize(1);
		assertTrue(ok);
		rsp = testDAO.findUserByID(id).waitFor(2000);
		assertEquals("White", rsp.get("lastName", ""));

		// Find
		rsp = testDAO.findUserByFirstName("Tom").waitFor(2000);
		assertEquals("Tom", rsp.get("firstName", ""));
		assertSize(1);

		// Delete
		ok = testDAO.deleteUserByID(id).waitFor(2000).asBoolean();
		assertSize(0);
		assertTrue(ok);

		// drop()
		createTable();
		rsp = testDAO.drop().waitFor(2000);
		printRsp(rsp, "drop");
		assertSize(0);

		// renameCollection(String databaseName, String collectionName)
		createTable();
		String databaseName = "db";
		String collectionName = "newcollection";
		rsp = testDAO.renameCollection(databaseName, collectionName).waitFor(2000);
		printRsp(rsp, "renameCollection");
		assertSize(0);

		// createAscendingIndexes(String... fieldNames)
		createTable();
		rsp = testDAO.createAscendingIndexes("a").waitFor(2000);
		printRsp(rsp, "createAscendingIndexes");

		// createDescendingIndexes(String... fieldNames)
		rsp = testDAO.createDescendingIndexes("b").waitFor(2000);
		printRsp(rsp, "createDescendingIndexes");

		// createHashedIndex(String fieldName)
		rsp = testDAO.createHashedIndex("c").waitFor(2000);
		printRsp(rsp, "createHashedIndex");

		// createTextIndex(String fieldName)
		rsp = testDAO.createTextIndex("d").waitFor(2000);
		printRsp(rsp, "createTextIndex");

		// listIndexes()
		rsp = testDAO.listIndexes().waitFor(2000);
		printRsp(rsp, "listIndexes");
		assertEquals(5, rsp.get("count", 0));

		assertEquals(1, (int) rsp.get("rows[0].key._id").asInteger());
		assertEquals(1, (int) rsp.get("rows[1].key.a").asInteger());
		assertEquals(-1, (int) rsp.get("rows[2].key.b").asInteger());
		assertEquals("hashed", rsp.get("rows[3].key.c").asString());
		assertEquals(1, (int) rsp.get("rows[4].weights.d").asInteger());

		// dropIndex(String indexName)
		rsp = testDAO.dropIndex(rsp.get("rows[1].name").asString()).waitFor(2000);
		printRsp(rsp, "dropIndex");
		rsp = testDAO.listIndexes().waitFor(2000);
		assertEquals(-1, (int) rsp.get("rows[1].key.b").asInteger());
		assertEquals(4, rsp.get("count", 0));

		// dropIndex(String indexName, EDropIndexOptions options)
		DropIndexOptions dio = new DropIndexOptions();
		dio.maxTime(10, TimeUnit.SECONDS);
		rsp = testDAO.dropIndex(rsp.get("rows[1].name").asString(), dio).waitFor(2000);
		printRsp(rsp, "dropIndex");
		rsp = testDAO.listIndexes().waitFor(2000);
		assertEquals("hashed", rsp.get("rows[1].key.c").asString());
		assertEquals(3, rsp.get("count", 0));

		// insertOne(Tree document)
		Tree document = new Tree();
		for (int i = 0; i < 10; i++) {
			document.put("key" + i, "value" + i);
		}
		rsp = testDAO.insertOne(document).waitFor(2000);
		printRsp(rsp, "insertOne");
		id = rsp.get("_id", "");
		assertTrue(id.length() > 0);
		rsp = testDAO.findOne(filters.eq(id)).waitFor(2000);
		for (int i = 0; i < 10; i++) {
			assertEquals("value" + i, rsp.get("key" + i, ""));
		}
		assertSize(11);

		// insertOne(Tree document, InsertOneOptions options)
		InsertOneOptions ioo = new InsertOneOptions();
		ioo.bypassDocumentValidation(false);
		document.remove("_id");
		rsp = testDAO.insertOne(document, ioo).waitFor(2000);
		printRsp(rsp, "insertOne");
		assertSize(12);

		// replaceOne(Tree filter, Tree replacement)
		Tree filter = filters.eq("a", 3);
		Tree replacement = new Tree();
		replacement.put("a", 3);
		replacement.put("d", "new");
		rsp = testDAO.replaceOne(filter, replacement).waitFor(2000);
		printRsp(rsp, "replaceOne");
		assertEquals(1, rsp.get("matched", 0));
		assertEquals(1, rsp.get("modified", 0));
		rsp = testDAO.findOne(filter).waitFor(2000);
		assertEquals(3, rsp.get("a", 0));
		assertEquals("new", rsp.get("d", ""));
		assertEquals(3, rsp.size());

		// replaceOne(Tree filter, Tree replacement, ReplaceOptions options)
		filter = filters.eq("a", 4);
		replacement = new Tree();
		replacement.put("a", 4);
		replacement.put("d", "new2");
		ReplaceOptions ro = new ReplaceOptions();
		ro.bypassDocumentValidation(false);
		rsp = testDAO.replaceOne(filter, replacement, ro).waitFor(2000);
		printRsp(rsp, "replaceOne");
		printRsp(rsp, "replaceOne");
		assertEquals(1, rsp.get("matched", 0));
		assertEquals(1, rsp.get("modified", 0));
		rsp = testDAO.findOne(filters.eq("a", 3)).waitFor(2000);
		assertEquals(3, rsp.get("a", 0));
		assertEquals("new", rsp.get("d", ""));
		assertEquals(3, rsp.size());
		rsp = testDAO.findOne(filters.eq("a", 4)).waitFor(2000);
		assertEquals(4, rsp.get("a", 0));
		assertEquals("new2", rsp.get("d", ""));
		assertEquals(3, rsp.size());

		// updateOne(Tree filter, Tree update)
		filter = filters.eq("a", 5);
		Tree update = new Tree();
		update.put("d", "new3");
		rsp = testDAO.updateOne(filter, update).waitFor(2000);
		printRsp(rsp, "updateOne");
		assertEquals(1, rsp.get("matched", 0));
		assertEquals(1, rsp.get("modified", 0));
		rsp = testDAO.findOne(filters.eq("a", 5)).waitFor(2000);
		assertEquals(5, rsp.get("a", 0));
		assertEquals("new3", rsp.get("d", ""));
		assertEquals(10, rsp.get("b", 0));
		assertFalse(rsp.get("c", true));
		assertEquals(5, rsp.size());

		// updateMany(Tree filter, Tree update)
		createTable();
		filter = filters.lt("a", 5);
		update = new Tree();
		update.put("b", 100);
		rsp = testDAO.updateMany(filter, update).waitFor(2000);
		printRsp(rsp, "updateMany");
		assertEquals(5, rsp.get("matched", 0));
		assertEquals(5, rsp.get("modified", 0));
		rsp = testDAO.find(filters.eq("b", 100), null, 0, 100).waitFor(2000);
		assertEquals(5, rsp.get("count", 0));
		for (Tree row : rsp.get("rows")) {
			assertEquals(100, row.get("b", 0));
			assertTrue(row.get("a", 5) < 5);
		}

		// deleteOne(Tree filter)
		filter = filters.eq("a", 5);
		rsp = testDAO.deleteOne(filter).waitFor(2000);
		assertEquals(1, rsp.get("deleted", 0));
		printRsp(rsp, "deleteOne");
		rsp = testDAO.find(filter, null, 0, 100).waitFor(2000);
		assertEquals(0, rsp.get("count", 1));
		assertEquals(0, rsp.get("rows").size());
		assertSize(9);

		// deleteOne(Tree filter, DeleteOptions options)
		DeleteOptions dop = new DeleteOptions();
		dop.collation(null);
		rsp = testDAO.deleteOne(filter, dop).waitFor(2000);
		printRsp(rsp, "deleteOne");
		assertEquals(0, rsp.get("deleted", 1));
		assertSize(9);

		// deleteMany(Tree filter)
		filter = filters.gt("a", 5);
		rsp = testDAO.deleteMany(filter).waitFor(2000);
		printRsp(rsp, "deleteMany");
		assertEquals(4, rsp.get("deleted", 1));
		assertSize(5);

		// deleteMany(Tree filter, DeleteOptions options)
		filter = filters.gte("a", 5);
		rsp = testDAO.deleteMany(filter, dop).waitFor(2000);
		printRsp(rsp, "deleteMany");
		assertEquals(0, rsp.get("deleted", 0));
		assertSize(5);

		// count()
		for (int i = 0; i < 5; i++) {
			Tree d = new Tree();
			d.put("a", 5 + i);
			d.putList("list").add(1).add(2).add(i);
			testDAO.insertOne(d).waitFor(2000);
		}
		rsp = testDAO.count().waitFor(2000);
		printRsp(rsp, "count");
		assertEquals(10, (long) rsp.asLong());

		// count(Tree filter)
		filter = filters.gte("a", 5);
		rsp = testDAO.count(filter).waitFor(2000);
		printRsp(rsp, "count");
		assertEquals(5, (int) rsp.asInteger());

		// count(Tree filter, CountOptions options)
		filter = filters.or(filters.eq("a", 1), filters.gt("a", 7));
		CountOptions co = new CountOptions();
		co.maxTime(10, TimeUnit.SECONDS);
		rsp = testDAO.count(filter, co).waitFor(2000);
		printRsp(rsp, "count");
		assertEquals(3, (int) rsp.asInteger());

		// findOne(Tree filter)
		filter = filters.eq("d", "val3");
		rsp = testDAO.findOne(filter).waitFor(2000);
		printRsp(rsp, "findOne");
		assertEquals(3, rsp.get("a", 0));
		assertEquals(5, rsp.size());

		// find(Tree filter, Tree sort, int first, int limit)
		filter = filters.gte("a", 0);
		Tree sort = new Tree();
		sort.put("a", -1);
		rsp = testDAO.find(filter, sort, 0, 5).waitFor(2000);
		printRsp(rsp, "find");
		assertEquals(10, rsp.get("count", 0));
		int c = 0;
		for (int i = 9; i > 4; i--) {
			int a = rsp.get("rows").get(c++).get("a", -1);
			assertEquals(i, a);
		}

		// findOneAndDelete(Tree filter)
		filter = filters.eq("d", "val3");
		rsp = testDAO.findOneAndDelete(filter).waitFor(2000);
		printRsp(rsp, "findOneAndDelete");
		assertEquals(3, rsp.get("a", 0));
		assertEquals(5, rsp.size());
		assertSize(9);

		// findOneAndDelete(Tree filter, FindOneAndDeleteOptions options)
		FindOneAndDeleteOptions fodo = new FindOneAndDeleteOptions();
		fodo.maxTime(10, TimeUnit.SECONDS);
		rsp = testDAO.findOneAndDelete(filters.eq("a", 2), fodo).waitFor(2000);
		printRsp(rsp, "findOneAndDelete");
		assertEquals("val2", rsp.get("d", ""));
		assertEquals(5, rsp.size());
		assertSize(8);

		// findOneAndReplace(Tree filter, Tree replacement)
		filter = filters.eq("a", 1);
		replacement = new Tree();
		replacement.put("a", 11);
		rsp = testDAO.findOneAndReplace(filter, replacement).waitFor(2000);
		printRsp(rsp, "findOneAndReplace");
		assertEquals("val1", rsp.get("d", ""));
		assertEquals(5, rsp.size());
		assertSize(8);
		filter = filters.eq("a", 11);
		rsp = testDAO.findOne(filter).waitFor(2000);
		assertEquals(11, rsp.get("a", 0));
		assertEquals(2, rsp.size());

		// findOneAndReplace(Tree filter, Tree replacement,
		// FindOneAndReplaceOptions options)
		FindOneAndReplaceOptions foro = new FindOneAndReplaceOptions();
		foro.bypassDocumentValidation(false);
		replacement.clear();
		replacement.put("a", 12);
		rsp = testDAO.findOneAndReplace(filter, replacement, foro).waitFor(2000);
		printRsp(rsp, "findOneAndReplace");
		assertEquals(11, rsp.get("a", 0));
		assertEquals(2, rsp.size());
		filter = filters.eq("a", 12);
		rsp = testDAO.findOne(filter).waitFor(2000);
		assertEquals(12, rsp.get("a", 0));
		assertEquals(2, rsp.size());
		assertSize(8);

		// findOneAndUpdate(Tree filter, Tree update)
		createTable();
		filter = filters.eq("a", 0);
		update = new Tree();
		update.put("d", "xY");
		rsp = testDAO.findOneAndUpdate(filter, update).waitFor(2000);
		printRsp(rsp, "findOneAndUpdate");
		assertEquals(0, rsp.get("a", 1));
		assertEquals("val0", rsp.get("d", ""));
		assertEquals(5, rsp.size());
		assertSize(10);

		// findOneAndUpdate(Tree filter, Tree update, FindOneAndUpdateOptions
		// options)
		update = new Tree();
		update.put("d", "ZZ");
		FindOneAndUpdateOptions fouo = new FindOneAndUpdateOptions();
		fouo.returnDocument(ReturnDocument.AFTER);
		rsp = testDAO.findOneAndUpdate(filter, update, fouo).waitFor(2000);
		printRsp(rsp, "findOneAndUpdate");
		assertEquals(0, rsp.get("a", 1));
		assertEquals("ZZ", rsp.get("d", ""));
		assertEquals(5, rsp.size());
		
		// Set max items per query
		createTable();
		testDAO.setMaxItemsPerQuery(5);
		assertEquals(5, testDAO.getMaxItemsPerQuery());
		rsp = testDAO.find(null, null).waitFor(2000);
		assertEquals(10, rsp.get("count", 0));
		assertEquals(5, rsp.get("rows").size());

		testDAO.setMaxItemsPerQuery(3);
		assertEquals(3, testDAO.getMaxItemsPerQuery());
		rsp = testDAO.find(null).waitFor(2000);
		assertEquals(10, rsp.get("count", 0));
		assertEquals(3, rsp.get("rows").size());
		
		rsp = testDAO.find(null, null, 2, 0).waitFor(2000);
		assertEquals(10, rsp.get("count", 0));
		assertEquals(3, rsp.get("rows").size());	
		
		// Collection name test
		XyzDAO xyz = new XyzDAO();
		xyz.setMongoConnectionPool(pool);
		assertEquals("xyz", xyz.collection.getNamespace().getCollectionName());
	}

	protected void assertSize(int requiredSize) throws Exception {
		int count = testDAO.count().waitFor(2000).asInteger();
		assertEquals(requiredSize, count);
	}

	protected void printRsp(Tree rsp, String header) {
		System.out.println("--------- " + header.toUpperCase() + " ---------");
		System.out.println(rsp.toString("json", true, true));
	}

	protected void createTable() throws Exception {
		testDAO.deleteAll().waitFor(5000);
		LinkedList<Promise> promises = new LinkedList<>();
		for (int i = 0; i < 10; i++) {
			Tree doc = new Tree();
			doc.put("a", i);
			doc.put("b", i * 2);
			doc.put("c", i % 2 == 0);
			doc.put("d", "val" + i);
			Promise p = testDAO.insertOne(doc);
			promises.add(p);
		}
		Promise.all(promises).waitFor(5000);
		assertSize(10);
	}
}
