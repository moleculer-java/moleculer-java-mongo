package services.moleculer.mongo;

import org.junit.Test;

import io.datatree.Tree;
import junit.framework.TestCase;

public class MongoTest extends TestCase {

	protected MongoConnectionPool pool;
	protected TestDAO testDAO;
	
	@Override
	protected void setUp() throws Exception {
		pool = new MongoConnectionPool();
		pool.started();
		testDAO = new TestDAO();
		testDAO.setMongoConnectionPool(pool);
		testDAO.drop().waitFor(2000);
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
		checkSize(0);
		
		// Insert a record
		Tree rsp = testDAO.insertUser("Tom", "Smith").waitFor();
		String id = rsp.asString();
		assertNotNull(id);
		assertTrue(id.length() > 0);
		checkSize(1);
		
		// Modify record
		boolean ok = testDAO.modifyUserName(id, "Tom", "White").waitFor().asBoolean();
		checkSize(1);
		assertTrue(ok);
		rsp = testDAO.findUserByID(id).waitFor();
		assertEquals("White", rsp.get("lastName", ""));
		
		// Find
		rsp = testDAO.findUserByFirstName("Tom").waitFor();
		assertEquals("Tom", rsp.get("firstName", ""));
		checkSize(1);
		
		// Delete
		ok = testDAO.deleteUserByID(id).waitFor().asBoolean();
		checkSize(0);
		assertTrue(ok);
		
		// Insert new record
		String id2 = testDAO.insertUser("A", "B").waitFor().asString();
		assertNotNull(id2);
		assertTrue(id2.length() > 0);
		assertNotSame(id, id2);
		id = id2;
		checkSize(1);
		
		// Replace record
		Tree rec = new Tree();
		rec.put("firstName", "C");
		rec.put("lastName", "D");
		testDAO.replaceOne(testDAO.eq(id), rec);
	}
	
	protected void checkSize(int requiredSize) throws Exception {
		int count = testDAO.count().waitFor().asInteger();
		assertEquals(requiredSize, count);		
	}
	
}
