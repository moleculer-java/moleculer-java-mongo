package services.moleculer.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.datatree.Promise;
import io.datatree.Tree;

@MongoCollection("test")
public class TestDAO extends MongoDAO {
	
	// --- LOGGER ---

	protected static final Logger log = LoggerFactory.getLogger(TestDAO.class);

	// --- SAMPLE METHODS ---
	
	public Promise insertUser(String firstName, String lastName) {
		
		// Create record
		Tree record = new Tree();
		record.put("firstName", firstName);
		record.put("lastName", lastName);
		
		// Insert record
		return insertOne(record);
	}

	public Promise findUserByID(String id) {
		return findOne(eq(id));
	}

	public Promise findUserByFirstName(String firstName) {
		return findOne(eq("firstName", firstName));
	}

	public Promise deleteUserByID(String id) {
		return deleteOne(eq(id));
	}
	
	public Promise modifyUserName(String id, String firstName, String lastName) {
		
		// Create record
		Tree record = new Tree();
		record.put("firstName", firstName);
		record.put("lastName", lastName);
		
		// Update record
		return updateOne(record, eq(id));
	}
	
}