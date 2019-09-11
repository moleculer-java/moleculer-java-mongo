package services.moleculer.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.datatree.Promise;
import io.datatree.Tree;

@MongoCollection("test")
public class TestDAO extends MongoDAO {
	
	// --- LOGGER ---

	protected static final Logger log = LoggerFactory.getLogger(TestDAO.class);

	// --- FIELD NAME CONSTANTS ---
	
	public static final String FIRST_NAME = "firstName";
	public static final String LAST_NAME = "lastName";
	
	// --- SAMPLE METHODS ---
	
	public Promise insertUser(String firstName, String lastName) {
		
		// Create record
		Tree record = new Tree();
		record.put(FIRST_NAME, firstName);
		record.put(LAST_NAME, lastName);
		
		// Insert record
		return insertOne(record).then(in -> {
			return in.get(ID, "");
		});
	}

	public Promise findUserByID(String id) {
		return findOne(eq(id));
	}

	public Promise findUserByFirstName(String firstName) {
		return findOne(eq(FIRST_NAME, firstName));
	}

	public Promise deleteUserByID(String id) {
		return deleteOne(eq(id)).then(in -> {
			
			// Return "true", if success
			return in.get(DELETED, 0) > 0;
		});
	}
	
	public Promise modifyUserName(String id, String firstName, String lastName) {
		
		// Create record
		Tree record = new Tree();
		record.put(FIRST_NAME, firstName);
		record.put(LAST_NAME, lastName);
		
		// Update record
		return updateOne(eq(id), record).then(in -> {
			
			// Return "true", if success
			return in.get(MODIFIED, 0) > 0;
		});
	}
	
}