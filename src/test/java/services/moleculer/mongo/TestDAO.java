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