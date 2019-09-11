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

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Connection Pool for MongoDB.
 * <br>
 * Init method: started()<br>
 * Destroy method: stopped()
 */
public class MongoConnectionPool {

	// --- LOGGER ---

	protected static final Logger log = LoggerFactory.getLogger(MongoConnectionPool.class);

	// --- CONECTION ---

	protected MongoClient mongoClient;
	protected MongoDatabase mongoDatabase;

	// --- CONECTION PARAMETERS ---
	
	protected String connectionString;
	protected String database = "db";

	// --- OPEN CONNECTION ---

	public void started() throws Exception {

		// Release resources
		closeResources();

		// Build connection
		if (connectionString == null) {
			log.info("Opening \"" + database + "\" database at localhost...");
			mongoClient = MongoClients.create();
		} else {
			log.info("Opening \"" + database + "\" database at " + connectionString + "...");
			mongoClient = MongoClients.create(new ConnectionString(connectionString));
		}

		// Open database
		mongoDatabase = mongoClient.getDatabase(database);
	}

	public void stopped() throws Exception {
		closeResources();
	}

	protected void closeResources() {

		// Close connection
		if (mongoClient != null) {
			log.info("Closing \"" + database + "\" database...");
			try {
				mongoClient.close();
			} catch (Throwable ignored) {
			}
			mongoClient = null;
		}
	}

	protected MongoDatabase getMongoDatabase() {
		return mongoDatabase;
	}

	// --- GETTERS / SETTERS ---

	public String getConnectionString() {
		return connectionString;
	}

	public void setConnectionString(String connectionString) {
		this.connectionString = connectionString;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}
	
}