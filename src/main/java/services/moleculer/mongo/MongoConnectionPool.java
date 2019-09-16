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

import io.datatree.Tree;

/**
 * Connection Pool for MongoDB. <br>
 * <br>
 * Init method: init()<br>
 * Destroy method: destroy()
 */
public class MongoConnectionPool {

	// --- LOGGER ---

	protected static final Logger logger = LoggerFactory.getLogger(MongoConnectionPool.class);

	// --- CONECTION ---

	protected MongoClient mongoClient;
	protected MongoDatabase mongoDatabase;

	// --- CONECTION PARAMETERS ---

	protected String connectionString;
	protected String database = "db";
	protected long connectionTimeout = 3000;

	// --- OPEN CONNECTION ---

	public void init() throws Exception {

		// Release resources
		closeResources(false);

		// Build connection
		if (connectionString == null) {
			logger.info("Opening \"" + database + "\" database at localhost...");
			mongoClient = MongoClients.create();
		} else {
			logger.info("Opening \"" + database + "\" database at " + connectionString + "...");
			ConnectionString cs = new ConnectionString(connectionString);
			mongoClient = MongoClients.create(cs);
		}

		// Check connection
		CollectAllPromise<String> databaseNames = new CollectAllPromise<>(mongoClient.listDatabaseNames());
		Tree collected = databaseNames.waitFor(connectionTimeout);
		logger.info("Databases available: " + collected.get(MongoDAO.ROWS).asList(String.class));
		
		// Open database
		mongoDatabase = mongoClient.getDatabase(database);		
	}

	public void destroy() throws Exception {
		closeResources(true);
	}

	@Override
	protected void finalize() throws Throwable {
		closeResources(false);
	}
	
	protected void closeResources(boolean writeLog) {

		// Close connection
		if (mongoClient != null) {
			if (writeLog) {
				logger.info("Closing \"" + database + "\" database...");
			}
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

	public final long getConnectionTimeout() {
		return connectionTimeout;
	}

	public final void setConnectionTimeout(long connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

}