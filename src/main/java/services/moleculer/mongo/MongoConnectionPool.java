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

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClientSettings.Builder;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;

/**
 * Init method: started()<br>
 * Destroy method: stopped()
 */
public class MongoConnectionPool {

	// --- LOGGER ---

	protected static final Logger log = LoggerFactory.getLogger(MongoConnectionPool.class);

	// --- CONECTION PARAMETERS ---

	protected MongoClient mongoClient;
	protected MongoDatabase mongoDatabase;
	protected String database = "db";
	protected String servers = "localhost:27017";
	protected long serverSelectionTimeout = 3000;

	// --- WORKING PARAMETERS ---

	protected long heartbeatFrequency = 10000;
	protected long minHeartbeatFrequency = 500;

	// default, local, vagy majority
	protected String readConcern = "default";

	// nearest, primary, secondary, primaryPreferred, secondaryPreferred
	protected String readPreference = "primaryPreferred";

	// acknowledged, journaled, majority, unacknowledged
	protected String writeConcern = "acknowledged";

	// standalone, sharded, replica-set
	protected String clusterType = "standalone";

	// single, multiple
	protected String clusterConnectionMode = "single";

	// --- AUTHENTICATION ---

	protected boolean authenticated = false;

	protected String authUser = "user";
	protected String authPassword = "password";
	protected String authDatabase = "users";

	// --- CONNECTION POOL PARAMETERS ---

	protected int maxSize = 100;
	protected int minSize = 0;
	protected int maxWaitQueueSize = 500;
	protected long maxWaitTime = 2000;
	protected long maxConnectionLifeTime = 0;
	protected long maxConnectionIdleTime = 0;
	protected long maintenanceInitialDelay = 0;
	protected long maintenanceFrequency = 60000;
	
	// --- OPEN CONNECTION ---

	public void started() throws Exception {
		
		// Release resources
		closeResources();
		
		// Set cluster
		ClusterSettings.Builder clusterSettings = ClusterSettings.builder();
		clusterSettings.description(new ObjectId().toHexString());
		LinkedList<ServerAddress> hosts = new LinkedList<ServerAddress>();

		// Comma separated list of servers, eg.
		// "localhost:50432,host1,host2:234234"
		String[] tokens = servers.split(",");
		for (String token : tokens) {
			ServerAddress address;
			int i = token.indexOf(':');
			if (i > 0) {
				address = new ServerAddress(token.substring(0, i), Integer.parseInt(token.substring(i + 1)));
			} else {
				address = new ServerAddress(token);
			}
			hosts.addLast(address);
		}
		clusterSettings.hosts(hosts);

		// Connection timeout
		clusterSettings.serverSelectionTimeout(serverSelectionTimeout, TimeUnit.MILLISECONDS);

		// single, multiple
		if (clusterConnectionMode == null || clusterConnectionMode.equalsIgnoreCase("single")) {
			clusterSettings.mode(ClusterConnectionMode.SINGLE);
		} else {
			clusterSettings.mode(ClusterConnectionMode.MULTIPLE);
		}

		// standalone, sharded, replica-set
		if (clusterType == null || clusterType.equalsIgnoreCase("standalone")) {
			clusterSettings.requiredClusterType(ClusterType.STANDALONE);
		} else if (clusterType.equalsIgnoreCase("sharded")) {
			clusterSettings.requiredClusterType(ClusterType.SHARDED);
		} else {
			clusterSettings.requiredClusterType(ClusterType.REPLICA_SET);
		}

		// Connection properties
		ServerSettings.Builder serverSettings = ServerSettings.builder();
		serverSettings.heartbeatFrequency(heartbeatFrequency, TimeUnit.MILLISECONDS);
		serverSettings.minHeartbeatFrequency(minHeartbeatFrequency, TimeUnit.MILLISECONDS);

		// Pool parameters
		ConnectionPoolSettings.Builder connectionPoolSettings = ConnectionPoolSettings.builder();
		connectionPoolSettings.maxSize(maxSize);
		connectionPoolSettings.minSize(minSize);
		connectionPoolSettings.maxWaitQueueSize(maxWaitQueueSize);
		connectionPoolSettings.maxWaitTime(maxWaitTime, TimeUnit.MILLISECONDS);
		connectionPoolSettings.maxConnectionLifeTime(maxConnectionLifeTime, TimeUnit.MILLISECONDS);
		connectionPoolSettings.maxConnectionIdleTime(maxConnectionIdleTime, TimeUnit.MILLISECONDS);
		connectionPoolSettings.maintenanceInitialDelay(maintenanceInitialDelay, TimeUnit.MILLISECONDS);
		connectionPoolSettings.maintenanceFrequency(maintenanceFrequency, TimeUnit.MILLISECONDS);

		// Mongo client parameters
		Builder clientSettings = MongoClientSettings.builder();
		clientSettings.clusterSettings(clusterSettings.build());
		clientSettings.connectionPoolSettings(connectionPoolSettings.build());
		clientSettings.serverSettings(serverSettings.build());

		// default, local, vagy majority
		if (readConcern == null || readConcern.equalsIgnoreCase("default")) {
			clientSettings.readConcern(ReadConcern.DEFAULT);
		} else if (readConcern.equalsIgnoreCase("local")) {
			clientSettings.readConcern(ReadConcern.LOCAL);
		} else {
			clientSettings.readConcern(ReadConcern.MAJORITY);
		}

		// nearest, primary, secondary, primaryPreferred, secondaryPreferred
		if (readPreference == null || readPreference.equalsIgnoreCase("nearest")) {
			clientSettings.readPreference(ReadPreference.nearest());
		} else if (readPreference.equalsIgnoreCase("primary")) {
			clientSettings.readPreference(ReadPreference.primary());
		} else if (readPreference.equalsIgnoreCase("secondary")) {
			clientSettings.readPreference(ReadPreference.secondary());
		} else if (readPreference.equalsIgnoreCase("primaryPreferred")) {
			clientSettings.readPreference(ReadPreference.primaryPreferred());
		} else {
			clientSettings.readPreference(ReadPreference.secondaryPreferred());
		}

		// acknowledged, journaled, majority, unacknowledged
		if (writeConcern == null || writeConcern.equalsIgnoreCase("acknowledged")) {
			clientSettings.writeConcern(WriteConcern.ACKNOWLEDGED);
		} else if (writeConcern.equalsIgnoreCase("journaled")) {
			clientSettings.writeConcern(WriteConcern.JOURNALED);
		} else if (writeConcern.equalsIgnoreCase("majority")) {
			clientSettings.writeConcern(WriteConcern.MAJORITY);
		} else {
			clientSettings.writeConcern(WriteConcern.UNACKNOWLEDGED);
		}

		// Authentication
		if (authenticated) {
			MongoCredential credential = MongoCredential.createCredential(authUser, authDatabase,
					authPassword.toCharArray());
			clientSettings.credential(credential);
		}

		// Build connection
		log.info("Opening \"" + database + "\" database at " + servers + "...");
		mongoClient = MongoClients.create(clientSettings.build());

		// Open database
		mongoDatabase = mongoClient.getDatabase(database);
	}

	public void stopped() throws Exception {
		closeResources();
	}

	protected void closeResources() {
		
		// Close connection
		if (mongoClient == null) {
			return;
		}
		log.info("Closing MongoDB connection...");
		try {
			mongoClient.close();
		} catch (Throwable ignored) {
		}
		mongoClient = null;
	}
	
	public MongoDatabase getDatabase() {
		return mongoDatabase;
	}
	
}