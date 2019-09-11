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

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import io.datatree.Promise;
import io.datatree.Tree;

/**
 * Superclass of all Mongo handlers. Sample DAO class for handling "User" entities:
 * 
 * <pre>
 * @MongoCollection("user")
 * public class UserDAO extends MongoDAO {
 * 
 *   public Promise insertUser(String firstName, String lastName) {
 *     Tree doc = new Tree();
 *     doc.put("firstName", firstName);
 *     doc.put("lastName", lastName);
 *     return insertOne(doc).then(res -&gt; {
 *       return res.get("_id", "");
 *     });
 *   }
 * 
 *   public Promise findUserById(String id) {
 *     return findOne(eq(id));
 *   }
 *
 *   public Promise deleteUserById(String id) {
 *     return deleteOne(eq(id)).then(res -&gt; {
 *       return res.get("deleted", 0) > 0;
 *     });
 *   }
 *   
 * }
 * </pre>
 * Example of using the code above:
 * <pre>
 * userDAO.insertUser("Tom", "Smith").then(res -&gt; {
 *   System.out.println("Record ID: " + res.asString());
 * });
 * </pre>
 */
public class MongoDAO extends MongoFilters {

	// --- FIELD NAME CONSTANTS ---

	// Root node of rows
	public static final String ROWS = "rows";

	// Row counter
	public static final String COUNT = "count";

	// Set function
	public static final String SET = "$set";

	// ObjectID
	public static final String ID = "_id";

	// Number of matched rows
	public static final String MATCHED = "matched";

	// Number of modified rows
	public static final String MODIFIED = "modified";

	// Number of deleted rows
	public static final String DELETED = "deleted";

	// True if the write was acknowledged
	public static final String ACKNOWLEDGED = "acknowledged";

	// --- COLLECTION OF THIS DAO ---

	protected MongoCollection<Document> collection;

	protected int maxItemsPerQuery = 1024 * 10;

	// --- GET CONNECTION AND COLLECTION ---

	/**
	 * Sets the MongoConnectionPool of this DAO. Processes the @MongoCollection
	 * annotation. If no annotation is specified, generates the collection name
	 * from the Class name (eg from "UserDAO" to "user").
	 * 
	 * @param pool
	 *            the MongoConnectionPool to use
	 */
	public void setMongoConnectionPool(MongoConnectionPool pool) {

		// Set collection by the @MongoCollection annotation
		// Eg. @MongoCollection("users")
		Class<? extends MongoDAO> c = getClass();
		if (c.isAnnotationPresent(services.moleculer.mongo.MongoCollection.class)) {
			services.moleculer.mongo.MongoCollection annotation = c
					.getAnnotation(services.moleculer.mongo.MongoCollection.class);
			String collectionName = annotation.value();
			if (collectionName != null) {
				collectionName = collectionName.trim();
				collection = pool.getMongoDatabase().getCollection(collectionName);
			}
		}
		if (collection == null) {

			// Generate collection name by class name
			// Eg. "UserDAO" -> "user"
			String className = c.getName().toLowerCase();
			int i = className.lastIndexOf('.');
			if (i > -1) {
				className = className.substring(i + 1);
			}
			if (className.endsWith("dao")) {
				className = className.substring(0, className.length() - 3);
			}
			collection = pool.getMongoDatabase().getCollection(className);
		}
	}

	// --- DROP COLLECTION ---

	/**
	 * Drops this collection from the Database. Sample of usage:
	 * 
	 * <pre>
	 * drop().then(res -&gt; {
	 *  // Drop operation finished
	 * });
	 * </pre>
	 *
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise drop() {
		return singleResult(collection.drop());
	}

	// --- RENAME COLLECTION ---

	/**
	 * Rename the collection. Sample of usage:
	 * 
	 * <pre>
	 * renameCollection("db", "collection").then(res -&gt; {
	 *  // Rename operation finished
	 * });
	 * </pre>
	 *
	 * @param databaseName
	 *            name of the database
	 * @param collectionName
	 *            new collection name
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise renameCollection(String databaseName, String collectionName) {
		return singleResult(collection.renameCollection(new MongoNamespace(databaseName, collectionName)));
	}

	// --- CREATE INDEX(ES) ---

	/**
	 * Creates ascending indexes. Sample of usage:
	 * 
	 * <pre>
	 * createAscendingIndexes("field1", "field2").then(res -&gt; {
	 *  // Index created successfully
	 * }).then(res -&gt; {
	 *  // ...
	 * }).then(res -&gt; {
	 *  // ...
	 * }).catchError(err -&gt; {
	 *  // Error handler
	 * });
	 * </pre>
	 *
	 * @param fieldNames
	 *            field names
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise createAscendingIndexes(String... fieldNames) {
		return createIndexes(Indexes.ascending(fieldNames));
	}

	/**
	 * Creates descending indexes. Sample of usage:
	 * 
	 * <pre>
	 * createDescendingIndexes("field1", "field2").then(res -&gt; {
	 *  // Index created successfully
	 * });
	 * </pre>
	 *
	 * @param fieldNames
	 *            field names
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise createDescendingIndexes(String... fieldNames) {
		return createIndexes(Indexes.descending(fieldNames));
	}

	/**
	 * Creates 2dsphere indexes. Sample of usage:
	 * 
	 * <pre>
	 * createGeo2DSphereIndexes("field1", "field2").then(res -&gt; {
	 *  // Index created successfully
	 * });
	 * </pre>
	 *
	 * @param fieldNames
	 *            field names
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise createGeo2DSphereIndexes(String... fieldNames) {
		return createIndexes(Indexes.geo2dsphere(fieldNames));
	}

	/**
	 * Creates a geo2d index. Sample of usage:
	 * 
	 * <pre>
	 * createGeo2DIndex("field1").then(res -&gt; {
	 *  // Index created successfully
	 * });
	 * </pre>
	 *
	 * @param fieldName
	 *            field name
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise createGeo2DIndex(String fieldName) {
		return createIndexes(Indexes.geo2d(fieldName));
	}

	/**
	 * Creates a hashed index. Sample of usage:
	 * 
	 * <pre>
	 * createHashedIndex("field1").then(res -&gt; {
	 *  // Index created successfully
	 * });
	 * </pre>
	 *
	 * @param fieldName
	 *            field name
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise createHashedIndex(String fieldName) {
		return createIndexes(Indexes.hashed(fieldName));
	}

	/**
	 * Creates a text index. Sample of usage:
	 * 
	 * <pre>
	 * createTextIndex("field1").then(res -&gt; {
	 *  // Index created successfully
	 * }).then(res -&gt; {
	 *  // ...
	 * }).then(res -&gt; {
	 *  // ...
	 * }).catchError(err -&gt; {
	 *  // Error handler
	 * });
	 * </pre>
	 *
	 * @param fieldName
	 *            field name
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise createTextIndex(String fieldName) {
		return createIndexes(Indexes.text(fieldName));
	}

	/**
	 * Creates an index by the specified Tree object. Sample of usage:
	 * 
	 * <pre>
	 * BsonTree indexes = new BsonTree(Indexes.text("field1"));
	 * createIndexes(indexes).then(res -&gt; {
	 *  // Index created successfully
	 * });
	 * </pre>
	 *
	 * @param indexes
	 *            list of fields
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise createIndexes(Tree indexes) {
		return createIndexes(toBson(indexes));
	}

	/**
	 * Creates an index by the specified Bson object. Sample of usage:
	 * 
	 * <pre>
	 * createIndexes(Indexes.text("field1")).then(res -&gt; {
	 *  // Index created successfully
	 * });
	 * </pre>
	 *
	 * @param key
	 *            an object describing the index key(s), which may not be null.
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise createIndexes(Bson key) {
		return singleResult(collection.createIndex(key));
	}

	// --- LIST INDEXES ---

	/**
	 * Get all the indexes in this collection.<br>
	 * <br>
	 * Sample return structure:
	 * 
	 * <pre>
	 * {
	 *   "count": 3,
	 *   "rows": ["collection1", "collection2", "collection3"]
	 * }
	 * </pre>
	 * 
	 * Sample of usage:
	 * 
	 * <pre>
	 * listIndexes().then(res -&gt; {
	 *  // Operation finished
	 * });
	 * </pre>
	 *
	 * @return a Promise with the list of indexes, and number of indexes
	 */
	protected Promise listIndexes() {
		return collectAll(collection.listIndexes());
	}

	// --- DROP INDEX ---

	/**
	 * Drops the given index. Sample of usage:
	 * 
	 * <pre>
	 * dropIndex("field1").then(res -&gt; {
	 *  // Drop index operation finished
	 * }).then(res -&gt; {
	 *  // ...
	 * }).then(res -&gt; {
	 *  // ...
	 * }).catchError(err -&gt; {
	 *  // Error handler
	 * });
	 * </pre>
	 *
	 * @param indexName
	 *            the name of the index to remove
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise dropIndex(String indexName) {
		return singleResult(collection.dropIndex(indexName));
	}

	/**
	 * Drops the given index. Sample of usage:
	 * 
	 * <pre>
	 * DropIndexOptions opts = new DropIndexOptions();
	 * opts.maxTime(10, TimeUnit.SECONDS);
	 * 
	 * dropIndex("field1", opts).then(res -&gt; {
	 *  // Drop index operation finished
	 * });
	 * </pre>
	 *
	 * @param indexName
	 *            the name of the index to remove
	 * @param options
	 *            options to use when dropping indexes
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise dropIndex(String indexName, DropIndexOptions options) {
		return singleResult(collection.dropIndex(indexName, options));
	}

	// --- INSERT ONE RECORD ---

	/**
	 * Inserts the provided document. If the document is missing an identifier,
	 * the driver should generate one. Sample of usage:
	 * 
	 * <pre>
	 * Tree doc = new Tree();
	 * doc.put("field1", 123);
	 * doc.put("field2.subfield", false);
	 * 
	 * insertOne(doc).then(res -&gt; {
	 * 
	 *  // Insert operation finished
	 *  String id = res.get("_id", "");
	 *  return id;
	 *  
	 * });
	 * </pre>
	 *
	 * @param document
	 *            the document to insert
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise insertOne(Tree document) {
		Document doc = (Document) toBson(document);
		return singleResult(collection.insertOne(doc)).then(in -> {
			return doc;
		});
	}

	/**
	 * Inserts the provided document. If the document is missing an identifier,
	 * the driver should generate one. Sample of usage:
	 * 
	 * <pre>
	 * Tree doc = new Tree();
	 * doc.put("field1", 123);
	 * 
	 * InsertOneOptions opts = new InsertOneOptions();
	 * opts.bypassDocumentValidation(false);
	 * 
	 * insertOne(doc, opts).then(res -&gt; {
	 * 
	 *  // Insert operation finished
	 *  String id = res.get("_id", "");
	 *  return id;
	 *  
	 * });
	 * </pre>
	 *
	 * @param document
	 *            the document to insert
	 * @param options
	 *            the options to apply to the operation
	 * @return a Promise with a single element indicating when the operation has
	 *         completed
	 */
	protected Promise insertOne(Tree document, InsertOneOptions options) {
		Document doc = (Document) toBson(document);
		return singleResult(collection.insertOne(doc, options)).then(in -> {
			return doc;
		});
	}

	// --- REPLACE ONE ---

	/**
	 * Replace a document in the collection according to the specified
	 * arguments. <br>
	 * Sample to update result structure:
	 * 
	 * <pre>
	 * {
	 *   "matched": 10,
	 *   "modified": 4,
	 *   "acknowledged": true
	 * }
	 * </pre>
	 *
	 * Sample of usage:
	 * 
	 * <pre>
	 * Tree replacement = new Tree();
	 * replacement.put("field1", 123);
	 * 
	 * replaceOne(eq("field1", 123), replacement).then(res -&gt; {
	 * 
	 *  // Replace operation finished
	 *  int modified = res.get("modified");
	 *  return modified > 0;
	 *  
	 * }).then(res -&gt; {
	 *  // ...
	 * }).then(res -&gt; {
	 *  // ...
	 * }).catchError(err -&gt; {
	 *  // Error handler
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter to apply the the replace operation
	 * @param replacement
	 *            the replacement document
	 * @return a Promise with a single element the update result structure
	 */
	protected Promise replaceOne(Tree filter, Tree replacement) {
		return singleResult(collection.replaceOne(toBson(filter), (Document) toBson(replacement)))
				.thenWithResult(res -> {
					return updateResultToTree(res);
				});
	}

	/**
	 * Replace a document in the collection according to the specified
	 * arguments. <br>
	 * Sample to update result structure:
	 * 
	 * <pre>
	 * {
	 *   "matched": 10,
	 *   "modified": 4,
	 *   "acknowledged": true
	 * }
	 * </pre>
	 *
	 * Sample of usage:
	 * 
	 * <pre>
	 * Tree replacement = new Tree();
	 * replacement.put("field1", 123);
	 * 
	 * ReplaceOptions opts = new ReplaceOptions();
	 * opts.bypassDocumentValidation(false);
	 * 
	 * replaceOne(eq("field1", 123), replacement, opts).then(res -&gt; {
	 * 
	 *  // Replace operation finished
	 *  int modified = res.get("modified");
	 *  return modified > 0;
	 *  
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter to apply the the replace operation
	 * @param replacement
	 *            the replacement document
	 * @param options
	 *            the options to apply to the replace operation
	 * @return a Promise with a single element the update result structure
	 */
	protected Promise replaceOne(Tree filter, Tree replacement, ReplaceOptions options) {
		return singleResult(collection.replaceOne(toBson(filter), (Document) toBson(replacement), options))
				.thenWithResult(res -> {
					return updateResultToTree(res);
				});
	}

	// --- UPDATE ONE ---

	/**
	 * Update a single document in the collection according to the specified
	 * arguments. <br>
	 * Sample to update result structure:
	 * 
	 * <pre>
	 * {
	 *   "matched": 10,
	 *   "modified": 4,
	 *   "acknowledged": true
	 * }
	 * </pre>
	 *
	 * Sample of usage:
	 * 
	 * <pre>
	 * Tree update = new Tree();
	 * update.put("field1", 123);
	 * 
	 * updateOne(eq("field1", 123), update).then(res -&gt; {
	 * 
	 *  // Replace operation finished
	 *  int modified = res.get("modified");
	 *  return modified > 0;
	 *  
	 * }).then(res -&gt; {
	 *  // ...
	 * }).then(res -&gt; {
	 *  // ...
	 * }).catchError(err -&gt; {
	 *  // Error handler
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            a document describing the query filter, which may not be null.
	 * @param update
	 *            a document describing the update, which may not be null. The
	 *            update to apply must include only update operators.
	 * @return a Promise with a single element the update result structure
	 */
	protected Promise updateOne(Tree filter, Tree update) {
		Tree rec = prepareForUpdate(update);
		return singleResult(collection.updateOne(toBson(filter), toBson(rec))).thenWithResult(res -> {
			return updateResultToTree(res);
		});
	}

	// --- UPDATE MANY ---

	/**
	 * Update all documents in the collection according to the specified
	 * arguments. <br>
	 * Sample to update result structure:
	 * 
	 * <pre>
	 * {
	 *   "matched": 10,
	 *   "modified": 4,
	 *   "acknowledged": true
	 * }
	 * </pre>
	 *
	 * Sample of usage:
	 * 
	 * <pre>
	 * Tree update = new Tree();
	 * update.put("field1", 123);
	 * 
	 * updateMany(eq("field1", 123), update).then(res -&gt; {
	 * 
	 *  // Replace operation finished
	 *  int modified = res.get("modified");
	 *  return modified > 0;
	 *  
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            a document describing the query filter, which may not be null.
	 * @param update
	 *            a document describing the update, which may not be null. The
	 *            update to apply must include only update operators.
	 * @return a Promise with a single element the update result structure
	 */
	protected Promise updateMany(Tree filter, Tree update) {
		Tree rec = prepareForUpdate(update);
		return singleResult(collection.updateMany(toBson(filter), toBson(rec))).thenWithResult(res -> {
			return updateResultToTree(res);
		});
	}

	// --- DELETE ONE ---

	/**
	 * Removes at most one document from the collection that matches the given
	 * filter. If no documents match, the collection is not modified.<br>
	 * Sample to delete result structure:
	 * 
	 * <pre>
	 * {
	 *   "matched": 10,
	 *   "deleted": 4,
	 *   "acknowledged": true
	 * }
	 * </pre>
	 *
	 * Sample of usage:
	 * 
	 * <pre>
	 * deleteOne(eq("field1", 123)).then(res -&gt; {
	 * 
	 *  // Delete operation finished
	 *  int deleted = res.get("deleted");
	 *  return deleted > 0;
	 *  
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter to apply the the delete operation
	 * @return a Promise with a single element the deleted result structure
	 */
	protected Promise deleteOne(Tree filter) {
		return singleResult(collection.deleteOne(toBson(filter))).thenWithResult(res -> {
			return deleteResultToTree(res);
		});
	}

	/**
	 * Removes at most one document from the collection that matches the given
	 * filter. If no documents match, the collection is not modified.<br>
	 * Sample to delete result structure:
	 * 
	 * <pre>
	 * {
	 *   "matched": 10,
	 *   "deleted": 4,
	 *   "acknowledged": true
	 * }
	 * </pre>
	 *
	 * Sample of usage:
	 * 
	 * <pre>
	 * DeleteOptions opts = new DeleteOptions();
	 * opts.collation(...);
	 * 
	 * deleteOne(eq("field1", 123), opts).then(res -&gt; {
	 * 
	 *  // Delete operation finished
	 *  int deleted = res.get("deleted");
	 *  return deleted > 0;
	 *  
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter to apply the the delete operation
	 * @param options
	 *            the options to apply to the delete operation
	 * @return a Promise with a single element the deleted result structure
	 */
	protected Promise deleteOne(Tree filter, DeleteOptions options) {
		return singleResult(collection.deleteOne(toBson(filter), options)).thenWithResult(res -> {
			return deleteResultToTree(res);
		});
	}

	// --- DELETE MANY ---

	/**
	 * Removes all documents from the collection that match the given query
	 * filter. If no documents match, the collection is not modified.<br>
	 * Sample to delete result structure:
	 * 
	 * <pre>
	 * {
	 *   "matched": 10,
	 *   "deleted": 4,
	 *   "acknowledged": true
	 * }
	 * </pre>
	 *
	 * Sample of usage:
	 * 
	 * <pre>
	 * deleteResultToTree(eq("field1", 123)).then(res -&gt; {
	 * 
	 *  // Delete operation finished
	 *  int deleted = res.get("deleted");
	 *  return deleted > 0;
	 *  
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter to apply the the delete operation
	 * @return a Promise with a single element the deleted result structure
	 */
	protected Promise deleteMany(Tree filter) {
		return singleResult(collection.deleteMany(toBson(filter))).thenWithResult(res -> {
			return deleteResultToTree(res);
		});
	}

	/**
	 * Removes all documents from the collection that match the given query
	 * filter. If no documents match, the collection is not modified.<br>
	 * Sample to delete result structure:
	 * 
	 * <pre>
	 * {
	 *   "matched": 10,
	 *   "deleted": 4,
	 *   "acknowledged": true
	 * }
	 * </pre>
	 *
	 * Sample of usage:
	 * 
	 * <pre>
	 * DeleteOptions opts = new DeleteOptions();
	 * opts.collation(...);
	 * 
	 * deleteResultToTree(eq("field1", 123), opts).then(res -&gt; {
	 * 
	 *  // Delete operation finished
	 *  int deleted = res.get("deleted");
	 *  return deleted > 0;
	 *  
	 * }).then(res -&gt; {
	 *  // ...
	 * }).then(res -&gt; {
	 *  // ...
	 * }).catchError(err -&gt; {
	 *  // Error handler
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter to apply the the delete operation
	 * @param options
	 *            the options to apply to the delete operation
	 * @return a Promise with a single element the deleted result structure
	 */
	protected Promise deleteMany(Tree filter, DeleteOptions options) {
		return singleResult(collection.deleteMany(toBson(filter), options)).thenWithResult(res -> {
			return deleteResultToTree(res);
		});
	}

	// --- COUNTS ---

	/**
	 * Counts the number of documents in the collection. Sample of usage:
	 * 
	 * <pre>
	 * count().then(res -&gt; {
	 * 
	 *  // Count operation finished
	 *  long numberOfDocuments = res.asLong();
	 *  return numberOfDocuments + " documents found.";
	 * 
	 * });
	 * </pre>
	 * 
	 * @return a Promise with a single element indicating the number of
	 *         documents
	 */
	protected Promise count() {
		return singleResult(collection.countDocuments());
	}

	/**
	 * Counts the number of documents in the collection according to the given
	 * filters. Sample of usage:
	 * 
	 * <pre>
	 * count(eq("field1", 123)).then(res -&gt; {
	 * 
	 *  // Count operation finished
	 *  long numberOfDocuments = res.asLong();
	 *  
	 *  Tree rsp = new Tree();
	 *  rsp.put("count", numberOfDocuments);
	 *  return rsp;
	 * 
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter
	 * @return a Promise with a single element indicating the number of
	 *         documents
	 */
	protected Promise count(Tree filter) {
		return singleResult(collection.countDocuments(toBson(filter)));
	}

	/**
	 * Counts the number of documents in the collection according to the given
	 * options. Sample of usage:
	 * 
	 * <pre>
	 * CountOptions opts = new CountOptions();
	 * opts.maxTime(10, TimeUnit.SECONDS);
	 * 
	 * count(eq("field1", 123), opts).then(res -&gt; {
	 * 
	 *  // Count operation finished
	 *  long numberOfDocuments = res.asLong();
	 *  
	 *  Tree rsp = new Tree();
	 *  rsp.put("count", numberOfDocuments);
	 *  return rsp;
	 * 
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter
	 * @param options
	 *            the options describing the count
	 * @return a Promise with a single element indicating the number of
	 *         documents
	 */
	protected Promise count(Tree filter, CountOptions options) {
		return singleResult(collection.countDocuments(toBson(filter), options));
	}

	// --- FIND ONE ---

	/**
	 * Finds one document by the specified query filter. Sample of usage:
	 * 
	 * <pre>
	 * findOne(eq("field1", 123)).then(res -&gt; {
	 * 
	 *  // Find operation finished
	 *  if (res != null) {
	 *    String firstName = res.get("firstName", "");
	 *    int age = res.get("age", 0);
	 *  }
	 *  return res;
	 *  
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter
	 * @return a Promise with the selected document.
	 */
	protected Promise findOne(Tree filter) {
		FindPublisher<Document> find = collection.find();
		if (filter != null) {
			find.filter(toBson(filter));
		}
		find.batchSize(1);
		return singleResult(find.first());
	}

	// --- FIND MANY ---

	/**
	 * Queries the specified number of records from the collection. Sample of
	 * usage:
	 * 
	 * <pre>
	 * find(eq("field1", 123), null, 0, 10).then(res -&gt; {
	 * 
	 *  // Find operation finished
	 *  int maxNumberOfSelectableDocuments = res.get("count");
	 *  for (Tree doc: res.get("rows")) {
	 *    String firstName = res.get("firstName", "");
	 *  }
	 *  return res;
	 * 
	 * }).then(res -&gt; {
	 *  // ...
	 * }).then(res -&gt; {
	 *  // ...
	 * }).catchError(err -&gt; {
	 *  // Error handler
	 * });
	 * </pre>
	 * 
	 * @param filter
	 *            the query filter
	 * @param sort
	 *            sort filter (or null)
	 * @param first
	 *            number of skipped documents (0 = get from the first record)
	 * @param limit
	 *            number of retrieved documents
	 * @return a Promise with the selected documents and the max number of
	 *         selectable documents.
	 */
	protected Promise find(Tree filter, Tree sort, int first, int limit) {

		// Set query limit
		final int l;
		if (limit < 1 || limit > maxItemsPerQuery) {
			l = maxItemsPerQuery;
		} else {
			l = limit;
		}

		// Find
		FindPublisher<Document> find = collection.find();
		Bson countFilter = filter == null ? null : toBson(filter);
		if (filter != null) {
			find.filter(countFilter);
		}
		if (sort != null) {
			find.sort(toBson(sort));
		}
		if (first > 0) {
			find.skip(first);
		}
		find.batchSize(Math.min(l, 50));
		find.limit(l);

		// Get rows and size
		return collectAll(find).then(rows -> {
			if (countFilter == null) {
				return singleResult(collection.countDocuments()).thenWithResult(max -> {
					rows.put(COUNT, max);
					return rows;
				});
			}
			return singleResult(collection.countDocuments(countFilter)).thenWithResult(max -> {
				rows.put(COUNT, max);
				return rows;
			});
		});
	}

	// --- FIND ONE AND DELETE ---

	/**
	 * Atomically find a document and remove it. Sample of usage:
	 * 
	 * <pre>
	 * findOneAndDelete(eq("field1", 123)).then(res -&gt; {
	 * 
	 *  // Delete operation finished
	 *  if (res != null) {
	 *    String firstName = res.get("firstName", "");
	 *    int age = res.get("age", 0);
	 *  }
	 *  return res;
	 *  
	 * });
	 * </pre>
	 *
	 * @param filter
	 *            the query filter to find the document with
	 * @return a Promise with a single element the document that was removed. If
	 *         no documents matched the query filter, then null will be returned
	 */
	protected Promise findOneAndDelete(Tree filter) {
		return singleResult(collection.findOneAndDelete(toBson(filter)));
	}

	/**
	 * Atomically find a document and remove it. Sample of usage:
	 * 
	 * <pre>
	 * FindOneAndDeleteOptions opts = new FindOneAndDeleteOptions();
	 * opts.maxTime(10, TimeUnit.SECONDS);
	 * 
	 * findOneAndDelete(eq("field1", 123), opts).then(res -&gt; {
	 * 
	 *  // Delete operation finished
	 *  if (res != null) {
	 *    String firstName = res.get("firstName", "");
	 *    int age = res.get("age", 0);
	 *  }
	 *  return res;
	 *  
	 * });
	 * </pre>
	 *
	 * @param filter
	 *            the query filter to find the document with
	 * @param options
	 *            the options to apply to the operation
	 * @return a Promise with a single element the document that was removed. If
	 *         no documents matched the query filter, then null will be returned
	 */
	protected Promise findOneAndDelete(Tree filter, FindOneAndDeleteOptions options) {
		return singleResult(collection.findOneAndDelete(toBson(filter), options));
	}

	// --- FIND ONE AND REPLACE ---

	/**
	 * Atomically find a document and replace it. Sample of usage:
	 * 
	 * <pre>
	 * Tree replacement = new Tree();
	 * replacement.put("field1", 123);
	 * 
	 * findOneAndReplace(eq("field1", 123), replacement).then(res -&gt; {
	 * 
	 *  // Replace operation finished
	 *  if (res != null) {
	 *    String firstName = res.get("firstName", "");
	 *    int age = res.get("age", 0);
	 *  }
	 *  return res;
	 *  
	 * });
	 * </pre>
	 *
	 * @param filter
	 *            the query filter to apply the the replace operation
	 * @param replacement
	 *            the replacement document
	 * @return a Promise with a single element the document that was replaced.
	 *         If no documents matched the query filter, then null will be
	 *         returned
	 */
	protected Promise findOneAndReplace(Tree filter, Tree replacement) {
		return singleResult(collection.findOneAndReplace(toBson(filter), (Document) toBson(replacement)));
	}

	/**
	 * Atomically find a document and replace it. Sample of usage:
	 * 
	 * <pre>
	 * Tree replacement = new Tree();
	 * replacement.put("field1", 123);
	 * 
	 * FindOneAndReplaceOptions opts = new FindOneAndReplaceOptions();
	 * opts.upsert(true);
	 * 
	 * findOneAndReplace(eq("field1", 123), replacement, opts).then(res -&gt; {
	 * 
	 *  // Replace operation finished
	 *  if (res != null) {
	 *    String firstName = res.get("firstName", "");
	 *    int age = res.get("age", 0);
	 *  }
	 *  return res;
	 *  
	 * }).then(res -&gt; {
	 *  // ...
	 * }).then(res -&gt; {
	 *  // ...
	 * }).catchError(err -&gt; {
	 *  // Error handler
	 * });
	 * </pre>
	 *
	 * @param filter
	 *            the query filter to apply the the replace operation
	 * @param replacement
	 *            the replacement document
	 * @param options
	 *            the options to apply to the operation
	 * @return a Promise with a single element the document that was replaced.
	 *         If no documents matched the query filter, then null will be
	 *         returned
	 */
	protected Promise findOneAndReplace(Tree filter, Tree replacement, FindOneAndReplaceOptions options) {
		return singleResult(collection.findOneAndReplace(toBson(filter), (Document) toBson(replacement), options));
	}

	// --- FIND ONE AND UPDATE ---

	/**
	 * Atomically find a document and update it. Sample of usage:
	 * 
	 * <pre>
	 * Tree update = new Tree();
	 * update.put("field1", 123);
	 * 
	 * findOneAndUpdate(eq("field1", 123), update).then(res -&gt; {
	 * 
	 *  // Update operation finished
	 *  if (res != null) {
	 *    String firstName = res.get("firstName", "");
	 *    int age = res.get("age", 0);
	 *  }
	 *  return res;
	 *  
	 * });
	 * </pre>
	 *
	 * @param filter
	 *            a document describing the query filter, which may not be null.
	 * @param update
	 *            a document describing the update, which may not be null. The
	 *            update to apply must include only update operators.
	 * @return a Promise with a single element the document that was updated
	 *         before the update was applied. If no documents matched the query
	 *         filter, then null will be returned
	 */
	protected Promise findOneAndUpdate(Tree filter, Tree update) {
		return singleResult(collection.findOneAndUpdate(toBson(filter), (Document) toBson(update)));
	}

	/**
	 * Atomically find a document and update it. Sample of usage:
	 * 
	 * <pre>
	 * Tree update = new Tree();
	 * update.put("field1", 123);
	 * 
	 * FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
	 * opts.upsert(true);
	 * 
	 * findOneAndUpdate(eq("field1", 123), update, opts).then(res -&gt; {
	 * 
	 *  // Update operation finished
	 *  if (res != null) {
	 *    String firstName = res.get("firstName", "");
	 *    int age = res.get("age", 0);
	 *  }
	 *  return res;
	 *  
	 * });
	 * </pre>
	 *
	 * @param filter
	 *            a document describing the query filter, which may not be null.
	 * @param update
	 *            a document describing the update, which may not be null. The
	 *            update to apply must include only update operators.
	 * @param options
	 *            the options to apply to the operation
	 * @return a Promise with a single element the document that was updated.
	 */
	protected Promise findOneAndUpdate(Tree filter, Tree update, FindOneAndUpdateOptions options) {
		return singleResult(collection.findOneAndUpdate(toBson(filter), (Document) toBson(update), options));
	}

	// --- MAP/REDUCE ---

	/**
	 * Aggregates documents according to the specified map-reduce function.
	 * Sample of usage:
	 * 
	 * <pre>
	 * String mapFunction = "...";
	 * String reduceFunction = "...";
	 * mapReduce(mapFunction, reduceFunction).then(res -&gt; {
	 *  // Operation finished
	 * }).then(res -&gt; {
	 *  // ...
	 * }).then(res -&gt; {
	 *  // ...
	 * }).catchError(err -&gt; {
	 *  // Error handler
	 * });
	 * </pre>
	 *
	 * @param mapFunction
	 *            A JavaScript function that associates or "maps" a value with a
	 *            key and emits the key and value pair.
	 * @param reduceFunction
	 *            A JavaScript function that "reduces" to a single object all
	 *            the values associated with a particular key.
	 * @return an Promise containing the result of the map-reduce operation
	 */
	protected Promise mapReduce(String mapFunction, String reduceFunction) {
		return collectAll(collection.mapReduce(mapFunction, reduceFunction));
	}

	// --- PRIVATE UTILITIES ---

	private <T> SingleResultPromise<T> singleResult(Publisher<T> publisher) {
		return new SingleResultPromise<T>(publisher);
	}

	private <T> CollectAllPromise<T> collectAll(Publisher<T> publisher) {
		return new CollectAllPromise<T>(publisher);
	}

	private Tree updateResultToTree(UpdateResult res) {
		Tree ret = new Tree();
		ret.put(MATCHED, res.getMatchedCount());
		ret.put(MODIFIED, res.getModifiedCount());
		ret.put(ACKNOWLEDGED, res.wasAcknowledged());
		BsonValue id = res.getUpsertedId();
		if (id != null) {
			ret.put(ID, id.toString());
		}
		return ret;
	}

	private Tree deleteResultToTree(DeleteResult res) {
		Tree ret = new Tree();
		ret.put(DELETED, res.getDeletedCount());
		ret.put(ACKNOWLEDGED, res.wasAcknowledged());
		return ret;
	}

	private Tree prepareForUpdate(Tree record) {
		if (record.get(SET) == null) {
			Tree rec = new Tree();
			rec.putMap(SET).copyFrom(record);
			return rec;
		}
		return record;
	}

	// --- GETTERS / SETTERS ---

	public final int getMaxItemsPerQuery() {
		return maxItemsPerQuery;
	}

	public final void setMaxItemsPerQuery(int maxItemsPerQuery) {
		this.maxItemsPerQuery = maxItemsPerQuery;
	}

}