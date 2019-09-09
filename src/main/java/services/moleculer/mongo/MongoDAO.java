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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.TextSearchOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import io.datatree.Promise;
import io.datatree.Resolver;
import io.datatree.Tree;

public class MongoDAO {

	// --- CONSTANTS ---

	// Max number of retrieved rows
	public static final int MAX_ITEMS_PER_QUERY = 1024 * 4;

	// Root node of rows
	public static final String ROWS = "rows";

	// Row counter
	public static final String COUNT = "count";

	// Set function
	public static final String SET = "$set";

	public static final String ID = "_id";

	// --- COLLECTION OF THIS DAO ---

	protected com.mongodb.async.client.MongoCollection<Document> collection;

	// --- GET CONNECTION AND COLLECTION ---

	public void setMongoConnectionPool(MongoConnectionPool pool) {

		// Set collection by the @MongoCollection annotation
		// Eg. @MongoCollection("users")
		Class<? extends MongoDAO> c = getClass();
		if (c.isAnnotationPresent(MongoCollection.class)) {
			MongoCollection annotation = c.getAnnotation(MongoCollection.class);
			String collectionName = annotation.value();
			if (collectionName != null) {
				collectionName = collectionName.trim();
				collection = pool.getDatabase().getCollection(collectionName);
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
			collection = pool.getDatabase().getCollection(className);
		}
	}

	// --- DROP COLLECTION ---

	public Promise drop() {
		return new Promise(res -> {
			collection.drop(toCallback(res));
		});
	}

	// --- CREATE INDEX(ES) ---

	public Promise createAscendingIndexes(String... fieldNames) {
		return createIndexes(Indexes.ascending(fieldNames));
	}

	public Promise createDescendingIndexes(String... fieldNames) {
		return createIndexes(Indexes.descending(fieldNames));
	}

	public Promise createGeo2DSphereIndexes(String... fieldNames) {
		return createIndexes(Indexes.geo2dsphere(fieldNames));
	}

	public Promise createIndexes(Tree indexes) {
		return createIndexes(toBson(indexes));
	}

	protected Promise createIndexes(Bson key) {
		return new Promise(res -> {
			collection.createIndex(key, toCallback(res));
		});
	}

	// --- LIST INDEXES ---

	public Promise listIndexes() {
		return new Promise(res -> {
			collectAll(collection.listIndexes(), res);
		});
	}

	// --- DROP INDEX ---

	public Promise dropIndex(String indexName) {
		return new Promise(res -> {
			collection.dropIndex(indexName, toCallback(res));
		});
	}

	// --- INSERT ONE ---

	public Promise insertOne(Tree record) {
		return insertOne(record, null);
	}

	public Promise insertOne(Tree record, InsertOneOptions options) {
		Document doc = (Document) toBson(record);
		return new Promise(res -> {
			if (options == null) {
				collection.insertOne(doc, toCallback(res));
			} else {
				collection.insertOne(doc, options, toCallback(res));
			}
		}).then(in -> {
			ObjectId id = (ObjectId) doc.get(ID);
			if (id == null) {
				return null;
			}
			return id.toHexString();
		});
	}

	// --- REPLACE ONE ---

	public Promise replaceOne(Tree record, Tree filter) {
		return new Promise(res -> {
			collection.replaceOne(toBson(filter), (Document) toBson(record), toCallback(res));
		}).then(res -> {
			UpdateResult result = (UpdateResult) res.asObject();
			return result.getModifiedCount() > 0;
		});
	}

	// --- UPDATE ONE ---

	public Promise updateOne(Tree record, Tree filter) {
		return new Promise(res -> {
			Tree set = new Tree();
			set.putMap(SET).copyFrom(record);
			collection.updateOne(toBson(filter), toBson(set), toCallback(res));
		}).then(res -> {
			UpdateResult result = (UpdateResult) res.asObject();
			return result.getModifiedCount() > 0;
		});
	}

	// --- UPDATE MANY ---

	public Promise updateMany(Tree record, Tree filter) {
		return new Promise(res -> {
			Tree set = new Tree();
			set.putMap(SET).copyFrom(record);
			collection.updateMany(toBson(filter), toBson(set), toCallback(res));
		}).then(res -> {
			UpdateResult result = (UpdateResult) res.asObject();
			return result.getModifiedCount();
		});
	}

	// --- DELETE ONE ---

	public Promise deleteOne(Tree filter) {
		return new Promise(res -> {
			collection.deleteOne(toBson(filter), toCallback(res));
		}).then(res -> {
			DeleteResult result = (DeleteResult) res.asObject();
			return result.getDeletedCount() > 0;
		});
	}

	// --- DELETE MANY ---

	public Promise deleteMany(Tree filter) {
		return new Promise(res -> {
			collection.deleteMany(toBson(filter), toCallback(res));
		}).then(res -> {
			DeleteResult result = (DeleteResult) res.asObject();
			return result.getDeletedCount();
		});
	}

	// --- COUNTS ---

	public Promise count() {
		return new Promise(res -> {
			collection.count(toCallback(res));
		});
	}

	public Promise count(Tree filter) {
		return new Promise(res -> {
			collection.count(toBson(filter), toCallback(res));
		});
	}

	// --- FIND ONE ---

	public Promise findOne(Tree filter) {
		return new Promise(res -> {
			FindIterable<Document> find = collection.find();
			if (filter != null) {
				find.filter(toBson(filter));
			}
			find.batchSize(1);
			find.first(toCallback(res));
		});
	}

	// --- FIND MANY ---

	public Promise find(Tree filter, Tree sort, int first, int limit) {
		return new Promise(res -> {

			// Set query limit
			final int l;
			if (limit < 1 || limit > MAX_ITEMS_PER_QUERY) {
				l = MAX_ITEMS_PER_QUERY;
			} else {
				l = limit;
			}

			// Find
			FindIterable<Document> find = collection.find();
			Bson bsonFilter = null;
			if (filter != null) {
				bsonFilter = toBson(filter);
				find.filter(bsonFilter);
			}
			final Bson finalBsonFilter = bsonFilter;
			if (sort != null) {
				find.sort(toBson(sort));
			}
			if (first > 0) {
				find.skip(first);
			}
			find.batchSize(Math.min(l, 50));
			find.limit(l);

			// Invoke callback
			LinkedList<Document> list = new LinkedList<Document>();
			final SingleResultCallback<Long> lastStep = new SingleResultCallback<Long>() {

				@Override
				public final void onResult(Long result, Throwable cause) {
					if (cause != null) {
						res.reject(cause);
					} else {
						Tree doc = new Tree();
						doc.put(COUNT, result);
						doc.putObject(ROWS, list);
						res.resolve(doc);
					}
				}

			};

			// Read list
			find.forEach(document -> {
				if (document != null) {
					list.addLast(document);
				}
			}, (result, t) -> {

				// Get max record number
				if (finalBsonFilter == null) {
					collection.count(lastStep);
				} else {
					collection.count(finalBsonFilter, lastStep);
				}

			});
		});
	}

	// --- FIND ONE AND DELETE ---

	public Promise findOneAndDelete(Tree filter) {
		return new Promise(res -> {
			collection.findOneAndDelete(toBson(filter), toCallback(res));
		});
	}

	// --- FIND ONE AND REPLACE ---

	public Promise findOneAndReplace(Tree filter, Tree replacement) {
		return new Promise(res -> {
			collection.findOneAndReplace(toBson(filter), (Document) toBson(replacement), toCallback(res));
		});
	}

	// --- FIND ONE AND UPDATE ---

	public Promise findOneAndUpdate(Tree filter, Tree update) {
		return new Promise(res -> {
			collection.findOneAndUpdate(toBson(filter), toBson(update), toCallback(res));
		});
	}

	// --- MAP/REDUCE ---

	public Promise mapReduce(String mapFunction, String reduceFunction, Consumer<Tree> rowHandler) {
		return new Promise(res -> {
			callOnEach(collection.mapReduce(mapFunction, reduceFunction), res, rowHandler);
		});
	}

	public Promise mapReduce(String mapFunction, String reduceFunction) {
		return new Promise(res -> {
			collectAll(collection.mapReduce(mapFunction, reduceFunction), res);
		});
	}

	// --- LOGICAL OPERATORS FOR FILTERS ---

	public Tree or(Tree... filters) {
		return new BsonTree(Filters.or(toBsonIterable(filters)));
	}

	public Tree and(Tree... filters) {
		return new BsonTree(Filters.and(toBsonIterable(filters)));
	}

	public Tree not(Tree filter) {
		return new BsonTree(Filters.not(toBson(filter)));
	}

	public Tree eq(String id) {
		return new BsonTree(Filters.eq(ID, new ObjectId(id)));
	}

	public Tree eq(String fieldName, Object value) {
		return new BsonTree(Filters.eq(fieldName, toObject(value)));
	}

	public Tree ne(String fieldName, Object value) {
		return new BsonTree(Filters.ne(fieldName, toObject(value)));
	}

	public Tree in(String fieldName, Object... values) {
		return new BsonTree(Filters.in(fieldName, toObjectIterable(values)));
	}

	public Tree nin(String fieldName, Object... values) {
		return new BsonTree(Filters.nin(fieldName, toObjectIterable(values)));
	}

	public Tree lt(String fieldName, Object value) {
		return new BsonTree(Filters.lt(fieldName, toObject(value)));
	}

	public Tree lte(String fieldName, Object value) {
		return new BsonTree(Filters.lte(fieldName, toObject(value)));
	}

	public Tree gt(String fieldName, Object value) {
		return new BsonTree(Filters.gt(fieldName, toObject(value)));
	}

	public Tree gte(String fieldName, Object value) {
		return new BsonTree(Filters.gte(fieldName, toObject(value)));
	}

	public Tree exists(String fieldName) {
		return new BsonTree(Filters.exists(fieldName, true));
	}

	public Tree notExists(String fieldName) {
		return new BsonTree(Filters.exists(fieldName, false));
	}

	public Tree regex(String fieldName, String pattern) {
		return new BsonTree(Filters.regex(fieldName, pattern));
	}

	public Tree where(String javaScriptExpression) {
		return new BsonTree(Filters.where(javaScriptExpression));
	}

	public Tree elemMatch(String fieldName, Tree filter) {
		return new BsonTree(Filters.elemMatch(fieldName, toBson(filter)));
	}

	public Tree elemMatch(Tree expression) {
		return new BsonTree(Filters.expr(toBson(expression)));
	}

	public Tree text(String search) {
		return new BsonTree(Filters.text(search));
	}

	public Tree text(String search, TextSearchOptions options) {
		return new BsonTree(Filters.text(search, options));
	}

	// --- PROTECTED UTILITIES ---

	protected void callOnEach(MongoIterable<Document> iterable, Resolver res, Consumer<Tree> rowHandler) {
		iterable.batchCursor((cursor, err1) -> {
			if (err1 != null) {
				closeCursor(cursor);
				res.reject(err1);
				return;
			}
			try {
				AtomicLong count = new AtomicLong();
				while (cursor != null && !cursor.isClosed()) {
					cursor.next((list, err2) -> {
						if (err2 != null) {
							closeCursor(cursor);
							res.reject(err2);
							return;
						}
						for (Document row : list) {
							rowHandler.accept(new Tree(row));
							count.incrementAndGet();
						}
					});
				}
				res.resolve(count.get());
			} catch (Throwable err3) {
				res.reject(err3);
			} finally {
				closeCursor(cursor);
			}
		});
	}

	protected void collectAll(MongoIterable<Document> iterable, Resolver res) {
		iterable.batchCursor((cursor, err1) -> {
			if (err1 != null) {
				closeCursor(cursor);
				res.reject(err1);
				return;
			}
			try {
				LinkedList<Document> docList = new LinkedList<Document>();
				AtomicLong count = new AtomicLong();
				while (cursor != null && !cursor.isClosed()) {
					cursor.next((list, err2) -> {
						if (err2 != null) {
							closeCursor(cursor);
							res.reject(err2);
							return;
						}
						for (Document row : list) {
							docList.addLast(row);
							count.incrementAndGet();
						}
					});
				}
				Tree doc = new Tree();
				doc.put(COUNT, count.get());
				doc.putObject(ROWS, docList);
				res.resolve(doc);
			} catch (Throwable err3) {
				res.reject(err3);
			} finally {
				closeCursor(cursor);
			}
		});
	}

	protected void closeCursor(AsyncBatchCursor<Document> cursor) {
		if (cursor != null) {
			try {
				cursor.close();
			} catch (Exception ignored) {
			}
		}
	}

	protected <T> SingleResultCallback<T> toCallback(Resolver res) {
		return ((result, cause) -> {
			if (cause == null) {
				res.resolve(result);
			} else {
				res.reject(cause);
			}
		});
	}

	protected Object toObject(Object value) {
		if (value != null && value instanceof Tree) {
			return ((Tree) value).asObject();
		}
		return value;
	}

	protected Iterable<Object> toObjectIterable(Object... array) {
		if (array == null || array.length == 0) {
			return Collections.emptyList();
		}
		ArrayList<Object> list = new ArrayList<>(array.length);
		for (Object object : array) {
			list.add(toObject(object));
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	protected Bson toBson(Tree tree) {
		if (tree == null) {
			return null;
		}
		Object o = tree.asObject();
		if (o == null) {
			return null;
		}
		if (o instanceof Bson) {
			return (Bson) o;
		}
		return new Document((Map<String, Object>) o);
	}

	protected Iterable<Bson> toBsonIterable(Tree[] array) {
		if (array == null || array.length == 0) {
			return Collections.emptyList();
		}
		ArrayList<Bson> list = new ArrayList<>(array.length);
		for (Tree tree : array) {
			list.add(toBson(tree));
		}
		return list;
	}

}