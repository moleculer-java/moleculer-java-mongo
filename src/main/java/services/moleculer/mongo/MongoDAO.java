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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.TextSearchOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import io.datatree.Promise;
import io.datatree.Tree;

public class MongoDAO {

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

	protected Promise drop() {
		return singleResult(collection.drop());
	}

	// --- CREATE INDEX(ES) ---

	protected Promise createAscendingIndexes(String... fieldNames) {
		return createIndexes(Indexes.ascending(fieldNames));
	}

	protected Promise createDescendingIndexes(String... fieldNames) {
		return createIndexes(Indexes.descending(fieldNames));
	}

	protected Promise createGeo2DSphereIndexes(String... fieldNames) {
		return createIndexes(Indexes.geo2dsphere(fieldNames));
	}

	protected Promise createIndexes(Tree indexes) {
		return createIndexes(toBson(indexes));
	}

	protected Promise createIndexes(Bson key) {
		return singleResult(collection.createIndex(key));
	}

	// --- LIST INDEXES ---

	protected Promise listIndexes() {
		return collectAll(collection.listIndexes());
	}

	// --- DROP INDEX ---

	protected Promise dropIndex(String indexName) {
		return singleResult(collection.dropIndex(indexName));
	}

	protected Promise dropIndex(String indexName, DropIndexOptions options) {
		return singleResult(collection.dropIndex(indexName, options));
	}

	// --- INSERT ONE ---

	protected Promise insertOne(Tree record) {
		Document doc = (Document) toBson(record);
		return singleResult(collection.insertOne(doc)).then(in -> {
			return doc;
		});
	}

	protected Promise insertOne(Tree record, InsertOneOptions options) {
		Document doc = (Document) toBson(record);
		return singleResult(collection.insertOne(doc, options)).then(in -> {
			return doc;
		});
	}

	// --- REPLACE ONE ---

	protected Promise replaceOne(Tree record, Tree filter) {
		return singleResult(collection.replaceOne(toBson(filter), (Document) toBson(record))).thenWithResult(res -> {
			return updateResultToTree(res);
		});
	}

	protected Promise replaceOne(Tree record, Tree filter, ReplaceOptions options) {
		return singleResult(collection.replaceOne(toBson(filter), (Document) toBson(record), options))
				.thenWithResult(res -> {
					return updateResultToTree(res);
				});
	}

	// --- UPDATE ONE ---

	protected Promise updateOne(Tree record, Tree filter) {
		Tree rec = prepareForUpdate(record);
		return singleResult(collection.updateOne(toBson(filter), toBson(rec))).thenWithResult(res -> {
			return updateResultToTree(res);
		});
	}

	// --- UPDATE MANY ---

	protected Promise updateMany(Tree record, Tree filter) {
		Tree rec = prepareForUpdate(record);
		return singleResult(collection.updateMany(toBson(filter), toBson(rec))).thenWithResult(res -> {
			return updateResultToTree(res);
		});
	}

	// --- DELETE ONE ---

	protected Promise deleteOne(Tree filter) {
		return singleResult(collection.deleteOne(toBson(filter))).thenWithResult(res -> {
			return deleteResultToTree(res);
		});
	}

	protected Promise deleteOne(Tree filter, DeleteOptions options) {
		return singleResult(collection.deleteOne(toBson(filter), options)).thenWithResult(res -> {
			return deleteResultToTree(res);
		});
	}

	// --- DELETE MANY ---

	protected Promise deleteMany(Tree filter) {
		return singleResult(collection.deleteMany(toBson(filter))).thenWithResult(res -> {
			return deleteResultToTree(res);
		});
	}

	protected Promise deleteMany(Tree filter, DeleteOptions options) {
		return singleResult(collection.deleteMany(toBson(filter), options)).thenWithResult(res -> {
			return deleteResultToTree(res);
		});
	}

	// --- COUNTS ---

	protected Promise count() {
		return singleResult(collection.countDocuments());
	}

	protected Promise count(Tree filter) {
		return singleResult(collection.countDocuments(toBson(filter)));
	}

	protected Promise count(Tree filter, CountOptions options) {
		return singleResult(collection.countDocuments(toBson(filter), options));
	}

	// --- FIND ONE ---

	protected Promise findOne(Tree filter) {
		FindPublisher<Document> find = collection.find();
		if (filter != null) {
			find.filter(toBson(filter));
		}
		find.batchSize(1);
		return singleResult(find.first());
	}

	// --- FIND MANY ---

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

	protected Promise findOneAndDelete(Tree filter) {
		return singleResult(collection.findOneAndDelete(toBson(filter)));
	}

	protected Promise findOneAndDelete(Tree filter, FindOneAndDeleteOptions options) {
		return singleResult(collection.findOneAndDelete(toBson(filter), options));
	}

	// --- FIND ONE AND REPLACE ---

	protected Promise findOneAndReplace(Tree filter, Tree replacement) {
		return singleResult(collection.findOneAndReplace(toBson(filter), (Document) toBson(replacement)));
	}

	protected Promise findOneAndReplace(Tree filter, Tree replacement, FindOneAndReplaceOptions options) {
		return singleResult(collection.findOneAndReplace(toBson(filter), (Document) toBson(replacement), options));
	}

	// --- FIND ONE AND UPDATE ---

	protected Promise findOneAndUpdate(Tree filter, Tree update) {
		return singleResult(collection.findOneAndUpdate(toBson(filter), (Document) toBson(update)));
	}

	protected Promise findOneAndUpdate(Tree filter, Tree update, FindOneAndUpdateOptions options) {
		return singleResult(collection.findOneAndUpdate(toBson(filter), (Document) toBson(update), options));
	}

	// --- MAP/REDUCE ---

	protected Promise mapReduce(String mapFunction, String reduceFunction) {
		return collectAll(collection.mapReduce(mapFunction, reduceFunction));
	}
	
	protected Promise mapReduce(String mapFunction, String reduceFunction, Consumer<Tree> rowHandler) {
		return new Promise(res -> {
			collection.mapReduce(mapFunction, reduceFunction).subscribe(new Subscriber<Document>() {

				AtomicLong counter = new AtomicLong();

				@Override
				public void onSubscribe(Subscription s) {

					// Do nothing
				}

				@Override
				public void onNext(Document t) {
					rowHandler.accept(new Tree(t));
					counter.incrementAndGet();
				}

				@Override
				public void onError(Throwable t) {
					res.reject(t);
				}

				@Override
				public void onComplete() {
					res.resolve(counter.get());
				}

			});
		});
	}

	// --- LOGICAL OPERATORS FOR FILTERS ---

	protected Tree or(Tree... filters) {
		return new BsonTree(Filters.or(toBsonIterable(filters)));
	}

	protected Tree and(Tree... filters) {
		return new BsonTree(Filters.and(toBsonIterable(filters)));
	}

	protected Tree not(Tree filter) {
		return new BsonTree(Filters.not(toBson(filter)));
	}

	protected Tree eq(String id) {
		return new BsonTree(Filters.eq(ID, new ObjectId(id)));
	}

	protected Tree eq(String fieldName, Object value) {
		return new BsonTree(Filters.eq(fieldName, toObject(value)));
	}

	protected Tree ne(String fieldName, Object value) {
		return new BsonTree(Filters.ne(fieldName, toObject(value)));
	}

	protected Tree in(String fieldName, Object... values) {
		return new BsonTree(Filters.in(fieldName, toObjectIterable(values)));
	}

	protected Tree nin(String fieldName, Object... values) {
		return new BsonTree(Filters.nin(fieldName, toObjectIterable(values)));
	}

	protected Tree lt(String fieldName, Object value) {
		return new BsonTree(Filters.lt(fieldName, toObject(value)));
	}

	protected Tree lte(String fieldName, Object value) {
		return new BsonTree(Filters.lte(fieldName, toObject(value)));
	}

	protected Tree gt(String fieldName, Object value) {
		return new BsonTree(Filters.gt(fieldName, toObject(value)));
	}

	protected Tree gte(String fieldName, Object value) {
		return new BsonTree(Filters.gte(fieldName, toObject(value)));
	}

	protected Tree exists(String fieldName) {
		return new BsonTree(Filters.exists(fieldName, true));
	}

	protected Tree notExists(String fieldName) {
		return new BsonTree(Filters.exists(fieldName, false));
	}

	protected Tree regex(String fieldName, String pattern) {
		return new BsonTree(Filters.regex(fieldName, pattern));
	}

	protected Tree where(String javaScriptExpression) {
		return new BsonTree(Filters.where(javaScriptExpression));
	}

	protected Tree elemMatch(String fieldName, Tree filter) {
		return new BsonTree(Filters.elemMatch(fieldName, toBson(filter)));
	}

	protected Tree elemMatch(Tree expression) {
		return new BsonTree(Filters.expr(toBson(expression)));
	}

	protected Tree text(String search) {
		return new BsonTree(Filters.text(search));
	}

	protected Tree text(String search, TextSearchOptions options) {
		return new BsonTree(Filters.text(search, options));
	}

	// --- PRIVATE UTILITIES ---

	private Object toObject(Object value) {
		if (value != null && value instanceof Tree) {
			return ((Tree) value).asObject();
		}
		return value;
	}

	private Iterable<Object> toObjectIterable(Object... array) {
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
	private Bson toBson(Tree tree) {
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

	private Iterable<Bson> toBsonIterable(Tree[] array) {
		if (array == null || array.length == 0) {
			return Collections.emptyList();
		}
		ArrayList<Bson> list = new ArrayList<>(array.length);
		for (Tree tree : array) {
			list.add(toBson(tree));
		}
		return list;
	}

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

}