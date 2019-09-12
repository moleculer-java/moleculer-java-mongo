[![Build Status](https://travis-ci.org/moleculer-java/moleculer-java-mongo.svg?branch=master)](https://travis-ci.org/moleculer-java/moleculer-java-mongo)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/665929c4e1454da1b7db00053ede6e75)](https://www.codacy.com/manual/berkesa/moleculer-java-mongo?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=moleculer-java/moleculer-java-mongo&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/moleculer-java/moleculer-java-mongo/branch/master/graph/badge.svg)](https://codecov.io/gh/moleculer-java/moleculer-java-mongo)

# (WIP) MongoDB Client for Moleculer Framework

## Description

The "moleculer-java-mongo" is an asynchronous MongoDB API,
specially designed for Java-based Moleculer ecosystem
(but it can be used without the Moleculer Framework).
The API can be conveniently used with the Spring Framework.
This project is currently in "work in progress" status.

## Methods

### Drop collection

Drops this collection from the Database.

```java
drop().then(res -> {

 // Drop operation finished
 
}).then(res -> {
 // ...
}).then(res -> {
 // ...
}).catchError(err -> {
 // Error handler
});
```

### Rename collection

Rename the collection.

```java
renameCollection("db", "collection").then(res -> {

 // Rename operation finished
 
});
```

### Create indexes

Creates ascending/descending/2dsphere/hash/text/etc. indexes.

```java
createAscendingIndexes("field1", "field2").then(res -> {

 // Index created successfully
 
});
```

### List indexes

Get all the indexes in this collection.

```java
listIndexes().then(res -> {

 // Operation finished
 for (Tree index: res.get("rows")) {
   System.out.println(index.get("name", ""));
 }

 int numberOfIndexes = res.get("count", 0); 
 
});
```

### Insert one document

Inserts the provided document. If the document is missing an identifier, the driver should generate one.

```java
Tree doc = new Tree();
doc.put("field1", 123);
doc.put("field2.subfield", false);

insertOne(doc).then(res -> {

 // Insert operation finished
 String id = res.get("_id", "");
 return id;
 
});
```

### Replace one document

Replace a document in the collection according to the specified arguments.

```java
Tree replacement = new Tree();
replacement.put("field1", 123);

replaceOne(eq("field1", 123), replacement).then(res -> {

 // Replace operation finished
 int modified = res.get("modified");
 return modified > 0;
 
});
```

### Update one document

Update a single document in the collection according to the specified arguments.

```java
Tree update = new Tree();
update.put("field1", 123);

updateOne(eq("field1", 123), update).then(res -> {

 // Replace operation finished
 int modified = res.get("modified");
 return modified > 0;
 
});
```

### Update many documents

Update all documents in the collection according to the specified arguments.

```java
Tree update = new Tree();
update.put("field1", 123);

updateMany(eq("field1", 123), update).then(res -> {

 // Replace operation finished
 int modified = res.get("modified");
 return modified > 0;
 
});
```

### Delete one document

Removes at most one document from the collection that matches the given filter. If no documents match, the collection is not modified.

```java
deleteOne(eq("field1", 123)).then(res -> {

 // Delete operation finished
 int deleted = res.get("deleted");
 return deleted > 0;
 
});
```

### Delete all documents

Removes all documents from the collection.

```java
deleteAll().then(res -&gt; {

 // Delete operation finished
 int deleted = res.get("deleted");
 return deleted > 0;
	 
});
```

### Delete many documents

Removes all documents from the collection that match the given query filter. If no documents match, the collection is not modified.

```java
deleteMany(eq("field1", 123)).then(res -> {

 // Delete operation finished
 int deleted = res.get("deleted");
 return deleted > 0;
 
});
```

### Count documents

Counts the number of documents in the collection according to the given filters.

```java
count(eq("field1", 123)).then(res -> {

 // Count operation finished
 long numberOfDocuments = res.asLong();
 return res;

});
```

### Find one document

Finds one document by the specified query filter.

```java
findOne(eq("field1", 123)).then(res -> {

 // Find operation finished
 if (res != null) {
   String firstName = res.get("firstName", "");
   int age = res.get("age", 0);
 }
 return res;
 
});
```

### Find many documents

Queries the specified number of records from the collection.

```java
find(eq("field1", 123), null, 0, 10).then(res -> {

 // Find operation finished
 int maxNumberOfSelectableDocuments = res.get("count");
 for (Tree doc: res.get("rows")) {
   String firstName = res.get("firstName", "");
 }
 return res;

});
```

### Find one and delete

Atomically find a document and remove it.

```java
findOneAndDelete(eq("field1", 123)).then(res -> {

 // Delete operation finished
 if (res != null) {
   String firstName = res.get("firstName", "");
   int age = res.get("age", 0);
 }
 return res;
 
});
```

### Find one and replace

Atomically find a document and replace it.

```java
Tree replacement = new Tree();
replacement.put("field1", 123);

findOneAndReplace(eq("field1", 123), replacement).then(res -> {

 // Replace operation finished
 if (res != null) {
   String firstName = res.get("firstName", "");
   int age = res.get("age", 0);
 }
 return res;
 
});
```

### Find one and update

Atomically find a document and update it.

```java
Tree update = new Tree();
update.put("field1", 123);

findOneAndUpdate(eq("field1", 123), update).then(res -> {

 // Update operation finished
 if (res != null) {
   String firstName = res.get("firstName", "");
   int age = res.get("age", 0);
 }
 return res;
 
});
```

### Map/Reduce

Aggregates documents according to the specified map-reduce function.

```java
String mapFunction = "...";    // JavaScript
String reduceFunction = "..."; // JavaScript
mapReduce(mapFunction, reduceFunction).then(res -> {

 // Operation finished
 
});
```

The MongoDAO superclass contains more functions. To use the not listed functions, see the JavaDoc of MongoDAO.

## License
MongoDB Client for Moleculer Framework is available under the [MIT license](https://tldrlegal.com/license/mit-license).
