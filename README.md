[![Build Status](https://travis-ci.org/moleculer-java/moleculer-java-mongo.svg?branch=master)](https://travis-ci.org/moleculer-java/moleculer-java-mongo)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/665929c4e1454da1b7db00053ede6e75)](https://www.codacy.com/manual/berkesa/moleculer-java-mongo?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=moleculer-java/moleculer-java-mongo&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/moleculer-java/moleculer-java-mongo/branch/master/graph/badge.svg)](https://codecov.io/gh/moleculer-java/moleculer-java-mongo)

# MongoDB Client API for Moleculer Framework

## Description

The "moleculer-java-mongo" is an asynchronous MongoDB API,
specially designed for Java-based
[Moleculer](https://moleculer-java.github.io/moleculer-java/)
ecosystem. The API can be conveniently used with the Spring Framework (but it works without Spring).

## Download

**Maven**

```xml
<dependencies>
	<dependency>
		<groupId>com.github.berkesa</groupId>
		<artifactId>moleculer-java-mongo</artifactId>
		<version>1.0.0</version>
		<scope>runtime</scope>
	</dependency>
</dependencies>
```

**Gradle**

```gradle
dependencies {
	compile group: 'com.github.berkesa', name: 'moleculer-java-mongo', version: '1.0.0' 
}
```

## Usage without Spring Framework

The key Superclass is the "MongoDAO". All DAO objects are inherited from this Class.
The name of the Mongo Collection can be specified by the annotation "MongoCollection".
The functions run in non-blocking mode, so all return types are Promise:

```java
import io.datatree.*;
import services.moleculer.mongo.*;

@MongoCollection("user")
public class UserDAO extends MongoDAO {

	public Promise createNewUser(String firstName, String lastName, String email) {
		Tree document = new Tree();
		document.put("firstName", firstName);
		document.put("lastName", lastName);
		document.put("email", email);
		return insertOne(document);
	}
	
	public Promise getUserByEmail(String email) {
		return findOne(eq("email", email));
	}

	public Promise deleteUserByEmail(String email) {
		return deleteOne(eq("email", email));
	}
	
	public Promise countUsers() {
		return count();
	}
	
}
```

The use of the UserDAO is illustrated by the following example (without Spring):

```java
public static void main(String[] args) throws Exception {

	// Create MongoDB connection pool
	MongoConnectionPool connection = new MongoConnectionPool();
	connection.setConnectionString("mongodb://localhost");
	connection.setDatabase("db");
	connection.init();

	// Create DAO Object and set the MongoDB connection pool
	UserDAO userDAO = new UserDAO();
	userDAO.setMongoConnectionPool(connection);

	// Example of blocking-style processing
	Tree count = userDAO.countUsers().waitFor();
	System.out.println("Number of users: " + count.asInteger());

	// Non-blocking, "waterfall-style" processing (this is the recommended)
	Promise.resolve().then(rsp -> {

		// Create new user
		return userDAO.createNewUser("Tom", "Smith", "tom.smith@company.com");

	}).then(rsp -> {

		// Get the new record ID
		String id = rsp.get("_id", "");
		System.out.println("New record ID: " + id);

	}).then(rsp -> {

		// Find new user
		return userDAO.getUserByEmail("tom.smith@company.com");

	}).then(rsp -> {

		// Print a property from the retrieved record
		String firstName = rsp.get("firstName", "");
		System.out.println("First name: " + firstName);

	}).then(rsp -> {

		// Delete record
		return userDAO.deleteUserByEmail("tom.smith@company.com");

	}).then(rsp -> {

		// Print the result of the previous operation
		int numberOfDeletedRecords = rsp.get("deleted", 0);
		System.out.println("Success: " + (numberOfDeletedRecords > 0));

	}).catchError(err -> {

		// Error handler
		err.printStackTrace();

	}).then(rsp -> {

		System.out.println("End of process.");

	});

	// Wait for few second before terminate the JVM
	Thread.sleep(3000);
}
```

## Usage with Spring Framework

When using the Spring Framework, you can create the MongoDB Connection Pool as Spring Bean:

```xml
<bean id="mongoPool"
      class="services.moleculer.mongo.MongoConnectionPool"
      init-method="init"
      destroy-method="destroy">
  <property name="connectionString"  value="mongodb://localhost" />
  <property name="database"          value="db" />
  <property name="connectionTimeout" value="3000" />
</bean>
```

This case is different from using without Spring Framework in that
the DAO classes are inherited from "SpringMongoDAO",
and the stereotype of the DAO classes is "Repository":

```java
import io.datatree.*;
import services.moleculer.mongo.*;
import org.springframework.stereotype.*;

@Repository
@MongoConnection("mongoPool")
@MongoCollection("user")
public class UserDAO extends SpringMongoDAO {

	public Promise createNewUser(String firstName, String lastName, String email) {
		Tree document = new Tree();
		document.put("firstName", firstName);
		document.put("lastName", lastName);
		document.put("email", email);
		return insertOne(document);
	}
	
	public Promise getUserByEmail(String email) {
		return findOne(eq("email", email));
	}

	public Promise deleteUserByEmail(String email) {
		return deleteOne(eq("email", email));
	}
	
	public Promise countUsers() {
		return count();
	}
	
}
```

MongoDAOs can be used like any other Spring Beans,
with the "Autowired" annotation any other Spring Bean can refer to them:

```java
@Autowired
private UserDAO userDAO;
```

## Usage with Moleculer Framework

Moleculer Framework allows you to build a distributed service-based application that uses MongoDB as a back-end.
An example of a Moleculer-based service available through Message Broker (eg. Redis or NATS) and via HTTP REST call:

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import services.moleculer.service.Action;
import services.moleculer.service.Name;
import services.moleculer.service.Service;
import services.moleculer.web.router.HttpAlias;

@Component
@Name("user")
public class UserService extends Service {

	@Autowired
	private UserDAO userDAO;

	@Name("create")
	@HttpAlias(method = "POST", path = "api/user/create")
	public Action createNewUser = ctx -> {

		// Get input parameter
		String firstName = ctx.params.get("firstName", "");
		String lastName = ctx.params.get("lastName", "");
		String email = ctx.params.get("email", "");

		// Verify input parameters
		// ...

		// Invoke UserDAO
		return userDAO.createNewUser(firstName, lastName, email);
	};

}
```

The REST part (the "HttpAlias" annotation) is optional in the code above.
However, if you want to build REST services, you'll need the
[moleculer-java-web](https://github.com/moleculer-java/moleculer-java-web)
dependency. From another
[Moleculer](https://github.com/moleculer-java/moleculer-java)
service (even from another host) you can
use the service above with the following code:

```java
ServiceBroker broker = ServiceBroker
                         .builder()
                         .transporter(new NatsTransporter("nats://host"))
                         .build();

broker.call("user.create",
            "firstName", "Tom",
            "lastName",  "Smith",
            "email",     "tom.smith@company.com")
      .then(rsp -> {

          // User created successfully
          String recordID = rsp.get("_id", "");
		    	  
      });
```

### Parallel processing

Parallel queries make sense when using clustered MongoDB.
If the queries are independent, they can be started at the same time.
The "Promise.all" function waits for all replies to be received:

```java
Promise p1 = userDAO.getUserByEmail(email);
Promise p2 = roleDAO.getRolesByEmail(email);
Promise p3 = roleDAO.getPostsByEmail(email);

Promise.all(p1, p2, p3).then(rsp -> {

  Tree users = rsp.get(0);
  Tree roles = rsp.get(1);
  Tree posts = rsp.get(2);
  
})
```

## Methods of the MongoDAO / SpringMongoDAO

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

The answer (the "res" JSON) will be similar to the following structure:

```json
{
  "count":2,
  "rows":[
    {
      "v":1,
      "key":{"_id":1},
      "name":"_id_",
      "ns":"db.test"
    }, {
      "v":1,
      "key":{"a":1},
      "name":"a_1",
      "ns":"db.test"
    }
  ]
}
```

### Insert one document

Inserts the provided document. If the document is missing an identifier, the driver should generate one.

```java
Tree doc = new Tree();
doc.put("field1", 123);
doc.put("field2.subfield", false);

insertOne(doc).then(res -> {

 // Insert operation finished
 // The "res" is a JSON structure,
 // with the inserted document + the "_id" field
 
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

The answer (the "res" JSON) will be similar to the following structure:

```json
{
  "matched": 10,
  "modified": 4,
  "acknowledged": true
}
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

The answer (the "res" JSON) will be similar to the following structure:

```json
{
  "deleted": 1,
  "acknowledged": true
}
```

### Delete all documents

Removes all documents from the collection.

```java
deleteAll().then(res -> {

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

The answer (the "res" JSON) will be similar to the following structure:

```json
{
  "count":10345,
  "rows":[
    {
      "firstName": "Tom",
      "lastName":  "Smith",
      "email":     "tom.smith@company.com"
    }    
  ]
}
```

The "count" field contains the max number of rows which meets the "filter" condition
(can be much more than the number of records in the "rows" structure).

### Find one and delete

Atomically find a document and remove it. The answer structure is the searched document, or null.

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

Atomically find a document and replace it. The answer structure is the searched document, or null.

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

Atomically find a document and update it. The answer structure is the searched document, or null.

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
