# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`moleculer-java-mongo` is a **library** (not an application): an asynchronous, Promise-based MongoDB
client for the Java Moleculer ecosystem. It wraps the official MongoDB **reactive streams** driver
and exposes a DAO-style API where every operation returns an `io.datatree.Promise` and every
document/filter is an `io.datatree.Tree` (the DataTree library) instead of raw `Bson`/`Document`.
It works standalone or as Spring beans.

Bytecode target **Java 17** (`<maven.compiler.release>17</maven.compiler.release>`). Minimum consumer
runtime: **JDK 17** (the optional Spring dep is compiled against; non-Spring consumers need JDK 11+
for the MongoDB driver 5.x). Build JDK 17+ (JDK 25 in use). Single package:
`services.moleculer.mongo`. Version **2.0.0** (one Maven `<version>`).

## Build & test commands

Maven build. Compiles with plain `javac`.

```bash
mvn clean verify             # compile + run all tests (the definition-of-done gate)
mvn clean test               # run tests only
mvn clean install            # full build + jar -> local ~/.m2 (output: target/moleculer-java-mongo-2.0.0-SNAPSHOT.jar)
mvn -Prelease -Dgpg.skip=true clean package   # also build sources + javadoc jars
mvn test -Dtest=MongoTest    # run a single test class
```

**`MongoTest` is a live integration test** that needs a real MongoDB on `localhost:27017`
(database `db`) — there is no embedded/mock Mongo. It is JUnit 5 (Jupiter) and guarded by an
`assumeTrue` reachability probe: when a server is up it **runs and asserts**; when the port is
closed it **skips cleanly** so `mvn clean verify` stays green offline (mirroring the broker /
port-guard integration tests elsewhere in the workspace).

## Architecture

The whole library is a thin translation layer between **DataTree** (`Tree`/`Promise`) and the
**MongoDB reactive streams driver**. Understanding three conversions explains most of the code:

**1. Reactive `Publisher` → `Promise`.** The Mongo reactive driver returns `Publisher<T>`. Two
`Subscriber` adapters convert these to DataTree Promises:
- `SingleResultPromise` — resolves with the single emitted value (for insert/update/delete/findOne/count).
- `CollectAllPromise` — collects every emitted item into a `Tree` shaped `{ "count": N, "rows": [...] }`
  (for find/listIndexes/mapReduce). The same `{count, rows}` shape appears throughout the public API.

**2. `Tree` ↔ `Bson`/`Document`.** `MongoFilters.toBson(Tree)` unwraps a Tree to the underlying
`Bson`/`Document`. `BsonTree` is the reverse: a `Tree` subclass that directly wraps a `Bson` so
filter builders can return Trees. This is why filters and documents are interchangeable as `Tree`.

**3. Class hierarchy.** `MongoFilters` → `MongoDAO` → `SpringMongoDAO`, and user DAOs extend
`MongoDAO` (or `SpringMongoDAO`):
- `MongoFilters` — `protected` filter-builder methods (`eq`, `ne`, `gt`, `and`, `or`, `regex`,
  `geoWithin`, …) mirroring `com.mongodb.client.model.Filters`, each returning a `Tree`.
- `MongoDAO` — `protected` CRUD/index/aggregation methods, all returning `Promise`. This is the
  superclass for every DAO. The intended pattern: a subclass adds **public** business methods
  (e.g. `insertUser`) that call these `protected` primitives — see `TestDAO` for the canonical example.
- `SpringMongoDAO` — adds `ApplicationContextAware` wiring; resolves its connection pool from the
  Spring context.

`MongoConnectionPool` owns the `MongoClient`/`MongoDatabase`. Lifecycle is `init()` (Spring
init-method) / `destroy()` (destroy-method); defaults to localhost and database `db` unless
`connectionString`/`database` are set.

## Conventions that aren't obvious from signatures

- **Collection name resolution** (`MongoDAO.setMongoConnectionPool`): uses the `@MongoCollection("name")`
  annotation if present; otherwise derives the name from the class — lowercased, `DAO` suffix stripped
  (`UserDAO` → `user`, `XyzDAO` → `xyz`).
- **Spring connection selection** (`SpringMongoDAO`): `@MongoConnection("beanName")` picks a specific
  `MongoConnectionPool` bean; without it, the single default pool bean is used.
- **`eq(String id)`** (single-arg) is special: it matches `_id` as an `ObjectId`, not a field named
  `id`. The two-arg `eq(field, value)` is the general equality filter.
- **Update auto-wrapping** (`prepareForUpdate`): `updateOne`/`updateMany`/`findOneAndUpdate` wrap the
  given Tree in `$set` automatically unless it already contains a `$set` key. Pass raw field/value
  Trees, not `$set` documents.
- **Result shapes**: update/replace → `{matched, modified, acknowledged}` (+ `_id` on upsert);
  delete → `{deleted, acknowledged}`; find → `{count, rows}` where `count` is the **total** matching
  the filter and `rows` is the (paged) result set.
- **`find` paging**: results are capped at `maxItemsPerQuery` (default 10240). `find(filter, sort,
  first, limit)` is the full form; `count` in the response is the unpaged total.
- **Blocking in tests/samples**: Promises are awaited with `.waitFor(timeoutMillis)`. Production code
  should chain `.then(...)`/`.catchError(...)` instead of blocking.

## Adding a new DAO operation

Add a `protected` method on `MongoDAO` that calls the driver's `collection.*` method and wraps the
returned `Publisher` with `singleResult(...)` or `collectAll(...)`, converting `Tree` args via
`toBson(...)` and (where relevant) mapping the driver result to the standard Tree shape via the
private `*ResultToTree` helpers. Mirror the existing methods' Javadoc-with-usage-example style.
