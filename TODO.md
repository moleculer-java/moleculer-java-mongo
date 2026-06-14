# TODO — Modernize `moleculer-java-mongo` to 2.0.0

> **You are the per-project Claude Code instance for `moleculer-java-mongo`.** Self-contained file.
> Goal: Gradle/Java 8 → **Maven + JDK 21**, deps upgraded (esp. the **MongoDB reactive driver 1.12 →
> 5.x**), tests green on **JUnit 5**, legacy files removed, version **2.0.0**. This is a **library**:
> an async, Promise-based MongoDB client where every operation returns an `io.datatree.Promise` and
> every document/filter is an `io.datatree.Tree`. Single package: `services.moleculer.mongo`.
>
> ⚠ **Despite the name, this module does NOT depend on `moleculer-java`.** Its only workspace
> dependency is **`datatree-promise`** (which brings `datatree-core` transitively). It builds on the
> datatree stack alone, so it only needs the phase-1/2 datatree artifacts in the local repo.

## Coordinates & facts
- Maven: `com.github.berkesa:moleculer-java-mongo`, `jar`, license **MIT**.
- `name`: *MongoDB Client for Moleculer Framework* · `description`: *Non-blocking, Promise-based
  MongoDB Client for Moleculer Applications.* · `inceptionYear`: 2019
- `url`: https://moleculer-java.github.io/moleculer-java-mongo/ · `scm`:
  https://github.com/moleculer-java/moleculer-java-mongo.git
- developer: `berkesa` / Andras Berkes / andras.berkes@programmer.net
- **Version → `2.0.0`.** Old Gradle build hard-codes it in `version` **and** `jar { baseName =
  'moleculer-mongo'; version = '1.0.0' }`. Maven names the jar after the artifactId → the file becomes
  `moleculer-java-mongo-2.0.0.jar` (was `moleculer-mongo-1.0.0.jar`); **coordinates are unchanged**, so
  the single Maven `<version>` is the only place left.
- This **is** a published Maven Central library → configure the release profile (sources + javadoc +
  GPG + `central-publishing-maven-plugin`), same as the other datatree/moleculer artifacts.

## Inter-project dependency (PIN to 2.0.0)
- `com.github.berkesa:datatree-promise:2.0.0` (was **1.0.6**) — the only workspace dep. It pulls
  `datatree-core:2.0.0` transitively. Build `datatree-core` + `datatree-promise` first (both already
  done & installed as `2.0.0-SNAPSHOT` in the local `~/.m2`). **Leaf module — no workspace dependents,
  so nothing to ripple downstream.**

## Target versions (confirm newest at execution; keep workspace lockstep)
| Dependency | Current | Target | Scope |
|---|---|---|---|
| `com.github.berkesa:datatree-promise` | 1.0.6 | **2.0.0** | compile |
| `org.mongodb:mongodb-driver-reactivestreams` | 1.12.0 | **5.x** (use **5.8.0** to match the locked `bson` 5.8.0 — same release train) | compile |
| `org.mongodb:bson` (transitive via driver) | 4.x | **5.8.0** (🔒 lockstep — locked workspace-wide) | (transitive) |
| `org.reactivestreams:reactive-streams` | (transitive 1.0.x) | **1.0.4** (declare explicitly — `Publisher`/`Subscriber`/`Subscription` are used directly) | compile |
| `org.slf4j:slf4j-api`, `slf4j-jdk14`, `log4j-over-slf4j`, `jcl-over-slf4j` | 1.7.28 | **2.0.18** (🔒 lockstep) | api compile / 3 bridges runtime |
| `org.springframework:spring-context` | 5.0.8.RELEASE | **6.2.8** ⚠ Java 17+ (🔒 lockstep — locked by moleculer-java) | compile, mark **`<optional>true</optional>`** |
| `junit:junit` 4.12 | → `org.junit.jupiter:junit-jupiter` | **5.14.4** (🔒 lockstep) | test |
| Eclipse `ecj` 4.4.2 | — | **remove** | — |
| Java | 1.8 | **21** | — |

**Notes on the driver bump (the only real code risk):**
- Package names are **unchanged** in 5.x: `com.mongodb.reactivestreams.client.{MongoClient,
  MongoClients,MongoDatabase,MongoCollection,FindPublisher}` and `com.mongodb.client.model.*` /
  `com.mongodb.client.result.*` / `com.mongodb.client.model.geojson.*` all still exist. Return types
  are still `org.reactivestreams.Publisher<T>`, so `SingleResultPromise` / `CollectAllPromise` (the two
  `Subscriber` adapters) keep working.
- Verify against 5.x: `MongoClients.create()` / `create(ConnectionString)`, `listDatabaseNames()`,
  `getDatabase()`, `getCollection()`, and every option type used in `MongoDAO` / `MongoTest`
  (`CountOptions`, `DeleteOptions`, `ReplaceOptions`, `FindOneAndUpdateOptions`, `RenameCollectionOptions`,
  `DropIndexOptions`, `TextSearchOptions`, `ReturnDocument`, `Indexes`, `Filters`, `geojson.Point/Position`).
  These are stable, but a few defaults/signatures changed across 2→5 — fix any that don't compile.
- `MongoFilters` / `BsonTree` compile against **`org.bson` 5.8.0** (the locked workspace BSON). Confirm
  `Filters`/`Indexes`/`Bson`/`conversions.Bson` API is unchanged for the methods used.
- `spring-context` is used **only** by `SpringMongoDAO` (`ApplicationContextAware`); the library "works
  standalone or as Spring beans", so mark Spring **`<optional>`** — non-Spring users don't pull it. No
  `javax`→`jakarta` touch points exist in this module's source.

## Steps
1. **`pom.xml`** (metadata + MIT + `<maven.compiler.release>21</maven.compiler.release>`). Apply the
   dependency table. Build plugins: compiler **3.15.0**, surefire **3.5.4** (lockstep). Release profile:
   `maven-source-plugin` 3.3.1 + `maven-javadoc-plugin` 3.11.2 (`<doclint>none</doclint>`) + `maven-gpg-plugin`
   3.2.7 + `central-publishing-maven-plugin` 0.9.0 (see `coordination/MAVEN-CENTRAL-PUBLISHING.md`). The old
   `javadoc { failOnError = false }` → `<doclint>none</doclint>`.
2. **Remove ECJ → javac.** Delete the `compileJava { options.fork … org.eclipse.jdt…Main }` block + the
   `ecj` configuration. Then fix any javac-vs-ECJ edge cases.
   - ⚠ `MongoConnectionPool.finalize()` overrides `Object.finalize()`, which is **deprecated for removal**
     on JDK 21 → javac emits a deprecation warning (not an error; build still passes). Optional cleanup:
     replace the `finalize()` safety-net with an explicit `close()` / `java.lang.ref.Cleaner`. Keeping it
     is acceptable for 2.0.0 — just don't enable `-Werror`.
3. **MongoDB reactive driver 1.12 → 5.x** (see notes above). Compile clean against 5.8.0 + bson 5.8.0.
4. **Tests → JUnit 5.** `MongoTest` is JUnit 4 (`org.junit.Test` / `org.junit.Assert`). Migrate to
   Jupiter (`org.junit.jupiter.api.*`).
   - ⚠ **`MongoTest` needs a live `mongod` on `localhost:27017`** (database `db`) — there is **no**
     embedded/mock Mongo. To keep `mvn verify` **green offline**, guard it with an `assumeTrue(...)`
     reachability probe (open a socket to `127.0.0.1:27017` with a short timeout, or attempt a connect
     with a 1–2 s `connectionTimeout`): the test then **runs** when a server is up and **skips cleanly**
     when it isn't — mirroring `moleculer-java-httpclient`'s port-8080 `assumeTrue` guard and the
     broker-integration exclusions elsewhere in the workspace. (Alternatives if you prefer: tag
     `@Tag("integration")` + surefire `<excludedGroups>`, or Testcontainers `MongoDBContainer` when Docker
     is available. Default to the zero-infra `assumeTrue`-skip so the DoD's `mvn clean verify` stays green
     with no server.)
   - `TestDAO`, `XyzDAO`, `Sample` are test-only helpers — keep them compiling (they demonstrate the
     `MongoDAO`/`SpringMongoDAO` subclassing pattern).
5. **Preserve API contracts** (public behavior is the product — do not drift):
   - The pervasive result shapes: find/list/mapReduce → `{ "count": N, "rows": [...] }` (count = unpaged
     total); update/replace → `{matched, modified, acknowledged}` (+ `_id` on upsert); delete →
     `{deleted, acknowledged}`.
   - `eq(String id)` (single-arg) matches **`_id` as `ObjectId`**; two-arg `eq(field, value)` is general
     equality. `prepareForUpdate` auto-wraps in `$set` unless `$set` is already present.
   - Collection-name resolution: `@MongoCollection("name")`, else class name lowercased with the `DAO`
     suffix stripped (`UserDAO` → `user`). Spring pool selection via `@MongoConnection("bean")`.
   - `find` paging cap `maxItemsPerQuery` (default 10240). `protected` filter/CRUD methods (subclass-extends
     convention). `MongoConnectionPool` lifecycle `init()`/`destroy()`, defaults localhost / db `db`.
6. **Cleanup — delete:** `build.gradle`, `settings.gradle`, `gradlew`, `gradlew.bat`, `gradle/`,
   `.gradle/`, `.travis.yml`, `.classpath`, `.project`, `.settings/`, and the stale `bin/` + `build/`
   output dirs. Remove the dead **Travis / Codacy / codecov badges** from `README.md`.
7. **VSCode + `.gitignore`.** Library — no `launch.json` needed (`Sample` is a manual test harness; add a
   launch entry only if you want one). `.gitignore` covers `target/`, `.gradle/`, `bin/`, `.classpath`,
   `.project`, `.settings/`.
8. **Build & install:** `mvn clean install` (→ local `~/.m2`), then `mvn clean verify` (DoD gate).
9. **Update `CLAUDE.md`:** Maven commands (`mvn clean verify` etc.); MongoDB reactive driver **5.x**;
   **Java 21**; **JUnit 5** with the `assumeTrue` live-Mongo skip-guard (replaces the old "tests require a
   real MongoDB" hard failure — now skips cleanly); version `2.0.0` in a single Maven `<version>`; ECJ
   dropped (compiles with `javac`).

## Definition of done
- `mvn clean verify` **green on JDK 21** (with no mongod running: `MongoTest` **skips**, build passes;
  with a live mongod on 27017: `MongoTest` **runs** and passes).
- MongoDB reactive driver on **5.x** (bson 5.8.0); `datatree-promise:2.0.0`; slf4j 2.0.18; Spring 6.2.8
  (`<optional>`); JUnit 5; ECJ dropped.
- No Gradle/ECJ/Travis/Eclipse files remain; `.vscode/` + `.gitignore` added; dead README badges removed.
- Version `2.0.0`; publishing (Central Portal) configured; installed to local `~/.m2`.
