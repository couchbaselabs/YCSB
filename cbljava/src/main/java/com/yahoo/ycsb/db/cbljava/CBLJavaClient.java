/*
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.cbljava;

import com.couchbase.lite.BasicAuthenticator;
import com.couchbase.lite.Collection;
import com.couchbase.lite.CollectionConfiguration;
import com.couchbase.lite.CouchbaseLite;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseConfiguration;
import com.couchbase.lite.Document;
import com.couchbase.lite.LogLevel;
import com.couchbase.lite.MutableDocument;
import com.couchbase.lite.Parameters;
import com.couchbase.lite.Query;
import com.couchbase.lite.Replicator;
import com.couchbase.lite.ReplicatorActivityLevel;
import com.couchbase.lite.ReplicatorConfiguration;
import com.couchbase.lite.ReplicatorStatus;
import com.couchbase.lite.ReplicatorType;
import com.couchbase.lite.Result;
import com.couchbase.lite.ResultSet;
import com.couchbase.lite.URLEndpoint;
import com.couchbase.lite.ValueIndexConfiguration;
import com.couchbase.lite.logging.ConsoleLogSink;
import com.couchbase.lite.logging.FileLogSink;
import com.couchbase.lite.logging.LogSinks;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding for Couchbase Lite Java (embedded database).
 *
 * Unlike the server-side bindings there is no remote cluster for CRUD: the CBL database lives
 * inside this JVM, one shared instance per YCSB process, used by all client threads. Sync Gateway
 * only enters the picture through the optional background replicator.
 */
public class CBLJavaClient extends DB {

  private static final String KEY_SEPARATOR = ":";

  private static final Object INIT_COORDINATOR = new Object();
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);

  private static volatile Database database;
  private static volatile Collection defaultCollection;
  private static final ConcurrentHashMap<String, Collection> COLLECTIONS = new ConcurrentHashMap<>();

  private static volatile Replicator replicator;
  private static volatile ReplicatorType replicatorType;
  private static volatile List<Collection> syncCollections;
  private static volatile boolean waitForSyncDrain;
  private static volatile long syncDrainTimeoutSeconds;

  private static volatile String dbName;
  private static volatile String dbPath;
  private static volatile boolean preserveDb;
  private static volatile boolean loadPhase;
  private static volatile boolean collectionEnabled;
  private static volatile String[] scopes;
  private static volatile String[] collectionNames;
  private static volatile boolean createIndexes;
  private static volatile String logLevel;
  private static volatile String logDir;
  private static volatile String sgUri;
  private static volatile String channels;
  private static volatile String replicationType;
  private static volatile boolean continuous;
  private static volatile String syncUser;
  private static volatile String syncPassword;

  private boolean upsert;
  private String queryMode;
  private String queryField;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    upsert = props.getProperty("cbl.upsert", "false").equals("true");
    queryMode = props.getProperty("cbl.queryMode", "idrange");
    queryField = props.getProperty("cbl.queryField", "field0");

    dbName = props.getProperty("cbl.dbName", "ycsb");
    dbPath = props.getProperty("cbl.dbPath", "");
    preserveDb = props.getProperty("cbl.preserveDb", "false").equals("true");
    loadPhase = !props.getProperty(Client.DO_TRANSACTIONS_PROPERTY, "true").equals("true");

    collectionEnabled = props.getProperty(Client.COLLECTION_ENABLED_PROPERTY,
        Client.COLLECTION_ENABLED_DEFAULT).equals("true");
    scopes = props.getProperty(Client.SCOPES_PARAM, Client.SCOPES_PARAM_DEFAULT).split(",");
    collectionNames = props.getProperty(Client.COLLECTIONS_PARAM,
        Client.COLLECTIONS_PARAM_DEFAULT).split(",");
    createIndexes = props.getProperty("cbl.createIndexes", "false").equals("true");

    logLevel = props.getProperty("cbl.logLevel", "");
    logDir = props.getProperty("cbl.logDir", "");

    sgUri = props.getProperty("cbl.sgUri", "");
    channels = props.getProperty("cbl.channels", "");
    replicationType = props.getProperty("cbl.replicationType", "pushAndPull");
    continuous = props.getProperty("cbl.continuous", "true").equals("true");
    syncUser = props.getProperty("cbl.user", "");
    syncPassword = props.getProperty("cbl.password", "");
    waitForSyncDrain = props.getProperty("cbl.waitForSyncDrain", "false").equals("true");
    syncDrainTimeoutSeconds =
        Long.parseLong(props.getProperty("cbl.syncDrainTimeoutSeconds", "600"));

    synchronized (INIT_COORDINATOR) {
      if (database == null) {
        initShared();
      }
      OPEN_CLIENTS.incrementAndGet();
    }
  }

  /**
   * One-time process-wide initialization: open the database, resolve collections, create
   * indexes and start the replicator, as requested by the properties.
   */
  private void initShared() throws DBException {
    CouchbaseLite.init();
    configureLogging();

    DatabaseConfiguration config = new DatabaseConfiguration();
    if (!dbPath.isEmpty()) {
      config.setDirectory(dbPath);
    }

    try {
      File dbDir = new File(config.getDirectory());
      if (loadPhase && !preserveDb && Database.exists(dbName, dbDir)) {
        System.err.println("Deleting existing database " + dbName + " in " + dbDir);
        Database.delete(dbName, dbDir);
      }

      long openStart = System.nanoTime();
      database = new Database(dbName, config);
      long openMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - openStart);
      if (preserveDb) {
        System.err.println("[UPGRADE], OpenTime(ms), " + openMillis);
      }

      defaultCollection = database.getDefaultCollection();

      if (collectionEnabled) {
        for (String scope : scopes) {
          for (String coll : collectionNames) {
            COLLECTIONS.put(scope + "." + coll, database.createCollection(coll, scope));
          }
        }
      }

      if (createIndexes) {
        List<Collection> indexed = collectionEnabled
            ? new ArrayList<>(COLLECTIONS.values()) : Arrays.asList(defaultCollection);
        for (Collection coll : indexed) {
          long indexStart = System.nanoTime();
          coll.createIndex("by_" + queryField, new ValueIndexConfiguration(queryField));
          System.err.println("[INDEX], CreateTime(ms), "
              + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - indexStart));
        }
      }

      if (!sgUri.isEmpty()) {
        startReplicator();
      }
    } catch (CouchbaseLiteException | URISyntaxException e) {
      database = null;
      throw new DBException("Failed to initialize Couchbase Lite", e);
    }
  }

  private static void configureLogging() {
    LogLevel level = logLevel.isEmpty()
        ? LogLevel.WARNING : LogLevel.valueOf(logLevel.toUpperCase(Locale.ROOT));
    if (!logLevel.isEmpty()) {
      LogSinks.get().setConsole(new ConsoleLogSink(level));
    }
    if (!logDir.isEmpty()) {
      LogSinks.get().setFile(new FileLogSink.Builder()
          .setDirectory(logDir)
          .setLevel(level)
          .setPlainText(true)
          .build());
    }
  }

  private static void startReplicator() throws CouchbaseLiteException, URISyntaxException {
    List<Collection> toSync = collectionEnabled
        ? new ArrayList<>(COLLECTIONS.values()) : Arrays.asList(defaultCollection);

    List<CollectionConfiguration> collectionConfigs = new ArrayList<>(toSync.size());
    for (Collection coll : toSync) {
      CollectionConfiguration collectionConfig = new CollectionConfiguration(coll);
      if (!channels.isEmpty()) {
        collectionConfig.setChannels(Arrays.asList(channels.split(",")));
      }
      collectionConfigs.add(collectionConfig);
    }

    ReplicatorConfiguration replConfig =
        new ReplicatorConfiguration(collectionConfigs, new URLEndpoint(new URI(sgUri)));
    replicatorType = parseReplicationType(replicationType);
    replConfig.setType(replicatorType);
    replConfig.setContinuous(continuous);

    if (!syncUser.isEmpty()) {
      replConfig.setAuthenticator(new BasicAuthenticator(syncUser, syncPassword.toCharArray()));
    }

    syncCollections = toSync;

    replicator = new Replicator(replConfig);
    replicator.start();
  }

  private static ReplicatorType parseReplicationType(final String type) {
    switch (type.toLowerCase(Locale.ROOT)) {
    case "push":
      return ReplicatorType.PUSH;
    case "pull":
      return ReplicatorType.PULL;
    case "pushandpull":
      return ReplicatorType.PUSH_AND_PULL;
    default:
      throw new IllegalArgumentException(
          "\"cbl.replicationType\" must be push, pull or pushAndPull, got: " + type);
    }
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COORDINATOR) {
      if (OPEN_CLIENTS.decrementAndGet() > 0 || database == null) {
        return;
      }
      try {
        if (replicator != null) {
          if (waitForSyncDrain) {
            drainReplicator();
          }
          stopReplicator();
          replicator = null;
        }
        database.close();
      } catch (CouchbaseLiteException e) {
        throw new DBException("Failed to close Couchbase Lite database", e);
      } finally {
        database = null;
        defaultCollection = null;
        syncCollections = null;
        COLLECTIONS.clear();
      }
    }
  }

  /**
   * Block until the replicator drains (no pending push docs), then log elapsed time and docs
   * replicated, mirroring cblite's sync timing so a one-shot (cbl.continuous=false) run is
   * measurable from the log, like [INDEX]/[UPGRADE].
   */
  private static void drainReplicator() {
    long start = System.currentTimeMillis();
    long deadline = start + syncDrainTimeoutSeconds * 1000L;
    while (System.currentTimeMillis() < deadline) {
      ReplicatorStatus status = replicator.getStatus();
      ReplicatorActivityLevel level = status.getActivityLevel();
      if (level == ReplicatorActivityLevel.STOPPED) {
        if (status.getError() != null) {
          System.err.println("replicator stopped with error: " + status.getError());
        } else {
          logSyncCompletion(start, status);
        }
        return;
      }
      if (level == ReplicatorActivityLevel.IDLE && !hasPendingDocuments()) {
        logSyncCompletion(start, status);
        return;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    System.err.println("replicator drain timed out after " + syncDrainTimeoutSeconds + "s");
  }

  private static void logSyncCompletion(long startMillis, ReplicatorStatus status) {
    long elapsedMillis = System.currentTimeMillis() - startMillis;
    System.err.println("[SYNC], ReplicationTime(ms), " + elapsedMillis);
    System.err.println("[SYNC], DocsReplicated, " + status.getProgress().getCompleted());
  }

  private static boolean hasPendingDocuments() {
    if (replicatorType == ReplicatorType.PULL) {
      return false;
    }
    try {
      for (Collection coll : syncCollections) {
        if (!replicator.getPendingDocumentIds(coll).isEmpty()) {
          return true;
        }
      }
      return false;
    } catch (CouchbaseLiteException e) {
      System.err.println("failed to fetch pending document ids: " + e);
      return false;
    }
  }

  private static void stopReplicator() {
    replicator.stop();
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
    while (replicator.getStatus().getActivityLevel() != ReplicatorActivityLevel.STOPPED
        && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /**
   * Resolve (creating on first use) the collection for the fork's scope/collection overloads.
   */
  private static Collection collection(final String scope, final String coll) {
    return COLLECTIONS.computeIfAbsent(scope + "." + coll, k -> {
        try {
          return database.createCollection(coll, scope);
        } catch (CouchbaseLiteException e) {
          throw new IllegalStateException("Failed to create collection " + k, e);
        }
      });
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    return readFrom(defaultCollection, table, key, fields, result);
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result, final String scope,
                     final String coll) {
    return readFrom(collection(scope, coll), table, key, fields, result);
  }

  private Status readFrom(final Collection coll, final String table, final String key,
                          final Set<String> fields, final Map<String, ByteIterator> result) {
    try {
      Document doc = coll.getDocument(formatId(table, key));
      if (doc == null) {
        return Status.NOT_FOUND;
      }
      extractFields(doc, fields, result);
      return Status.OK;
    } catch (Throwable t) {
      System.err.println("read failed with exception: " + t);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key,
                       final Map<String, ByteIterator> values) {
    return updateIn(defaultCollection, table, key, values);
  }

  @Override
  public Status update(final String table, final String key,
                       final Map<String, ByteIterator> values, final String scope,
                       final String coll) {
    return updateIn(collection(scope, coll), table, key, values);
  }

  private Status updateIn(final Collection coll, final String table, final String key,
                          final Map<String, ByteIterator> values) {
    try {
      MutableDocument doc;
      if (upsert) {
        // CBL save is an upsert with last-write-wins by default: blind write, no read.
        doc = new MutableDocument(formatId(table, key));
      } else {
        Document existing = coll.getDocument(formatId(table, key));
        if (existing == null) {
          return Status.NOT_FOUND;
        }
        doc = existing.toMutable();
      }
      applyValues(doc, values);
      coll.save(doc);
      return Status.OK;
    } catch (Throwable t) {
      System.err.println("update failed with exception: " + t);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key,
                       final Map<String, ByteIterator> values) {
    return insertInto(defaultCollection, table, key, values);
  }

  @Override
  public Status insert(final String table, final String key,
                       final Map<String, ByteIterator> values, final String scope,
                       final String coll) {
    return insertInto(collection(scope, coll), table, key, values);
  }

  private Status insertInto(final Collection coll, final String table, final String key,
                            final Map<String, ByteIterator> values) {
    try {
      MutableDocument doc = new MutableDocument(formatId(table, key));
      applyValues(doc, values);
      coll.save(doc);
      return Status.OK;
    } catch (Throwable t) {
      System.err.println("insert failed with exception: " + t);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      Document doc = defaultCollection.getDocument(formatId(table, key));
      if (doc == null) {
        return Status.NOT_FOUND;
      }
      defaultCollection.delete(doc);
      return Status.OK;
    } catch (Throwable t) {
      System.err.println("delete failed with exception: " + t);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount,
                     final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    return scanFrom(defaultCollection, table, startkey, recordcount, fields, result);
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount,
                     final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result,
                     final String scope, final String coll) {
    return scanFrom(collection(scope, coll), table, startkey, recordcount, fields, result);
  }

  private Status scanFrom(final Collection coll, final String table, final String startkey,
                          final int recordcount, final Set<String> fields,
                          final Vector<HashMap<String, ByteIterator>> result) {
    if (queryMode.equals("fieldmatch")) {
      return fieldMatchQuery(coll, table, startkey, recordcount, result);
    }
    return idRangeScan(coll, table, startkey, recordcount, fields, result);
  }

  private Status idRangeScan(final Collection coll, final String table, final String startkey,
                             final int recordcount, final Set<String> fields,
                             final Vector<HashMap<String, ByteIterator>> result) {
    try {
      // LIMIT does not accept a query parameter in CBL SQL++, so the count is inlined.
      Query query = database.createQuery("SELECT META().id AS id FROM " + keyspace(coll)
          + " WHERE META().id >= $startKey ORDER BY META().id LIMIT " + recordcount);
      Parameters params = new Parameters();
      params.setString("startKey", formatId(table, startkey));
      query.setParameters(params);

      try (ResultSet rows = query.execute()) {
        for (Result row : rows) {
          String id = row.getString("id");
          Document doc = id == null ? null : coll.getDocument(id);
          if (doc == null) {
            continue;
          }
          HashMap<String, ByteIterator> tuple = new HashMap<>();
          extractFields(doc, fields, tuple);
          result.add(tuple);
        }
      }
      return Status.OK;
    } catch (Throwable t) {
      System.err.println("scan failed with exception: " + t);
      return Status.ERROR;
    }
  }

  /**
   * Indexed field-equality lookup: the predicate value is taken from the document at
   * {@code startkey}, so the query always has at least one match to find.
   */
  private Status fieldMatchQuery(final Collection coll, final String table, final String startkey,
                                 final int recordcount,
                                 final Vector<HashMap<String, ByteIterator>> result) {
    try {
      Document seed = coll.getDocument(formatId(table, startkey));
      if (seed == null || seed.getString(queryField) == null) {
        return Status.NOT_FOUND;
      }

      Query query = database.createQuery("SELECT META().id AS id FROM " + keyspace(coll)
          + " WHERE `" + queryField + "` = $fieldValue LIMIT " + recordcount);
      Parameters params = new Parameters();
      params.setString("fieldValue", seed.getString(queryField));
      query.setParameters(params);

      try (ResultSet rows = query.execute()) {
        for (Result row : rows) {
          String id = row.getString("id");
          if (id == null) {
            continue;
          }
          HashMap<String, ByteIterator> tuple = new HashMap<>();
          tuple.put("id", new StringByteIterator(id));
          result.add(tuple);
        }
      }
      return Status.OK;
    } catch (Throwable t) {
      System.err.println("query failed with exception: " + t);
      return Status.ERROR;
    }
  }

  private static String keyspace(final Collection coll) {
    return "`" + coll.getScope().getName() + "`.`" + coll.getName() + "`";
  }

  private static void applyValues(final MutableDocument doc,
                                  final Map<String, ByteIterator> values) {
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      doc.setString(value.getKey(), value.getValue().toString());
    }
  }

  private static void extractFields(final Document doc, final Set<String> fields,
                                    final Map<String, ByteIterator> result) {
    Iterable<String> names = (fields == null || fields.isEmpty()) ? doc.getKeys() : fields;
    for (String name : names) {
      String value = doc.getString(name);
      if (value != null) {
        result.put(name, new StringByteIterator(value));
      }
    }
  }

  /**
   * Helper method to turn the prefix (table) and key into a proper document ID.
   */
  private static String formatId(final String prefix, final String key) {
    return prefix + KEY_SEPARATOR + key;
  }

}
