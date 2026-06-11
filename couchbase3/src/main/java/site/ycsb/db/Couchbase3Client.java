/*
 * Copyright (c) 2019 Yahoo! Inc. All rights reserved.
 * Copyright (c) 2023-2026 benchANT GmbH. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agrlaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.AsyncCollection;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CreateCollectionOptions;
import com.couchbase.client.java.manager.collection.CreateCollectionSettings;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.IndexableDB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DatabaseField;

/**
 * A class that wraps the 3.x Couchbase SDK to be used with YCSB.
 *
 * <p> The following options can be passed when using this database client to override the defaults.
 *
 * <ul>
 * <li><b>couchbase.host=127.0.0.1</b> The hostname from one server.</li>
 * <li><b>couchbase.bucket=ycsb</b> The bucket name to use.</li>
 * <li><b>couchbase.scope=_default</b> The scope to use.</li>
 * <li><b>couchbase.collection=_default</b> The collection to use.</li>
 * <li><b>couchbase.password=</b> The password of the bucket.</li>
 * <li><b>couchbase.durability=</b> Durability level to use.</li>
 * <li><b>couchbase.persistTo=0</b> Persistence durability requirement.</li>
 * <li><b>couchbase.replicateTo=0</b> Replication durability requirement.</li>
 * <li><b>couchbase.upsert=false</b> Use upsert instead of insert or replace.</li>
 * <li><b>couchbase.adhoc=false</b> If set to true, prepared statements are not used.</li>
 * <li><b>couchbase.maxParallelism=1</b> The server parallelism for all n1ql queries.</li>
 * <li><b>couchbase.kvEndpoints=1</b> The number of KV sockets to open per server.</li>
 * <li><b>couchbase.sslMode=false</b> Set to true to use SSL to connect to the cluster.</li>
 * <li><b>couchbase.sslNoVerify=true</b> Set to false to check the SSL server certificate.</li>
 * </ul>
 */

/* <li><b>couchbase.certificateFile=</b> Path to file containing certificates to trust.</li> */
public class Couchbase3Client extends DB implements IndexableDB {

  // constants
  static final String INDEX_LIST_PROPERTY = "couchbase.indexlist";
  private static final String KEY_SEPARATOR = ":";
  private static final String KEYSPACE_SEPARATOR = ".";
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static AtomicInteger primaryKeySeq = new AtomicInteger();
  // configuration options
  private static int batchSize = 1;
  private static boolean debug = false;
  private static boolean useTypedFields = true;
  // generated, but constant values
  private static ClusterEnvironment environment;
  static Cluster cluster;
  private static Bucket bucket;
  private static boolean useDurabilityLevels;
  private static PersistTo persistTo;
  private static ReplicateTo replicateTo;
  private DurabilityLevel durabilityLevel;

  private final Map<String, Map<String,Object>> bulkInserts = new HashMap<String, Map<String,Object>>();
  private String keyspaceName;
  String bucketName;
  private String collectionName;
  private String scopeName;
  private boolean collectionEnabled;
  private boolean scopeEnabled;
  private boolean upsert;
  private boolean adhoc;
  private int maxParallelism;
  private Collection myCollection;

  /**
   * Helper function to convert the key to a numeric value.
   * @param key the key text
   * @return a string with non-numeric characters removed
   */
  private static String numericId(final String key) {
    return key.replaceAll("[^\\d.]", "");
  }

  private static ClusterEnvironment createClusterEnvironment(Properties props) {
    boolean sslMode = props.getProperty("couchbase.sslMode", "false").equals("true");
    boolean sslNoVerify = props.getProperty("couchbase.sslNoVerify", "true").equals("true");
    String certificateFile = props.getProperty("couchbase.certificateFile", "none");
    int kvTimeoutMillis = Integer.parseInt(props.getProperty("couchbase.kvTimeout", "2000"));
    int queryTimeoutMillis = Integer.parseInt(props.getProperty("couchbase.queryTimeout", "14000"));
    if(debug) {
      System.err.println("Couchbase3Client: sslMode=" + sslMode + " sslNoVerify=" + sslNoVerify + " certificateFile=" + certificateFile);
    }
    ClusterEnvironment.Builder b = ClusterEnvironment.builder();
    b.securityConfig(v -> {
      v.enableTls(sslMode);
      v.enableHostnameVerification(sslNoVerify);
    });
    b.timeoutConfig(t -> {
      t.kvTimeout(Duration.ofMillis(kvTimeoutMillis));
      t.queryTimeout(Duration.ofMillis(queryTimeoutMillis));
      t.kvScanTimeout(Duration.ofMillis(kvTimeoutMillis));
    });
    b.disableAppTelemetry(true);
    if(debug) {
      System.err.println("Couchbase3Client: Creating ClusterEnvironment");
    }
    return b.build();
  }

  private static Cluster createCluster(Properties props) {
      String username = props.getProperty("couchbase.username", "Administrator");
      String password = props.getProperty("couchbase.password", "password");
      String hostname = props.getProperty("couchbase.host", "127.0.0.1");
      ClusterOptions clusterOptions = ClusterOptions.clusterOptions(username, password);
      if(debug) {
        System.err.println("Couchbase3Client: connecting to " + hostname + " with user " + username);
        System.err.println("Couchbase3Client: created ClusterOptions");
      }
      environment = createClusterEnvironment(props);
      clusterOptions.environment(environment);
      if(debug) {
        System.err.println("Couchbase3Client: connecting ... ");
      }
      return Cluster.connect(hostname, clusterOptions);
  }

  private Bucket checkBucketExists() {
    Bucket lBucket = cluster.bucket(bucketName);
    // check if bucket exists
    if(lBucket != null) {
      lBucket.waitUntilReady(Duration.ofSeconds(30));
      // this is just a dummy approach to see if the bucket exists or not
      // driver will throw exception if not
    } else {
      System.err.println("Bucket " + bucketName + " does not exist");
      System.exit(-2);
    }
    return lBucket;
  }

  private Scope createScope(CollectionManager mng) {
    if(scopeEnabled) {
      try {
        mng.createScope(scopeName);
        System.err.println("Creating scope " + scopeName);
      } catch (ScopeExistsException e) {
        System.err.println("cope " + scopeName + " already exists. Using that one.");
      } catch (CouchbaseException e) {
        System.err.println("problems creating scope " + scopeName + ". Leaving.");
        System.exit(-3);
      }
      return cluster.bucket(bucketName).scope(scopeName);
    } else {
      return cluster.bucket(bucketName).defaultScope();
    }
  }

  private Collection createCollection(Scope scope, CollectionManager mng) {
    if(collectionEnabled) {
      try {
        /* String createCollectionCommand = Couchbase3QueryBuilder.bindCreateCollectionWithSchemaQuery(bucketName, scopeName, collectionName);
        QueryResult result = cluster.query(createCollectionCommand, QueryOptions.queryOptions().readonly(false));
        if(result.metaData().status() == QueryStatus.SUCCESS) {
          System.err.println("Created collection " + collectionName + " in scope " + scopeName);
        } else {
          System.err.println("problems creating collection " + collectionName + " in scope " + scopeName + ". Leaving.");
          System.exit(-4);
        } */
        mng.createCollection(scopeName, collectionName,
          CreateCollectionSettings.createCollectionSettings(),
          CreateCollectionOptions.createCollectionOptions().timeout(null));
        System.err.println("Creating collection " + collectionName + " in scope " + scopeName);
      } catch (CollectionExistsException e) {
        System.err.println("Collection " + collectionName + " already exists in scope " + scopeName + ". Using that one.");
      } catch (CouchbaseException e) {
        System.err.println("problems creating collection " + collectionName + " in scope " + scopeName + ". Leaving.");
        System.exit(-4);
      }
      return cluster.bucket(bucketName).scope(scopeName).collection(collectionName);
    } else {
      return cluster.bucket(bucketName).defaultCollection();
    }
  }

  private Collection initDatabaseStructure(Properties props) {
    // Create buckets, scopes, collections, indexes, etc. here if needed
    // use cluster object to figure out if bucket exists; abort if not
    bucket = checkBucketExists();
    // let's assume, we have a bucket. Let's create scope/collection if needed
    CollectionManager mng = bucket.collections();
    Scope scope = createScope(mng);
    // TODO: are we able to put a schema on the collection?
    Collection collection = createCollection(scope, mng);
    // TODO:
    List<JsonObject> indexes = Couchbase3IndexHelper.getIndexList(props);
    Couchbase3IndexHelper.setIndexes(props, indexes, collection);
    return collection;
  }

  private void printConfig() {
    System.err.println("Couchbase3Client configuration:");
    System.err.println("  bucket: " + bucketName);
    System.err.println("  scope: " + scopeName);
    System.err.println("  collection: " + collectionName);
    System.err.println("  keyspaceName: " + keyspaceName);
    System.err.println("  upsert: " + upsert);
    System.err.println("  adhoc: " + adhoc);
    System.err.println("  maxParallelism: " + maxParallelism);
    System.err.println("  durabilityLevel: " + durabilityLevel);
    System.err.println("  useDurabilityLevels: " + useDurabilityLevels);
    System.err.println("  persistTo: " + persistTo);
    System.err.println("  replicateTo: " + replicateTo);
    System.err.println("  useTypedFields: " + useTypedFields);
    System.err.println("  batchSize: " + batchSize);
    System.err.println("  debug: " + debug);
    System.err.println("  collectionEnabled: " + collectionEnabled);
    System.err.println("  scopeEnabled: " + scopeEnabled);
    System.err.println("End of Couchbase3Client configuration");
  }

  private Collection getCollectionForFollowupThread() {
    return collectionEnabled
        ? bucket.scope(this.scopeName).collection(this.collectionName)
        : bucket.defaultCollection();
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    OPEN_CLIENTS.getAndIncrement();
    debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
    bucketName = props.getProperty("couchbase.bucket", "ycsb");
    scopeName = props.getProperty("couchbase.scope", "_default");
    upsert = props.getProperty("couchbase.upsert", "false").equals("true");
    collectionName = props.getProperty("couchbase.collection", "_default");
    scopeEnabled = !scopeName.equals("_default");
    collectionEnabled = !collectionName.equals("_default");
    adhoc = props.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(props.getProperty("couchbase.maxParallelism", "0"));
    keyspaceName = getKeyspaceName();
    synchronized (INIT_COORDINATOR) {
      if (cluster != null) {
        myCollection = getCollectionForFollowupThread();
        return;
      }
      durabilityLevel = DurabilityLevel.NONE;
      String rawDurabilityLevel = props.getProperty("couchbase.durability", null);
      useTypedFields = "true".equalsIgnoreCase(props.getProperty(TYPED_FIELDS_PROPERTY));
      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("db.batchsize", "1"));
      // numRetries = Integer.parseInt(props.getProperty(MAX_RETRY_PROPERTY, MAX_RETRY_DEFAULT));
      if (rawDurabilityLevel != null) {
        if (props.containsKey("couchbase.persistTo") || props.containsKey("couchbase.replicateTo")) {
          throw new DBException("Durability setting and persist/replicate settings are mutually exclusive.");
        }
        try {
          durabilityLevel = parseDurabilityLevel(rawDurabilityLevel);
          useDurabilityLevels = true;
        } catch (DBException e) {
          System.err.println("Failed to parse durability level using defaults");
        }
      } else {
        try {
          persistTo = parsePersistTo(props.getProperty("couchbase.persistTo", "0"));
          replicateTo = parseReplicateTo(props.getProperty("couchbase.replicateTo", "0"));
          useDurabilityLevels = false;
        } catch (DBException e) {
          System.err.println("Failed to parse persist/replicate levels using defaults");
        }
      }
      printConfig();
      cluster = createCluster(props);
      bucket = cluster.bucket(bucketName);
      myCollection = initDatabaseStructure(props);

      // boolean enableMutationToken = Boolean.parseBoolean(props.getProperty("couchbase.enableMutationToken", "false"));

      // kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));
        /*
        if (sslMode) {
          ClusterEnvironment.Builder clusterEnvironment = ClusterEnvironment
              .builder()
              .timeoutConfig(
                TimeoutConfig.builder()
                .kvTimeout(Duration.ofMillis(kvTimeoutMillis))
                .queryTimeout(Duration.ofMillis(queryTimeoutMillis))
              )
              .ioConfig(IoConfig.builder()
                .enableMutationTokens(enableMutationToken)
                .numKvConnections(kvEndpoints)
              );

          if (sslNoVerify) {
            clusterEnvironment.securityConfig(SecurityConfig.enableTls(true)
                .enableHostnameVerification(false)
                .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE));
          } else if (!certificateFile.equals("none")) {
            clusterEnvironment.securityConfig(SecurityConfig.enableTls(true)
                .trustCertificate(Paths.get(certificateFile)));
          } else {
            clusterEnvironment.securityConfig(SecurityConfig.enableTls(true));
          }

          environment = clusterEnvironment.build();
        } else {
          environment = ClusterEnvironment
              .builder()
              .timeoutConfig(
                TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeoutMillis)))
              .ioConfig(IoConfig.enableMutationTokens(enableMutationToken)
                  .numKvConnections(kvEndpoints))
              .build();
        }
        */
        // reactiveCluster = cluster.reactive();
    }
  }

  /**
   * Checks the replicate parameter value.
   * @param property provided replicateTo parameter.
   * @return ReplicateTo value.
   */
  private static ReplicateTo parseReplicateTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return ReplicateTo.NONE;
    case 1:
      return ReplicateTo.ONE;
    case 2:
      return ReplicateTo.TWO;
    case 3:
      return ReplicateTo.THREE;
    default:
      throw new DBException("\"couchbase.replicateTo\" must be between 0 and 3");
    }
  }

  /**
   * Checks the persist parameter value.
   * @param property provided persistTo parameter.
   * @return PersistTo value.
   */
  private static PersistTo parsePersistTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return PersistTo.NONE;
    case 1:
      return PersistTo.ONE;
    case 2:
      return PersistTo.TWO;
    case 3:
      return PersistTo.THREE;
    case 4:
      return PersistTo.FOUR;
    default:
      throw new DBException("\"couchbase.persistTo\" must be between 0 and 4");
    }
  }

  /**
   * Checks the durability parameter.
   * @param property provided durability parameter.
   * @return DurabilityLevel value.
   */
  private static DurabilityLevel parseDurabilityLevel(final String property) throws DBException {

    int value = Integer.parseInt(property);

    switch(value){
    case 0:
      return DurabilityLevel.NONE;
    case 1:
      return DurabilityLevel.MAJORITY;
    case 2:
      return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
    case 3:
      return DurabilityLevel.PERSIST_TO_MAJORITY;
    default :
      throw new DBException("\"couchbase.durability\" must be between 0 and 3");
    }
  }

  @Override
  public synchronized void cleanup() {
    int clients = OPEN_CLIENTS.decrementAndGet();
    if (clients == 0 && environment != null) {
      cluster.disconnect();
      environment.shutdown();
      environment = null;
    }
    /*
    System.err.println(Thread.currentThread().getName() + ": dumping errors");
    Iterator<Throwable> it = errors.iterator();
    while(it.hasNext()) {
      Throwable t = (Throwable)it.next();
      t.printStackTrace(System.err);
    } */
  }

    /**
   * Helper function to generate the keyspace name.
   * @return a string with the computed keyspace name
   */
  private String getKeyspaceName() {
    if (scopeEnabled || collectionEnabled) {
      return bucketName + KEYSPACE_SEPARATOR + this.scopeName + KEYSPACE_SEPARATOR + this.collectionName;
    } else {
      return bucketName;
    }
  }

  /**
   * Helper method to turn the prefix and key into a proper document ID.
   *
   * @param prefix the prefix (table).
   * @param key the key itself.
   * @return a document ID that can be used with Couchbase.
   */
  private static String formatId(final String prefix, final String key) {
    return prefix + KEY_SEPARATOR + key;
  }

  /**
   * Insert a record.
   * @param table The name of the table.
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record.
   */
  @Override
  public Status insert(final String table, final String key, List<DatabaseField> values_) {
    Map<String,?> encoding = useTypedFields
      ? Couchbase3QueryHelper.encodeWithTypes(values_)
      : Couchbase3QueryHelper.encode(DB.fieldListAsIteratorMap(values_));
    if(batchSize > 1) {
      return batchInsert(table, key, (Map<String,Object>) encoding);
    }
    // while (true) {
    try {
      Collection collection = myCollection;
      // not adding "record_id" as other implementations will need to runs scans as well
      // values.put("record_id", new StringByteIterator(String.valueOf(primaryKeySeq.incrementAndGet())));
      if (useDurabilityLevels) {
        if (upsert) {
          collection.upsert(formatId(table, key), encoding, upsertOptions().durability(durabilityLevel));
        } else {
          collection.insert(formatId(table, key), encoding, insertOptions().durability(durabilityLevel));
        }
      } else {
        if (upsert) {
          collection.upsert(formatId(table, key), encoding, upsertOptions().durability(persistTo, replicateTo));
        } else {
          collection.insert(formatId(table, key), encoding, insertOptions().durability(persistTo, replicateTo));
        }
      }
      return Status.OK;
    } catch(CouchbaseException e) {
      if(debug) {
        System.err.println("insert failed with exception :");
        e.printStackTrace(System.err);
      }
      return Status.ERROR;
    }
  }

  /**
   * Perform key/value read ("get").
   * @param table The name of the table.
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A HashMap of field/value pairs for the result.
   */
  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    if(debug) {
        System.err.println("reading key " + key + " from table " + table);
    }
    try {
      Collection collection = myCollection;
      GetResult document = collection.get(formatId(table, key));
      if(useTypedFields) {
        Couchbase3QueryHelper.extractTypedFields(document.contentAsObject(), fields, result);
      } else {
        Couchbase3QueryHelper.extractFields(document.contentAsObject(), fields, result);
      }
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch( CouchbaseException e) {
      if(debug) {
        System.err.println("insert failed with exception :");
        e.printStackTrace(System.err);
      }
      return Status.ERROR;
    }
  }

  /**
   * Update record.
   * @param table The name of the table.
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record.
   */
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      Collection collection = myCollection;
      values.put("record_id", new StringByteIterator(String.valueOf(primaryKeySeq.incrementAndGet())));
      if (useDurabilityLevels) {
        collection.replace(formatId(table, key),
          Couchbase3QueryHelper.encode(values), replaceOptions().durability(durabilityLevel));
      } else {
        collection.replace(formatId(table, key),
          Couchbase3QueryHelper.encode(values), replaceOptions().durability(persistTo, replicateTo));
      }
      return Status.OK;
    } catch (DocumentNotFoundException dnf) {
      return Status.NOT_FOUND;
    }catch (CouchbaseException ex) {
      if(debug) {
        System.err.println("insert failed with exception :");
        ex.printStackTrace(System.err);
      }
      return Status.ERROR;
    }
  }

  /**
   * Query for specific rows of data using SQL++.
   * @param table The name of the table.
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   */
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    /*
      try {
        if (fields == null || fields.isEmpty()) {
          return scanAllFields(table, startkey, recordcount, result);
        } else {
          return scanSpecificFields(table, startkey, recordcount, fields, result);
        }
      } catch (Throwable t) {
        if (retryCount == numRetries) {
          addToErrors(t);
          if(debug) {
            System.err.println("scan failed with exception");
            t.printStackTrace(System.err);
          }
          return Status.ERROR;
        } else {
          ++retryCount;
          Couchbase3QueryHelper.retryWait(retryCount);
        }
    }*/
    throw new UnsupportedOperationException("Scan is not yet supported.");
  }

  /**
   * Remove a record.
   * @param table The name of the table.
   * @param key The record key of the record to delete.
   */
  @Override
  public Status delete(final String table, final String key) {
    String pKey = formatId(table, key);
    if(debug) {
      System.err.println("deleting key: " + pKey);
    }
    try {
      Collection collection = myCollection;
      if (useDurabilityLevels) {
        collection.remove(pKey, removeOptions().durability(durabilityLevel));
      } else {
        collection.remove(pKey, removeOptions().durability(persistTo, replicateTo));
      }
      return Status.OK;
    } catch (DocumentNotFoundException dnf) {
      return Status.NOT_FOUND;
    } catch (CouchbaseException ex) {
      if(debug) {
        System.err.println("delete failed with exception :");
        ex.printStackTrace(System.err);
      }
      return Status.ERROR;
    }
  }
  @Override
  public Status findOne(String table, List<Comparison> filters, Set<String> fields, Map<String, ByteIterator> result) {
    if(filters == null || filters.size() == 0) {
      throw new NullPointerException();
    }
    if(fields != null) {
      throw new UnsupportedOperationException("cannot read results by field");
    }

    String query = Couchbase3QueryBuilder.buildFindOnePlaceholderQuery(keyspaceName, filters);
    JsonArray params = JsonArray.create();
    Couchbase3QueryBuilder.bindFindOneQuery(params, filters);
    if(debug) {
      System.err.println("sending query and params:\n\t" + query + "\n\t" + params.toString());
    }
    try {
      QueryOptions options = QueryOptions.queryOptions();
      QueryResult qResult = cluster.query(query, options
                    .adhoc(adhoc)
                    .readonly(true)
                    .parameters(params)
                    .maxParallelism(maxParallelism)
                    // .metrics(true)
      );
      if(qResult.metaData().status() != QueryStatus.SUCCESS) {
        if(debug) {
          System.err.println("unexpected query status: " + qResult.metaData().status());
        }
        return Status.UNEXPECTED_STATE;
      }
      // no other way found to get the amount of results
      List<JsonObject> returned = qResult.rowsAsObject();
      if(returned.size() == 0) {
        return Status.NOT_FOUND;
      }
      if(returned.size() > 1) {
        return Status.UNEXPECTED_STATE;
      }
      Couchbase3QueryHelper.extractTypedFields(returned.get(0), fields, result);
      return Status.OK;
    } catch (DocumentNotFoundException dnf) {
      return Status.NOT_FOUND;
    } catch (CouchbaseException ex) {
      if(debug) {
        System.err.println("delete failed with exception :");
        ex.printStackTrace(System.err);
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status aggregate(String table, String[] airports, int minOccurrences, Vector<HashMap<String, ByteIterator>> results){
    String query = Couchbase3QueryBuilder.buildAggregatePlaceholderQuery(keyspaceName);
    JsonArray params = JsonArray.create();
    Couchbase3QueryBuilder.bindAggregatePlaceholderQuery(params, keyspaceName, airports, minOccurrences);
        if(debug) {
      System.err.println("aggregate: sending query and params:\n\t" + query + "\n\t" + params.toString());
    }
    try {
      QueryOptions options = QueryOptions.queryOptions();
      // QueryResult qResult = bucket.defaultScope().query(query,
      QueryResult qResult = cluster.query(query,
          options.adhoc(adhoc)
                .parameters(params)
                .readonly(true)
                .maxParallelism(maxParallelism)
                // .asTransaction(SingleQueryTransactionOptions.singleQueryTransactionOptions().durabilityLevel(DurabilityLevel.NONE))
                // .metrics(true)
      );
      if(qResult.metaData().status() != QueryStatus.SUCCESS) {
        if(debug) {
          System.err.println("unexpected query status: " + qResult.metaData().status());
        }
        return Status.UNEXPECTED_STATE;
      }
      List<JsonObject> returned = qResult.rowsAsObject();
      if(returned.size() == 0) {
        return Status.NOT_FOUND;
      }
      if(returned.size() > Couchbase3QueryBuilder.AGGREAGTE_QUERY_LIMIT) {
        return Status.UNEXPECTED_STATE;
      }
      for(JsonObject obj : returned) {
        HashMap<String, ByteIterator> row = new HashMap<String, ByteIterator>();
        Couchbase3QueryHelper.extractTypedFields(obj, null, row);
        results.add(row);
      }
      return Status.OK;
    } catch (DocumentNotFoundException dnf) {
      return Status.NOT_FOUND;
    } catch (CouchbaseException ex) {
      if(debug) {
        System.err.println("aggregate failed with exception :");
        ex.printStackTrace(System.err);
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status updateOne(String table, List<Comparison> filters, List<DatabaseField> fields) {
    if(filters == null || filters.size() == 0) {
      throw new NullPointerException();
    }
    if(fields == null || fields.size() == 0) {
      throw new NullPointerException();
    }
    String query = Couchbase3QueryBuilder.buildUpdateOnePlaceholderQuery(keyspaceName, filters, fields);
    JsonArray params = JsonArray.create();
    Couchbase3QueryBuilder.bindUpdateOneQuery(params, fields, filters);
    if(debug) {
      System.err.println("queryOne: sending query and params:\n\t" + query + "\n\t" + params.toString());
    }
    try {
      QueryOptions options = QueryOptions.queryOptions();
      // QueryResult qResult = bucket.defaultScope().query(query,
      QueryResult qResult = cluster.query(query,
        options.adhoc(adhoc)
                .parameters(params)
                .readonly(false)
                .maxParallelism(maxParallelism)
                // .asTransaction(SingleQueryTransactionOptions.singleQueryTransactionOptions().durabilityLevel(DurabilityLevel.NONE))
                // .metrics(true)
      );
      if(qResult.metaData().status() != QueryStatus.SUCCESS) {
        if(debug) {
          System.err.println("unexpected query status: " + qResult.metaData().status());
        }
        return Status.UNEXPECTED_STATE;
      }
      List<JsonObject> returned = qResult.rowsAsObject();
      if(returned.size() == 0) {
        return Status.NOT_FOUND;
      }
      if(returned.size() > 1) {
        return Status.UNEXPECTED_STATE;
      }
      return Status.OK;
    } catch (DocumentNotFoundException dnf) {
      return Status.NOT_FOUND;
    } catch (CasMismatchException ex) {
      if(debug) {
        System.err.println("updateOne failed with CasMismatchException");
        ex.printStackTrace(System.err);
      }
      return Status.CONCURRENT_UPDATE;
    } catch (CouchbaseException ex) {
      if(debug) {
        System.err.println("delete failed with exception :");
        ex.printStackTrace(System.err);
      }
      return Status.ERROR;
    }
  }

  private Status batchInsert(final String table, final String key, Map<String,Object> encoding) {
    final String theId = formatId(table, key);
    bulkInserts.put(theId, encoding);
    if(bulkInserts.size() < batchSize) {
      return Status.BATCHED_OK;
    }
    // System.exit(-1);
    final Map<String, Map<String,Object>> localInserts = new HashMap<String, Map<String,Object>>(bulkInserts);
    bulkInserts.clear();
    // ReactiveBucket rBucket = bucket.reactive();
    // ReactiveCollection collection = collectionEnabled
    //   ? rBucket.scope(this.scopeName).collection(this.collectionName)
    //   : rBucket.defaultCollection();
    AsyncCollection collection = myCollection.async();
    // bulk inserts has right size, let's send it
    // Iterate over a list of documents to insert.
    Map<String, CompletableFuture<MutationResult>> futures = new HashMap<>();
    for(Map.Entry<String, Map<String,Object>> entry : localInserts.entrySet()) {
      CompletableFuture<MutationResult> future;
      if (useDurabilityLevels) {
        if (upsert) {
          future = collection.upsert(entry.getKey(), entry.getValue(), upsertOptions().durability(durabilityLevel));
        } else {
          future = collection.insert(entry.getKey(), entry.getValue(), insertOptions().durability(durabilityLevel));
        }
      } else {
        if (upsert) {
          future = collection.upsert(entry.getKey(), entry.getValue(), upsertOptions().durability(persistTo, replicateTo));
        } else {
          future = collection.insert(entry.getKey(), entry.getValue(), insertOptions().durability(persistTo, replicateTo));
        }
      }
      futures.put(entry.getKey(), future);
    }
    int errorCount = 0;
    for(Map.Entry<String, CompletableFuture<MutationResult>> entry : futures.entrySet()) {
      try {
        entry.getValue().join();
      } catch (Exception e) {
        if(debug) {
          System.err.println("one insert failed in batch insert loop: " + entry.getKey());
        }
        errorCount++;
      }
    }
    if(errorCount == 0) {
      return Status.OK;
    }
    // at least one document failed
    if(debug) {
      System.err.println("at least one insert failed in batch insert loop");
    }
    return Status.ERROR;
  }

   /**
   * Performs the {@link #scan(String, String, int, Set, Vector)} operation for all fields.
   * @param table The name of the table.
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   * /
  private Status scanAllFields(final String table, final String startkey, final int recordcount,
                               final Vector<HashMap<String, ByteIterator>> result) {
/*
    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    final String query = "SELECT record_id FROM " + keyspaceName +
        " WHERE record_id >= \"$1\" ORDER BY record_id LIMIT $2";
    QueryOptions scanQueryOptions = QueryOptions.queryOptions();

    if (maxParallelism > 0) {
      scanQueryOptions.maxParallelism(maxParallelism);
    }

    cluster.reactive().query(query,
            scanQueryOptions
            .pipelineBatch(128)
            .pipelineCap(1024)
            .scanCap(1024)
            .adhoc(adhoc)
            .readonly(true)
            .parameters(JsonArray.from(numericId(startkey), recordcount)))
            .flatMapMany(ReactiveQueryResult::rowsAsObject)
              .onErrorResume(e -> {
                  if(debug) {
                    System.err.println("Start Key: " + startkey + " Count: "
                      + recordcount + " Error:" + e.getClass() + " Info: " + e.getMessage());
                  }
                  return Mono.empty();
                })
              .map(row -> {
                  HashMap<String, ByteIterator> tuple = new HashMap<>();
                  tuple.put("record_id", new StringByteIterator(row.getString("record_id")));
                  return tuple;
                })
              .toStream()
              .forEach(data::add);

    result.addAll(data);
    return Status.OK;
    * /
    throw new UnsupportedOperationException("Scan all fields is not yet supported.");
  }

  /**
   * Performs the {@link #scan(String, String, int, Set, Vector)} operation only for a subset of the fields.
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   * /

  private Status scanSpecificFields(final String table, final String startkey, final int recordcount,
                                    final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    final Collection collection = bucket.defaultCollection();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    final String query =  "SELECT RAW meta().id FROM " + keyspaceName +
        " WHERE record_id >= $1 ORDER BY record_id LIMIT $2";
    final ReactiveCollection reactiveCollection = collection.reactive();
    QueryOptions scanQueryOptions = QueryOptions.queryOptions();

    if (maxParallelism > 0) {
      scanQueryOptions.maxParallelism(maxParallelism);
    }

    reactiveCluster.query(query,
            scanQueryOptions
            .adhoc(adhoc)
            .parameters(JsonArray.from(numericId(startkey), recordcount)))
        .flatMapMany(res -> {
            return res.rowsAs(String.class);
          })
        .flatMap(id -> {
            return reactiveCollection
              .get(id, GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE));
          })
        .map(getResult -> {
            HashMap<String, ByteIterator> tuple = new HashMap<>();
            decodeStringSource(getResult.contentAs(String.class), fields, tuple);
            return tuple;
          })
        .toStream()
        .forEach(data::add);

    result.addAll(data);
    return Status.OK;
  }
  /**
   * Get string values from fields.
   * @param source JSON source data.
   * @param fields Fields to return.
   * @param dest Map of Strings where each value is a requested field.
   * /
  private void decodeStringSource(final String source, final Set<String> fields,
                      final Map<String, ByteIterator> dest) {
    try {
      JsonNode json = JacksonTransformers.MAPPER.readTree(source);
      boolean checkFields = fields != null && !fields.isEmpty();
      for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
        Map.Entry<String, JsonNode> jsonField = jsonFields.next();
        String name = jsonField.getKey();
        if (checkFields && !fields.contains(name)) {
          continue;
        }
        JsonNode jsonValue = jsonField.getValue();
        if (jsonValue != null && !jsonValue.isNull()) {
          dest.put(name, new StringByteIterator(jsonValue.asText()));
        }
      }
    } catch (Exception e) {
      if(debug) {
          System.err.println("Could not decode JSON response from scanSpecificFields");
      }
    }
  }
    */
}