/*
* Copyright (c) 2019 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb.db.couchbase3;


import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.env.ClusterEnvironment.Builder;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;

import com.couchbase.client.java.ClusterOptions;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

import com.couchbase.client.java.codec.RawJsonTranscoder;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import com.yahoo.ycsb.*;

import java.io.*;

import java.nio.file.Paths;

import java.security.KeyStore;

import java.time.Duration;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
* Full YCSB implementation based on the new Couchbase Java SDK 3.x.
*/
public class Couchbase3Client extends DB {

  private static final String KEY_SEPARATOR = ":";

  private static volatile ClusterEnvironment environment;
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();

  private volatile Cluster cluster;
  private static volatile ReactiveCluster reactiveCluster;
  private static volatile Bucket bucket;
  private static volatile ClusterOptions clusterOptions;
  //private volatile Collection collectiont;
  // private static Transactions transactions;
  private static boolean transactionEnabled;
  private int[] transactionKeys;

  private volatile DurabilityLevel transDurabilityLevel;

  private volatile DurabilityLevel durabilityLevel;
  private volatile PersistTo persistTo;
  private volatile ReplicateTo replicateTo;
  private volatile boolean useDurabilityLevels;
  private  volatile  HashSet errors = new HashSet<Throwable>();

  private boolean adhoc;
  private int maxParallelism;
  private String scanAllQuery;
  private String bucketName;

  private static boolean collectionenabled;
  private static String username;
  private static String password;
  private static String hostname;
  private static int kvPort;
  private static int managerPort;
  private static long kvTimeoutMillis;
  private static int kvEndpoints;
  private boolean upsert;

  private static KeyStore keyStore;
  private String sslMode;
  private String certificateFile;
  private String certKeystoreFile;
  private String certKeystorePassword;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    boolean outOfOrderExecution = Boolean.parseBoolean(props.getProperty("couchbase.outOfOrderExecution", "false"));
    if (outOfOrderExecution) {
      System.setProperty("com.couchbase.unorderedExecutionEnabled", "true");
    } else {
      System.setProperty("com.couchbase.unorderedExecutionEnabled", "false");
    }

    bucketName = props.getProperty("couchbase.bucket", "ycsb");
    // durability options
    String rawDurabilityLevel = props.getProperty("couchbase.durability", null);
    if (rawDurabilityLevel == null) {
      persistTo = parsePersistTo(props.getProperty("couchbase.persistTo", "0"));
      replicateTo = parseReplicateTo(props.getProperty("couchbase.replicateTo", "0"));
      useDurabilityLevels = false;
    } else {
      durabilityLevel = parseDurabilityLevel(rawDurabilityLevel);
      useDurabilityLevels = true;
    }

    adhoc = props.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(props.getProperty("couchbase.maxParallelism", "1"));
    scanAllQuery =  "SELECT RAW meta().id FROM `" + bucketName +
        "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";
    bucketName = props.getProperty("couchbase.bucket", "default");
    upsert = props.getProperty("couchbase.upsert", "false").equals("true");


    int numATRS = Integer.parseInt(props.getProperty("couchbase.atrs", "20480"));

    hostname = props.getProperty("couchbase.host", "127.0.0.1");
    managerPort = Integer.parseInt(props.getProperty("couchbase.managerPort", "8091"));
    username = props.getProperty("couchbase.username", "Administrator");

    password = props.getProperty("couchbase.password", "password");

    collectionenabled = props.getProperty(Client.COLLECTION_ENABLED_PROPERTY,
        Client.COLLECTION_ENABLED_DEFAULT).equals("true");

    certKeystoreFile = props.getProperty("couchbase.certKeystoreFile", "");
    certKeystorePassword = props.getProperty("couchbase.certKeystorePassword", "");
    sslMode = props.getProperty("couchbase.sslMode", "none");
    certificateFile = props.getProperty("couchbase.certificateFile", "none");

    if (sslMode.equals("none")) {
      kvPort = Integer.parseInt(props.getProperty("couchbase.kvPort", "11210"));
    } else {
      kvPort = Integer.parseInt(props.getProperty("couchbase.kvPort", "11207"));
    }

    synchronized (INIT_COORDINATOR) {
      if (environment == null) {

        boolean enableMutationToken = Boolean.parseBoolean(props.getProperty("couchbase.enableMutationToken", "false"));

        kvTimeoutMillis = Integer.parseInt(props.getProperty("couchbase.kvTimeoutMillis", "60000"));
        kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));

        transactionEnabled = Boolean.parseBoolean(props.getProperty("couchbase.transactionsEnabled", "false"));
        try {
          durabilityLevel = parseDurabilityLevel(props.getProperty("couchbase.durability", "0"));
        } catch (DBException e) {
          System.out.println("Failed to parse Durability Level");
        }

        try {
          transDurabilityLevel = parseDurabilityLevel(props.getProperty("couchbase.durability", "0"));
        } catch (DBException e) {
          System.out.println("Failed to parse TransactionDurability Level");
        }

        if (sslMode.equals("auth")){
          try {
            char[] pass = certKeystorePassword.toCharArray();

            File keystoreFile = new File(certKeystoreFile);
            FileInputStream is = new FileInputStream(keystoreFile);

            keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(is, pass);
          } catch (Exception e) {
            e.printStackTrace();
          } 
        }
        setupClusterEnv(enableMutationToken);
        clusterOptions = ClusterOptions.clusterOptions(username, password);
        clusterOptions.environment(environment);
        if (!sslMode.equals("auth")) {
          Set<SeedNode> seedNodes = new HashSet<>(Arrays.asList(
              SeedNode.create(hostname,
                  Optional.of(kvPort),
                  Optional.of(managerPort))));
          cluster = Cluster.connect(seedNodes, clusterOptions);
        } else {
          cluster = Cluster.connect(hostname, clusterOptions);
        }

        reactiveCluster = cluster.reactive();
        bucket = cluster.bucket(bucketName);
      }
    }
    OPEN_CLIENTS.incrementAndGet();
  }


  private void setupClusterEnv(boolean enableMutationToken) throws DBException {

    Builder envBuilder = ClusterEnvironment.builder();
    if (sslMode.equals("data")) {
      envBuilder = envBuilder
          .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeoutMillis)))
          .ioConfig(IoConfig.enableMutationTokens(enableMutationToken).numKvConnections(kvEndpoints))
          .securityConfig(SecurityConfig.enableTls(true)
              .trustCertificate(Paths.get(certificateFile)));
    } else if (sslMode.equals("capella")) {
      envBuilder = envBuilder
          .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeoutMillis)))
          .ioConfig(IoConfig.enableMutationTokens(enableMutationToken).numKvConnections(kvEndpoints)
                  .enableDnsSrv(true))
          .securityConfig(SecurityConfig.enableTls(true)
                  .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE));
    } else if (sslMode.equals("auth")) {
      envBuilder = envBuilder
          .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeoutMillis)))
          .ioConfig(IoConfig.enableMutationTokens(enableMutationToken).numKvConnections(kvEndpoints))
          .securityConfig(SecurityConfig.enableTls(true)
              .trustStore(keyStore));
      environment.eventBus().subscribe(System.out::println);
    } else {
      envBuilder = envBuilder
          .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeoutMillis)))
          .ioConfig(IoConfig.enableMutationTokens(enableMutationToken).numKvConnections(kvEndpoints));
    }

    if(transactionEnabled){
      envBuilder = envBuilder
          .transactionsConfig(TransactionsConfig
          .durabilityLevel(transDurabilityLevel).timeout(Duration.ofSeconds(120)));
    }
    environment = envBuilder.build();
  }
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
    OPEN_CLIENTS.get();
    if (OPEN_CLIENTS.get() == 0 && environment != null) {
      cluster.disconnect();
      environment.shutdown();
      environment = null;
      Iterator<Throwable> it = errors.iterator();
      while(it.hasNext()){
        it.next().printStackTrace();
      }
    }
  }

  public Status read(final String table, final String key, final Set<String> fields,
                    final Map<String, ByteIterator> result) {

    try {

      Collection collection = bucket.defaultCollection();

      GetResult document = collection.get(formatId(table, key));
      extractFields(document.contentAsObject(), fields, result);
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("read failed with exception : " + t);
      return Status.ERROR;
    }
  }

  public Status read(final String table, final String key, final Set<String> fields,
                    final Map<String, ByteIterator> result, String scope, String coll) {

    try {

      Collection collection = collectionenabled ? bucket.scope(scope).collection(coll) : bucket.defaultCollection();

      GetResult document = collection.get(formatId(table, key));
      extractFields(document.contentAsObject(), fields, result);
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("read failed with exception : " + t);
      return Status.ERROR;
    }
  }

  private static void extractFields(final JsonObject content, Set<String> fields,
                                    final Map<String, ByteIterator> result) {
    if (fields == null || fields.isEmpty()) {
      fields = content.getNames();
    }

    for (String field : fields) {
      result.put(field, new StringByteIterator(content.getString(field)));
    }
  }

  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {

    try {

      Collection collection = bucket.defaultCollection();

      if (useDurabilityLevels) {
        if (upsert) {
          collection.upsert(formatId(table, key), encode(values), upsertOptions().durability(durabilityLevel));
        } else {
          collection.replace(formatId(table, key), encode(values), replaceOptions().durability(durabilityLevel));
        }
      } else {
        if (upsert) {
          collection.upsert(formatId(table, key), encode(values), upsertOptions().durability(persistTo, replicateTo));
        } else {
          collection.replace(formatId(table, key), encode(values), replaceOptions().durability(persistTo, replicateTo));
        }
      }
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("update failed with exception :" + t);
      return Status.ERROR;
    }
  }

  public Status update(final String table, final String key,
                      final Map<String, ByteIterator> values, String scope, String coll) {

    try {

      Collection collection = collectionenabled ? bucket.scope(scope).collection(coll) : bucket.defaultCollection();

      if (useDurabilityLevels) {
        if (upsert) {
          collection.upsert(formatId(table, key), encode(values), upsertOptions().durability(durabilityLevel));
        } else {
          collection.replace(formatId(table, key), encode(values), replaceOptions().durability(durabilityLevel));
        }
      } else {
        if (upsert) {
          collection.upsert(formatId(table, key), encode(values), upsertOptions().durability(persistTo, replicateTo));
        } else {
          collection.replace(formatId(table, key), encode(values), replaceOptions().durability(persistTo, replicateTo));
        }
      }
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("update failed with exception :" + t);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {

    try {
      Collection collection = bucket.defaultCollection();

      if (useDurabilityLevels) {
        if (upsert) {
          collection.upsert(formatId(table, key), encode(values), upsertOptions().durability(durabilityLevel));
        } else {
          collection.insert(formatId(table, key), encode(values), insertOptions().durability(durabilityLevel));
        }
      } else {
        if (upsert) {
          collection.upsert(formatId(table, key), encode(values), upsertOptions().durability(persistTo, replicateTo));
        } else {
          collection.insert(formatId(table, key), encode(values), insertOptions().durability(persistTo, replicateTo));

        }
      }
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("insert failed with exception :" + t);
      return Status.ERROR;
    }
  }

  public Status insert(final String table, final String key, final Map<String,
      ByteIterator> values, String scope, String coll) {

    try {

      Collection collection = collectionenabled ? bucket.scope(scope).collection(coll) : bucket.defaultCollection();

      if (useDurabilityLevels) {
        if (upsert) {
          collection.upsert(formatId(table, key), encode(values), upsertOptions().durability(durabilityLevel));
        } else {
          collection.insert(formatId(table, key), encode(values), insertOptions().durability(durabilityLevel));
        }
      } else {
        if (upsert) {
          collection.upsert(formatId(table, key), encode(values), upsertOptions().durability(persistTo, replicateTo));
        } else {
          collection.insert(formatId(table, key), encode(values), insertOptions().durability(persistTo, replicateTo));

        }
      }
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("insert failed with exception :" + t);
      return Status.ERROR;
    }
  }

  @Override
  public Status transaction(String table, String[] transationKeys, Map<String, ByteIterator>[] transationValues,
                            String[] transationOperations, Set<String> fields, Map<String, ByteIterator> result) {
    if (transactionEnabled) {
      return transactionContext(table, transationKeys, transationValues, transationOperations, fields, result);
    }
    return simpleCustomSequense(table, transationKeys, transationValues, transationOperations, fields, result);

  }

  public Status transaction(String table, String[] transationKeys, Map<String, ByteIterator>[] transationValues,
                            String[] transationOperations, Set<String> fields, Map<String, ByteIterator> result,
                            String scope, String coll) {
    if (transactionEnabled) {
      return transactionContext(table, transationKeys, transationValues, transationOperations, fields, result,
          scope, coll);
    }
    //return simpleCustomSequense(table, transationKeys, transationValues, transationOperations, fields, result);
    return Status.NOT_IMPLEMENTED;

  }


  public Status simpleCustomSequense(String table, String[] transationKeys,
                                    Map<String, ByteIterator>[] transationValues, String[] transationOperations,
                                    Set<String> fields, Map<String, ByteIterator> result) {

    Collection collection = bucket.defaultCollection();

    try {

      for (int i = 0; i < transationKeys.length; i++) {
        switch (transationOperations[i]) {
        case "TRREAD":
          try {
            GetResult document = collection.get(formatId(table, transationKeys[i]));
            extractFields(document.contentAsObject(), fields, result);
          } catch (DocumentNotFoundException e) {
            System.out.println("Key NOT_FOUND");
            return Status.NOT_FOUND;
          } catch (Throwable e) {
            return Status.ERROR;
          }
          break;
        case "TRUPDATE":
          collection.replace(formatId(table, transationKeys[i]), encode(transationValues[i]));
          break;
        case "TRINSERT":
          collection.upsert(formatId(table, transationKeys[i]), encode(transationValues[i]));
          break;
        default:
          break;
        }
      }
      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  public Status transactionContext(String table, String[] transationKeys, Map<String,
                                  ByteIterator>[] transationValues,
                                  String[] transationOperations, Set<String> fields,
                                  Map<String, ByteIterator> result,
                                  String scope, String coll) {


    Collection collection = collectionenabled ? bucket.scope(scope).collection(coll) : bucket.defaultCollection();

    try {

      cluster.transactions().run((ctx) -> {
        // Init and Start transaction here
          for (int i = 0; i < transationKeys.length; i++) {
            final String formattedDocId = formatId(table, transationKeys[i]);
            switch (transationOperations[i]) {
            case "TRREAD":
              TransactionGetResult doc = ctx.get(collection, formattedDocId);
              extractFields(doc.contentAs(JsonObject.class), fields, result);
              break;
            case "TRUPDATE":
              TransactionGetResult docToReplace = ctx.get(collection, formattedDocId);
              JsonObject content = docToReplace.contentAs(JsonObject.class);
              for (Map.Entry<String, String> entry: encode(transationValues[i]).entrySet()){
                content.put(entry.getKey(), entry.getValue());
              }
              ctx.replace(docToReplace, content);
              break;
            case "TRINSERT":
              ctx.insert(collection, formattedDocId, encode(transationValues[i]));
              break;
            default:
              break;
            }
          }
        });
    } catch (TransactionFailedException e) {
      Logger logger = LoggerFactory.getLogger(getClass().getName() + ".bad");
      //System.err.println("Transaction failed " + e.result().transactionId() + " " +
          //e.result().timeTaken().toMillis() + "msecs");
      for (TransactionLogEvent err : e.logs()) {
        String s = err.toString();
        logger.warn("transaction failed with exception :" + s);
      }
      return Status.ERROR;
    }
    return Status.OK;
  }


  public Status transactionContext(String table, String[] transationKeys, Map<String,
      ByteIterator>[] transationValues,
                                  String[] transationOperations, Set<String> fields,
                                  Map<String, ByteIterator> result) {

    Collection collection = bucket.defaultCollection();

    try {

      cluster.transactions().run((ctx) -> {
        // Init and Start transaction here
          for (int i = 0; i < transationKeys.length; i++) {
            final String formattedDocId = formatId(table, transationKeys[i]);
            switch (transationOperations[i]) {
            case "TRREAD":
              TransactionGetResult doc = ctx.get(collection, formattedDocId);
              extractFields(doc.contentAs(JsonObject.class), fields, result);
              break;
            case "TRUPDATE":
              TransactionGetResult docToReplace = ctx.get(collection, formattedDocId);
              JsonObject content = docToReplace.contentAs(JsonObject.class);
              for (Map.Entry<String, String> entry: encode(transationValues[i]).entrySet()){
                content.put(entry.getKey(), entry.getValue());
              }
              ctx.replace(docToReplace, content);
              break;
            case "TRINSERT":
              ctx.insert(collection, formattedDocId, encode(transationValues[i]));
              break;
            default:
              break;
            }
          }
        });
    } catch (TransactionFailedException e) {
      Logger logger = LoggerFactory.getLogger(getClass().getName() + ".bad");
      //System.err.println("Transaction failed " + e.result().transactionId() + " " +
      //e.result().timeTaken().toMillis() + "msecs");
      for (TransactionLogEvent err : e.logs()) {
        String s = err.toString();
        logger.warn("transaction failed with exception :" + s);
      }
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
  * Helper method to turn the passed in iterator values into a map we can encode to json.
  *
  * @param values the values to encode.
  * @return the map of encoded values.
  */
  private static Map<String, String> encode(final Map<String, ByteIterator> values) {
    Map<String, String> result = new HashMap<>(values.size());
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      result.put(value.getKey(), value.getValue().toString());
    }
    return result;
  }

  @Override
  public Status delete(final String table, final String key) {
    try {

      Collection collection = bucket.defaultCollection();

      if (useDurabilityLevels) {
        collection.remove(formatId(table, key), removeOptions().durability(durabilityLevel));
      } else {
        collection.remove(formatId(table, key), removeOptions().durability(persistTo, replicateTo));
      }

      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("delete failed with exception :" + t);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                    final Vector<HashMap<String, ByteIterator>> result) {
    try {
      if (fields == null || fields.isEmpty()) {
        return scanAllFields(table, startkey, recordcount, result);
      } else {
        return scanSpecificFields(table, startkey, recordcount, fields, result);
        // need to implement
      }
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("scan failed with exception :" + t);
      return Status.ERROR;
    }
  }

  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                    final Vector<HashMap<String, ByteIterator>> result, String scope, String coll) {
    try {
      if (fields == null || fields.isEmpty()) {
        return scanAllFields(table, startkey, recordcount, result, scope, coll);
      } else {
        return scanSpecificFields(table, startkey, recordcount, fields, result);
        // need to implement
      }
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("scan failed with exception :" + t);
      return Status.ERROR;
    }
  }

  private Status scanAllFields(final String table, final String startkey, final int recordcount,
                              final Vector<HashMap<String, ByteIterator>> result) {

    final Collection collection = bucket.defaultCollection();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    final String query =  "SELECT RAW meta().id FROM `" + bucketName +
          "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";
    final ReactiveCollection reactiveCollection = collection.reactive();
    reactiveCluster.query(query,
          queryOptions()
                .adhoc(adhoc)
                .maxParallelism(maxParallelism)
                .parameters(JsonArray.from(formatId(table, startkey), recordcount)))
          .flatMapMany(res -> {
              return res.rowsAs(String.class);
            })
          .flatMap(id -> {
              return reactiveCollection
                  .get(id, GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE));
            })
          .map(getResult -> {
              HashMap<String, ByteIterator> tuple = new HashMap<>();
              decodeStringSource(getResult.contentAs(String.class), null, tuple);
              return tuple;
            })
          .toStream()
          .forEach(data::add);

    result.addAll(data);
    return Status.OK;
  }

  private Status scanAllFields(final String table, final String startkey, final int recordcount,
                              final Vector<HashMap<String, ByteIterator>> result, String scope, String coll) {
    final Collection collection = collectionenabled ? bucket.scope(scope).collection(coll) : bucket.defaultCollection();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    final String query =  "SELECT RAW meta().id FROM default:`" + bucketName +
          "`.`" + scope + "`.`"+ coll + "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";

    final ReactiveCollection reactiveCollection = collection.reactive();
    reactiveCluster.query(query,
          queryOptions()
                  .adhoc(adhoc)
                  .maxParallelism(maxParallelism)
                  .parameters(JsonArray.from(formatId(table, startkey), recordcount)))
          .flatMapMany(res -> {
              return res.rowsAs(String.class);
            })
          .flatMap(id -> {
              return reactiveCollection
                    .get(id, GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE));
            })
          .map(getResult -> {
              HashMap<String, ByteIterator> tuple = new HashMap<>();
              decodeStringSource(getResult.contentAs(String.class), null, tuple);
              return tuple;
            })
          .toStream()
          .forEach(data::add);

    result.addAll(data);
    return Status.OK;
  }

  /**
  * Performs the {@link #scan(String, String, int, Set, Vector)} operation N1Ql only for a subset of the fields.
  *
  * @param table The name of the table
  * @param startkey The record key of the first record to read.
  * @param recordcount The number of records to read
  * @param fields The list of fields to read, or null for all of them
  * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
  * @return The result of the operation.
  */

  private Status scanSpecificFields(final String table, final String startkey, final int recordcount,
                                    final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    final Collection collection = bucket.defaultCollection();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    final String query =  "SELECT RAW meta().id FROM `" + bucketName +
        "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";
    final ReactiveCollection reactiveCollection = collection.reactive();
    reactiveCluster.query(query,
        queryOptions()
            .adhoc(adhoc)
            .maxParallelism(maxParallelism)
            .parameters(JsonArray.from(formatId(table, startkey), recordcount)))
        .flatMapMany(res -> {
            return res.rowsAs(String.class);
          })
        .flatMap(id -> {
            return reactiveCollection
              .get(id, GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE));
          })
        .map(getResult -> {
            HashMap<String, ByteIterator> tuple = new HashMap<>();
            decodeStringSource(getResult.contentAs(String.class), null, tuple);
            return tuple;
          })
        .toStream()
        .forEach(data::add);

    result.addAll(data);
    return Status.OK;
  }

  private void decode(final byte[] source, final Set<String> fields,
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
      throw new RuntimeException("Could not decode JSON");
    }
  }

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
      throw new RuntimeException("Could not decode JSON");
    }
  }


  /**
  * Helper method to join the set of fields into a String suitable for N1QL.
  *
  * @param fields the fields to join.
  * @return the joined fields as a String.
  */
  private static String joinFields(final Set<String> fields) {
    if (fields == null || fields.isEmpty()) {
      return "*";
    }
    StringBuilder builder = new StringBuilder();
    for (String f : fields) {
      builder.append("`").append(f).append("`").append(",");
    }
    String toReturn = builder.toString();
    return toReturn.substring(0, toReturn.length() - 1);
  }


  /**
  * Helper method to turn the prefix and key into a proper document ID.
  *
  * @param prefix the prefix (table)
  * @param key the key itself.
  * @return a document ID that can be used with Couchbase.
  */
  private static String formatId(final String prefix, final String key) {
    return prefix + KEY_SEPARATOR + key;
  }

}
