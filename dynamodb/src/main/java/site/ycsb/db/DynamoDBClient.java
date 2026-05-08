/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2015-2016 YCSB Contributors. All Rights Reserved.
 * Copyright 2023-2026 benchANT GmbH. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package site.ycsb.db;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.IndexableDB;
import site.ycsb.NumericByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.core.CoreConstants;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DatabaseField;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.BillingModeSummary;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.paginators.QueryPublisher;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

/**
 * DynamoDB client for YCSB.
 */

public class DynamoDBClient extends DB implements IndexableDB {
  static class IndexDescriptor {
    String name;
    List<String> hashKeyAttributes = new ArrayList<>();
    int readCap;
    int writeCap;
    List<String> sortsKeyAttributes = new ArrayList<>();
    @Override
    public String toString() {
      return "IndexDescriptor [name=" + name + ", hashKeyAttributes=" + hashKeyAttributes + ", readCap=" + readCap
          + ", writeCap=" + writeCap + ", sortsKeyAttributes=" + sortsKeyAttributes + "]";
    }
  }
  /**
   * Defines the primary key type used in this particular DB instance.
   * <p>
   * By default, the primary key type is "HASH". Optionally, the user can
   * choose to use hash_and_range key type. See documentation in the
   * DynamoDB.Properties file for more details.
   */
  private enum PrimaryKeyType {
    HASH,
    HASH_AND_RANGE
  }
  public static final int AGGREGATE_QUERY_LIMIT = 10;
  public static final String INDEX_LIST_PROPERTY = "dynamodb.indexlist";
  public static final String INDEX_READ_CAP_PROPERTY = "dynamodb.indexreadcap";
  public static final String INDEX_READ_CAP_DEFAULT = "5";
  public static final String INDEX_WRITE_CAP_PROPERTY = "dynamodb.indexwritecap";
  public static final String INDEX_WRITE_CAP_DEFAULT = "5";
  private static DynamoDbClient dynamoDB;
  private static DynamoDbAsyncClient asyncDynamoDB;
  static String primaryKeyName;
  static PrimaryKeyType primaryKeyType = PrimaryKeyType.HASH;

  // If the user choose to use HASH_AND_RANGE as primary key type, then
  // the following two variables become relevant. See documentation in the
  // DynamoDB.Properties file for more details.
  private static String hashKeyValue;
  private static String hashKeyName;

  private static boolean consistentRead = false;
  private static String region = "us-east-1";
  private static String endpoint = null;
  private static int maxConnects = 50;
  private static int maxRetries = -1;
  static final Logger LOGGER = Logger.getLogger(DynamoDBClient.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";
  private static boolean useTypedFields;
  /** The batch size to use for inserts. */
  private static int batchSize;
  private static final int MAX_RETRY = 3;
  private static int defaultReadCap;
  private static int defaultWriteCap;
  private static List<IndexDescriptor> indexes = null;
  private static boolean aggregatesUseScan = true;
  /** The bulk inserts pending for the thread. */
  private final List<WriteRequest> bulkInserts = new ArrayList<WriteRequest>();
  private static boolean doDebug = false;

  @Override
  public void init() throws DBException {
    synchronized(DynamoDBClient.class) {
      if(dynamoDB != null) return;
      String debug = getProperties().getProperty("debug", null);

      if (null != debug && "true".equalsIgnoreCase(debug)) {
        LOGGER.setLevel(Level.DEBUG);
        doDebug = true;
      }

      batchSize = Integer.parseInt(getProperties().getProperty("db.batchsize", "1"));
      String configuredEndpoint = getProperties().getProperty("dynamodb.endpoint", null);
      String credentialsFile = getProperties().getProperty("dynamodb.awsCredentialsFile", null);
      String primaryKey = getProperties().getProperty("dynamodb.primaryKey", null);
      String primaryKeyTypeString = getProperties().getProperty("dynamodb.primaryKeyType", null);
      String consistentReads = getProperties().getProperty("dynamodb.consistentReads", null);
      String connectMax = getProperties().getProperty("dynamodb.connectMax", null);
      maxRetries = Integer.parseInt(getProperties().getProperty("dynamodb.maxRetries", "0"));
      String configuredRegion = getProperties().getProperty("dynamodb.region", null);
      aggregatesUseScan = Boolean.parseBoolean(getProperties().getProperty("dynamodb.aggregateWithScan", "false"));
      System.err.println("do aggregate by scanning: " + aggregatesUseScan);

      if (null != connectMax) {
        maxConnects = Integer.parseInt(connectMax);
      }

      if (null != consistentReads && "true".equalsIgnoreCase(consistentReads)) {
        consistentRead = true;
      }

      if (null != configuredEndpoint) {
        endpoint = configuredEndpoint;
      }

      if (null == primaryKey || primaryKey.length() < 1) {
        throw new DBException("Missing primary key attribute name, cannot continue");
      }

      if (null != primaryKeyTypeString) {
        try {
          primaryKeyType = PrimaryKeyType.valueOf(primaryKeyTypeString.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new DBException("Invalid primary key mode specified: " + primaryKeyTypeString +
              ". Expecting HASH or HASH_AND_RANGE.");
        }
      }
      useTypedFields = "true".equalsIgnoreCase(getProperties().getProperty(TYPED_FIELDS_PROPERTY));
      if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
        // When the primary key type is HASH_AND_RANGE, keys used by YCSB
        // are range keys so we can benchmark performance of individual hash
        // partitions. In this case, the user must specify the hash key's name
        // and optionally can designate a value for the hash key.

        String configuredHashKeyName = getProperties().getProperty("dynamodb.hashKeyName", null);
        if (null == configuredHashKeyName || configuredHashKeyName.isEmpty()) {
          throw new DBException("Must specify a non-empty hash key name when the primary key type is HASH_AND_RANGE.");
        }
        hashKeyName = configuredHashKeyName;
        hashKeyValue = getProperties().getProperty("dynamodb.hashKeyValue", DEFAULT_HASH_KEY_VALUE);
      }

      if (null != configuredRegion && configuredRegion.length() > 0) {
        region = configuredRegion;
      }
      if(batchSize > 25) {
        throw new DBException("batch size must be <= 25 for dynamodb.");
      }
      try {
        AwsCredentialsProvider credentialsProvider;
        if(credentialsFile != null) {
          // if credentials file, try reading the credentials from the file
          credentialsProvider = ProfileCredentialsProvider.builder().profileName(credentialsFile).build();
        } else {
          // otherwise, use the default credentials provider chain, which will look for credentials in environment variables, system properties, etc.
          credentialsProvider = DefaultCredentialsProvider.builder().build();
        }
        DynamoDbClientBuilder builder = DynamoDbClient.builder()
          .region(Region.of(region))
          .credentialsProvider(
            /* DefaultCredentialsProvider, Static, etc. */
            credentialsProvider
          )
          .httpClient(ApacheHttpClient.builder()
            .maxConnections(maxConnects)  // Tune based on your fixed client count and AWS limits
            .tcpKeepAlive(true)
            .build());
        DynamoDbAsyncClientBuilder asyncBuilder = DynamoDbAsyncClient.builder()
          .region(Region.of(region))
          .credentialsProvider(
            /* DefaultCredentialsProvider, Static, etc. */
            credentialsProvider
          );
        if(endpoint != null) {
          builder.endpointOverride(URI.create(endpoint));
          asyncBuilder.endpointOverride(URI.create(endpoint));
        }      
        // any http/retry configuration …
        dynamoDB = builder.build();
        asyncDynamoDB = asyncBuilder.build();
        /*dynamoDBBuilder
            .withClientConfiguration(
                new ClientConfiguration()
                    .withTcpKeepAlive(true)
                    .withMaxConnections(this.maxConnects)
                    .withMaxErrorRetry(maxRetries)
            );
        */
        primaryKeyName = primaryKey;
        LOGGER.info("dynamodb connection created with " + endpoint);
      } catch (Exception e1) {
        LOGGER.error("DynamoDBClient.init(): Could not initialize DynamoDB client.", e1);
        throw new DBException(e1);
      }

      if(indexes == null) {
        final String table = getProperties().getProperty(CoreConstants.TABLENAME_PROPERTY, CoreConstants.TABLENAME_PROPERTY_DEFAULT);
        defaultReadCap = Integer.parseInt(getProperties().getProperty(INDEX_READ_CAP_PROPERTY, INDEX_READ_CAP_DEFAULT));
        defaultWriteCap = Integer.parseInt(getProperties().getProperty(INDEX_WRITE_CAP_PROPERTY, INDEX_WRITE_CAP_DEFAULT));
        List<IndexDescriptor> localIndexes = DynamoDBInitHelper.getIndexList(getProperties(), defaultReadCap, defaultWriteCap);
        LOGGER.info("indexes loaded from config: " + localIndexes.toString());
        setIndexes(table, getProperties(), localIndexes);
        indexes = loadRemoteIndexes(table);
        LOGGER.info("loaded remote idexes: " + indexes.toString());
      }
    }
  }

  private List<IndexDescriptor> loadRemoteIndexes(String table) {
      DescribeTableResponse tableDesc = dynamoDB.describeTable(DescribeTableRequest.builder().tableName(table).build());
      List<GlobalSecondaryIndexDescription> indexes = tableDesc.table().globalSecondaryIndexes();
      if(indexes == null || indexes.size() == 0) {
        return Collections.emptyList();
      }
      List<IndexDescriptor> result = new ArrayList<>(indexes.size());
      for(GlobalSecondaryIndexDescription index : indexes) {
        IndexDescriptor desc = new IndexDescriptor();
        desc.name = index.indexName();
        List<KeySchemaElement> schema = index.keySchema();
        for(KeySchemaElement el : schema) {
          if(KeyType.HASH.toString().equals(el.keyTypeAsString())) {
            desc.hashKeyAttributes.add(el.attributeName());
          } else if(KeyType.RANGE.toString().equals(el.keyTypeAsString())) {
            desc.sortsKeyAttributes.add(el.attributeName());
          }
        }
        result.add(desc);
      }
      return result;
  }

  private void waitForIndexCreation(String table, String name) {
    LOGGER.info("initial wait");
    try {
      Thread.sleep(10000);
    } catch(InterruptedException ex) {
      // ignore for now
    }
    while(true) {
      DescribeTableResponse result = dynamoDB.describeTable(DescribeTableRequest.builder().tableName(table).build());
      List<GlobalSecondaryIndexDescription> indexes = result.table().globalSecondaryIndexes();
      GlobalSecondaryIndexDescription myIndex = null;
      String names = "";
      for(GlobalSecondaryIndexDescription index : indexes) {
        names = names + index.indexName() + ", ";
        if(name.equals(index.indexName())) {
          myIndex = index;
        }
      }
      if(myIndex == null) {
        throw new IllegalStateException("Newly created index " + name + " was not in index list: " + names);
      }
      final IndexStatus status = myIndex.indexStatus();
      LOGGER.info("status of newly created index " + name + " " + status);
      if(IndexStatus.CREATING == status ||
          IndexStatus.UPDATING == status) {
        LOGGER.info("waiting more");
        try {
          Thread.sleep(10000);
        } catch(InterruptedException ex) {
          // ignore for now
        }
      } else {
        LOGGER.info("let's move on");
        break;
      }
    }
  }

  private void setIndexes(String table, Properties props, List<IndexDescriptor> indexes) {
    // we do not return here, as it is beneficial to set table properties
    final boolean setProperties = props.getProperty("dynamodb.settableproperties", "false").equals("true");
    final boolean forciblySkipIndexCreation = props.getProperty("dynamodb.forciblySkipIndexCreation", "false").equals("true");
    if(forciblySkipIndexCreation) {
      return;
    }
    if(indexes.size() == 0 && !setProperties) {
      return;
    }
    List<AttributeDefinition> attributes = DynamoDBInitHelper.getFullAttributeDefinitionList();
    attributes.add(AttributeDefinition.builder().attributeName(primaryKeyName).attributeType(ScalarAttributeType.S).build());
    BillingMode billing = getCurrentBillingMode(table);
    if(indexes.size() == 0 && setProperties) {
      System.err.println("updating primary key to: " + primaryKeyName);
      UpdateTableRequest req = UpdateTableRequest.builder()
        .attributeDefinitions(attributes)
        .billingMode(billing)
        .tableName(table).build();
        UpdateTableResponse result = dynamoDB.updateTable(req);
        System.err.println("updated table: " +result.responseMetadata());
    }
    for(IndexDescriptor idx : indexes) {
      CreateGlobalSecondaryIndexAction action = DynamoDBInitHelper.getCreateSecondaryIndexAction(idx, billing);
      GlobalSecondaryIndexUpdate up = GlobalSecondaryIndexUpdate.builder().create(action).build();
      System.err.println("trying to add another Global Secondary Index: " + up );
      try {
        UpdateTableRequest req = UpdateTableRequest.builder()
          .globalSecondaryIndexUpdates(up)
          .attributeDefinitions(attributes)
          .tableName(table).build();
        UpdateTableResponse result = dynamoDB.updateTable(req);
        waitForIndexCreation(table, idx.name);
        System.err.println("prepared index creation: " + idx);
      } catch(DynamoDbException ex) {
        System.err.println("index creation failed for '" + idx.name + "' because of " + ex.getMessage());
      }
    }
    System.err.println("all indexes created");
  }

  private BillingMode getCurrentBillingMode(String tableName) {
    DescribeTableResponse response = dynamoDB.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
    TableDescription table = response.table();
    if(table != null) {
      BillingModeSummary bms = table.billingModeSummary();
      if(bms != null) {
        return bms.billingMode();
      }
    }
    return null;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    GetItemRequest.Builder req = GetItemRequest.builder().tableName(table)
      .key(createPrimaryKey(key))
      .attributesToGet(fields)
      .consistentRead(consistentRead);
    GetItemResponse res;

    try {
      res = dynamoDB.getItem(req.build());
    } catch (DynamoDbException ex) {  // Replaces AmazonServiceException
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkException ex) {  // Replaces AmazonClientException
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (null != res.item()) {
      result.putAll(DynamoDBQueryParameterHelper.extractResult(res.item()));
    }
    if(result.size() == 0) return Status.NOT_FOUND;
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
    }
    /*
     * on DynamoDB's scan, startkey is *exclusive* so we need to
     * getItem(startKey) and then use scan for the res
    */
    GetItemRequest greq = GetItemRequest.builder().tableName(table).key(createPrimaryKey(startkey)).attributesToGet(fields).build();

    GetItemResponse gres;
    try {
      gres = dynamoDB.getItem(greq);
    } catch (DynamoDbException ex) {  // Replaces AmazonServiceException
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkException ex) {  // Replaces AmazonClientException
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    if (null != gres.item()) {
      result.add(DynamoDBQueryParameterHelper.extractResult(gres.item()));
    }

    int count = 1; // startKey is done, rest to go.
    Map<String, AttributeValue> startKey = createPrimaryKey(startkey);
    ScanRequest.Builder req = ScanRequest.builder().tableName(table).attributesToGet(fields);
    while (count < recordcount) {
      req.exclusiveStartKey(startKey);
      req.limit(recordcount - count);
      ScanResponse res;
      try {
        res = dynamoDB.scan(req.build());
      } catch (DynamoDbException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (SdkException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }

      count += res.count();
      for (Map<String, AttributeValue> items : res.items()) {
        result.add(DynamoDBQueryParameterHelper.extractResult(items));
      }
      startKey = res.lastEvaluatedKey();
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("updatekey: " + key + " from table: " + table);
    }

    Map<String, AttributeValueUpdate> attributes = new HashMap<>(values.size());
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      AttributeValue v = AttributeValue.builder().s(val.getValue().toString()).build();
      attributes.put(val.getKey(), AttributeValueUpdate.builder().value(v).action("PUT").build());
    }
    
    return internalUpdateItem(table, key, attributes);
  }

  @Override
  public Status insert(String table, String key, List<DatabaseField> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("insertkey: " + primaryKeyName + "-" + key + " from table: " + table);
    }

    Map<String, AttributeValue> attributes = useTypedFields
      ? DynamoDBQueryParameterHelper.createTypedAttributes(values)
      : DynamoDBQueryParameterHelper.createAttributes(fieldListAsIteratorMap(values));
    // adding primary key
    attributes.put(primaryKeyName, AttributeValue.builder().s(key).build());
    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // If the primary key type is HASH_AND_RANGE, then what has been put
      // into the attributes map above is the range key part of the primary
      // key, we still need to put in the hash key part here.
      attributes.put(hashKeyName, AttributeValue.builder().s(hashKeyValue).build());
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("insertkey: sending attributes " + attributes);
    }
    try {
      if(batchSize < 2) {
        PutItemRequest putItemRequest = PutItemRequest.builder()
          .tableName(table)
          .item(attributes)
          .conditionExpression("attribute_not_exists(" + primaryKeyName + ")")
          .build();
        dynamoDB.putItem(putItemRequest);
      } else {
        WriteRequest ww = WriteRequest.builder().putRequest(PutRequest.builder().item(attributes).build()).build();
        bulkInserts.add(ww);
        if(bulkInserts.size() < batchSize) { return Status.BATCHED_OK; }
        List<WriteRequest> local = new ArrayList<>(bulkInserts);
        bulkInserts.clear();
        for(int retries = 0; retries < MAX_RETRY && local.size() > 0; retries++) {
          BatchWriteItemRequest.Builder batch = BatchWriteItemRequest.builder();
          batch.requestItems(Collections.singletonMap(table, local));
          BatchWriteItemResponse response = dynamoDB.batchWriteItem(batch.build());
          local = response.unprocessedItems().get(table);
          if(local == null || local.size() == 0) { return Status.OK; }
          LOGGER.error("could not process all requests in a batch: " + local.size() + " requests missing; retrying.");
        }
        return Status.ERROR;
      }
    } catch (DynamoDbException ex) {  // Replaces AmazonServiceException
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkException ex) {  // Replaces AmazonClientException
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("deletekey: " + key + " from table: " + table);
    }
    DeleteItemRequest req = DeleteItemRequest.builder()
      .tableName(table)
      .key(createPrimaryKey(key))
      .returnValues(ReturnValue.ALL_OLD)
      .build();
    try {
      DeleteItemResponse response = dynamoDB.deleteItem(req);
      Map<String, AttributeValue> oldAttrs = response.attributes();
      if (oldAttrs != null && !oldAttrs.isEmpty()) {
        // Item existed and was deleted
        return Status.OK;
    } else {
        // Item did not exist
        return Status.NOT_FOUND;  // or Status.OK, depending on your logic
    }
    } catch (DynamoDbException ex) {  // Replaces AmazonServiceException
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkException ex) {  // Replaces AmazonClientException
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
  }

  private IndexDescriptor findIndexMatchingFilter(List<Comparison> filters) {
    // what we probably need is an index whose key attributes are fully covered
    // with the filters and 
    List<String> compFields = new ArrayList<>();
    for(Comparison f : filters) {
      Comparison c = f;
      String fieldName = c.getFieldname();
      while(c.isSimpleNesting()) {
        c = c.getSimpleNesting();
        fieldName = fieldName + "." + c.getFieldname();
      }
      compFields.add(fieldName);
    }
    return findIndexMatchingFilterByFieldNames(compFields);
  }

  IndexDescriptor findIndexMatchingFilterByFieldNames(List<String> fieldNames) {
    // what we probably need is an index whose key attributes are fully covered
    // with the filters and 
    for(IndexDescriptor idx : indexes) {
      List<String> compFields = new ArrayList<>(fieldNames);
      // all elements contained in "filters" need to be contained
      // in either the key attributes or sort attributes
      compFields.removeAll(idx.hashKeyAttributes);
      compFields.removeAll(idx.sortsKeyAttributes);
      if(compFields.isEmpty() && idx.hashKeyAttributes.size() <= fieldNames.size()) return idx;
    }
    // we got here, no index found
    return null;
  }

  public Status internalFindOne(String table, List<Comparison> filters, Set<String> fields, List<Map<String, AttributeValue>> resultList, Integer qLimit) {
      IndexDescriptor desc = findIndexMatchingFilter(filters);
      if(desc == null) {
        LOGGER.error("did not find a matching index for filter: " + filters);
        return Status.ERROR;
      } else {
        if(LOGGER.isDebugEnabled()) {
          LOGGER.debug("found matching index '" + desc.name + "' for filters: " + filters + " => " + desc);
        }
      }
      // Index idx = dynamoDB.query(null).getIndex(desc.name);
      QueryRequest.Builder queryB = QueryRequest.builder().tableName(table).indexName(desc.name).consistentRead(consistentRead);
      // QueryRequest.Builder queryB = QueryRequest.builder().tableName(table).consistentRead(consistentRead);
      DynamoDBQueryHelper.buildPreparedQuery(queryB, desc, filters);
      DynamoDBQueryHelper.bindPreparedQuery(queryB, filters);
      if(qLimit != null && qLimit.intValue() > 0) {
        queryB.limit(qLimit.intValue());
      }
      QueryRequest query = queryB.build();
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("query spec: " + query);
      }
      QueryResponse response = dynamoDB.query(query);
      if(!response.hasItems()) {
        return Status.NOT_FOUND;
      }
      // there does not seem to be a way to read results one by one
      List<Map<String, AttributeValue>> col = response.items();
      if(col.isEmpty()) {
        return Status.NOT_FOUND;
      }
      // we cannot use limit to limit the number of returned results to 1
      /* if(col.size() > 1) {
        LOGGER.error("retrieved more than one element");
        return Status.UNEXPECTED_STATE;
      } */
      resultList.add(col.get(0));
      return Status.OK;
  }

  @Override
  public Status findOne(String table, List<Comparison> filters, Set<String> fields,
    Map<String, ByteIterator> result) {
      if(fields != null) {
        throw new IllegalArgumentException("fields can only be null by now");
      }
      List<Map<String, AttributeValue>> resultList = new ArrayList<>(1);
      Status proxy = internalFindOne(table, filters, fields, resultList, null);
      if(proxy != Status.OK) return proxy;
      result.putAll(DynamoDBQueryParameterHelper.extractResultFromItem(resultList.get(0)));
      return Status.OK;
  }

  private Status internalUpdateItem(String table, String localPrimaryKey, Map<String, AttributeValueUpdate> attributes){
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    StringBuilder propString = new StringBuilder();
    for(Map.Entry<String,AttributeValueUpdate> e : attributes.entrySet()) {
      String propName = e.getKey();
      AttributeValueUpdate up = e.getValue();
      if(up.action() != AttributeAction.PUT) {
        System.err.println("cannot handle update actions other than PUT");
        continue;
      }
      String placeholder = ":v_" + propName;
      propString
        .append(propString.length() == 0 ? "SET " : ", ")
        .append(propName).append(" = ").append(placeholder);
      expressionAttributeValues.put(placeholder, up.value());
    }
    Map<String, AttributeValue> primaryKey = createPrimaryKey(localPrimaryKey);
    UpdateItemRequest req = UpdateItemRequest.builder()
      .tableName(table)
      .key(primaryKey)
      .updateExpression(propString.toString())
      .expressionAttributeValues(expressionAttributeValues)
      .conditionExpression("attribute_exists(" + primaryKeyName + ")") 
      .build();

    try {
      dynamoDB.updateItem(req);
    } catch(ConditionalCheckFailedException ex) {
      LOGGER.error(ex);
      return Status.NOT_FOUND;
    } catch (DynamoDbException ex) {  // Replaces AmazonServiceException
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkException ex) {  // Replaces AmazonClientException
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }   
    return Status.OK;
  }

  @Override
  public Status updateOne(String table, List<Comparison> filters, List<DatabaseField> fields) {
    List<Map<String, AttributeValue>> resultList = new ArrayList<>(1);
    Status found = internalFindOne(table, filters, null, resultList, 10);
    if(found != Status.OK) return found;
    Map<String, AttributeValue> it = resultList.get(0);
    String id = it.get(primaryKeyName).s();
    Map<String, AttributeValueUpdate> attributes = new HashMap<>();
    Map<String, AttributeValue> plainAttributes = DynamoDBQueryParameterHelper.createTypedAttributes(fields);
    for(Map.Entry<String, AttributeValue> e : plainAttributes.entrySet()) {
      attributes.put(e.getKey(), AttributeValueUpdate.builder().value(e.getValue()).action(AttributeAction.PUT).build());
    }
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("updating item: " + id + " with " + plainAttributes);
    }
    return internalUpdateItem(table, id, attributes);
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    Map<String, AttributeValue> k = new HashMap<>();
    if (primaryKeyType == PrimaryKeyType.HASH) {
      k.put(primaryKeyName, AttributeValue.builder().s(key).build());
    } else if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      k.put(hashKeyName, AttributeValue.builder().s(hashKeyValue).build());
      k.put(primaryKeyName, AttributeValue.builder().s(key).build());
    } else {
      throw new RuntimeException("Assertion Error: impossible primary key type");
    }
    return k;
  }

  @Override
  public Status aggregate(String table, String[] airports, int minOccurrences, Vector<HashMap<String, ByteIterator>> results){
    List<AggregateResult> finalResults = null;
    try {
      if(aggregatesUseScan) {
        finalResults = aggregateWithScan(table, airports, minOccurrences);
      } else {
        finalResults = aggregateWithParallel(table, airports, minOccurrences);
      }
    } catch(Exception ex) {
      LOGGER.error("error when aggregating", ex);
      return Status.ERROR;
    }
    if(finalResults.isEmpty()) {
      return Status.NOT_FOUND;
    }
    convertAggregateResults(finalResults, results);
    if(doDebug) {
      System.err.println("aggregated: " +  results);
    }
    return Status.OK;
  }

  private final List<String> aggregateParams = Arrays.asList(new String[]{"src_airport", "dst_airport"});
  private List<AggregateResult> aggregateWithParallel(String table, String[] airports, int minOccurrences) {
    IndexDescriptor src_idx = findIndexMatchingFilterByFieldNames(Collections.singletonList("src_airport"));
    IndexDescriptor dst_idx = findIndexMatchingFilterByFieldNames(Collections.singletonList("dst_airport"));
    if(src_idx == null ||  dst_idx == null) {
      LOGGER.error("did not find a matching index for filter: " + aggregateParams);
      throw new IllegalStateException("");
    } else {
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("found matching index '" + src_idx.name + "' and '" + dst_idx.name + "' for filters");
      }
    }
    
    List<AggregateResult> agg_results = new ArrayList<>();
    for(int i = 0; i < airports.length; i++) {
      // src_airport
      AggregateResult result = new AggregateResult();
      result.srcAirport = airports[i];
      agg_results.add(result);
      queryAsync(table, src_idx.name, result);
      // dst_airport
      result = new AggregateResult();
      result.dstAirport = airports[i];
      agg_results.add(result);
      queryAsync(table, dst_idx.name, result);
    }
    harvestAsync(agg_results, minOccurrences);
    return agg_results;
  }

    static void convertAggregateResults(List<AggregateResult> aggregateResults, Vector<HashMap<String, ByteIterator>> results) {
        for(AggregateResult r : aggregateResults) {
            HashMap<String, ByteIterator> result = new HashMap<>();
            result.put("src_airport", new StringByteIterator(r.srcAirport));
            result.put("dst_airport", new StringByteIterator(r.dstAirport));
            result.put("nmb", new NumericByteIterator(r.count));
            result.put("avg_stops", new NumericByteIterator(r.avgStops));
            results.add(result);
        }
    }

  private List<AggregateResult> aggregateWithScan(String table, String[] airports, int minOccurrences) {
      Map<String,AttributeValue> params = new HashMap<>();
      for(int i = 0; i < airports.length; i++) {
        params.put(":a" + (i+1) , AttributeValue.builder().s(airports[i]).build());
      }
      IndexDescriptor airport_idx = findIndexMatchingFilterByFieldNames(aggregateParams);
      ScanRequest.Builder scanB = ScanRequest.builder()
      .tableName(table)
      .indexName(airport_idx.name)
      .expressionAttributeValues(params)
      .filterExpression("attribute_exists(codeshares_0) AND (src_airport IN (:a1, :a2, :a3) OR dst_airport IN (:a1, :a2, :a3))");

      Map<String, AggregateResult> fullResults = new HashMap<>();
      ScanIterable scanIt = dynamoDB.scanPaginator(scanB.build());
      Iterator<ScanResponse> isr = scanIt.iterator();
      while(isr.hasNext()) {
        ScanResponse response = isr.next();
        List<Map<String,AttributeValue>> l = response.items();
        processListOfItems(l, fullResults);
      }
      List<AggregateResult> finalResults = new ArrayList<>();
      completeAggregation(finalResults, fullResults, minOccurrences);
      
      return finalResults;
  }

  private void queryAsync(final String tableName, final String indexName, AggregateResult result) {
    QueryRequest.Builder queryB = QueryRequest.builder()
      .tableName(tableName)
      .indexName(indexName)
      .consistentRead(consistentRead);
    Map<String,AttributeValue> params = new HashMap<>();
    String keyCond = "";
    if(result.srcAirport != null) {
      params.put(":airport", AttributeValue.builder().s(result.srcAirport).build());
      keyCond = "src_airport = :airport";
    } else {
      params.put(":airport", AttributeValue.builder().s(result.dstAirport).build());
      keyCond = "dst_airport = :airport";
    }
    // params.put(":v_dst", AttributeValue.builder().s(result.dstAirport).build());
    queryB
      .keyConditionExpression(keyCond)
      .filterExpression("attribute_exists(codeshares_0)")
      .expressionAttributeValues(params);
    Map<String, AggregateResult> fullResults = new HashMap<>();
    result.future = paginateAsync(queryB.build(), fullResults);
    
  }

  private CompletableFuture<Map<String, AggregateResult>> paginateAsync(final QueryRequest query, Map<String, AggregateResult> fullResults) {
    return asyncDynamoDB.query(query).thenCompose(response -> {
      // Add items from this page
      if(response.hasItems()) {
        processListOfItems(response.items(), fullResults);
      }
      
      // Check if there are more pages
      Map<String, AttributeValue> lastEvaluatedKey = response.lastEvaluatedKey();
      if(lastEvaluatedKey == null || lastEvaluatedKey.isEmpty()) {
        return CompletableFuture.completedFuture(fullResults);
      }
      // Recursively fetch next page
      QueryRequest nextQuery = query.toBuilder()
        .exclusiveStartKey(lastEvaluatedKey)
        .build();
      return paginateAsync(nextQuery, fullResults);
    });
  }

  private void completeAggregation(List<AggregateResult> agg_results, Map<String, AggregateResult> fullResults, int minOccurrences) {
    for(Map.Entry<String,AggregateResult> e : fullResults.entrySet()) {
      AggregateResult r = e.getValue();
      if(r.count <= minOccurrences) continue;
      r.avgStops = ((double) r.sumStops) / ((double) r.count);
      agg_results.add(r);
    }
    // Sort by avg_stops in descending order and limit results
    agg_results.sort((r1, r2) -> Double.compare(r2.avgStops, r1.avgStops));
    if(agg_results.size() > AGGREGATE_QUERY_LIMIT) {
      agg_results = agg_results.subList(0, AGGREGATE_QUERY_LIMIT);
    }
  }

  private void processListOfItems(List<Map<String,AttributeValue>> items, Map<String, AggregateResult> fullResults) {
    for(Map<String,AttributeValue> m : items) {
      String src_airport = m.get("src_airport").s();
      String dst_airport = m.get("dst_airport").s();
      int i = Integer.parseInt(m.get("stops").n());
      String airport_key = src_airport + "," + dst_airport;
      AggregateResult r = fullResults.get(airport_key);
      if(r == null) {
        r = new AggregateResult();
        r.srcAirport = src_airport;
        r.dstAirport = dst_airport;
        fullResults.put(airport_key, r);
      }
      r.count = r.count + 1;
      r.sumStops = r.sumStops + i;
    }
  }

  private void mergeListsOfItems(Map<String, AggregateResult> src, Map<String, AggregateResult> dst) {
    for(Map.Entry<String,AggregateResult> in : src.entrySet()) {
      String key = in.getKey();
      AggregateResult src_val = src.get(key);
      AggregateResult dst_val = dst.get(key);
      if(dst_val == null) {
        dst.put(key, src_val);
      } else {
        dst_val.count = dst_val.count + src_val.count;
        dst_val.sumStops = dst_val.sumStops + src_val.sumStops;
      }
    }
  }

  private void harvestAsync(List<AggregateResult> agg_results, int minOccurrences) {
    Map<String, AggregateResult> dst = new HashMap<>();
    for(AggregateResult result : agg_results) {
      Map<String, AggregateResult> src = result.future.join();
      mergeListsOfItems(src, dst);
    }
    agg_results.clear();
    for(Map.Entry<String,AggregateResult> e : dst.entrySet()) {
      AggregateResult r = e.getValue();
      if(r.count <= minOccurrences) continue;
      r.avgStops = ((double) r.sumStops) / ((double) r.count);
      agg_results.add(r);
    }
    // Sort by avg_stops in descending order and limit results
    agg_results.sort((r1, r2) -> Double.compare(r2.avgStops, r1.avgStops));
    if(agg_results.size() > AGGREGATE_QUERY_LIMIT) {
      agg_results = agg_results.subList(0, AGGREGATE_QUERY_LIMIT);
    }
  }

  static class AggregateResult {
    String srcAirport;
    String dstAirport;
    int count;
    long sumStops;
    double avgStops;
    CompletableFuture<Map<String, AggregateResult>> future;
  }
}