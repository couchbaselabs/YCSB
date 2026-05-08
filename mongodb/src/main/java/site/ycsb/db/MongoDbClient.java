/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 * Copyright (c) 2023 - 2026 benchANT GmbH. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package site.ycsb.db;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;  
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Projections.*;


import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.IndexableDB;
import site.ycsb.Status;
import site.ycsb.workloads.core.CoreConstants;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

import org.bson.conversions.Bson;
import org.bson.BsonArray;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends DB implements IndexableDB {
  public static final String INDEX_LIST_PROPERTY = "mongodb.indexlist";

  /** Used to include a field in a response. */
  private static final Integer INCLUDE = Integer.valueOf(1);

  /** The options to use for inserting many documents. */
  private static final InsertManyOptions INSERT_UNORDERED =
      new InsertManyOptions().ordered(false);

  /** The options to use for inserting a single document. */
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions()
      .upsert(true);

  /**
   * The database name to access.
   */
  private static String databaseName;

  /** The database name to access. */
  private static MongoDatabase database;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** A singleton Mongo instance. */
  private static MongoClient mongoClient;

  /** The default read preference for the test. */
  private static ReadPreference readPreference;

  /** The default write concern for the test. */
  private static WriteConcern writeConcern;

  /** The batch size to use for inserts. */
  private static int batchSize;

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /** The bulk inserts pending for the thread. */
  private final List<Document> bulkInserts = new ArrayList<Document>();

  private static boolean useTypedFields;
  private static boolean debug = false;

  public static final int AGGREAGTE_QUERY_LIMIT = 10;
  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        mongoClient.close();
      } catch (Exception e1) {
        System.err.println("Could not close MongoDB connection pool: "
            + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        database = null;
        mongoClient = null;
      }
    }
  }

  /**
   * Delete a record from the database.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      DeleteResult result =
          collection.withWriteConcern(writeConcern).deleteOne(query);
      if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
        if(debug) {
          System.err.println("Nothing deleted for key " + key);
        }
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  private static List<BsonValue> getIndexList(Properties props) {
    String indexeslist = props.getProperty(INDEX_LIST_PROPERTY);
    if(indexeslist == null) {
      return Collections.emptyList();
    }
    BsonArray barray = BsonArray.parse(indexeslist);
    return barray.getValues();
  }
  
  private static void setIndexes(Properties props, List<BsonValue> indexes, MongoDatabase database) {
    if(indexes.size() == 0) {
      return;
    }

    final String table = props.getProperty(CoreConstants.TABLENAME_PROPERTY, CoreConstants.TABLENAME_PROPERTY_DEFAULT);
    
    List<IndexModel> iModel = new ArrayList<>();
      for(BsonValue idx : indexes) {
        if(!idx.isDocument()) {
          System.err.println("illegal index format");
          System.exit(-2);
        }
        iModel.add(new IndexModel(idx.asDocument()));
      }
      MongoCollection<Document> collection = database.getCollection(table);
      List<String> names = collection.createIndexes(iModel);
      System.err.println("created indexes: " + names);
  }
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {
      if (mongoClient != null) {
        return;
      }

      Properties props = getProperties();
      debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = Boolean.parseBoolean(
          props.getProperty("mongodb.upsert", "false"));

      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }

      url = OptionsSupport.updateUrl(url, props);

      if (!url.startsWith("mongodb://") && !url.startsWith("mongodb+srv://")) {
        System.err.println("ERROR: Invalid URL: '" + url
            + "'. Must be of the form "
            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options' "
            + "or 'mongodb+srv://<host>/database?options'. "
            + "http://docs.mongodb.org/manual/reference/connection-string/");
        System.exit(1);
      }

      ConnectionString cs = new ConnectionString(url);
      MongoClientSettings.Builder settingsBuilder = 
        MongoClientSettings.builder().applyConnectionString(cs);
      readPreference = cs.getReadPreference();
      if(cs.getReadPreference() == null) {
        readPreference = ReadPreference.primary();
        System.err.println("read preference not set, using default: " + readPreference);
        settingsBuilder.readPreference(readPreference);
      } else {
        System.err.println("using user-defined read prefernence: " + readPreference);
      }
      writeConcern = cs.getWriteConcern();
      if(writeConcern == null) {
        writeConcern = WriteConcern.MAJORITY;
        System.err.println("write concern not set, using default: " + writeConcern);
        settingsBuilder.writeConcern(writeConcern);
      } else {
        System.err.println("using user-defined write concern: " + writeConcern);
      }
      final String uriDb = cs.getDatabase();
      if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
          && !"admin".equals(uriDb)) {
        databaseName = uriDb;
        System.err.println("using URI database name: " + databaseName);
      } else {
        // If no database is specified in URI, use "ycsb"
        System.err.println("using default database name: ycsb");
        databaseName = "ycsb";
      }

      try {
        mongoClient = MongoClients.create(settingsBuilder.build());
        database =
            mongoClient.getDatabase(databaseName)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);
        System.out.println("mongo client connection created with " + url);
      } catch (Exception e1) {
        System.err
            .println("Could not initialize MongoDB connection pool for Loader: "
                + e1.toString());
        e1.printStackTrace();
        return;
      }
      List<BsonValue> indexes = getIndexList(props);
      setIndexes(props, indexes, database);
      useTypedFields = "true".equalsIgnoreCase(props.getProperty(TYPED_FIELDS_PROPERTY));
    }
  }

  private Document buildLegacyDocument(String key, List<DatabaseField> values) {
      Document toInsert = new Document("_id", key);
      for (DatabaseField field : values) {
        toInsert.put(
          field.getFieldname(), 
          field.getContent().asIterator().toArray());
      }
      return toInsert;
  }

  private void fillDocument(DatabaseField field, Document toInsert) {
    DataWrapper wrapper = field.getContent();
      Object content = null;
      if(wrapper.isTerminal() || wrapper.isArray()) {
        // this WILL BREAK if content is a nested
        // document within the array
        content = wrapper.asObject();
      } else if(wrapper.isNested()) {
        Document inner = new Document();
        List<DatabaseField> innerFields = wrapper.asNested();
        for(DatabaseField iF : innerFields) {
          fillDocument(iF, inner);
        }
        content = inner;
      } else {
        throw new IllegalStateException("neither terminal, nor array, nor nested");
      }
      toInsert.put(
        field.getFieldname(),
        content
      );
  }

  private Document buildKeylessTypedDocument(List<DatabaseField> values) {
    Document toInsert = new Document();
    for (DatabaseField field : values) {
      fillDocument(field, toInsert);
    }
    return toInsert;
  }
  private Document buildTypedDocument(String key, List<DatabaseField> values) {
    Document toInsert = buildKeylessTypedDocument(values);
    toInsert.put("_id", key);
    return toInsert;
  }
  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, List<DatabaseField> values) {
    final Document toInsert = useTypedFields
      ? buildTypedDocument(key, values)
      : buildLegacyDocument(key, values);
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      if (batchSize == 1) {
        if (useUpsert) {
          // this is effectively an insert, but using an upsert instead due
          // to current inability of the framework to clean up after itself
          // between test runs.
          collection.replaceOne(new Document("_id", toInsert.get("_id")),
              toInsert, new ReplaceOptions().upsert(true));
        } else {
          collection.insertOne(toInsert);
        }
      } else {
        bulkInserts.add(toInsert);
        if (bulkInserts.size() >= batchSize) {
          List<Document> local = new ArrayList<>(bulkInserts);
          bulkInserts.clear();
          if (useUpsert) {
            List<UpdateOneModel<Document>> updates = 
                new ArrayList<UpdateOneModel<Document>>(local.size());
            for (Document doc : local) {
              updates.add(new UpdateOneModel<Document>(
                  new Document("_id", doc.get("_id")),
                  doc, UPDATE_WITH_UPSERT));
            }
            collection.bulkWrite(updates);
          } else {
            collection.insertMany(local, INSERT_UNORDERED);
          }
        } else {
          return Status.BATCHED_OK;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      if(debug) {
        System.err.println("Exception while trying bulk insert with " + bulkInserts.size());
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status findOne(String table, List<Comparison> filters,
      Set<String> fields, Map<String, ByteIterator> result) {
    if(filters == null || filters.size() == 0) {
      throw new NullPointerException();
    }
    // FIXME: this implementation does not support reading specific fields
    if(fields != null) {
      throw new UnsupportedOperationException("cannot read results by field");
    }
    // building the filter
    Bson query = FilterBuilder.buildConcatenatedFilter(filters);
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document queryResult = collection.find(query).first();
      if (queryResult != null) {
        fillMap(result, queryResult);
        return Status.OK;
      }
      if(debug) {
        System.err.println("NOT FOUND: " + filters);
      }
      return Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println("Exception while findOne");
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  /*private Status findOneByFilter(String table) {

  }*/
  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", key);

      FindIterable<Document> findIterable = collection.find(query);

      if (fields != null) {
        Document projection = new Document();
        for (String field : fields) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
      }

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindIterable<Document> findIterable =
          collection.find(query).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        if(debug) {
          System.err.println("Nothing found in scan for key " + startkey);
        }
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
      }
      Document update = new Document("$set", fieldsToSet);

      UpdateResult result = collection.updateOne(query, update);
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        if(debug) {
          System.err.println("Nothing updated for key " + key);
        }
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
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
    // building the filter
    Bson query = FilterBuilder.buildConcatenatedFilter(filters);
    Document update = new Document("$set", buildKeylessTypedDocument(fields));
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      UpdateResult queryResult = collection.updateOne(query, update);
      if (queryResult.wasAcknowledged() && queryResult.getMatchedCount() == 0) {
        if(debug) {
          System.err.println("Nothing updated for filters " + filters);
        }
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  @Override
  public Status aggregate(String table, String[] airports, int minOccurrences, Vector<HashMap<String, ByteIterator>> results) {
    /*
    this is a sample implementation of an aggregate function, which is not
    part of the standard YCSB API. It is specific to the Airport workload
    and maps to the following SQL query:
    ```
    "SELECT src_airport, dst_airport, count(*) as nmb, avg(stops) as avg_stops FROM " + keyspaceName + " " +
                " WHERE ARRAY_COUNT( codeshares ) > 0 " +
                " AND (src_airport IN [ ?, ?, ? ] OR dst_airport IN [ ?, ?, ? ] ) " +
                " GROUP BY src_airport, dst_airport " + 
                " HAVING COUNT(*) > ? " + 
                " ORDER BY avg_stops DESC LIMIT " + AGGREAGTE_QUERY_LIMIT;
    ```
    */
   if(debug) {
     System.out.println("Running aggregate query with airports " + String.join(", ", airports) + " and minOccurrences " + minOccurrences);
   }
    // implement aggregate query using MongoDB Pipeline API
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      List<Bson> pipeline = new ArrayList<>();
      pipeline.add(match(and(
        or(in("src_airport", airports), in("dst_airport", airports)), // in any of the two fields
        and(exists("codeshares"), not(size("codeshares", 0)))
      )));
      pipeline.add(group(new Document("src_airport", "$src_airport").append("dst_airport", "$dst_airport"),
         sum("nmb", 1), avg("avg_stops", "$stops")));
      pipeline.add(match(gt("nmb", minOccurrences)));
      pipeline.add(sort(new Document("avg_stops", -1)));
      pipeline.add(limit(AGGREAGTE_QUERY_LIMIT));
      // project _id.src_airport to src_airport and _id.dst_airport to dst_airport, 
      // but keep nmb and avg_stops as they are, and remove _id
      pipeline.add(project(fields(
        computed("src_airport", "$_id.src_airport"),
        computed("dst_airport", "$_id.dst_airport"),
        include("nmb", "avg_stops"),
        excludeId()
      )));
      for (Document doc : collection.aggregate(pipeline)) {
        HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
        fillMap(resultMap, doc);
        results.add(resultMap);
      }
      if(results.size() == 0) {
        return Status.NOT_FOUND;
      }
      if(debug){
        System.out.println("Found " + results.size() + " results: " + results);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.toString());
      return Status.ERROR;
    }
    
  }
  /**
   * Fills the map with the values from the DBObject.
   * 
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(),
            new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      } else if(entry.getValue() == null) {
        resultMap.put(entry.getKey(), null);
      } else {
        resultMap.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue().toString().getBytes()));
        // System.err.println("WARNING: cannot convert value of type " + entry.getValue().getClass() + " to byte array for field " + entry.getKey());
      }
    }
  }
}
