/**
 * Copyright (c) 2024 YCSB contributors. All rights reserved.
 * Copyright (c) 2026 benchANT GmbH. All Rights Reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Filter;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.common.base.Strings;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.GoogleFirestoreQueryHelper.AggregateResult;
import site.ycsb.wrappers.DatabaseField;
import site.ycsb.IndexableDB;
import site.ycsb.NumericByteIterator;
import site.ycsb.wrappers.Comparison;
import site.ycsb.DB;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * Idiomatic Java client for Google Firestore client for YCSB framework.
 */
public class GoogleFirestoreClient extends DB  implements IndexableDB {

  /**
   * Property names for the CLI.
   */
  public static final int AGGREGATE_QUERY_LIMIT = 10;

  private static final String PROP_PREFIX = "googlefirestore";

  private static final String DEBUG_KEY = "debug";
  private static final String PROJECT_KEY = PROP_PREFIX + ".project";
  private static final String DATABASE_KEY = PROP_PREFIX + ".database";
  private static final String ENDPOINT_KEY = PROP_PREFIX + ".endpoint";
  private static final String CREDENTIALS_KEY = PROP_PREFIX + ".credentials";

  /**
   * Print debug information to standard out.
   */
  private static boolean debug = false;

  /**
   * Tracks running thread counts so we know when to close the session.
   */
  private static int clientRefCount = 0;

  /**
   * Global Firestore client object.
   */
  private static Firestore firestore;

  // configuration options
  private static int batchSize = 1;
  private static boolean useTypedFields = false;
  // private final List<Map<String, Object>> batchMap = new ArrayList<>();
  private int batchCounter = 0;
  private WriteBatch batch; 

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    try {
      globalInit(props);
    } catch (Exception e) {
      throw new DBException("Failed to initialize Firestore client", e);
    }
    if(batchSize > 1) {
      batch = firestore.batch();
    }
  }

  private void globalInit(Properties props) throws IOException {
    synchronized (GoogleFirestoreClient.class) {
      clientRefCount++;
      if (clientRefCount > 1) {
        return;
      }

      debug = Boolean.parseBoolean(props.getProperty(DEBUG_KEY, "false"));
      useTypedFields = this.getBooleanProperty(TYPED_FIELDS_PROPERTY, true); 
      batchSize = this.getIntProperty("db.batchsize", 1);
      // Build Firestore options
      FirestoreOptions.Builder builder = FirestoreOptions.newBuilder()
          .setProjectId(getRequiredProp(props, PROJECT_KEY));

      // Optional: Service account credentials file path.
      // If not provided, falls back to Application Default Credentials
      // (GOOGLE_APPLICATION_CREDENTIALS env var or gcloud auth).
      Optional.ofNullable(props.getProperty(CREDENTIALS_KEY))
          .filter(s -> !s.isEmpty())
          .ifPresent(credentialsPath -> {
            try {
              ServiceAccountCredentials credentials =
                  ServiceAccountCredentials.fromStream(new FileInputStream(credentialsPath));
              builder.setCredentials(credentials);
              if (debug) {
                System.out.println("Using service account credentials from: " + credentialsPath);
              }
            } catch (IOException e) {
              throw new IllegalStateException(
                  "Failed to load credentials from: " + credentialsPath, e);
            }
          });

      // Optional: Database name (defaults to "(default)" if not specified)
      Optional.ofNullable(props.getProperty(DATABASE_KEY))
          .ifPresent(builder::setDatabaseId);

      // Optional: Custom endpoint (useful for emulator)
      Optional<String> emulatorHost =
          Optional.ofNullable(System.getenv().get("FIRESTORE_EMULATOR_HOST"));
      Optional<String> endpoint = Optional.ofNullable(props.getProperty(ENDPOINT_KEY));

      if (emulatorHost.isPresent()) {
        // Emulator host format: "host:port"
        String emulator = emulatorHost.get();
        if (debug) {
          System.out.println("Using Firestore emulator: " + emulator);
        }
        // When using emulator, the endpoint is automatically configured
      } else {
        endpoint.ifPresent(builder::setHost);
      }

      FirestoreOptions options = builder.build();

      if (debug) {
        System.out.println("Initializing Firestore client with settings:");
        System.out.println("  Project: " + options.getProjectId());
        System.out.println("  Database: " + options.getDatabaseId());
        System.out.println("  Host: " + options.getHost());
      }

      firestore = options.getService();
    }
  }

  private static String getRequiredProp(Properties props, String key) {
    String val = props.getProperty(key);
    if (Strings.isNullOrEmpty(val)) {
      throw new IllegalStateException("Missing required property: " + key);
    }
    return val;
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (GoogleFirestoreClient.class) {
      clientRefCount--;
      if (clientRefCount <= 0 && firestore != null) {
        try {
          firestore.close();
          firestore = null;
        } catch (Exception e) {
          throw new DBException("Error closing Firestore client", e);
        }
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (debug) {
      System.out.println("Doing read for key: " + key);
    }

    try {
      // Get the document from Firestore
      ApiFuture<DocumentSnapshot> future =
          firestore.collection(table).document(key).get();

      DocumentSnapshot document = future.get();

      if (debug) {
        System.out.println("Result document exists: " + document.exists());
      }

      // Check if document exists
      if (!document.exists()) {
        return Status.NOT_FOUND;
      }

      // Convert document to map
      Map<String, Object> data = document.getData();
      if (data == null) {
        return Status.NOT_FOUND;
      }

      // Filter fields if specified, otherwise return all fields
      if (fields == null || fields.isEmpty()) {
        // Return all fields
        for (Map.Entry<String, Object> entry : data.entrySet()) {
          result.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
        }
      } else {
        // Return only requested fields
        for (String field : fields) {
          Object value = data.get(field);
          if (value != null) {
            result.put(field, new StringByteIterator(value.toString()));
          }
        }
      }

      if (debug) {
        System.out.println("Read completed for key: " + key + ", fields: " + result.size());
      }

      return Status.OK;
    } catch (InterruptedException e) {
      if (debug) {
        System.err.println("Read interrupted for key: " + key);
        e.printStackTrace();
      }
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      if (debug) {
        System.err.println("Read failed for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unexpected error during read for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (debug) {
      System.out.println("Doing update for key: " + key);
    }

    // Convert ByteIterator values to strings for Firestore document
    Map<String, Object> data = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      data.put(entry.getKey(), entry.getValue().toString());
    }
    return doUpdateDocument(table, key, data);
  }

  private Status doUpdateDocument(String table, String key, Map<String, Object> data) {
    try {
      // Update the document in Firestore (creates if doesn't exist due to set with merge)
      ApiFuture<com.google.cloud.firestore.WriteResult> future =
          firestore.collection(table).document(key).update(data);
      // Wait for the operation to complete
      future.get();

      if (debug) {
        System.out.println("Update completed for key: " + key);
      }

      return Status.OK;
    } catch (InterruptedException e) {
      if (debug) {
        System.err.println("Update interrupted for key: " + key);
        e.printStackTrace();
      }
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      if (debug) {
        System.err.println("Update failed for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unexpected error during update for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, List<DatabaseField> values) {
    if (debug) {
      System.out.println("Doing insert for key: " + key);
    }

    try {
      // Convert ByteIterator values to strings for Firestore document
      Map<String, Object> data = new HashMap<>();
      if(useTypedFields) {
        GoogleFirestoreQueryHelper.populateTypedFields(data, values);
      } else {
        Map<String, ByteIterator> valuesMap = DB.fieldListAsIteratorMap(values);
        for (Map.Entry<String, ByteIterator> entry : valuesMap.entrySet()) {
          data.put(entry.getKey(), entry.getValue().toString());
        }
      }
      DocumentReference docRef = firestore.collection(table).document(key);
      if(batchSize > 1) {
        batch.set(docRef, data);
        batchCounter++;
        if(batchCounter >= batchSize) {
          // Commit the batch
          ApiFuture<List<WriteResult>> future = batch.commit();
          future.get(); // Wait for commit to complete
          batch = firestore.batch(); // Start a new batch
          batchCounter = 0;
          return Status.OK;
        } else {
          return Status.BATCHED_OK;
        }
      }
      // no batching. Single element inserts
      // Set the document in Firestore (creates if doesn't exist)
      ApiFuture<com.google.cloud.firestore.WriteResult> future = docRef.set(data);

      // Wait for the operation to complete
      future.get();

      if (debug) {
        System.out.println("Insert completed for key: " + key);
      }

      return Status.OK;
    } catch (InterruptedException e) {
      if (debug) {
        System.err.println("Insert interrupted for key: " + key);
        e.printStackTrace();
      }
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      if (debug) {
        System.err.println("Insert failed for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unexpected error during insert for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }

    try {
      // Delete the document from Firestore
      ApiFuture<com.google.cloud.firestore.WriteResult> future =
          firestore.collection(table).document(key).delete();

      // Wait for the operation to complete
      WriteResult result = future.get();
      if (debug) {
        System.out.println("Delete completed for key: " + key + ", update time: " + result.getUpdateTime());
      }

      return Status.OK;
    } catch (InterruptedException e) {
      if (debug) {
        System.err.println("Delete interrupted for key: " + key);
        e.printStackTrace();
      }
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      if (debug) {
        System.err.println("Delete failed for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unexpected error during delete for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  public Status findOne(String table, List<Comparison> filters, Set<String> fields, Map<String, ByteIterator> result) {
    if(filters == null || filters.size() == 0) {
      throw new NullPointerException();
    }
    if(fields != null) {
      throw new UnsupportedOperationException("cannot read results by field");
    }
    // transform filters into a document key lookup
    Query q = GoogleFirestoreQueryHelper.buildAndBindFindOneQuery(firestore.collection(table), filters);

    try {
      QuerySnapshot snapshot = q.get().get();
      List<QueryDocumentSnapshot> documents = snapshot.getDocuments();
      if(documents.isEmpty()) {
        return Status.NOT_FOUND;
      }
      if(documents.size() > 1) {
        return Status.UNEXPECTED_STATE;
      }
      // There is only one result
      QueryDocumentSnapshot document = documents.get(0);
      extractSingleRow(document, result);
      
      if(debug) {
        System.out.println("findOne completed: " + result);
      }
      return Status.OK;
    } catch(InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Unexpected error during findOne: " + e.getMessage());
      return Status.ERROR;
    } catch (ExecutionException e) {
      System.err.println("Unexpected error during findOne: " + e.getMessage());
      return Status.ERROR;
    }
  }

  public Status aggregate(String table, String[] airports, int minOccurrences, Vector<HashMap<String, ByteIterator>> results) {
    Query q = firestore.collection(table)
                        .whereNotEqualTo("codeshares", null)
                        .where(Filter.or(
                                    Filter.inArray("src_airport", Arrays.asList(airports)),
                                    Filter.inArray("dst_airport", Arrays.asList(airports)))
                        ).orderBy("src_airport").orderBy("dst_airport");
    try {
      QuerySnapshot snapshot = q.get().get();
      List<QueryDocumentSnapshot> documents = snapshot.getDocuments();
      List<AggregateResult> aggregateResults =
         GoogleFirestoreQueryHelper.postprocessAggregateResults(documents, minOccurrences, debug);
      if(aggregateResults.isEmpty()) {
        return Status.NOT_FOUND;
      }
      GoogleFirestoreQueryHelper.convertAggregateResults(aggregateResults, results);
      if(debug) {
        System.out.println("Aggregate completed: " + results);
      } 
      return Status.OK;
     } catch(InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Unexpected error during findOne: " + e.getMessage());
      return Status.ERROR;
    } catch (ExecutionException e) {
      System.err.println("Unexpected error during findOne: " + e.getMessage());
      return Status.ERROR;
    } 
  }
  
  public Status updateOne(String table, List<Comparison> filters, List<DatabaseField> fields) {
    Query q = GoogleFirestoreQueryHelper.buildAndBindFindOneQuery(firestore.collection(table), filters).select("id");
    String updateId;
    try {
      QuerySnapshot snapshot = q.get().get();
      List<QueryDocumentSnapshot> documents = snapshot.getDocuments();
      if(documents.isEmpty()) {
        return Status.NOT_FOUND;
      }
      if(documents.size() > 1) {
        return Status.UNEXPECTED_STATE;
      }
      // There is only one result
      QueryDocumentSnapshot document = documents.get(0);
      updateId = document == null ? null : document.getId();
      if(debug) {
        System.out.println("Document to update: " + updateId);
      }
    } catch(InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Unexpected error during findOne: " + e.getMessage());
      return Status.ERROR;
    } catch (ExecutionException e) {
      System.err.println("Unexpected error during findOne: " + e.getMessage());
      return Status.ERROR;
    }
    if(updateId == null) {
      return Status.UNEXPECTED_STATE;
    }
    Map<String, Object> data = new HashMap<>();
    if(useTypedFields) {
      GoogleFirestoreQueryHelper.populateTypedFields(data, fields);
    }
    return doUpdateDocument(table, updateId, data);
  }

  private void extractSingleRow(QueryDocumentSnapshot document, Map<String, ByteIterator> result) {
    result.put("key", new StringByteIterator(document.getId()));
    for (Map.Entry<String, Object> entry : document.getData().entrySet()) {
      Object value = entry.getValue();
      if(value instanceof String) {
        result.put(entry.getKey(), new StringByteIterator((String) value));
      } else if(value instanceof Number) {
        // Convert numbers and booleans to their string representation
        result.put(entry.getKey(), new NumericByteIterator(((Number) value).longValue()));;
      } else if (value instanceof List) {
        // Firestore returns arrays as List, convert to string representation
        List<?> listValue = (List<?>) value;
        for(Object lEntry : listValue) {
          if(lEntry.getClass() != String.class) {
            System.out.println("List entry has non-string value: " + lEntry);
            // only put out warning once
            break;
          }
        }
        // omitting arrays for now, as we don't have a way to represent
        // them in the ByteIterator result map
        result.put(entry.getKey(), new StringByteIterator(value.toString()));
      } else if(value instanceof Map) {
        // Firestore returns nested objects as Map, convert to string representation
        Map<?,?> mapValue = (Map<?,?>) value;
        String prefix = entry.getKey() + ".";
        for(Map.Entry<?,?> mapEntry : mapValue.entrySet()) {
          Object vvalue = mapEntry.getValue();
          result.put(prefix + mapEntry.getKey(), new StringByteIterator(vvalue.toString()));
          if(vvalue.getClass() != String.class) {
            System.out.println("Nested object field " + prefix + mapEntry.getKey() + " has non-string value: " + vvalue);
          }
        }
      } else if(value.getClass().isArray()) {
        // Firestore returns arrays as List, convert to string representation
        // value = value.toString();
        // omitting arrays for now, as we don't have a way to represent them in the ByteIterator result map
        throw new IllegalArgumentException("Unsupported nested object: " + value);
      } else {
        throw new IllegalArgumentException("Unsupported field type for field: " + entry.getKey() + " with value: " + value);
      }
    }
  }

  private boolean getBooleanProperty(String propertyName, boolean defaultValue) {
    String stringVal = getProperties().getProperty(propertyName, null);
    if (stringVal == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(stringVal);
  }

  private int getIntProperty(String propertyName, int defaultValue) {
    String stringVal = getProperties().getProperty(propertyName, null);
    if (stringVal == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(stringVal);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
