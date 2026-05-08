/**
 * Copyright (c) 2024 YCSB contributors. All rights reserved.
 * Copyright 2026 benchANT GmbH. All Rights Reserved.
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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatchingException;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.ChainFilter;
import com.google.cloud.bigtable.data.v2.models.MutationApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.IndexableDB;
import site.ycsb.NumericByteIterator;
import site.ycsb.Status;
import site.ycsb.workloads.core.CoreConstants;
import site.ycsb.workloads.schema.SchemaHolder;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumn;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnType;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

/**
 * Idiomatic Java client for Google Bigtable Proto client for YCSB framework.
 */
public class GoogleBigtable2Client extends site.ycsb.DB implements IndexableDB {

  /**
   * Property names for the CLI.
   */
  private static final String PROP_PREFIX = "googlebigtable2";

  private static final String AUTH_FILE = PROP_PREFIX + ".authfile";

  private static final String DEBUG_KEY = "debug";
  private static final String ENDPOINT_KEY = PROP_PREFIX + ".data-endpoint";
  private static final String PROJECT_KEY = PROP_PREFIX + ".project";
  private static final String INSTANCE_KEY = PROP_PREFIX + ".instance";
  private static final String APP_PROFILE_ID_KEY = PROP_PREFIX + ".app-profile";
  private static final String FAMILY_KEY = PROP_PREFIX + ".family";
  private static final String USE_SQL_QUERIES = PROP_PREFIX + ".use-sql-queries";

  private static final String MAX_SCAN_RATE = "maxscanrate";
  private static final String SCAN_OP_TIMELIMIT = "scanoptimelimit";
  private static final String DISCARD_SCANNED_RECORD = "discardscannedrecord";

  private static final String MAX_OUTSTANDING_BYTES_KEY = PROP_PREFIX + ".max-outstanding-bytes";
  private static final String CLIENT_SIDE_BUFFERING_KEY = PROP_PREFIX + ".use-batching";
  private static final String REVERSE_SCANS_KEY = PROP_PREFIX + ".reverse-scans";
  private static final String FIXED_TIMESTAMP_KEY = PROP_PREFIX + ".timestamp";
  private static final String INIT_SCHEMA_KEY = PROP_PREFIX + ".init-schema";
  // Defaults to autosized
  private static final String CHANNEL_POOL_SIZE = PROP_PREFIX + ".channel-pool-size";

  /**
   * Print debug information to standard out.
   */
  static boolean debug = false;

  static boolean useSqlQueries = true;
  /**
   * Tracks running thread counts so we know when to close the session.
   */
  private static int clientRefCount = 0;

  /**
   * Global Bigtable native API objects.
   */
  static BigtableDataClient client;

  static String columnFamily;
  /**
   * If true, buffer mutations on the client. For measuring insert/update/delete latencies, client
   * side buffering should be disabled.
   */
  private static boolean clientSideBuffering = true;
  private static boolean reverseScans = false;
  private static Optional<Long> fixedTimestamp = Optional.empty();

  /**
   * The max number of records to scan per second, used to slow down the
   * client-side consumption of scanned records when the actual scan rate
   * is high and the scan range is large. If set zero, there is no throttling.
   */
  private static long maxScanRate = 0;

  /**
   * The time limit of a single scan operation (in seconds). No effect if zero.
   * It's useful when the scan operation runs very slowly due to intended throttling.
   * As the operation may take hours to finish, ycsb_timelimit is not well respected.
   */
  private static long scanOpTimelimit = 0;

  /**
   * If true, scanned record will be parsed but not kept in memory.
   * It's useful when doing large but slow scans, so that the client
   * doesn't hit out-of-memory issues.
   */
  private static boolean discardScannedRecord = false;

  private static boolean useTypedFields = true;

  /**
   * Thread local Bigtable native API objects.
   */
  private final Map<String, Batcher<RowMutationEntry, Void>> batchers = new HashMap<>();

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    try {
      globalInit(props);
    } catch (Exception e) {
      throw new DBException("Failed to initialize client", e);
    }
  }

  private static synchronized void globalInit(Properties props) throws IOException {
    clientRefCount++;
    if (clientRefCount > 1) {
      return;
    }

    debug = Boolean.parseBoolean(props.getProperty(DEBUG_KEY, "false"));
    useSqlQueries = "true".equalsIgnoreCase(props.getProperty(USE_SQL_QUERIES, "true"));
    System.out.println((useSqlQueries ? "U" : "Not u") + "sing SQL queries [" + useSqlQueries + "]");
    useTypedFields = "true".equalsIgnoreCase(props.getProperty(TYPED_FIELDS_PROPERTY));
    // Resource names
    BigtableDataSettings.Builder builder =
        BigtableDataSettings.newBuilder()
            .setProjectId(getRequiredProp(props, PROJECT_KEY))
            .setInstanceId(getRequiredProp(props, INSTANCE_KEY));

    Optional.ofNullable(props.getProperty(APP_PROFILE_ID_KEY)).ifPresent(builder::setAppProfileId);

    Optional.ofNullable(props.getProperty(CHANNEL_POOL_SIZE))
        .map(Integer::parseInt)
        .ifPresent(size ->
            builder.stubSettings().setTransportChannelProvider(
              EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder()
                  .setChannelPoolSettings(ChannelPoolSettings.staticallySized(size))
                  .build()
        ));

    columnFamily = getRequiredProp(props, FAMILY_KEY);

    // Endpoint
    Optional<String> emulatorHost =
        Optional.ofNullable(System.getenv().get("BIGTABLE_EMULATOR_HOST"));
    Optional<String> dataEndpoint = Optional.ofNullable(props.getProperty(ENDPOINT_KEY));
    if (emulatorHost.isPresent()) {
      Preconditions.checkState(
          !dataEndpoint.equals(emulatorHost),
          "Can't override endpoint when BIGTABLE_EMULATOR_HOST is set");
    } else {
      dataEndpoint.ifPresent(ep -> builder.stubSettings().setEndpoint(ep));
    }

    // Other settings
    fixedTimestamp = Optional.ofNullable(props.getProperty(FIXED_TIMESTAMP_KEY))
        .map(Long::parseLong);

    clientSideBuffering =
        Optional.ofNullable(props.getProperty(CLIENT_SIDE_BUFFERING_KEY))
            .map(Boolean::parseBoolean)
            .orElse(true);

    maxScanRate =
        Optional.ofNullable(props.getProperty(MAX_SCAN_RATE))
            .map(Long::parseLong)
            .orElse(0L);
    scanOpTimelimit =
        Optional.ofNullable(props.getProperty(SCAN_OP_TIMELIMIT))
            .map(Long::parseLong)
            .orElse(0L);
    discardScannedRecord =
        Optional.ofNullable(props.getProperty(DISCARD_SCANNED_RECORD))
            .map(Boolean::parseBoolean)
            .orElse(false);

    reverseScans = Optional.ofNullable(props.getProperty(REVERSE_SCANS_KEY))
        .map(Boolean::parseBoolean)
        .orElse(false);

    Optional.ofNullable(props.getProperty(MAX_OUTSTANDING_BYTES_KEY))
        .map(Long::parseLong)
        .ifPresent(newLimit -> {
            BatchingSettings oldSettings =
                builder.stubSettings().bulkMutateRowsSettings().getBatchingSettings();

            builder
                .stubSettings()
                .bulkMutateRowsSettings()
                .setBatchingSettings(
                    oldSettings.toBuilder()
                        .setFlowControlSettings(
                            oldSettings.getFlowControlSettings().toBuilder()
                                .setMaxOutstandingRequestBytes(newLimit)
                                .build())
                        .build());
          });
    String authFile = props.getProperty(AUTH_FILE);
    if(authFile == null || authFile.isEmpty()) {
      if (debug) {
        System.out.println("Using default application credentials");
      }
    } else {
      if (debug) {
        System.out.println("Using credentials from file: " + authFile);
      }
      CredentialsProvider provider = FixedCredentialsProvider.create(
        ServiceAccountCredentials.fromStream(new FileInputStream(authFile)));
      builder.setCredentialsProvider(provider);
    }
    BigtableDataSettings settings = builder.build();

    if (debug) {
      System.out.println("Initializing client with settings: " + settings);
    }
    boolean initSchema = "true".equalsIgnoreCase(props.getProperty(INIT_SCHEMA_KEY, "false"));
    if(initSchema){
      System.err.println("GoogleBigtableClient: initializing schema");
      initSchema(props, builder);
    } else {
      System.err.println("GoogleBigtableClient: skipping schema initialization");
    }
    client = BigtableDataClient.create(settings);
  }

  private static void initSchema(Properties props, BigtableDataSettings.Builder builder) throws IOException  {
    // BigtableInstanceAdminClient admin;
    BigtableTableAdminClient tAdmin = BigtableTableAdminClient.create(
      BigtableTableAdminSettings.newBuilder()
        .setCredentialsProvider(builder.getCredentialsProvider())
        .setInstanceId(builder.getInstanceId())
        .setProjectId(builder.getProjectId())
        .build()
    );
    String tablename = props.getProperty(CoreConstants.TABLENAME_PROPERTY, CoreConstants.TABLENAME_PROPERTY_DEFAULT);
    tAdmin.createTable(
      CreateTableRequest.of(tablename)
        .addFamily(columnFamily, GCRules.GCRULES.maxVersions(1))
    );
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
    for (Batcher<RowMutationEntry, Void> batcher : batchers.values()) {
      try {
        batcher.close();
      } catch (BatchingException e) {
        System.err.println(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    try {
      globalCleanup();
    } catch (Exception e) {
      throw new DBException("Failed to clean up the shared client", e);
    }
  }

  private static synchronized void globalCleanup() {
    clientRefCount--;
    if (clientRefCount == 0) {
      client.close();
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (debug) {
      System.out.println("Doing read for key: " + key);
    }

    Filters.Filter filter = buildFilter(fields);

    try {
      Row row = client.readRow(TableId.of(table), key, filter);
      if (debug) {
        System.out.println("Result row: " + row);
      }

      if (row == null) {
        return Status.NOT_FOUND;
      }

      rowToMap(row, result);
      return Status.OK;
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> results) {
    if (debug) {
      System.out.printf("Doing scan for %s for %d records%n", startkey, recordcount);
    }
    Filters.Filter filter = buildFilter(fields);

    ByteStringRange range = reverseScans
        ? Range.ByteStringRange.unbounded().endClosed(startkey)
        : Range.ByteStringRange.unbounded().startClosed(startkey);

    Query query =
        Query.create(table)
            .reversed(reverseScans)
            .filter(filter)
            .range(range)
            .limit(recordcount);

    ServerStream<Row> stream;
    try {
      stream = client.readRowsCallable().call(query);
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }

    long rowCount = 0;
    long opStartMillis = System.currentTimeMillis();
    for (Row row : stream) {
      long rowStartMillis = System.currentTimeMillis();
      HashMap<String, ByteIterator> rowResult = new HashMap<>();
      rowToMap(row, rowResult);

      if (discardScannedRecord) {
        rowResult.clear();
      }

      rowCount++;
      results.add(rowResult);

      if (scanOpTimelimit > 0 &&
          System.currentTimeMillis() - opStartMillis > scanOpTimelimit * 1000) {
        System.out.println("Stop the scan operation after the specified per-operation time limit");
        break;
      }

      if (maxScanRate > 0 && rowCount % maxScanRate == 0) {
        long timePassedMillis = System.currentTimeMillis() - rowStartMillis;
        try {
          // If the argument is less than or equal to zero, do not sleep at all.
          TimeUnit.MILLISECONDS.sleep(1000 - timePassedMillis);
        } catch (InterruptedException e) {
          System.err.println("Exception during scan throttling: " + e);
          return Status.ERROR;
        }
      }
    }
    return Status.OK;
  }

  static final SchemaHolder schema = SchemaHolder.INSTANCE;
  static void rowToMap(Row row, Map<String, ByteIterator> result) {
    Map<String, SchemaColumn> sc = new HashMap<>();
    for (SchemaColumn c : schema.getOrderedListOfColumns()) {
      sc.put(c.getColumnName(), c);
    }
    result.put("_key", new ByteStringWrapper(row.getKey()));
    for (RowCell s : row.getCells()) {
      final String name = s.getQualifier().toStringUtf8();
      final SchemaColumn c = sc.get(name);
      if(c == null || c.getColumnType() == SchemaColumnType.TEXT) {
        result.put(name, new ByteStringWrapper(s.getValue()));
      } else if(c.getColumnType() == SchemaColumnType.INT) {
        long v = Longs.fromByteArray(s.getValue().toByteArray());
        result.put(name, new NumericByteIterator(v));
      } else if(c.getColumnType() == SchemaColumnType.LONG) {
        long v = Longs.fromByteArray(s.getValue().toByteArray());
        result.put(name, new NumericByteIterator(v));
      } else {
        System.err.println("colum type " + c.getColumnType() + " not supported");
      }
    }
  }

  @Override
  public Status delete(String table, String key) {
    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }

    if (clientSideBuffering) {
      getBatcher(table).add(RowMutationEntry.create(key).deleteRow());
      return Status.BATCHED_OK;
    }

    RowMutation m = RowMutation.create(TableId.of(table), key).deleteRow();
    try {
      client.mutateRow(m);
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }

    return Status.OK;
  }

  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status insert(String table, String key, List<DatabaseField> values) {
    if(useTypedFields) {
      if (clientSideBuffering) {
        return writeBatchedTyped(table, key, values);
      } else {
        return writeDirectTyped(table, key, values);
      }
    } else {
      return update(table, key, DB.fieldListAsIteratorMap(values));
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (debug) {
      System.out.println("Setting up write for key: " + key);
    }

    try {
      if (clientSideBuffering) {
        return writeBatched(table, key, values);
      } else {
        return writeDirect(table, key, values);
      }
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  public Status findOne(String table, List<Comparison> filters, Set<String> fields, Map<String, ByteIterator> result) {
     if(useSqlQueries) {
      return Bigtable2Sql.findOne(table, filters, fields, result);
    } else {
      return Bigtable2FullNative.findOne(table, filters, fields, result);
    }
  }

  public Status updateOne(String table, List<Comparison> filters, List<DatabaseField> fields) {
    String primaryKey;
    try {
      if(useSqlQueries) {
        primaryKey = Bigtable2Sql.updateOne(table, filters);
      } else {
        primaryKey = Bigtable2FullNative.updateOne(table, filters);
      }
    } catch(Exception e) {
      return Status.ERROR;
    }
    if(primaryKey == null) {
      return Status.NOT_FOUND;
    }
    if(GoogleBigtable2Client.debug) {
      System.err.println("UpdateOne (pt 2) updating row: [ _key ] => "  + primaryKey);
    }
    return GoogleBigtable2Client.writeDirectTyped(table, primaryKey, fields);
  }
  
  public Status aggregate(String table, String[] airports, int minOccurrences, Vector<HashMap<String, ByteIterator>> results) {
    if(useSqlQueries) {
      return Bigtable2Sql.aggregate(table, airports, minOccurrences, results);
    } else {
      return Bigtable2FullNative.aggregate(table, airports, minOccurrences, results);
    }
  }

  static Map<String, String> encode(final Map<String, ByteIterator> values) {
    Map<String, String> result = new HashMap<>(values.size());
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      result.put(value.getKey(), value.getValue().toString());
    }
    return result;
  }

  static Status writeDirectTyped(String table, String key, List<DatabaseField> values) {
    RowMutation m = RowMutation.create(TableId.of(table), key);
    databaseFieldToMutation(values, m);
    return sendMutation(m);
  }

  private Status writeDirect(String table, String key, Map<String, ByteIterator> values) {
    RowMutation m = RowMutation.create(TableId.of(table), key);
    mapToMutation(values, m);
    return sendMutation(m);
  }

  private static Status sendMutation(RowMutation m) {
    try {
      client.mutateRow(m);
    } catch (Exception e) {
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }
    return Status.OK;
  }

  private Status writeBatchedTyped(String table, String key, List<DatabaseField> values) {
    RowMutationEntry m = RowMutationEntry.create(key);
    databaseFieldToMutation(values, m);
    getBatcher(table).add(m);
    return Status.BATCHED_OK;
  }
  

  private Status writeBatched(String table, String key, Map<String, ByteIterator> values) {
    RowMutationEntry m = RowMutationEntry.create(key);
    mapToMutation(values, m);
    getBatcher(table).add(m);
    return Status.BATCHED_OK;
  }

  private Batcher<RowMutationEntry, Void> getBatcher(String table) {
    return batchers.computeIfAbsent(table, (t) -> client.newBulkMutationBatcher(TableId.of(table)));
  }

  static void databaseFieldToMutation(List<DatabaseField> values, MutationApi<?> m) {
    for (DatabaseField f : values) {
      ByteString bytes = databaseFieldAsByteArray(f);
      if (fixedTimestamp.isPresent()) {
        m.setCell(
            columnFamily,
            ByteString.copyFromUtf8(f.getFieldname()),
            fixedTimestamp.get(),
            bytes);
      } else {
        m.setCell(
            columnFamily,
            ByteString.copyFromUtf8(f.getFieldname()),
            bytes);
      }
    }
  }

  private static ByteString databaseFieldAsByteArray(DatabaseField field) {
    DataWrapper wrapper = field.getContent();
    byte[] bytes = null;
    if(wrapper.isLong()) {
      bytes = Longs.toByteArray(wrapper.asLong());
    } else if(wrapper.isInteger()) {
      bytes = Longs.toByteArray(wrapper.asInteger());
    } else if(wrapper.isString()) {
      bytes = wrapper.asString().getBytes(StandardCharsets.UTF_8);
    } else {
      // let's assume it is a byte array iterator
      ByteIterator it = wrapper.asIterator();
      bytes = it.toArray();
    }
    return ByteString.copyFrom(bytes);
  }

  private void mapToMutation(Map<String, ByteIterator> values, MutationApi<?> m) {
    for (Entry<String, ByteIterator> e : values.entrySet()) {
      if (fixedTimestamp.isPresent()) {
        m.setCell(
            columnFamily,
            ByteString.copyFromUtf8(e.getKey()),
            fixedTimestamp.get(),
            ByteString.copyFrom(e.getValue().toArray()));
      } else {
        m.setCell(
            columnFamily,
            ByteString.copyFromUtf8(e.getKey()),
            ByteString.copyFrom(e.getValue().toArray()));
      }
    }
  }

  private static class ByteStringWrapper extends ByteIterator {

    private final ByteString.ByteIterator it;
    private int remaining;

    public ByteStringWrapper(ByteString value) {
      it = value.iterator();
      remaining = value.size();
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public byte nextByte() {
      remaining--;
      return it.nextByte();
    }

    @Override
    public long bytesLeft() {
      return remaining;
    }
  }

  private static Filters.Filter buildFilter(Set<String> fields) {
    ChainFilter chain = FILTERS.chain()
        .filter(FILTERS.family().exactMatch(columnFamily))
        .filter(FILTERS.limit().cellsPerColumn(1));

    if (fields != null && !fields.isEmpty()) {
      chain = chain.filter(FILTERS.qualifier().exactMatch(String.join("|", fields)));
    }
    return chain;
  }
}
