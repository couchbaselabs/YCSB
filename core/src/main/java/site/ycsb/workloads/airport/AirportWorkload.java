/**
 * Copyright (c) 2023-2026 benchANT GmbH. All rights reserved.
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
package site.ycsb.workloads.airport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.IndexableDB;
import site.ycsb.RandomByteIterator;
import site.ycsb.WorkloadException;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.workloads.CoreWorkload;
import site.ycsb.workloads.core.CoreConstants;
import site.ycsb.workloads.schema.SchemaHolder;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnType;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;
import site.ycsb.wrappers.Comparisons;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;
import site.ycsb.wrappers.Wrappers;

/**
  {
    "airline":{
      "name": str, pre-generated 50 unique values,
      "alias": str, align with name
    },
    "src_airport": str, pre-generated 500 unique values,
    "dst_airport": str, pre-generated 500 unique values,
    "codeshares": str array, length 0 to 3, from airline aliases
    "stops": int, from 0 to 3
    "airplane": str, pre-generated 10 unique values,
    "field1": str, random length from 0 to 1000
  }
 */

 /**
  * field length: only for field1
  */
 public final class AirportWorkload extends CoreWorkload {
  /**
 * The name of the property for the proportion of transactions that are deletes.
 */
  public static final String FINDONE_PROPORTION_PROPERTY = "findoneproportion";

  /**
   * The name of the property for the proportion of transactions that are aggregates.
   */
  public static final String AGGREGATE_PROPORTION_PROPERTY = "aggregateproportion";
  /**
   * The default proportion of transactions that are deletes.
   */
  public static final String FINDONE_PROPORTION_PROPERTY_DEFAULT = "0.70";
  /**
   * The name of the property for the proportion of transactions that are deletes.
   */
  public static final String DELETE_PROPORTION_PROPERTY = "deleteproportion";

  /**
   * The default proportion of transactions that are deletes.
   */
  public static final String DELETE_PROPORTION_PROPERTY_DEFAULT = "0.05";
  /**
   * The default proportion of transactions that are inserted.
   */
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.05";

  public static final String AGGREGATE_MIN_OCCURRENCES_PROPERTY = "aggregate.minoccurrences";
  public static final String AGGREGATE_MIN_OCCURRENCES_PROPERTY_DEFAULT = "100";
  /**
   * The default proportion of transactions that are reads.
  */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.00";
  public static final String NESTED_DATA_STRUCTURE_KEY = "nesteddata";
  public static final String NESTED_DATA_STRUCTURE_DEFAULT = "true";

  private static final String[] FIELD_NAMES = {
    "airline_name",
    "airline_alias",
    "src_airport",
    "dst_airport",
    "airplane",
    "codeshares",
    "stops",
    "field1"
  };

  protected DiscreteGenerator airportchooser;
  protected DiscreteGenerator aircraftchooser;
  protected DiscreteGenerator airlinechooser;
  protected UniformLongGenerator codeshareslengthgenerator;
  protected UniformLongGenerator stopsgenerator;
  protected UniformLongGenerator occurrencesGenerator;
  protected boolean useNestedDataStructure;
  protected int aggregateMinOccurrences;

  private void registerSchema() {
    if(useNestedDataStructure) {
      //         .addColumn("airline_alias", SchemaColumnType.TEXT)
      SchemaHolder.schemaBuilder()
        .addEmbeddedColumn("airline",
            SchemaHolder.schemaBuilder()
            .addColumn("name", SchemaColumnType.TEXT)
            .addColumn("alias", SchemaColumnType.TEXT)
            .build()
        )
        .addColumn("src_airport", SchemaColumnType.TEXT)
        .addColumn("dst_airport", SchemaColumnType.TEXT)
        .addColumn("airplane", SchemaColumnType.TEXT)
        .addArrayColumn("codeshares", SchemaColumnType.INT)
        .addColumn("stops", SchemaColumnType.INT)
        .addColumn("field1", SchemaColumnType.TEXT)
        .register();
    } else {
      SchemaHolder.schemaBuilder()
        .addColumn("airline_name", SchemaColumnType.TEXT)
        .addColumn("airline_alias", SchemaColumnType.TEXT)
        .addColumn("src_airport", SchemaColumnType.TEXT)
        .addColumn("dst_airport", SchemaColumnType.TEXT)
        .addColumn("airplane", SchemaColumnType.TEXT)
        .addColumn("codeshares_0", SchemaColumnType.TEXT)
        .addColumn("codeshares_1", SchemaColumnType.TEXT)
        .addColumn("codeshares_2", SchemaColumnType.TEXT)
        .addColumn("stops", SchemaColumnType.INT)
        .addColumn("field1", SchemaColumnType.TEXT)
        .register();
    }
  }
  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
    @Override
    public void init(Properties p) throws WorkloadException {
      super.init(p);
      airlinechooser = Airlines.createAirlinesGenerator();
      aircraftchooser = Airplanes.createAircraftGenerator();
      airportchooser = Airports.createAirportsGenerator();
      codeshareslengthgenerator = new UniformLongGenerator(0, 3);
      stopsgenerator = new UniformLongGenerator(0, 3);
      fieldnames = new ArrayList<String>(Arrays.asList(FIELD_NAMES));
      aggregateMinOccurrences = Integer.parseInt(
        p.getProperty(AGGREGATE_MIN_OCCURRENCES_PROPERTY, AGGREGATE_MIN_OCCURRENCES_PROPERTY_DEFAULT)
      );
      if(aggregateMinOccurrences < 0) {
        throw new WorkloadException("invalid value for " + AGGREGATE_MIN_OCCURRENCES_PROPERTY + ": " + aggregateMinOccurrences);
      } else {
        System.out.println("Using min occurrences for aggregate queries: " + aggregateMinOccurrences);
      }
      occurrencesGenerator = new UniformLongGenerator(0 , aggregateMinOccurrences);
      useNestedDataStructure = Boolean.parseBoolean(
        p.getProperty(NESTED_DATA_STRUCTURE_KEY, NESTED_DATA_STRUCTURE_DEFAULT)
      );
      registerSchema();
      operationchooser = createOperationGenerator(p);
    }

  private String getDstAirport(String src) {
    String dst = null;
    do {
      dst = airportchooser.nextString();
    } while(dst == null || src.equals(dst));
    return dst;
  }
  /**
   * Builds values for all fields.
   */
  @Override
  protected List<DatabaseField> buildValues(String key) {
    List<DatabaseField> values = new ArrayList<>();
    String airlineKey = airlinechooser.nextString();
    String src = airportchooser.nextString();
    String dst = getDstAirport(src);
    String plane = aircraftchooser.nextString();
    int codeshareslength = (int) codeshareslengthgenerator.nextValue().longValue();
    List<String> codeshares = new ArrayList<>();
    while(codeshares.size() < codeshareslength) {
      String next = airlinechooser.nextValue();
      if(next != null && !next.equals(airlineKey) && !codeshares.contains(next)) {
        codeshares.add(next);
      }
    }
    values.add(new DatabaseField("airplane", Wrappers.wrapString(plane)));
    values.add(new DatabaseField("src_airport", Wrappers.wrapString(src)));
    values.add(new DatabaseField("dst_airport", Wrappers.wrapString(dst)));
    values.add(new DatabaseField("stops", Wrappers.wrapInteger(stopsgenerator.nextValue().intValue())));
    values.add(new DatabaseField("field1", Wrappers.wrapIterator(new RandomByteIterator(fieldlengthgenerator.nextValue().longValue()))));
    final DataWrapper airlineNameWrapper = Wrappers.wrapString(Airlines.nameForKey(airlineKey));
    final DataWrapper airlineAliasWrapper = Wrappers.wrapString(airlineKey);
    final DataWrapper[] csa = new DataWrapper[codeshareslength];
      for(int i = 0; i < codeshareslength; i++) {
        csa[i] = Wrappers.wrapString(codeshares.get(i));
      }
    if(useNestedDataStructure) {
      values.add(new DatabaseField(
        "airline",
        Wrappers.wrapNested(new DatabaseField[] {
            new DatabaseField("name", airlineNameWrapper),
            new DatabaseField("alias", airlineAliasWrapper)
          }
      )));
      values.add(new DatabaseField("codeshares", Wrappers.wrapArray(csa)));
    } else {
      values.add(new DatabaseField("airline_alias", airlineAliasWrapper));
      values.add(new DatabaseField("airline_name", airlineNameWrapper));
      for(int i = 0; i < csa.length; i++) {
        values.add(new DatabaseField("codeshares_" + i, csa[i]));
      }
    }
    // build 
    return values;
  }

  public final void doTransactionDelete(DB db) {
    // choose a random key
    long keynum = nextKeynum();
    // build the key
    String keyname = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);
    // delete it
    db.delete(table, keyname);
  }

  @Override
  public void doTransactionUpdate(DB db) {
    IndexableDB idb = (IndexableDB) db;
    List<Comparison> filters = new ArrayList<>();
    List<DatabaseField> values = new ArrayList<>();
    final String alias = airlinechooser.nextString();
    // filter by airline.alias 
    if(useNestedDataStructure) {
      filters.add(
        Comparisons.createSimpleNestingComparison("airline", 
          Comparisons.createStringComparison("alias", ComparisonOperator.STRING_EQUAL, alias)
        ));
    } else {
      filters.add(Comparisons.createStringComparison("airline_alias", ComparisonOperator.STRING_EQUAL, alias));
    }
    // and update field1 with random string
    values.add(new DatabaseField("field1", Wrappers.wrapIterator(new RandomByteIterator(fieldlengthgenerator.nextValue().longValue()))));
    idb.updateOne(table, filters, values);
  }

  public void doTransactionFind(IndexableDB db) {
    // choose a src airport
    String src = airportchooser.nextString();
    // choose a dst airport != src airport
    String dst = getDstAirport(src);
    // choose the max stops
    int maxStops = stopsgenerator.nextValue().intValue();
    List<Comparison> values = new ArrayList<>();
    values.add(Comparisons.createStringComparison("src_airport", ComparisonOperator.STRING_EQUAL, src));
    values.add(Comparisons.createStringComparison("dst_airport", ComparisonOperator.STRING_EQUAL, dst));
    values.add(Comparisons.createIntComparison("stops", ComparisonOperator.INT_LTE, maxStops));
    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    // we always search for all fields
    db.findOne(table, values, null, cells);
  }

  public void doTransactionAggregate(IndexableDB db) {
    String[] airports = new String[3];
    for(int i = 0; i < 3; i++) {
      airports[i] = airportchooser.nextString();
    }
    int minOccurrences = occurrencesGenerator.nextValue().intValue();
    Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
    // TODO implement aggregation query
    db.aggregate(table, airports, minOccurrences, results);
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }
    switch (operation) {
    case "DELETE":
      // Delete: delete_one by _id. This can be implemented by
      // deleting the record 5 out of 70 times when a find
      // command is executed. 
      doTransactionDelete(db);
      break;
    case "INSERT":
      // Insert: insert a new record generated in the same way with initial records
      doTransactionInsert(db);
      break;
    case "FINDONE":
      // Find: find by src_airport, dest_airport and stops (using $lte), for example:
      // {Src_airport: “SEA”, dest_airport: “JFK”, stops: {$lte: 0}}
      if(! (db instanceof IndexableDB)) {
        throw new IllegalStateException("cannot handle default dbs for find operations");
      }
      doTransactionFind((IndexableDB) db);
      break;
    case "UPDATE":
      // probably more a readmodifywrite
      // Update: filter by airline.alias and update field1 with random string
      doTransactionUpdate(db);
      break;
    case "READ":
      // probably more a readmodifywrite
      // Update: filter by airline.alias and update field1 with random string
      doTransactionRead(db);
      break;
    case "AGGREGATE":
      // execute a certain aggregation query
      doTransactionAggregate((IndexableDB) db);
      break;
    default:
      // dont!
      throw new UnsupportedOperationException("unknown operation " + operation);
    }

    return true;
  }
    /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
        p.getProperty(CoreConstants.READ_PROPORTION_PROPERTY, "0"));
    final double updateproportion = Double.parseDouble(
        p.getProperty(CoreConstants.UPDATE_PROPORTION_PROPERTY, "0"));
    final double findoneproportion = Double.parseDouble(p.getProperty(
        FINDONE_PROPORTION_PROPERTY, "0"));
    final double insertproportion = Double.parseDouble(
        p.getProperty(CoreConstants.INSERT_PROPORTION_PROPERTY, "0"));
    final double deleteproportion = Double.parseDouble(
        p.getProperty(DELETE_PROPORTION_PROPERTY, "0"));
    final double aggregateproportion = Double.parseDouble(
        p.getProperty(AGGREGATE_PROPORTION_PROPERTY, "0"));
    /*final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
    */
    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }
    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }
    if (findoneproportion > 0) {
      operationchooser.addValue(findoneproportion, "FINDONE");
    }
    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }
    if (deleteproportion > 0) {
      operationchooser.addValue(deleteproportion, "DELETE");
    }
    if (aggregateproportion > 0) {
      operationchooser.addValue(aggregateproportion, "AGGREGATE");
    }
    /*
    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }
     */
    return operationchooser;
  }
}
