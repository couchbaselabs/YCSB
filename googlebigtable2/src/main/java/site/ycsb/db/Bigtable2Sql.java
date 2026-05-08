/*
 * Copyright 2026 benchANT GmbH. All Rights Reserved.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.google.cloud.bigtable.data.v2.models.sql.ColumnMetadata;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSet;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSetMetadata;
import com.google.cloud.bigtable.data.v2.models.sql.SqlType;
import com.google.cloud.bigtable.data.v2.models.sql.Statement;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.NumericByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.schema.SchemaHolder;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumn;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnType;
import site.ycsb.wrappers.Comparison;

public final class Bigtable2Sql {
    
    static String updateOne(String table, List<Comparison> filters) throws Exception {
        String filterPortion = Bigtable2Helper.buildConcatenatedSQLFilter(GoogleBigtable2Client.columnFamily, filters, false);
        String statement = "SELECT _key FROM " + table + " WHERE " + filterPortion + " LIMIT 1";
        Statement stmt = Statement.of(statement);
        boolean hasResult = false;
        Map<String, ByteIterator> myResult = new HashMap<>();
        try (ResultSet resultSet = GoogleBigtable2Client.client.executeQuery(stmt)) {
          while (resultSet.next()) {
            hasResult = true;
            handleResultRow(resultSet, myResult);
          }
        } catch (Exception e) {
          if (GoogleBigtable2Client.debug) {
            e.printStackTrace();
          }
          // we are catching here to make sure
          // the ResultSet is cleaned up afterwards
          throw e;
        }
        if(!hasResult) {
          return null;
        } else if(GoogleBigtable2Client.debug) {
            System.err.println("UpdateOne (pt 1) statement: " + statement);
        }
        return myResult.get("_key").toString();
    }

  static Status aggregate(String table, String[] airports, int minOccurrences, Vector<HashMap<String, ByteIterator>> results) {
    String query = Bigtable2Helper.buildAggregateQueryString(table, airports, minOccurrences);
    Statement stmt = Statement.of(query);
    boolean hasResult = false;
    HashMap<String, ByteIterator> myResult = new HashMap<>();
    try (ResultSet resultSet = GoogleBigtable2Client.client.executeQuery(stmt)) {
      while (resultSet.next()) {
        hasResult = true;
        handleResultRow(resultSet, myResult);
        results.add(myResult);
        myResult = new HashMap<>();
      }
    } catch (Exception e) {
      if (GoogleBigtable2Client.debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }
    if(!hasResult) {
      return Status.NOT_FOUND;
    }
    if(GoogleBigtable2Client.debug) {
      System.out.println("Aggregate query: " + query);
      System.err.println("Aggregate result: " + results);
    }
    return Status.OK;
  }

  static Status findOne(String table, List<Comparison> filters, Set<String> fields, Map<String, ByteIterator> result) {
    if(filters == null || filters.size() == 0) {
      throw new NullPointerException();
    }
    if(fields != null) {
      throw new UnsupportedOperationException("cannot read results by field");
    }
    // throw new UnsupportedOperationException("findOne is not implemented yet");
    String filterPortion = Bigtable2Helper.buildConcatenatedSQLFilter(GoogleBigtable2Client.columnFamily, filters, false);
    String statement = "SELECT * FROM " + table + " WHERE " + filterPortion + " LIMIT 1";
    Statement stmt = Statement.of(statement);
    // Filters.Filter f = Bigtable2Helper.buildConcatenatedFilter(filters, columnFamily, false);
    // Query query = Query.create(table).filter(f);
    // ServerStream<Row> rows = client.readRows( query );
    /* for (Row row : rows) {
      rowToMap(row, result);
      System.out.println("Result row: " + result);
    } */
    boolean hasResult = false;
    try (ResultSet resultSet = GoogleBigtable2Client.client.executeQuery(stmt)) {
        while (resultSet.next()) {
        hasResult = true;
        handleResultRow(resultSet, result);
        }
        } catch (Exception e) {
        if (GoogleBigtable2Client.debug) {
            e.printStackTrace();
        }
        return Status.ERROR;
        }
        if(!hasResult) {
        return Status.NOT_FOUND;
        }
        if(GoogleBigtable2Client.debug) {
            System.out.println("FindOne statement: " + statement);
            System.out.println("FindOne result: " + result);
        }
        return Status.OK;
    }

    static void handleResultRow(ResultSet resultSet, Map<String, ByteIterator> result) {
      ResultSetMetadata metadata = resultSet.getMetadata();
      for(ColumnMetadata col : metadata.getColumns()) {
        String colName = col.name();
        SqlType<?> colType = col.type();
        switch(colType.getCode()) {
          case INT64:
            long longValue = resultSet.getLong(colName);
            result.put(colName, new NumericByteIterator(longValue));
            break;
          case STRING:
            String strValue = resultSet.getString(colName);
            result.put(colName, new StringByteIterator(strValue));
            break;
          case BYTES:
            byte[] byteVal = resultSet.getBytes(colName).toByteArray();
            result.put(colName, new ByteArrayByteIterator(byteVal));
            break;
          case MAP:
            Map<ByteString,ByteString> map = resultSet.getMap(colName, SqlType.mapOf(SqlType.bytes(), SqlType.bytes()));
            for(ByteString key : map.keySet()) {
              String cName = key.toStringUtf8();
              result.put(cName, decodeColumnElementByType(cName, map.get(key)));
            }
            break;
          case FLOAT64:
            double doubleValue = resultSet.getDouble(colName);
            result.put(colName, new NumericByteIterator(doubleValue));
            break;
          default:
            throw new IllegalArgumentException("Unsupported column type: " + colType.getCode());
        }
      }
  }

  static final SchemaHolder schema = SchemaHolder.INSTANCE;
  static ByteIterator decodeColumnElementByType(String cName, ByteString value) {
    List<SchemaColumn> columns = schema.getOrderedListOfColumns();
    Map<String, SchemaColumn> columnMap = new HashMap<>();
    for(SchemaColumn c : columns) {
      columnMap.put(c.getColumnName(), c);
    }
    SchemaColumn c = columnMap.get(cName);
    if(c == null || c.getColumnType() == SchemaColumnType.TEXT) {
      return new StringByteIterator(value.toStringUtf8());
    }
    if(c.getColumnType() == SchemaColumnType.INT || c.getColumnType() == SchemaColumnType.LONG) {
      byte[] v = value.toByteArray();
      long longValue = Longs.fromByteArray(v);
      return new NumericByteIterator(longValue);
    } else {
        throw new IllegalArgumentException("Invalid long/int value for column " + cName);
    }
 }
}
