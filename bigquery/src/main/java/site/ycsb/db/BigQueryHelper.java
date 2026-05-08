/*
 * Copyright 2026 benchANT GmbH. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableResult;

import site.ycsb.ByteIterator;
import site.ycsb.NumericByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DatabaseField;

final class BigQueryHelper {
    private BigQueryHelper() {
        // no instances
    }
    
    private static String TABLE_PH = "<table>";
    private static String COLUMNS_PH = "<columns>";
    private static String VALUES_PH = "( <values> )";
    public static final String INSERT_QUERY = "INSERT INTO " + TABLE_PH + " ( " + COLUMNS_PH + " ) VALUES " + VALUES_PH;
    
    static String getInsertQuery(String fullTableName, String primaryKey, List<DatabaseField> fields) {
        return getInsertQuery(fullTableName, primaryKey, fields, 1);
    }

    static String getInsertQuery(String fullTableName, String primaryKeyName, List<DatabaseField> fields, int rows) {
        // String columnsString = BigQueryFilterBuilder.buildTypedInsertColumnString(fields);
        // String placeholdersString = BigQueryFilterBuilder.buildTypedPlaceholderInsertString(fields);
        String columnsString = String.join(", ", BigQueryClient.databaseSchema);
        String[] placeholders = new String[BigQueryClient.databaseSchema.length];
        Arrays.fill(placeholders, "?");
        String placeholdersString = BigQueryClient.databaseSchema.length == 0
            ? ""
            : ", " + String.join(", ", placeholders);
        StringBuilder parameters = new StringBuilder();
        for(int i = 0; i < rows; i++) {
            parameters.append(i == 0 ? " ( ?" : ", ( ?")
            .append(new String(placeholdersString))
            .append(") \n");
        }
        // TODO: add caching
        return INSERT_QUERY.replace(TABLE_PH, fullTableName)
            .replace(COLUMNS_PH, primaryKeyName + (fields.size() > 0 ? (", " + columnsString) : ""))
            .replace(VALUES_PH, parameters.toString());
    }

    private static final String PK_PH = "<primary_key>";
    public static final String DELETE_QUERY = "DELETE FROM " + TABLE_PH + " WHERE " + PK_PH + " = ? ";
    static String getDeleteQuery(String fullTableName, String primaryKeyName) {
        return DELETE_QUERY.replace(TABLE_PH, fullTableName).replace(PK_PH, primaryKeyName);
    }

    public static final String READ_QUERY = "SELECT " + COLUMNS_PH + " FROM " + TABLE_PH + " WHERE " + PK_PH + " = ? ";
    static String getReadQuery(String fullTableName, String primaryKeyName, String[] fields) {
        String fieldString = fields.length > 0 
            ? primaryKeyName + ", " + String.join(",", fields)
            : " * ";
        return READ_QUERY.replace(TABLE_PH, fullTableName)
            .replace(COLUMNS_PH, fieldString)
            .replace(PK_PH, primaryKeyName);
    }

    public static final String UPDATE_QUERY = "UPDATE " + TABLE_PH + " SET " + COLUMNS_PH + " WHERE " + PK_PH + " = ? ";
    static String getUpdateQuery(String fullTableName, String primaryKeyName, String[] fieldNames) {
        StringBuilder parameters = new StringBuilder();
        for(int i = 0; i < fieldNames.length; i++) {
            if(i != 0) {
                parameters.append(" , ");    
            }
            parameters.append(fieldNames[i]).append(" = ? ");
        }
        return UPDATE_QUERY.replace(TABLE_PH, fullTableName)
            .replace(COLUMNS_PH, parameters.toString())
            .replace(PK_PH, primaryKeyName);
    }

    private static final String FILTERS_PH = "<filters>";
    public static final String FINDONE_QUERY = "SELECT * FROM " + TABLE_PH + " WHERE " + FILTERS_PH + " LIMIT 1";
    static String getFindOneQuery(String fullTableName, List<Comparison> filters) {
        String filterString = BigQueryFilterBuilder.buildConcatenatedPlaceholderFilter(filters);
        return FINDONE_QUERY.replace(TABLE_PH, fullTableName)
            .replace(FILTERS_PH, filterString);
    }

    public static final String UPDATEONE_QUERY = "UPDATE " + TABLE_PH + " SET " + COLUMNS_PH + 
        " WHERE " + PK_PH + " IN (SELECT " + PK_PH + " FROM " + TABLE_PH + " WHERE " + FILTERS_PH +
         " LIMIT 1)";
    static String getUpdateOneQuery(String fullTableName, String primaryKeyName, List<Comparison> filters, List<DatabaseField> fields) {
        String filterString = BigQueryFilterBuilder.buildConcatenatedPlaceholderFilter(filters);
        String setString = BigQueryFilterBuilder.buildTypedPlaceholderSetString(fields);
        return UPDATEONE_QUERY.replaceAll(TABLE_PH, fullTableName)
            .replace(FILTERS_PH, filterString)
            .replaceAll(PK_PH, primaryKeyName)
            .replace(COLUMNS_PH, setString);
    }

    public static final int AGGREAGTE_QUERY_LIMIT = 10;
    static String buildAggregatePlaceholderQuery(String fullTableName) {

        return "SELECT src_airport, dst_airport, count(*) as nmb, avg(stops) as avg_stops FROM " + fullTableName + " " +
                " WHERE codeshares_0 IS NOT NULL " +
                " AND (src_airport IN ( ?, ?, ? ) OR dst_airport IN ( ?, ?, ? ) ) " +
                " GROUP BY src_airport, dst_airport " + 
                " HAVING COUNT(*) > ? " + 
                " ORDER BY avg_stops DESC LIMIT " + AGGREAGTE_QUERY_LIMIT;
    }

    static void drainSingleElementResult(TableResult tResults, Map<String, ByteIterator> result) {
        FieldList schema = tResults.getSchema().getFields();
        drainRow(schema, tResults.getValues().iterator().next(), result);
    }
    
    static void drainMultiElementResult(TableResult tResults, Vector<HashMap<String, ByteIterator>> results) {
        Iterator<FieldValueList> it = tResults.iterateAll().iterator();
        FieldList schema = tResults.getSchema().getFields();
        while(it.hasNext()) {
            HashMap<String,ByteIterator> tmp = new HashMap<>();
            FieldValueList row = it.next();
            drainRow(schema, row, tmp);
            results.add(tmp);
        }
    }

    private static void drainRow(FieldList fields, FieldValueList row, Map<String, ByteIterator> result) {
        Iterator<Field> it = fields.iterator();
        while(it.hasNext()) {
            Field fd = it.next();
            FieldValue value = row.get(fd.getName());
            LegacySQLTypeName type = fd.getType();
            if(value == null || value.isNull()) {
                // if(BigQueryClient.debug) System.err.println("value is null for field '" + fd + "' in row " + row);
                // this just happens when the cell is null, for instance for codeshares_0 etc.
                result.put(fd.getName(), null);
            } else if(LegacySQLTypeName.STRING.equals(type)) {
                result.put(fd.getName(), new StringByteIterator(value.getStringValue()));
            } else if(LegacySQLTypeName.INTEGER.equals(type)) {
                result.put(fd.getName(), new NumericByteIterator(value.getLongValue()));
            } else if(LegacySQLTypeName.FLOAT.equals(type)) {
                result.put(fd.getName(), new NumericByteIterator(value.getDoubleValue()));
            } else {
                throw new IllegalArgumentException("unknown type: " + fd.getType());
            }
        }
    }
}