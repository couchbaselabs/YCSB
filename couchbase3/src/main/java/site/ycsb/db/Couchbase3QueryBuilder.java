/**
 * Copyright (c) 2019 Yahoo! Inc. All rights reserved.
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
package site.ycsb.db;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.json.JsonArray;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

final class Couchbase3QueryBuilder {

    public static final int AGGREAGTE_QUERY_LIMIT = 10;

    static String bindCreateCollectionWithSchemaQuery(String bucketName, String scopeName, String collectionName) {
        String command = "CREATE COLLECTION `" + bucketName + "`.`" + scopeName + "`.`"
            + collectionName + "`";
        return command;
    }

    static void bindUpdateOneQuery(JsonArray params, List<DatabaseField> fields, List<Comparison> filters) {
        bindSetPortion(params, fields);
        FilterBuilder.bindFilters(params, filters);
    }

    static void bindAggregatePlaceholderQuery(JsonArray params, String keyspaceName, String[] airports, int minOccurrences) {
        for(String s : airports) {
            params.add(s);
        }
        for(String s : airports) {
            params.add(s);
        }
        params.add(minOccurrences);
    }

    static String buildAggregatePlaceholderQuery(String keyspaceName) {

        return "SELECT src_airport, dst_airport, count(*) as nmb, avg(stops) as avg_stops FROM " + keyspaceName + " " +
                " WHERE ARRAY_COUNT( codeshares ) > 0 " +
                " AND (src_airport IN [ ?, ?, ? ] OR dst_airport IN [ ?, ?, ? ] ) " +
                " GROUP BY src_airport, dst_airport " + 
                " HAVING COUNT(*) > ? " + 
                " ORDER BY avg_stops DESC LIMIT " + AGGREAGTE_QUERY_LIMIT;
    }

    static String buildUpdateOnePlaceholderQuery(String keyspaceName, List<Comparison> filters,
        List<DatabaseField> fields) {
        String setPortion = buildPlaceholderTypedSetPortion(fields);
        String filterPortion = FilterBuilder.buildConcatenatedPlaceholderFilter(filters);
        String filter = " WHERE " + filterPortion + " LIMIT 1";
        String query = "UPDATE " + keyspaceName + " AS d SET " 
            + setPortion + " " + filter + " RETURNING d";
        return query;
    }

    static String buildFindOnePlaceholderQuery(String keyspaceName, List<Comparison> filters) {
        String filterPortion = FilterBuilder.buildConcatenatedPlaceholderFilter(filters);
        String query = "SELECT * FROM " + keyspaceName + " WHERE " + filterPortion + " LIMIT 1";
        return query;
    }

    static void bindFindOneQuery(JsonArray params, List<Comparison> filters) {
        FilterBuilder.bindFilters(params, filters);
    }

    private static String buildPlaceholderTypedSetPortion(List<DatabaseField> fields) {
        return innerBuildTypedSetPortion(fields, true);
    }

    private static String buildTypedSetPortion(List<DatabaseField> fields) {
        return innerBuildTypedSetPortion(fields, false);
    }

    private static String innerBuildTypedSetPortion(List<DatabaseField> fields, boolean placeholder) {
        List<String> parts = new ArrayList<>(fields.size());
        for(DatabaseField field : fields){
            buildTypedSetString("", field, parts, placeholder);
        }
        return String.join(", ", parts);
    }

    private static void bindSetPortion(JsonArray params, List<DatabaseField> fields) {
        for(DatabaseField field : fields){
            DataWrapper wrapper = field.getContent();
            if(wrapper.isNested()) {
                List<DatabaseField> innerFields = wrapper.asNested();
                bindSetPortion(params, innerFields);
            } else if(wrapper.isTerminal()) {
                if(wrapper.isInteger()) {
                    params.add(wrapper.asInteger());
                } else if (wrapper.isLong()) {
                    params.add(wrapper.asLong());
                } else if(wrapper.isString()) {
                    params.add(wrapper.asString());
                }  else {
                    params.add(new String(wrapper.asIterator().toArray()));
                }
            } else if(wrapper.isArray()) {
                throw new IllegalArgumentException("setting arrays or array content is currently not supported");
            } else {
                throw new IllegalStateException("neither terminal, nor array, nor nested");
            }
        }
    }

    private static void buildTypedSetString(String prefix, DatabaseField field,
        List<String> parts, boolean placeholder) {
        DataWrapper wrapper = field.getContent();
        if(wrapper.isTerminal()) {
            String value = "";
            if(wrapper.isInteger() || wrapper.isLong()) {
                value = placeholder ? " ? " : wrapper.asString();
            } else if(wrapper.isString()) {
                value = placeholder ? " ? " : ("\"" + wrapper.asString() + "\"");
            }  else {
                // value = "\"" + encoder.encodeToString(wrapper.asIterator().toArray()) + "\"";
                value = placeholder ? " ? " : "\"" + new String(wrapper.asIterator().toArray()) + "\"";
            }
            parts.add(prefix + field.getFieldname() + " = " + value);
        } else if(wrapper.isNested()) {
            String fieldPrefix = prefix + field.getFieldname() + ".";
            List<DatabaseField> innerFields = wrapper.asNested();
            for(DatabaseField iF : innerFields) {
                buildTypedSetString(fieldPrefix, iF, parts, placeholder);
            }
        } else if(wrapper.isArray()) {
            throw new IllegalArgumentException("setting arrays or array content is currently not supported");
        } else {
            throw new IllegalStateException("neither terminal, nor array, nor nested");
        }
    }
    private Couchbase3QueryBuilder() {

    }
}
