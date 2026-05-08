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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;

import site.ycsb.ByteIterator;
import site.ycsb.NumericByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

final class GoogleFirestoreQueryHelper {

    static void populateTypedFields(Map<String, Object> data, List<DatabaseField> fields) {
        for (DatabaseField field : fields) {
            String fieldName = field.getFieldname();
            DataWrapper content = field.getContent();
            if(content.isNested()) {
                Map<String, Object> nestedData = new HashMap<>();
                // Recursively populate nested fields
                populateTypedFields(nestedData, content.asNested());
                data.put(fieldName, nestedData);
            } else if (content.isInteger()) {
                data.put(fieldName, content.asInteger());
            } else if (content.isLong()) {
                data.put(fieldName, content.asLong());
            } else if (content.isArray()) {
                data.put(fieldName, content.asObject());
            } else {
                // we use string as a fallback for all other types (including string)
                data.put(fieldName, content.asString());
                // throw new IllegalArgumentException("Unsupported field type for field: " + field + " with content: " + content);
            }
        }
    }

    static Query buildAndBindFindOneQuery(CollectionReference collection, List<Comparison> filters) {
        Query currentQuery = collection;
        for(Comparison d : filters) {
            Comparison c = d;
            String fieldName = c.getFieldname();
            while(c.isSimpleNesting()) {
                c = c.getSimpleNesting();
                fieldName += "." + c.getFieldname();
            }
            if(c.comparesInts()) {
                switch (c.getOperator()) {
                    case INT_LTE:
                        currentQuery = currentQuery.whereLessThanOrEqualTo(fieldName, c.getOperandAsInt());
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported operator for integer comparison: " + c.getOperator());
                }
            } else if(c.comparesStrings()) {
                switch (c.getOperator()) {
                    case STRING_EQUAL:
                        currentQuery = currentQuery.whereEqualTo(fieldName, c.getOperandAsString());
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported operator for string comparison: " + c.getOperator());
                }
            } else {
                throw new IllegalArgumentException("Unsupported comparison type for field: " + fieldName + " with comparison: " + c);
            }
        }
        return currentQuery.limit(1);
    }

    static class AggregateResult {
        String srcAirport;
        String dstAirport;
        int count;
        long sumStops;
        double avgStops;
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

    static List<AggregateResult> postprocessAggregateResults(List<QueryDocumentSnapshot> documents, int minOccurrences, boolean debug) {
        // Group documents by src_airport,dst_airport and collect stops values
        Map<String, AggregateResult> groupedStops = new HashMap<>();
        for (QueryDocumentSnapshot doc : documents) {
            String srcAirport = doc.getString("src_airport");
            String dstAirport = doc.getString("dst_airport");
            Long stops = doc.getLong("stops");
            if (srcAirport != null && dstAirport != null && stops != null) {
                String key = srcAirport + "," + dstAirport;
                AggregateResult result = groupedStops.get(key);
                if(result == null) {
                    result = new AggregateResult();
                    result.srcAirport = srcAirport;
                    result.dstAirport = dstAirport;
                    result.count = 1;
                    result.sumStops = stops;
                    groupedStops.put(key, result);
                } else {
                    result.count++;
                    result.sumStops += stops;
                }
            }
        }
        // Compute aggregations and filter by minOccurrences
        List<AggregateResult> aggregatedResults = new ArrayList<>();
        for (Map.Entry<String, AggregateResult> entry : groupedStops.entrySet()) {
            AggregateResult result = entry.getValue();
            if (result.count > minOccurrences) {
                if(debug) {
                    System.out.println("Aggregate intermediate result for: count=" + result.count + ", minOccurrences=" + minOccurrences);
                }
                result.avgStops = (double) result.sumStops / result.count;
                aggregatedResults.add(result);
            } else {
                if(debug) {
                    System.out.println("Aggregate intermediate result for: count=" + result.count + " did not meet minOccurrences=" + minOccurrences);
                }
            }
        }
        // Sort by avg_stops in descending order and limit results
        aggregatedResults.sort((r1, r2) -> Double.compare(r2.avgStops, r1.avgStops));
        if(aggregatedResults.size() > GoogleFirestoreClient.AGGREGATE_QUERY_LIMIT) {
            aggregatedResults = aggregatedResults.subList(0, GoogleFirestoreClient.AGGREGATE_QUERY_LIMIT);
        }
        return aggregatedResults;
    }
    
    private GoogleFirestoreQueryHelper() {
        // prevent instantiation
    }
}
