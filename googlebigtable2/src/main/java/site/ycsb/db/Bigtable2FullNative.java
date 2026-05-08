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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Vector;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.ChainFilter;
import com.google.cloud.bigtable.data.v2.models.Filters.ConditionFilter;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;

import site.ycsb.ByteIterator;
import site.ycsb.NumericByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;

import static site.ycsb.db.GoogleBigtable2Client.columnFamily;
import static site.ycsb.db.GoogleBigtable2Client.client;

public class Bigtable2FullNative {

    static String updateOne(String table, List<Comparison> filters) throws Exception {
        if(filters == null || filters.size() == 0) {
            throw new NullPointerException();
        }
        ChainFilter base = filterChainFromComparisons(filters);
        Query q = Query.create(table).filter(base).limit(1);
        Vector<HashMap<String, ByteIterator>> results = new Vector<>();
        Status retured = runQuery(q, results);
        if(results.size() > 1) {
            if(GoogleBigtable2Client.debug) {
                System.err.println("findOne: too many results");
            }
            throw new Exception("findOne: too many results");
        }
        if(results.size() == 0) {
            if(retured != Status.ERROR)
                return null;
            throw new Exception("Exception when running the query");
        }
        return results.get(0).get("_key").toString();
    }

    static Status findOne(String table, List<Comparison> filters, Set<String> fields, Map<String, ByteIterator> result) {
        if(filters == null || filters.size() == 0) {
            throw new NullPointerException();
        }
        if(fields != null) {
           throw new UnsupportedOperationException("cannot read results by field");
        }

        ChainFilter base = filterChainFromComparisons(filters);
        Query q = Query.create(table).filter(base).limit(1);
        Vector<HashMap<String, ByteIterator>> results = new Vector<>();
        Status retured = runQuery(q, results);
        if(results.size() > 1) {
            if(GoogleBigtable2Client.debug) {
                System.err.println("findOne: too many results");
            }
            return Status.UNEXPECTED_STATE;
        }
        if(results.size() == 1) {
            result.putAll(results.firstElement());
            if(GoogleBigtable2Client.debug) {
                System.err.println("findOne: found: " + result);
            }
        } else {
            if(GoogleBigtable2Client.debug) {
                System.err.println("findOne: nothing found or exception");
            }
        }       
        return retured;
    }

    /*
    SELECT YCSB['src_airport'], YCSB"['dst_airport'], count(*) as nmb, avg(TO_INT64(YCSB['stops'])) as avg_stops 
    FROM usertable 
    WHERE ( YCSB['codeshares_0'] IS NOT NULL " AND YCSB['codeshares_0'] NOT LIKE '')
	AND ( YCSB['src_airport'] IN ( ?, ?, ?)  OR YCSB['dst_airport'] IN ( ?, ?, ? ) ) ) 
    GROUP BY YCSB['src_airport'], YCSB['dst_airport'] HAVING nmb > ? ORDER BY avg_stops DESC LIMIT 10 */
    static Status aggregate(String table, String[] airports, int minOccurrences, Vector<HashMap<String, ByteIterator>> results) {
        // filter for right column family and only latest cell per entry
        Filters.ChainFilter base = buildBaseFilter();
        // Filters.Filter onlyMatchingColumns = FILTERS.qualifier().regex("src_airport|dst_airport|stops|codeshares_0");
        Filters.Filter onlyMatchingColumns = FILTERS.qualifier().regex("src_airport|dst_airport|stops");
        Filters.Filter airport = buildAirportFilter(".*_airport", airports);
        
        ConditionFilter secondLevel = FILTERS.condition(airport).then(FILTERS.pass());
        ConditionFilter firstLevel = FILTERS.condition(
            FILTERS.chain()
                .filter(FILTERS.qualifier().exactMatch("codeshares_0"))
                // .filter(FILTERS.value().regex(".*")) 
        ).then(FILTERS.pass());

        base.filter(firstLevel).filter(onlyMatchingColumns).filter(secondLevel);
        // base.filter(onlyMatchingColumns).filter(secondLevel);
        // base.filter(secondLevel);
        
        // we cannot use .limit() as the spec requires the limit on the
        // aggregated results, not on the sources
        Query q = Query.create(table).filter(base);
        // System.err.println("aggregate using filter: " + q);
        Status returned = doAggregate(q, minOccurrences, results);
        if(GoogleBigtable2Client.debug) {
            System.err.println("aggregate results for minOccurrences > " + minOccurrences + ": " + results);
        }
        return returned;
    }

    static Status doAggregate(Query query, int minOccurrences, Vector<HashMap<String, ByteIterator>> results) {
        // System.err.println("running query" + query);
        final PriorityQueue<AggregationStats> myQueue;
        final HashMap<String,AggregationStats> rawResults = new HashMap<>();
        try {
            ServerStream<Row> rows = client.readRows(query);
            doAggregateByRow(rows, rawResults);
            if(rawResults.size() == 0) {
                return Status.NOT_FOUND;
            }
            myQueue = new PriorityQueue<>(rawResults.size(), new Comparator<AggregationStats>() {
                @Override
                public int compare(AggregationStats a1, AggregationStats a2) {
                    double av1 = a1.computeAvg();
                    double av2 = a2.computeAvg();
                    if(av1 < av2) return 1;
                    if(av2 < av1) return -1;
                    return 0;
                }
            });
            myQueue.addAll(rawResults.values());
            AggregationStats current;
            while((current = myQueue.poll()) != null && results.size() < Bigtable2Helper.AGGREAGTE_QUERY_LIMIT) {
                if(current.count > minOccurrences) {
                    HashMap<String, ByteIterator> actuHashMap = new HashMap<>();
                    actuHashMap.put("src_airport", new StringByteIterator(current.sAirport));
                    actuHashMap.put("dst_airport", new StringByteIterator(current.dAirport));
                    actuHashMap.put("nmb", new NumericByteIterator(current.count));
                    actuHashMap.put("avg_stops", new NumericByteIterator(current.computeAvg()));
                    results.add(actuHashMap);
                }
            }
            if(results.size() == 0) return Status.NOT_FOUND;
            return Status.OK;
        } catch (Exception e) {
            if (GoogleBigtable2Client.debug) {
                e.printStackTrace();
            }
            return Status.ERROR;
        }
    }

    static void doAggregateByRow(ServerStream<Row> rows, HashMap<String,AggregationStats> rawResults) {
        for (Row row : rows) {
            // System.err.println("\t reading row: " + row);
            AggregationStats eStats = readAggregationStatsFromRow(row);
            AggregationStats cStats = rawResults.get(eStats.getGroupByKey());
            if(cStats == null) {
                rawResults.put(eStats.getGroupByKey(), eStats);
            } else {
                cStats.count = cStats.count + 1;
                cStats.stops = cStats.stops + eStats.stops;
            }
        }
    }

    static AggregationStats readAggregationStatsFromRow(Row row) {
        AggregationStats stats = new AggregationStats();
        boolean hasCodeshares = false;
        for (RowCell c : row.getCells()) {
            switch(c.getQualifier().toStringUtf8()){
                case "src_airport": {
                    stats.sAirport = c.getValue().toStringUtf8();
                    break;
                }
                case "dst_airport": {
                    stats.dAirport = c.getValue().toStringUtf8();
                    break;
                }
                case "stops": {
                    stats.stops = Longs.fromByteArray(c.getValue().toByteArray());
                    break;
                }
                case "codeshares_0":{
                    hasCodeshares = true;
                }
                default:
                    continue;
            }
        }
        /*if(!hasCodeshares) {
            System.err.println("found row without codeshares_0: " + row);
        }*/
        if(stats.allSet()) return stats;
        if(GoogleBigtable2Client.debug) {
            if(stats.dAirport == null) { System.err.println("dAirport not set. Ignoring this entry."); }
            if(stats.sAirport == null) { System.err.println("sAirport not set. Ignoring this entry."); }
            if(stats.stops == null) { System.err.println("stops not set. Ignoring this entry."); }
        }
        return null;
    }

    static class AggregationStats {
        String sAirport = null;
        String dAirport = null;
        Long stops = null;
        long count = 1;
        Double avg_stops = null;
        boolean allSet() {
            return sAirport != null && dAirport != null && stops != null;
        }
        String getGroupByKey() {
            return "#" + sAirport + "#" + dAirport;
        }
        double computeAvg() {
            if(avg_stops == null) {
                avg_stops = ((double) stops) / ((double) count);
            }
            return avg_stops;
        }
    }

    static Status runQuery(Query q, Vector<HashMap<String, ByteIterator>> results) {
        try {
            ServerStream<Row> rows = client.readRows(q);
            boolean hasResults = false;
            for (Row row : rows) {
                hasResults = true;
                HashMap<String, ByteIterator> result = new HashMap<>();
                GoogleBigtable2Client.rowToMap(row, result);
                results.add(result);
            }
            return hasResults ? Status.OK : Status.NOT_FOUND;
        } catch (Exception e) {
            if (GoogleBigtable2Client.debug) {
                e.printStackTrace();
            }
            return Status.ERROR;
        }
    }

    private Bigtable2FullNative() {
        //  
    }

    static Filters.ChainFilter buildAirportFilter(String regex, String[] airportsToFilter) {
        Filters.InterleaveFilter interleave = FILTERS.interleave();
        for(String ap : airportsToFilter) {
            interleave.filter(
                FILTERS.value().exactMatch(ap)
            );
        }
        return FILTERS.chain()
            .filter(FILTERS.qualifier().regex(regex))
            .filter(interleave); 
    }

    static Filters.ChainFilter filterChainFromComparisons(List<Comparison> filters) {
        Filters.Filter last = FILTERS.pass();
        for(int i = filters.size() - 1; i != 0; i--) {
            Comparison c = filters.get(i);
            ConditionFilter cond = FILTERS.condition(
                comparisonToFilter(c)
            ).then(last);
            last = cond;
        }
        // InterleaveFilter ilf = FILTERS.interleave().filter(buildFilterForValueInColum("dst_airport", "SXB"));
        return buildBaseFilter().filter(last);
    }

    static Filters.Filter comparisonToFilter(Comparison d) {
        String fieldName = d.getFieldname();
        if(d.comparesStrings()) {
            // fieldName = familyName + ":" + fieldName;
            return buildStringFilter(fieldName, d.getOperator(), d.getOperandAsString());
        } else if(d.comparesInts()) {
            return buildIntFilter(fieldName, d.getOperator(), d.getOperandAsInt());
        } else {
            throw new IllegalStateException("" + d);
        }
    }

    public static Filters.Filter buildStringFilter(String fieldName, ComparisonOperator op, String value) {
        switch (op) {
            case STRING_EQUAL:
                return buildFilterForValueInColum(fieldName, value);
            default:
                throw new IllegalArgumentException("no string operator");
        }
    }

    public static Filters.Filter buildIntFilter(String fieldName, ComparisonOperator op, int value) {
        switch (op) {
            case INT_LTE:
                return FILTERS.value().range()
                    .startClosed(ByteString.copyFrom(Longs.toByteArray(0)))
                    .endClosed(ByteString.copyFrom(Longs.toByteArray(value + 1)));
            default:
                throw new IllegalArgumentException("no string operator");
        }
    }

    private static ChainFilter buildFilterForValueInColum(String column, String value) {
        return FILTERS.chain()
            .filter(FILTERS.qualifier().exactMatch(column))
            .filter(FILTERS.value().exactMatch(value)); 
    }

    private static ChainFilter buildBaseFilter() {
        ChainFilter chain = FILTERS.chain()
            .filter(FILTERS.family().exactMatch(columnFamily))
            .filter(FILTERS.limit().cellsPerColumn(1));

        return chain;
  }
}

/*

SELECT * FROM usertable WHERE YCSB['src_airport'] LIKE  "APF" AND YCSB['dst_airport'] LIKE  "SXB" AND TO_INT64(YCSB['stops'] ) <= 2 LIMIT 1
FindOne result: {field1=>@;56*'Sq#A9.Dc?/,.1t&>p!*t#6v#L{+(f6)r*[s90. La)Qk'I=&+0-@! [o;R1'-(5!x"K{:S7$&,)\9/,z'Su,C#!&<:Jm*, airplane=B37M, dst_airport=SXB, src_airport=APF, stops=2, _key=user775, airline_alias=8P, airline_name=Pacific Coastal Airlines}

*/
