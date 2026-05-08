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

import java.util.ArrayList;
import java.util.List;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;

import static site.ycsb.db.GoogleBigtable2Client.columnFamily;

public final class Bigtable2Helper {

  public static final int AGGREAGTE_QUERY_LIMIT = 10;
  
  private final static String AGGREGATE_0 = "SELECT " + columnFamily + "['src_airport'], " +
                                  columnFamily + "['dst_airport'], count(*) as nmb, " + 
                                  "avg(TO_INT64(" + columnFamily + "['stops'])) as avg_stops FROM ";
  private final static String AGGREGATE_1 = " WHERE ( " + columnFamily + "['codeshares_0'] IS NOT NULL " + 
                                            " AND " + columnFamily + "['codeshares_0'] NOT LIKE '') " +
                                            " AND ( " + columnFamily + "['src_airport'] IN ( '";
  private final static String AGGREGATE_2 = "' ) OR " + columnFamily + "['dst_airport'] IN ( '";
  private final static String AGGREGATE_3 = "' ) ) GROUP BY " + columnFamily + "['src_airport'], " +
                                            columnFamily + "['dst_airport'] HAVING nmb > "; 
  private final static String AGGREGATE_4 = " ORDER BY avg_stops DESC LIMIT " + AGGREAGTE_QUERY_LIMIT;

  static String buildAggregateQueryString(String tableName, String[] airports, int minOccurrences) {
    String airportList = String.join("', '", airports);
    return new StringBuilder(AGGREGATE_0)
      .append(tableName).append(AGGREGATE_1).append(airportList)
      .append(AGGREGATE_2).append(airportList).append(AGGREGATE_3)
      .append(minOccurrences).append(AGGREGATE_4)
      .toString();
  }

    static String buildConcatenatedSQLFilter(String familyName, List<Comparison> filters, boolean placeholder) {
        List<String> lFilters = new ArrayList<>(filters.size());
        for(Comparison c : filters) {
            Comparison d = c;
            String fieldName = d.getFieldname();
            /* while(d.isSimpleNesting()) {
                d = d.getSimpleNesting();
                fieldName = fieldName + "." + d.getFieldname();
            } */
            if(d.comparesStrings()) {
                lFilters.add(
                    buildStringSQLFilter(
                        familyName,
                        fieldName,
                        d.getOperator(),
                        placeholder ? null : d.getOperandAsString()
                ));
            } else if(d.comparesInts()) {
                lFilters.add(
                    buildIntFilter(
                        familyName,
                        fieldName,
                        d.getOperator(),
                        placeholder ? null : d.getOperandAsInt()
                ));
            } else {
                throw new IllegalStateException();
            }
        }
        return and(lFilters);
    }

    public static String buildStringSQLFilter(String familyName, String fieldName, ComparisonOperator op, String value) {
        String operand  = value == null ? " ? " : " \"" + value + "\" ";
        switch (op) {
            case STRING_EQUAL:
                return familyName + "['" + fieldName + "'] LIKE " + operand;
            default:
                throw new IllegalArgumentException("no string operator");
        }
    }

    public static String buildIntFilter(String familyName, String fieldName, ComparisonOperator op, Integer value) {
        String operand = value == null ? " ? " : value.toString();
        switch (op) {
            case INT_LTE:
                return "TO_INT64(" + familyName + "['" + fieldName + "'] ) <= " + operand;
            default:
                throw new IllegalArgumentException("no int operator");
        }
    }

    public static String and(List<String> args){
        String result = args.get(0);
        for(int i = 1; i < args.size(); i++) {
            result = result + " AND " + args.get(i);
        }
        return result;
    }

  /**
   * Find a single row in `table` where `YCSB.src_airport` LIKE `srcLike` AND
   * `YCSB.dst_airport` LIKE `dstLike` AND `YCSB.stops` <= `maxStops`.
   *
   * This translates the SQL WHERE into a Bigtable scan using cell filters that
   * pre-select candidate rows and then verifies on the client-side that all
   * three predicates are satisfied. Returns the first matching row if any.

  public Optional<Row> findOneByAirportsAndStopsLike(final String table,
      final String srcLike, final String dstLike, final long maxStops) {

    final String srcRegex = sqlLikeToRegex(srcLike);
    final String dstRegex = sqlLikeToRegex(dstLike);

    Filters.Filter stopsPredicate = FILTERS.chain()
        .filter(FILTERS.qualifier().exactMatch("stops"))
        .filter(FILTERS.value()
            .range()
            .endClosed(ByteString.copyFrom(Longs.toByteArray(maxStops))));

    Filters.Filter filter = FILTERS.chain()
        .filter(FILTERS.family().exactMatch(GoogleBigtable2Client.columnFamily))
        .filter(FILTERS.interleave()
            .filter(srcPredicate)
            .filter(dstPredicate)
            .filter(stopsPredicate));

    Query q = Query.create(table).filter(filter).limit(1);

    try {
      ServerStream<Row> rows = client.readRows(q);
      for (Row row : rows) {
        boolean hasSrc = false, hasDst = false, hasStops = false;
        for (RowCell cell : row.getCells()) {
          String qual = cell.getQualifier().toStringUtf8();
          if ("src_airport".equals(qual) && cell.getValue().toStringUtf8().matches(srcRegex)) {
            hasSrc = true;
          } else if ("dst_airport".equals(qual) && cell.getValue().toStringUtf8().matches(dstRegex)) {
            hasDst = true;
          } else if ("stops".equals(qual)) {
            byte[] v = cell.getValue().toByteArray();
            if (v.length >= 8) {
              long stopsVal = Longs.fromByteArray(v);
              if (stopsVal <= maxStops) {
                hasStops = true;
              }
            } else {
              try {
                long stopsVal = Long.parseLong(cell.getValue().toStringUtf8());
                if (stopsVal <= maxStops) {
                  hasStops = true;
                }
              } catch (NumberFormatException ignored) {
              }
            }
          }
        }
        if (hasSrc && hasDst && hasStops) {
          return Optional.of(row);
        }
      }
    }

    return Optional.empty();
  }
   */
  private static String sqlLikeToRegex(final String like) {
    if (like == null || like.isEmpty()) {
      return ".*";
    }
    StringBuilder sb = new StringBuilder();
    sb.append('^');
    for (int i = 0; i < like.length(); i++) {
      char c = like.charAt(i);
      switch (c) {
        case '%':
          sb.append(".*");
          break;
        case '_':
          sb.append('.');
          break;
        default:
          // escape regex meta characters
          if ("[].(){}*+?^$|\\".indexOf(c) >= 0) {
            sb.append('\\');
          }
          sb.append(c);
      }
    }
    sb.append('$');
    return sb.toString();
  }


    private Bigtable2Helper() {
        // prevent instantiation
    }
}
