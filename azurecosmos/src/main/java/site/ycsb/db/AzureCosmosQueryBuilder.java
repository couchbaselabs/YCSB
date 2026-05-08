/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
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

import java.util.List;

import com.azure.cosmos.models.SqlParameter;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;
import site.ycsb.wrappers.DatabaseField;

final class AzureCosmosQueryBuilder {

  public static final int AGGREGATE_QUERY_LIMIT = 10;

  static String buildFindOnePlaceholderQuery(List<Comparison> filters) {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM c WHERE ");
    
    for (int i = 0; i < filters.size(); i++) {
      if (i > 0) {
        sb.append(" AND ");
      }
      Comparison comp = filters.get(i);
      String fieldName = comp.getFieldname();
      while(comp.isSimpleNesting()) {
        comp = comp.getSimpleNesting();
        fieldName = fieldName + "." + comp.getFieldname();
      }
      sb.append("c.").append(fieldName).append(" ");
      sb.append(buildComparisonOperator(comp));
      sb.append(" @param").append(i);
    }
    sb.append(" OFFSET 0 LIMIT 1 ");
    return sb.toString();
  }

  static void bindFindOneQuery(List<SqlParameter> params, List<Comparison> filters) {
    for (int i = 0; i < filters.size(); i++) {
      Comparison comp = filters.get(i);
      while(comp.isSimpleNesting()) {
        comp = comp.getSimpleNesting();
      }
      if(comp.comparesInts()) {
        params.add(new SqlParameter("@param" + i, comp.getOperandAsInt()));
      } else if (comp.comparesStrings()) {
        params.add(new SqlParameter("@param" + i, comp.getOperandAsString()));
      } else {
        throw new IllegalArgumentException("Unsupported comparison operand type for filter: " + comp);
      }
    }
  }

  static String buildUpdateOnePlaceholderQuery(List<Comparison> filters, List<DatabaseField> fields) {
    return buildFindOnePlaceholderQuery(filters);
  }

  static void bindUpdateOneQuery(List<SqlParameter> params, List<DatabaseField> fields, List<Comparison> filters) {
    bindFindOneQuery(params, filters);
    /*
    // Bind field values
    for (DatabaseField field : fields) {
      params.add(new SqlParameter("@field" + paramIndex, field.getValue()));
      paramIndex++;
    }
    */
  }

  static final String AVG_STOPS_FIELD = "avg_stops";
  static String buildAggregatePlaceholderQuery() {
    return "SELECT * FROM ( " +
                "SELECT c.src_airport, c.dst_airport, avg(c.stops) as " + AVG_STOPS_FIELD + ", count(1) as nmb " + 
                " FROM c " +
                " WHERE (c.src_airport IN (@airport1, @airport2, @airport3) OR c.dst_airport IN (@airport1, @airport2, @airport3)) " +
                " AND ARRAY_LENGTH(c.codeshares) > 0 " + 
                " GROUP BY c.src_airport, c.dst_airport" +
                " )  as r WHERE r.nmb > @minOccurrences "; // WHERE nmb > 1"; //sWHERE nmb > 0"; // ORDER BY avg_stops DESC"; //  LIMIT " + AGGREGATE_QUERY_LIMIT;
  }

  static void bindAggregatePlaceholderQuery(List<SqlParameter> params, String[] airports, int minOccurrences) {
    params.add(new SqlParameter("@airport1", airports[0]));
    params.add(new SqlParameter("@airport2", airports[1]));
    params.add(new SqlParameter("@airport3", airports[2])); 
    params.add(new SqlParameter("@minOccurrences", minOccurrences)); 
  }

  private static String buildComparisonOperator(Comparison comp) {
    ComparisonOperator op = comp.getOperator();
    if(op == null) {
      throw new IllegalArgumentException("Comparison operator cannot be null for filter: " + comp);
    }
    if(comp.comparesInts()) {
        switch (op) {
            case INT_LTE:
                return " <= ";        
            default:
                throw new IllegalArgumentException("Unsupported comparison operator for integer filter: " + op);
        }
    } else if (comp.comparesStrings()) {
        switch (op) {
            case STRING_EQUAL:
                return " = ";
            default:
                throw new IllegalArgumentException("Unsupported comparison operator for string filter: " + op);
        }
    } else {
      throw new IllegalArgumentException("Unsupported comparison operand type for filter: " + comp);
    }
  }

  private AzureCosmosQueryBuilder() {
  }
}