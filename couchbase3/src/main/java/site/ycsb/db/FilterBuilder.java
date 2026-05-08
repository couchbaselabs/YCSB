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
package site.ycsb.db;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.json.JsonArray;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;

public final class FilterBuilder {
    
    static void bindFilters(JsonArray params, List<Comparison> filters) {
        for(Comparison c : filters) {
            Comparison d = c;
            while(d.isSimpleNesting()) {
                d = d.getSimpleNesting();
            }
            if(d.comparesStrings()) {
                params.add(d.getOperandAsString());
            } else if(d.comparesInts()) {
                params.add(d.getOperandAsInt());
            } else {
                throw new IllegalStateException();
            }
        }
    }

    static String buildConcatenatedPlaceholderFilter(List<Comparison> filters) {
        return innerBuildConcatenatedFilter(filters, true);
    }

    static String buildConcatenatedFilter(List<Comparison> filters) {
        return innerBuildConcatenatedFilter(filters, false);
    }

    private static String innerBuildConcatenatedFilter(List<Comparison> filters, boolean placeholder) {
        // Bson[] bFilters = new Bson[filters.size()]; Arrays.asList(bFilters);
        List<String> lFilters = new ArrayList<>(filters.size());
        for(Comparison c : filters) {
            Comparison d = c;
            String fieldName = d.getFieldname();
            while(d.isSimpleNesting()) {
                d = d.getSimpleNesting();
                fieldName = fieldName + "." + d.getFieldname();
            }
            if(d.comparesStrings()) {
                lFilters.add(
                    FilterBuilder.buildStringFilter(
                        fieldName,
                        d.getOperator(),
                        placeholder ? null : d.getOperandAsString()
                ));
            } else if(d.comparesInts()) {
                lFilters.add(
                    FilterBuilder.buildIntFilter(
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

        public static String buildStringFilter(String fieldName, ComparisonOperator op, String value) {
        String operand  = value == null ? " ? " : " \"" + value + "\" ";
        switch (op) {
            case STRING_EQUAL:
                return fieldName + " LIKE " + operand;
            default:
                throw new IllegalArgumentException("no string operator");
        }
    }

    public static String buildIntFilter(String fieldName, ComparisonOperator op, Integer value) {
        String operand = value == null ? " ? " : value.toString();
        switch (op) {
            case INT_LTE:
                return fieldName + " <= " + operand;
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
    private  FilterBuilder() {}
}
