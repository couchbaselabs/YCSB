/*
 * Copyright 2023-2026 benchANT GmbH. All Rights Reserved.
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

import site.ycsb.db.DynamoDBClient.IndexDescriptor;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

public final class DynamoDBQueryHelper {
    
    private static void bindQueryString(Map<String, AttributeValue> m, Comparison c) {
        Comparison d = c;
        String fieldName = c.getFieldname();
        while(d.isSimpleNesting()) {
            d = d.getSimpleNesting();
            fieldName = fieldName + "_" + d.getFieldname();
        }
        if(d.comparesInts()) {
            m.put(":v_"+ fieldName, AttributeValue.builder().n(Integer.toString(d.getOperandAsInt())).build());
        } else if(d.comparesStrings()) {
            m.put(":v_"+ fieldName, AttributeValue.builder().s(d.getOperandAsString()).build());
        } else {
            throw new IllegalArgumentException("unknown: " + c);
        }
    }

    private static String buildQueryString(String fieldName, Comparison d) {
        if(d.isSimpleNesting()) {
            throw new IllegalStateException("no nesting expected here");
        }
        String placeholderName = fieldName.replaceAll("\\.", "_");
        if(d.comparesInts()) {
            ComparisonOperator co = d.getOperator();
            if(co == ComparisonOperator.INT_LTE) {
                return fieldName + " <= " + ":v_"+ placeholderName;
            } else {
                throw new IllegalArgumentException("unknown: " + co);
            }
        } else if(d.comparesStrings()) {
            ComparisonOperator co = d.getOperator();
            if(co == ComparisonOperator.STRING_EQUAL) {
                return fieldName + " = " + ":v_"+ placeholderName;
            } else {
                throw new IllegalArgumentException("unknown: " + co);
            }
        } else {
                throw new IllegalArgumentException("unknown: " + d);
        }
    }

    static void bindPreparedQuery(QueryRequest.Builder query, List<Comparison> filters) {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        for(Comparison c : filters) {
            bindQueryString(expressionAttributeValues, c);
        }
        query.expressionAttributeValues(expressionAttributeValues);
    }

    static void buildPreparedQuery(QueryRequest.Builder query, IndexDescriptor idx, List<Comparison> filters) {
        String filterExpression = null;
        String keyCondExpression = null;
        for(Comparison d : filters) {
            Comparison c = d;
            String fieldName = c.getFieldname();
            while(c.isSimpleNesting()) {
                c = c.getSimpleNesting();
                fieldName = fieldName + "." + c.getFieldname();
            }
            if(idx.hashKeyAttributes.contains(fieldName) || idx.sortsKeyAttributes.contains(fieldName)) {
                String g = buildQueryString(fieldName, c);
                keyCondExpression = keyCondExpression == null ? g : keyCondExpression + " AND " + g;
            } else {
                String g = buildQueryString(fieldName, c);
                filterExpression = filterExpression == null ? g : filterExpression + " AND " + g;
            }
        }
        query.keyConditionExpression(keyCondExpression);
        if(filterExpression != null) {
            query.filterExpression(filterExpression);
        }
    }

    private DynamoDBQueryHelper() {

    }
}
