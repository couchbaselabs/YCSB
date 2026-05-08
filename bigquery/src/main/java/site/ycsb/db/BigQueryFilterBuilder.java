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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.google.cloud.bigquery.Parameter;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;

import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.ComparisonOperator;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

final class BigQueryFilterBuilder {

    static void bindAggregateQuery(List<Parameter> params, String[] airports, int minOccurrences) {
        for(String a : airports) {
            params.add(
                Parameter.newBuilder().setValue(QueryParameterValue.string(a)).build()
            );
        }
        for(String a : airports) {
            params.add(
                Parameter.newBuilder().setValue(QueryParameterValue.string(a)).build()
            );
        }
        params.add(
                Parameter.newBuilder().setValue(QueryParameterValue.int64(minOccurrences)).build()
            );
    }

    static void bindAggregateQuery(QueryJobConfiguration.Builder builder, String[] airports, int minOccurrences) {
        for(String a : airports) {
            builder.addPositionalParameter(QueryParameterValue.string(a));
        }
        for(String a : airports) {
            builder.addPositionalParameter(QueryParameterValue.string(a));
        }
        builder.addPositionalParameter(QueryParameterValue.int64(minOccurrences));
    }

    static void bindUpdateOneQuery(QueryJobConfiguration.Builder builder, List<Comparison> filters, List<DatabaseField> fields) {
        bindSetPortion(builder, fields);
        bindFindOneQuery(builder, filters);
    }

    static void bindInsertRow(QueryJobConfiguration.Builder builder, List<DatabaseField> fields) {
        HashMap<String,QueryParameterValue> valuesToAdd = new HashMap<>();
        for(DatabaseField field : fields){
            String fieldname = field.getFieldname();
            DataWrapper wrapper = field.getContent();
            if(wrapper.isNested()) {
                List<DatabaseField> innerFields = wrapper.asNested();
                bindSetPortion(builder, innerFields);
            } else if(wrapper.isTerminal()) {
                if(wrapper.isInteger()) {
                    valuesToAdd.put(fieldname, QueryParameterValue.int64(wrapper.asInteger()));
                } else if (wrapper.isLong()) {
                    valuesToAdd.put(fieldname, QueryParameterValue.int64(wrapper.asInteger()));
                } else if(wrapper.isString()) {
                    valuesToAdd.put(fieldname, QueryParameterValue.string(wrapper.asString()));
                }  else {
                    valuesToAdd.put(fieldname, QueryParameterValue.string(
                        new String(wrapper.asIterator().toArray()))
                    );
                }
            } else if(wrapper.isArray()) {
                throw new IllegalArgumentException("setting arrays or array content is currently not supported");
            } else {
                throw new IllegalStateException("neither terminal, nor array, nor nested");
            }
        }
        // we got all fields in the right format. Now, let's iterate over the schema 
        // and see which ones are missing. we assume the primary has already been added
        for(String field : BigQueryClient.databaseSchema){
            QueryParameterValue val = valuesToAdd.get(field);
            if(val != null)
                builder.addPositionalParameter(val);
            else
                builder.addPositionalParameter(QueryParameterValue.string(null));
        }
    }

    static void bindSetPortion(QueryJobConfiguration.Builder builder, List<DatabaseField> fields) {
        for(DatabaseField field : fields){
            DataWrapper wrapper = field.getContent();
            if(wrapper.isNested()) {
                List<DatabaseField> innerFields = wrapper.asNested();
                bindSetPortion(builder, innerFields);
            } else if(wrapper.isTerminal()) {
                if(wrapper.isInteger()) {
                    builder.addPositionalParameter(QueryParameterValue.int64(wrapper.asInteger()));
                } else if (wrapper.isLong()) {
                    builder.addPositionalParameter(QueryParameterValue.int64(wrapper.asLong()));
                } else if(wrapper.isString()) {
                    builder.addPositionalParameter(QueryParameterValue.string(wrapper.asString()));
                }  else {
                    builder.addPositionalParameter(QueryParameterValue.string(
                        new String(wrapper.asIterator().toArray()))
                    );
                }
            } else if(wrapper.isArray()) {
                throw new IllegalArgumentException("setting arrays or array content is currently not supported");
            } else {
                throw new IllegalStateException("neither terminal, nor array, nor nested");
            }
        }
    }

    /*
    static String buildTypedInsertColumnString(List<DatabaseField> fields) {
        List<String> elements = new ArrayList<>();
        for(DatabaseField field : fields) {
            String fieldname = field.getFieldname();
            DataWrapper wrapper = field.getContent();
            if(wrapper.isNested()) {
                throw new IllegalArgumentException("nested data structures are not supported");
            }
            elements.add(fieldname);
        }
        return String.join(", ", elements);
    }*/

    static String buildTypedPlaceholderInsertString(List<DatabaseField> fields) {
        List<String> elements = new ArrayList<>();
        for(DatabaseField field : fields) {
            DataWrapper wrapper = field.getContent();
            if(wrapper.isNested()) {
                throw new IllegalArgumentException("nested data structures are not supported");
            }
            elements.add(" ? ");
        }
        if(elements.size() > 0) 
            return ", " + String.join(", ", elements);
        return "";
    }

    static String buildTypedPlaceholderSetString(List<DatabaseField> fields) {
        List<String> elements = new ArrayList<>();
        for(DatabaseField field : fields) {
            String fieldname = field.getFieldname();
            DataWrapper wrapper = field.getContent();
            if(wrapper.isNested()) {
                throw new IllegalArgumentException("nested data structures are not supported");
            }
            elements.add(fieldname + " = ? ");
        }
        return String.join(", ", elements);
    }

    static void bindFindOneQuery(QueryJobConfiguration.Builder builder, List<Comparison> filters) {
        for(Comparison c : filters) {
            Comparison d = c;
            while(d.isSimpleNesting()) {
                d = d.getSimpleNesting();
            }
            if(d.comparesStrings()) {
                builder.addPositionalParameter(QueryParameterValue.string(d.getOperandAsString()));
            } else if(d.comparesInts()) {
                builder.addPositionalParameter(QueryParameterValue.int64(d.getOperandAsInt()));
            } else {
                throw new IllegalStateException();
            }
        }
    }

    static String buildConcatenatedPlaceholderFilter(List<Comparison> filters) {
        List<String> filterList = new LinkedList<>();
        for(Comparison c : filters) {
            Comparison d = c;
            String fieldName = d.getFieldname();
            while(d.isSimpleNesting()) {
                // should never be needed, as we do not use nesting
                d = d.getSimpleNesting();
                fieldName = fieldName + "." + d.getFieldname();
            }
            if(d.comparesStrings()) {
                filterList.add(
                    getStringPlaceholderFilter(fieldName, d.getOperator())
                );
            } else if(d.comparesInts()) {
                filterList.add(
                    getIntPlaceholderFilter(fieldName, d.getOperator())
                );
            }
        }
        return String.join(" AND ", filterList);
    }

    private static String getStringPlaceholderFilter(String fieldname, ComparisonOperator op) {
        switch (op) {
            case STRING_EQUAL:
                return "(" + fieldname + " LIKE ? )";        
            default:
                throw new IllegalArgumentException("unknown operator: " + op);
        }
    }

    private static String getIntPlaceholderFilter(String fieldname, ComparisonOperator op) {
        switch (op) {
            case INT_LTE:
                return "( " + fieldname + " <= ? )";        
            default:
                throw new IllegalArgumentException("unknown operator: " + op);
        }
    }

    private BigQueryFilterBuilder() {
        // no instances
    }
}
