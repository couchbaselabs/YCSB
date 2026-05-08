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
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

public final class AzureCosmosQueryHelper {

  static void extractTypedFields(final ObjectNode content, Set<String> fields,
                                 final Map<String, ByteIterator> result) {
    boolean checkFields = fields != null && !fields.isEmpty();
    
    content.fields().forEachRemaining(entry -> {
      String name = entry.getKey();
      if (checkFields && !fields.contains(name)) {
        return;
      }
      
      JsonNode value = entry.getValue();
      if (value != null && !value.isNull()) {
        if (value.isTextual()) {
          result.put(name, new StringByteIterator(value.asText()));
        } else if (value.isNumber()) {
          result.put(name, new StringByteIterator(value.asText()));
        } else if (value.isBoolean()) {
          result.put(name, new StringByteIterator(String.valueOf(value.asBoolean())));
        } else {
          result.put(name, new StringByteIterator(value.toString()));
        }
      }
    });
  }

  static void extractFields(final ObjectNode content, Set<String> fields,
                           final Map<String, ByteIterator> result) {
    boolean checkFields = fields != null && !fields.isEmpty();
    
    content.fields().forEachRemaining(entry -> {
      String name = entry.getKey();
      if (checkFields && !fields.contains(name)) {
        return;
      }
      
      JsonNode value = entry.getValue();
      if (value != null && !value.isNull()) {
        result.put(name, new StringByteIterator(value.asText()));
      }
    });
  }

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static ObjectNode encode(String key, Map<String, ByteIterator> values) {
    ObjectNode node = OBJECT_MAPPER.createObjectNode();
    node.put("id", key);
    for (Map.Entry<String, ByteIterator> pair : values.entrySet()) {
        node.put(pair.getKey(), pair.getValue().toString());
      }
    return node;
  }

  static ObjectNode encodeWithTypes(String key, List<DatabaseField> fields) {
    ObjectNode node = OBJECT_MAPPER.createObjectNode();
    node.put("id", key);
    for (DatabaseField field : fields) {
      fillDocument(field, node);
    }
    return node;
  }

  static void fillDocument(DatabaseField field, ObjectNode parent) {
    String name = field.getFieldname();
    DataWrapper wrapper = field.getContent();
    if(wrapper.isTerminal()){
      if(wrapper.isString()) {
        parent.put(name, wrapper.asString());
      } else if(wrapper.isInteger()) {
        parent.put(name, wrapper.asInteger());
      } else if(wrapper.isLong()) {
        parent.put(name, wrapper.asLong());
      } else {
        // default to string representation for other terminal types
        parent.put(name, wrapper.asString());
      }
    } else if (wrapper.isArray()) {
      // this WILL BREAK if content is a nested
      // document within the array throw new
      // IllegalArgumentException("cannot handle arrays yet");
      parent.putPOJO(name, wrapper.asObject());
    } else if (wrapper.isNested()) {
      ObjectNode inner = OBJECT_MAPPER.createObjectNode();
      List<DatabaseField> innerFields = wrapper.asNested();
      for(DatabaseField iF : innerFields) {
        fillDocument(iF, inner);
      }
      parent.set(name, inner);
    } else {
      throw new IllegalStateException("neither terminal, nor array, nor nested");
    }
  }

  private AzureCosmosQueryHelper() {
  }
}