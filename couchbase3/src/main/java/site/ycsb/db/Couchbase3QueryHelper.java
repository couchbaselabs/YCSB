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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;

public final class Couchbase3QueryHelper {

    /**
   * Helper function to wait before a retry.
   */
  static void retryWait(int count) {
    try {
      Thread.sleep(count * 200L);
    } catch(InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  static void extractTypedFields(final JsonObject content,
                              Set<String> fields, final Map<String, ByteIterator> result) {
    if (fields == null || fields.isEmpty()) {
      fields = content.getNames();
    }
    // a very sloppy way to transform the data
    for (String field : fields) {
      Object o = content.get(field);
      if(o instanceof String) {
        result.put(field, new StringByteIterator((String) o));
      } else if(o instanceof Number) {
        result.put(field, new StringByteIterator(((Number) o).toString()));
      } else if(o instanceof JsonArray) {
        result.put(field, new ByteArrayByteIterator(((JsonArray) o).toBytes()));
      } else if(o instanceof JsonObject) {
        result.put(field, new ByteArrayByteIterator(((JsonObject) o).toBytes()));
      }
    }
  }
  static void extractFields(final JsonObject content, Set<String> fields,
                                    final Map<String, ByteIterator> result) {
    if (fields == null || fields.isEmpty()) {
      fields = content.getNames();
    }

    for (String field : fields) {
      result.put(field, new StringByteIterator(content.getString(field)));
    }
  }

  /**
   * Helper method to turn the passed in iterator values into a map we can encode to json.
   *
   * @param values the values to encode.
   * @return the map of encoded values.
   */
  static Map<String, ?> encodeWithTypes(final List<DatabaseField> values) {
    Map<String, Object> toInsert = new HashMap<>(values.size());
    for (DatabaseField field : values) {
      fillDocument(field, toInsert);
    }
    return toInsert;
  }

  /**
   * Helper method to turn the passed in iterator values into a map we can encode to json.
   *
   * @param values the values to encode.
   * @return the map of encoded values.
   */
  static Map<String, String> encode(final Map<String, ByteIterator> values) {
    Map<String, String> result = new HashMap<>(values.size());
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      result.put(value.getKey(), value.getValue().toString());
    }
    return result;
  }

  static void fillDocument(DatabaseField field, Map<String, Object> toInsert) {
    DataWrapper wrapper = field.getContent();
    Object content = null;
    if(wrapper.isTerminal() || wrapper.isArray()) {
      // this WILL BREAK if content is a nested
      // document within the array
      content = wrapper.asObject();
    } else if(wrapper.isNested()) {
      Map<String, Object> inner = new HashMap<>();
      List<DatabaseField> innerFields = wrapper.asNested();
      for(DatabaseField iF : innerFields) {
        fillDocument(iF, inner);
      }
      content = inner;
    } else {
      throw new IllegalStateException("neither terminal, nor array, nor nested");
    }
    toInsert.put(
      field.getFieldname(),
      content
    );
  }

  private Couchbase3QueryHelper(){

  }
}