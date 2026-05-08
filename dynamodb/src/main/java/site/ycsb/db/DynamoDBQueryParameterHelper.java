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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.wrappers.DataWrapper;
import site.ycsb.wrappers.DatabaseField;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class DynamoDBQueryParameterHelper {

  private static AttributeValue fillAttribute(Object o) {
    AttributeValue.Builder content = AttributeValue.builder();
    if(String.class.isInstance(o)) {
        content.s((String) o);
    } else if(
        Integer.class.isInstance(o) || int.class.isInstance(o) ||
        Long.class.isInstance(o) || long.class.isInstance(o) ||
        Double.class.isInstance(o) || double.class.isInstance(o) ||
        Float.class.isInstance(o) || float.class.isInstance(o)) {
          content.n(o.toString());
    } else {
      throw new IllegalArgumentException("what? " + o);
    }
    return content.build();
  }
  private static void fillDocument(DatabaseField field, Map<String, AttributeValue> toInsert) {
    DataWrapper wrapper = field.getContent();
    AttributeValue.Builder content;
    if(wrapper.isTerminal()){
      content = AttributeValue.builder();
      if(wrapper.isInteger() || wrapper.isLong()) {
        content.n(wrapper.asObject().toString());
      } else if(wrapper.isString()) {
        content.s(wrapper.asString());
      } else {
        // as iterator
        Object o = field.getContent().asObject();
        byte[] b = (byte[]) o;
        content.s(new String(b));
      }
    } else if (wrapper.isArray()) {
      final List<Object> oa = (List<Object>) wrapper.asObject();
      List<AttributeValue> elements = new ArrayList<>(oa.size());
      for(int i = 0; i < oa.size(); i++) {
        // this WILL BREAK if content is a nested
        // document within the array
        elements.add(fillAttribute(oa.get(i)));
      }
      content = AttributeValue.builder();
      content.l(elements);
    } else if(wrapper.isNested()) {
        content = AttributeValue.builder();
        Map<String, AttributeValue> inner = new HashMap<>();
        List<DatabaseField> innerFields = wrapper.asNested();
        for(DatabaseField iF : innerFields) {
          fillDocument(iF, inner);
        }
        content.m(inner);
      } else {
        throw new IllegalStateException("neither terminal, nor array, nor nested");
      }
      toInsert.put(
        field.getFieldname(),
        content.build()
      );
  }

  static Map<String, AttributeValue> createTypedAttributes(List<DatabaseField> values) {
    Map<String, AttributeValue> toInsert = new HashMap<>(values.size() + 1);
    for (DatabaseField field : values) {
      fillDocument(field, toInsert);
    }
    return toInsert;
  }

  static Map<String, AttributeValue> createAttributes(Map<String, ByteIterator> values) {
    Map<String, AttributeValue> attributes = new HashMap<>(values.size() + 1);
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      attributes.put(val.getKey(), AttributeValue.builder().s(val.getValue().toString()).build());
    }
    return attributes;
  }

  static HashMap<String, ByteIterator> extractResultFromItem(Map<String, AttributeValue> item) {
    HashMap<String, ByteIterator> rItems = new HashMap<>(item.size());
    for(Map.Entry<String, AttributeValue> attr : item.entrySet()) {
      AttributeValue v = attr.getValue();
      if(v.s() != null) {
        rItems.put(attr.getKey(), new StringByteIterator(v.s()));
      } else if(v.n() != null) {
        rItems.put(attr.getKey(), new StringByteIterator(v.n()));
      } else if(v.l() != null) {
        // this is a bit hacky, but we do not have a better way to represent arrays
        List<AttributeValue> l = v.l();
        List<String> stringList = new ArrayList<>(l.size());
        for(AttributeValue av : l) {
          if(av.s() != null) {
            stringList.add(av.s());
          } else if(av.n() != null) {
            stringList.add(av.n());
          } else {
            throw new IllegalStateException("unknown array element type: " + av);
          }
        }
        rItems.put(attr.getKey(), new StringByteIterator(stringList.toString()));
      } else if(v.m() != null) {
        // this is a bit hacky, but we do not have a better way to represent nested documents
        Map<String, AttributeValue> m = v.m();
        Map<String, String> stringMap = new HashMap<>();
        for(Map.Entry<String, AttributeValue> e : m.entrySet()) {
          if(e.getValue().s() != null) {
            stringMap.put(e.getKey(), e.getValue().s());
          } else if(e.getValue().n() != null) {
            stringMap.put(e.getKey(), e.getValue().n());
          } else {
            throw new IllegalStateException("unknown map element type: " + e.getValue());
          }
        }
        rItems.put(attr.getKey(), new StringByteIterator(stringMap.toString()));
      } else {
        throw new IllegalStateException("unknown attribute value type: " + v);
      }
    }
    return rItems;
  }

  static HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
    if (null == item) {
      return null;
    }
    HashMap<String, ByteIterator> rItems = new HashMap<>(item.size());

    for (Entry<String, AttributeValue> attr : item.entrySet()) {
      if (DynamoDBClient.LOGGER.isDebugEnabled()) {
        DynamoDBClient.LOGGER.debug(String.format("Result- key: %s, value: %s", attr.getKey(), attr.getValue()));
      }
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().s()));
    }
    return rItems;
  }

  private DynamoDBQueryParameterHelper() {
    // random
  } 
}
