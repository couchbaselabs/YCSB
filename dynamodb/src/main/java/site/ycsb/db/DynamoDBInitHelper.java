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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

import site.ycsb.db.DynamoDBClient.IndexDescriptor;
import site.ycsb.workloads.schema.SchemaHolder;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumn;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnKind;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public final class DynamoDBInitHelper {
    
  static List<IndexDescriptor> getIndexList(Properties props, int defaultReadCap, int defaultWriteCap) {
    String indexeslist = props.getProperty(DynamoDBClient.INDEX_LIST_PROPERTY);
    System.err.println("read indexlist from config: " + indexeslist);
    if(indexeslist == null) {
      return Collections.emptyList();
    }
    DynamoDBClient.LOGGER.info("raw index property: " + indexeslist);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = null;
    try {
      root = mapper.readTree(indexeslist);
    } catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    DynamoDBClient.LOGGER.info("parsed index property: " + root.toString());
    if(!root.isArray()) {
      throw new IllegalArgumentException("index specification must be a JSON array");
    }
    ArrayNode array = (ArrayNode) root;
    if(array.size() == 0) {
      return Collections.emptyList();
    }
    DynamoDBClient.LOGGER.info("parsed index array with size: " + array.size());
    List<IndexDescriptor> retVal = new ArrayList<>();
    for(int i = 0; i < array.size(); i++) {
      JsonNode el = array.get(i);
      if(!el.isObject()) {
        throw new IllegalArgumentException("index elements must be a JSON object");
      }
      IndexDescriptor desc = new IndexDescriptor();
      ObjectNode object = (ObjectNode) el;
      JsonNode name = object.get("name");
      if(name == null || !name.isTextual()) {
        throw new IllegalArgumentException("index elements must be a JSON object with 'name' of type string");
      }
      desc.name = ((TextNode) name).asText();

      JsonNode hash = object.get("hashAttributes");
      if(hash != null && hash.isArray()) {
        ArrayNode hashArray = (ArrayNode) hash;
        for(int j = 0; j < hashArray.size(); j++) {
          JsonNode sortEl = hashArray.get(j);
          if(sortEl == null || !sortEl.isTextual()) {
            throw new IllegalArgumentException("index elements must be a JSON object with 'hashAttributes' set as an array of strings");
          }
          desc.hashKeyAttributes.add(((TextNode) sortEl).asText());
        }
      } else if(hash != null) {
        throw new IllegalArgumentException("index elements must be a JSON object. If 'hashAttributes' is set, it must be an array of strings");
      }

      JsonNode sorts = object.get("sortAttributes");
      if(sorts != null && sorts.isArray()) {
        ArrayNode sortArray = (ArrayNode) sorts;
        for(int j = 0; j < sortArray.size(); j++) {
          JsonNode sortEl = sortArray.get(j);
          if(sortEl == null || !sortEl.isTextual()) {
            throw new IllegalArgumentException("index elements must be a JSON object with 'sortAttributes' set as an array of strings");
          }
          desc.sortsKeyAttributes.add(((TextNode) sortEl).asText());
        }
      } else if(sorts != null) {
        throw new IllegalArgumentException("index elements must be a JSON object with 'sortAttributes' set as an array of strings");
      }

      JsonNode readCap = object.get("readCap");
      if(readCap == null || !readCap.isNumber()) {
        desc.readCap = defaultReadCap;
      } else {
        desc.readCap = ((IntNode) readCap).asInt();
      }

      JsonNode writeCap = object.get("writeCap");
      if(writeCap == null || !writeCap.isNumber()) {
        desc.writeCap = defaultWriteCap;
      } else {
        desc.writeCap = ((IntNode) writeCap).asInt();
      }

      DynamoDBClient.LOGGER.info("parsed array[" + i + "]: " + desc);
      retVal.add(desc);
    }
    return retVal;
  }

  private static AttributeDefinition attributeDefinitionForColumn(SchemaColumn c) {
    // fixme: consider nested columns
    if(c.getColumnKind() == SchemaColumnKind.SCALAR) {
        AttributeDefinition.Builder def = AttributeDefinition.builder().attributeName(c.getColumnName());
        switch(c.getColumnType()) {
            case BYTES:
                def.attributeType(ScalarAttributeType.B);
                break;
            case INT:
            case LONG:
                def.attributeType(ScalarAttributeType.N);
                break;
            case TEXT:
                def.attributeType(ScalarAttributeType.S);
                break;
            case CUSTOM:
            default:
                throw new IllegalArgumentException("not supported: " + c.getColumnType());
        }
        return def.build();
    } else if(c.getColumnKind() == SchemaColumnKind.NESTED) {
        if(c.getColumnName() == "airline") {
        return AttributeDefinition.builder()
          .attributeName("airline.alias")
          .attributeType(ScalarAttributeType.S)
          .build();
        }
        return null;
    } else if(c.getColumnKind() == SchemaColumnKind.ARRAY) {
        return null;
    }
    throw new IllegalArgumentException("not supported: " + c.getColumnType());
  }

  static List<AttributeDefinition> getFullAttributeDefinitionList() {
    List<SchemaColumn> l = SchemaHolder.INSTANCE.getOrderedListOfColumns();
    List<AttributeDefinition> fields = new ArrayList<>();
    for(SchemaColumn c : l) {
        AttributeDefinition d = attributeDefinitionForColumn(c);
        if(d != null) fields.add(d);
    }
    return fields;
  }

  static CreateGlobalSecondaryIndexAction getCreateSecondaryIndexAction(IndexDescriptor idx, BillingMode billing) {
    // GlobalSecondaryIndex
    System.err.println("building a Secondary Index Action for " + idx.name + " and its properties " + idx.hashKeyAttributes + " and " + idx.sortsKeyAttributes);
    CreateGlobalSecondaryIndexAction.Builder action = CreateGlobalSecondaryIndexAction.builder();
    action.indexName(idx.name);
    action.projection(Projection.builder().projectionType(ProjectionType.ALL).build());
    List<KeySchemaElement> schema = new ArrayList<>();
    for(String e : idx.hashKeyAttributes) {
      schema.add(KeySchemaElement.builder().attributeName(e).keyType(KeyType.HASH).build());
    }
    for(String s : idx.sortsKeyAttributes) {
        schema.add(KeySchemaElement.builder().attributeName(s).keyType(KeyType.RANGE).build());
    }
    action.keySchema(schema);
    if(BillingMode.PAY_PER_REQUEST != billing) {
      action.provisionedThroughput(
          ProvisionedThroughput.builder()
          .readCapacityUnits(Long.valueOf(idx.readCap))
          .writeCapacityUnits(Long.valueOf(idx.writeCap))
          .build()
      );
    }
    return action.build();
  }

    private DynamoDBInitHelper() {

    }
}
