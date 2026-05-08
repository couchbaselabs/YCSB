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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;

final class Couchbase3IndexHelper {

  static List<JsonObject> getIndexList(Properties props) {
    String indexeslist = props.getProperty(Couchbase3Client.INDEX_LIST_PROPERTY);
    if(indexeslist == null || indexeslist.isEmpty()) {
      return Collections.emptyList();
    }
    JsonArray barray = JsonArray.fromJson(indexeslist);
    List<JsonObject> llist = new ArrayList<>();
    for(int i = 0; i < barray.size(); i++) {
      // Object o = barray.get(i);
      // System.err.println("JSON ARRAY: " + i + " --- " + o + "---" + o.getClass());
      llist.add(barray.getObject(i));
    }
    // List<HashMap<String,Object>> list = (List<HashMap<String,Object>>) (Object) barray.toList();
    // System.err.println("JSON LIST: " + list.toString());
    return llist;
  }

  static void setIndexes(Properties props, List<JsonObject> indexes, Collection collection) {
    if(indexes.size() == 0) {
      return;
    }
    // final String table = props.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    for(JsonObject jo : indexes) {
      System.err.println("get my index: " + jo);
      if(jo.getBoolean("isPrimary") == Boolean.TRUE) {
        CreatePrimaryQueryIndexOptions options = CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions();
        options.deferred(false);
        options.ignoreIfExists(true);
        collection.queryIndexes().createPrimaryIndex(options);
      } else {
        final String indexName = jo.getString("name");
        final JsonArray fieldArray = jo.getArray("fields");
        final String[] fields = new String[fieldArray.size()];
        for(int i = 0 ; i < fieldArray.size(); i++) {
          fields[i] = fieldArray.getString(i);
        }
        CreateQueryIndexOptions options = CreateQueryIndexOptions.createQueryIndexOptions();
        options.deferred(false);
        options.ignoreIfExists(true);
        collection.queryIndexes().createIndex(indexName, Arrays.asList(fields), options);
      }
    }
    System.err.println("created indexes");
  }

    private Couchbase3IndexHelper() {

    }
}
