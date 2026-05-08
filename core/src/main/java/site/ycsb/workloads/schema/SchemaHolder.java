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
package site.ycsb.workloads.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public enum SchemaHolder {
    INSTANCE;
    private volatile List<SchemaColumn> theList;
    public List<SchemaColumn> getOrderedListOfColumns() {
        return theList == null ? Collections.emptyList() : new ArrayList<>(theList);
    }
    public static SchemaBuilder schemaBuilder() {
        return new SchemaBuilder();
    }
    /*public static EmbeddedColumnBuilder embeddingBuilder() {
        return new EmbeddedColumnBuilder();
    }*/
    private void register(List<SchemaColumn> schema) {
        theList = new ArrayList<>(schema);
    }
    public static enum SchemaColumnKind {
        SCALAR,
        NESTED,
        ARRAY,
    }
    public static enum SchemaColumnType {
        INT,
        LONG,
        TEXT,
        BYTES,
        CUSTOM
    }
    /*
    public static class EmbeddedColumnBuilder {
        private final Map<String, SchemaColumnType> entries = new HashMap<>();
        public EmbeddedColumnBuilder addEntry(String name, SchemaColumnType type) {
            entries.put(name, type);
            return this;
        }
    }*/
    public static class SchemaBuilder {
        private final List<SchemaColumn> schema = new ArrayList<>();
        public SchemaBuilder addEmbeddedColumn(String name, List<SchemaColumn> elements) {
            SchemaColumn c = new EmbeddedSchemaColumn(name, elements);
            schema.add(c);
            return this;
        }
        public SchemaBuilder addArrayColumn(String name, SchemaColumnType t) {
            SchemaColumn c = new ArraySchemaColumn(name, t);
            schema.add(c);
            return this;
        }
        public SchemaBuilder addScalarColumn(String name, SchemaColumnType t) {
            SchemaColumn c = new ScalarSchemaColumn(name, t);
            schema.add(c);
            return this;
        }
        public SchemaBuilder addColumn(String name, SchemaColumnType t) {
            return addScalarColumn(name, t);
        }
        public void register() {
            SchemaHolder.INSTANCE.register(schema);
        }
        public List<SchemaColumn> build() {
            return schema;
        }
    }

    public static interface SchemaColumn {
        public String getColumnName();
        public SchemaColumnKind getColumnKind();
        public SchemaColumnType getColumnType();
    }
}
