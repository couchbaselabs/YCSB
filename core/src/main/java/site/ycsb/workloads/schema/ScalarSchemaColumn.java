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

import site.ycsb.workloads.schema.SchemaHolder.SchemaColumn;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnKind;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnType;

final class ScalarSchemaColumn implements SchemaColumn {
    final String name;
    final SchemaColumnType t;
    ScalarSchemaColumn(String name, SchemaColumnType t) {
        this.name = name;
        this.t = t;
    }
    @Override
    public String getColumnName() {
        return name;
    }
    @Override
    public SchemaColumnKind getColumnKind() {
        return SchemaColumnKind.SCALAR;
    }
    @Override
    public SchemaColumnType getColumnType() {
        return t;
    }
}
