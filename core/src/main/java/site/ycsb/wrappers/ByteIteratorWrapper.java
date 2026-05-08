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
package site.ycsb.wrappers;

import java.util.List;

import site.ycsb.ByteIterator;

public final class ByteIteratorWrapper implements DataWrapper {
    private final ByteIterator theIterator;
    ByteIteratorWrapper(ByteIterator it) {
        theIterator = it;
    }
    @Override
    public long asLong() {
        throw new UnsupportedOperationException("byte iterator is not a long");
    }
    @Override
    public int asInteger() {
        throw new UnsupportedOperationException("byte iterator is not an integer");
    }
    @Override
    public String asString() {
        return theIterator.toString();
    }
    @Override
    public boolean isInteger() {
        return false;
    }
    @Override
    public boolean isLong() {
        return false;
    }
    @Override
    public boolean isString() {
        return false;
    }
    @Override
    public List<DataWrapper> arrayAsList() {
        throw new UnsupportedOperationException("byte iterator is not an arrays nested");
    }
    @Deprecated
    public static ByteIteratorWrapper create(ByteIterator it) {
        return new ByteIteratorWrapper(it);
    }
    public ByteIterator asIterator() {
        return theIterator;
    }
    @Override
    public List<DatabaseField> asNested() {
        throw new UnsupportedOperationException("String cannot be nested");
    }
    @Override
    public Object asObject() {
        return theIterator.toArray();
    }

    @Override
    public boolean isArray() {
        return false;
    }
    @Override
    public boolean isNested() {
        return false;
    }
    @Override
    public boolean isTerminal() {
        return true;
    }
}
