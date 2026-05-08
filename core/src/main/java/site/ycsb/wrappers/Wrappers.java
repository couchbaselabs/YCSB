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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import site.ycsb.ByteIterator;
import site.ycsb.NumericByteIterator;
import site.ycsb.StringByteIterator;

public final class Wrappers {
    public static DataWrapper wrapString(String string) {
        return new StringWrapper(string);
    }
    public static DataWrapper wrapInteger(Integer string) {
        return new NumberWrapper(string);
    }
    public static DataWrapper wrapLong(Long string) {
        return new NumberWrapper(string);
    }
    public static DataWrapper wrapIterator(ByteIterator it) {
        return new ByteIteratorWrapper(it);
    }
    public static DataWrapper wrapArray(List<DataWrapper> entries) {
        return new ArrayWrapper(entries);
    }
    public static DataWrapper wrapArray(DataWrapper[] entries) {
        return new ArrayWrapper(Arrays.asList(entries));
    }
    public static DataWrapper wrapNested(DatabaseField[] fields) {
        return new NestedWrapper(Arrays.asList(fields));
    }
    private Wrappers() {}
}

final class NestedWrapper implements DataWrapper {
    private final List<DatabaseField> myEntries;
    NestedWrapper(List<DatabaseField> entries) {
        myEntries = entries;
    }
    @Override
    public List<DataWrapper> arrayAsList() {
        throw new UnsupportedOperationException("Nested objects are no arrays");
    }
    @Override
    public long asLong() {
        throw new UnsupportedOperationException("Nested objects cannot be represented as long");
    }
    @Override
    public int asInteger() {
        throw new UnsupportedOperationException("Nested objects are not integer");
    }
    @Override
    public String asString() {
        throw new UnsupportedOperationException("Nested objects are not strings");
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
    public ByteIterator asIterator() {
        throw new UnsupportedOperationException("Nested objects cannot be transferred into a ByteIterator");
    }
    @Override
    public List<DatabaseField> asNested() {
        return new ArrayList<DatabaseField>(myEntries);
    }
    @Override
    public Object asObject() {
        throw new UnsupportedOperationException("Nested objecsts cannot be returned as Object");
    }

    @Override
    public boolean isArray() {
        return false;
    }
    @Override
    public boolean isNested() {
        return true;
    }
    @Override
    public boolean isTerminal() {
        return false;
    }
}

final class ArrayWrapper implements DataWrapper {
    private final List<DataWrapper> myEntries;
    ArrayWrapper(List<DataWrapper> entries) {
        myEntries = entries;
    }
    @Override
    public int asInteger() {
        throw new UnsupportedOperationException("Arrays are not integer");
    }
    @Override
    public String asString() {
        throw new UnsupportedOperationException("Arrays are not strings");
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
        return Collections.unmodifiableList(myEntries);
    }
    @Override
    public long asLong() {
        throw new UnsupportedOperationException("arrays cannot be represented as long");
    }
    @Override
    public ByteIterator asIterator() {
        throw new UnsupportedOperationException("Arrays cannot be transferred into a ByteIterator");
    }
    @Override
    public List<DatabaseField> asNested() {
        throw new UnsupportedOperationException("Arrays cannot be nested");
    }
    @Override
    public Object asObject() {
        // Object[] o = new Object[myEntries.size()];
        List o = new ArrayList<>();
        for(int i = 0; i < myEntries.size(); i++) {
            o.add(i,myEntries.get(i).asObject());
        }
        return o;
    }

    @Override
    public boolean isArray() {
        return true;
    }
    @Override
    public boolean isNested() {
        return false;
    }
    @Override
    public boolean isTerminal() {
        return false;
    }
}

final class NumberWrapper implements DataWrapper {
    boolean isLong;
    private final Number myContent;
    NumberWrapper(Integer myInt) {
        isLong = false;
        myContent = myInt;
    }
    NumberWrapper(Long myLong) {
        isLong = true;
        myContent = myLong;
    }
    @Override
    public long asLong() {
        return myContent.longValue();
    }
    @Override
    public int asInteger() {
        return myContent.intValue();
    }
    @Override
    public String asString() {
        return myContent.toString();
    }
    @Override
    public boolean isInteger() {
        return !isLong;
    }
    @Override
    public boolean isLong() {
        return isLong;
    }
    @Override
    public boolean isString() {
        return false;
    }
    @Override
    public ByteIterator asIterator() {
        return new NumericByteIterator(myContent.longValue());
    }
    @Override
    public List<DatabaseField> asNested() {
        throw new UnsupportedOperationException("Numbers cannot be nested");
    }
    @Override
    public List<DataWrapper> arrayAsList() {
        throw new UnsupportedOperationException("Numbers are no arrays nested");
    }
    @Override
    public Object asObject() {
        return myContent;
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

final class StringWrapper implements DataWrapper {
    private final String myContent;
    StringWrapper(String string) {
        myContent = string;
    }
    @Override
    public List<DataWrapper> arrayAsList() {
        throw new UnsupportedOperationException("Strings are no arrays");
    }
    @Override
    public long asLong() {
        throw new UnsupportedOperationException("Strings are not long");
    }
    @Override
    public int asInteger() {
        throw new UnsupportedOperationException("Strings are not integer");
    }
    @Override
    public String asString() {
        return myContent;
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
        return true;
    }
    @Override
    public ByteIterator asIterator() {
        return new StringByteIterator(myContent);
    }
    @Override
    public List<DatabaseField> asNested() {
        throw new UnsupportedOperationException("String cannot be nested");
    }
    @Override
    public Object asObject() {
        return myContent;
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