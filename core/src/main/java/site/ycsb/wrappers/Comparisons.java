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

public final class Comparisons {
    
    public static Comparison createStringComparison(String fieldName, ComparisonOperator op, String operand) {
        return new StringComparison(fieldName, op, operand);
    }

    public static Comparison createIntComparison(String fieldName, ComparisonOperator op, int operand) {
        return new IntComparison(fieldName, op, operand);
    }

    public static Comparison createSimpleNestingComparison(String fieldName, Comparison c) {
        return new SimpleNestingComparison(fieldName, c);
    }

    private Comparisons() {
        // private constructor
    }
}

class StringComparison implements Comparison {
    private final String fieldName;
    private final ComparisonOperator operator;
    private final String operand;
    StringComparison(String fieldName, ComparisonOperator op, String operand) {
        this.fieldName = fieldName;
        operator = op;
        this.operand = operand;
    }
    @Override
    public StringComparison normalized() {
        return new StringComparison(fieldName, operator, null);
    }
    @Override
    public final String getFieldname() {
        return fieldName;
    }
    @Override
    public String getContentAsString() {
        return operator + " " + operand;
    }
    @Override
    public boolean comparesStrings() {
        return true;
    }
    @Override
    public boolean comparesInts() {
        return false;
    }
    @Override
    public ComparisonOperator getOperator(){
        return operator;
    }
    @Override
    public String getOperandAsString() {
        return operand;
    }
    @Override
    public int getOperandAsInt() {
        throw new UnsupportedOperationException("wrong type");
    }
    @Override
    public String toString() {
        return fieldName + "_" + operator + "_" + operand;
    }
    @Override
    public boolean isSimpleNesting() {
        return false;
    }
    @Override
    public Comparison getSimpleNesting() {
        throw new UnsupportedOperationException("wrong type");
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
        result = prime * result + ((operator == null) ? 0 : operator.hashCode());
        result = prime * result + ((operand == null) ? 0 : operand.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StringComparison other = (StringComparison) obj;
        if (fieldName == null) {
            if (other.fieldName != null)
                return false;
        } else if (!fieldName.equals(other.fieldName))
            return false;
        if (operator != other.operator)
            return false;
        if (operand == null) {
            if (other.operand != null)
                return false;
        } else if (!operand.equals(other.operand))
            return false;
        return true;
    }  
}
class IntComparison implements Comparison {
    private final String fieldName;
    private final ComparisonOperator operator;
    private final Integer operand;
    IntComparison(String fieldName, ComparisonOperator op, Integer operand) {
        this.fieldName = fieldName;
        operator = op;
        this.operand = operand;
    }
    public IntComparison normalized() {
        return new IntComparison(fieldName, operator, null);
    }
    @Override
    public final String getFieldname() {
        return fieldName;
    }
    @Override
    public String getContentAsString() {
        return operator + " " + operand;
    }
    @Override
    public boolean comparesStrings() {
        return false;
    }
    @Override
    public boolean comparesInts() {
        return true;
    }
    @Override
    public ComparisonOperator getOperator(){
        return operator;
    }
    @Override
    public String getOperandAsString() {
        throw new UnsupportedOperationException("wrong type");
    }
    @Override
    public int getOperandAsInt() {
        return operand;
    }
    @Override
    public String toString() {
        return fieldName + "_" + operator + "_" + operand;
    }
    @Override
    public boolean isSimpleNesting() {
        return false;
    }
    @Override
    public Comparison getSimpleNesting() {
        throw new UnsupportedOperationException("wrong type");
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
        result = prime * result + ((operator == null) ? 0 : operator.hashCode());
        result = prime * result + ((operand == null) ? 0 : operand.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IntComparison other = (IntComparison) obj;
        if (fieldName == null) {
            if (other.fieldName != null)
                return false;
        } else if (!fieldName.equals(other.fieldName))
            return false;
        if (operator != other.operator)
            return false;
        if (operand != other.operand)
            return false;
        return true;
    }
    
}
class SimpleNestingComparison implements Comparison {
    private final String fieldName;
    private final Comparison nesting;
    SimpleNestingComparison(String fieldName, Comparison nesting) {
        this.fieldName = fieldName;
        this.nesting = nesting;
    }
    public SimpleNestingComparison normalized() {
        return new SimpleNestingComparison(fieldName, nesting.normalized());
    }
    @Override
    public final String getFieldname() {
        return fieldName;
    }
    @Override
    public String getContentAsString() {
        throw new UnsupportedOperationException("wrong type");
    }
    @Override
    public boolean comparesStrings() {
        return false;
    }
    @Override
    public boolean comparesInts() {
        return true;
    }
    @Override
    public ComparisonOperator getOperator(){
        return null;
    }
    @Override
    public String getOperandAsString() {
        throw new UnsupportedOperationException("wrong type");
    }
    @Override
    public int getOperandAsInt() {
        throw new UnsupportedOperationException("wrong type");
    }
    @Override
    public String toString() {
        return fieldName + "." + nesting;
    }
    @Override
    public boolean isSimpleNesting() {
        return true;
    }
    @Override
    public Comparison getSimpleNesting() {
        return nesting;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
        result = prime * result + ((nesting == null) ? 0 : nesting.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleNestingComparison other = (SimpleNestingComparison) obj;
        if (fieldName == null) {
            if (other.fieldName != null)
                return false;
        } else if (!fieldName.equals(other.fieldName))
            return false;
        if (nesting == null) {
            if (other.nesting != null)
                return false;
        } else if (!nesting.equals(other.nesting))
            return false;
        return true;
    }
    
}