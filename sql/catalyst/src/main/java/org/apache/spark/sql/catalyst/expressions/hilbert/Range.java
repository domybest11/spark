/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.hilbert;

public final class Range {

    private final long low;
    private final long high;

    public Range(long low, long high) {
        this.low = Math.min(low, high);
        this.high = Math.max(low, high);
    }

    public static Range create(long low, long high) {
        return new Range(low, high);
    }

    public static Range create(long value) {
        return new Range(value, value);
    }

    public long low() {
        return low;
    }

    public long high() {
        return high;
    }

    public boolean contains(long value) {
        return low <= value && value <= high;
    }

    @Override
    public String toString() {
        return "Range [low=" + low + ", high=" + high + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (high ^ (high >>> 32));
        result = prime * result + (int) (low ^ (low >>> 32));
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
        Range other = (Range) obj;
        if (high != other.high)
            return false;
        if (low != other.low)
            return false;
        return true;
    }

    public Range join(Range range) {
        return Range.create(Math.min(low, range.low), Math.max(high, range.high));
    }

}
