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

import java.util.Arrays;

final class Util {

    private Util() {
        // prevent instantiation
    }

    // visible for testing
    static void reverse(byte[] array) {
        if (array == null) {
            return;
        }
        int i = 0;
        int j = array.length - 1;
        byte tmp;
        while (j > i) {
            tmp = array[j];
            array[j] = array[i];
            array[i] = tmp;
            j--;
            i++;
        }
    }

    /**
     * Returns the least value x where min(a,b) <= x < max(a,b) and x has the
     * maximum number of trailing zeroes in its binary representation.
     * 
     * @param a
     *            first value
     * @param b
     *            second value
     * @return most significant (in terms of divisors of powers of 2) between a and
     *         b
     */
    static long mostSignificantBetween(long a, long b) {
        if (a > b) {
            return mostSignificantBetween(b, a);
        } else if (a == b) {
            return a;
        } else {
            long x = a == 0 ? 1 : a;
            int bit = 0;
            while (x < b) {
                if ((x & (1 << bit)) == 0) {
                    bit++;
                } else {
                    long y = x + (1 << bit);
                    if (y < b) {
                        bit++;
                        x = y;
                    } else {
                        break;
                    }
                }
            }
            return x;
        }
    }

    public static void zero(long[] x) {
        Arrays.fill(x, 0);
    }

}
