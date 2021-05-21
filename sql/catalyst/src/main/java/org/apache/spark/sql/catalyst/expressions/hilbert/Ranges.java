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

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.guavamini.Preconditions;

/**
 * Adds ranges to a collection and combines ranges optimally when the internal
 * buffer is exceeded.
 * 
 */
// NotThreadSafe
public class Ranges implements Iterable<Range> {

    private final int bufferSize;

    // set is ordered by increasing distance to next node (Node is a linked list)
    private final TreeSet<Node> set;

    // mutable
    private Node ranges; // in descending order of ranges e.g. Range(5,7) -> Range(1,3)
    private Node last;
    private int count; // count of items in ranges

    public Ranges(int bufferSize) {
        Preconditions.checkArgument(bufferSize >= 0);
        this.bufferSize = bufferSize;
        this.ranges = null;
        if (bufferSize == 0) {
            // save on allocations
            this.set = null;
        } else {
            this.set = new TreeSet<>();
        }
    }

    public Ranges add(long low, long high) {
        Preconditions.checkArgument(low <= high);
        return add(Range.create(low, high));
    }

    public Ranges add(Range r) {
        Preconditions.checkArgument(ranges == null || ranges.value.high() < r.low(),
                "ranges must be added in increasing order and without overlap");
        Node node = new Node(r);
        count++;
        if (ranges == null) {
            ranges = node;
            last = node;
        } else if (bufferSize == 0) {
            node.setNext(ranges);
            ranges = node;
        } else {
            // and set new head and recalculate distance for ranges
            node.setNext(ranges);

            // add old head to set (now that the distanceToPrevious has been calculated)
            set.add(ranges);

            ranges = node;

            if (count > bufferSize) {
                // remove node from set with least distance to next node
                Node first = set.pollFirst();

                // replace that node in linked list (ranges) with a new Node
                // that has the concatenation of that node with previous node's range
                // also remove its predecessor. We dont' need to remove the predecessor from the
                // set because it's distanceToPrevious will remain the same

                // first.previous will not be null because distance was present to be in set
                Range joined = first.value.join(first.previous().value);
                set.remove(first.previous());

                Node n = new Node(joined);
                // link and recalculate distance (won't change because the lower bound of the
                // new ranges is the same as the lower bound of the range of first)
                if (first.next() != null) {
                    n.setNext(first.next());
                } else {
                    // first is last in linked list so update last
                    last = n;
                }
                // link and calculate the distance for n
                Node firstPrevious = first.previous();
                if (firstPrevious == ranges) {
                    ranges = n;
                } else {
                    first.previous().previous().setNext(n);
                }
                set.add(n);

                // clear pointers from first to help gc out
                // there new gen to old gen promotion can cause problems
                first.clearForGc();

                // we have reduced number of nodes in list so reduce count
                count--;
            }
        }
        return this;
    }

    @Override
    public Iterator<Range> iterator() {
        return new Iterator<Range>() {

            Node r = last;

            @Override
            public boolean hasNext() {
                return r != null;
            }

            @Override
            public Range next() {
                Range v = r.value;
                r = r.previous();
                return v;
            }

        };
    }

    public Stream<Range> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public void println() {
        forEach(System.out::println);
    }

    public int size() {
        return count;
    }

    public List<Range> toList() {
        return Lists.newArrayList(this);
    }
    
    @Override
    public String toString() {
    	return toList().toString();
    }

}
