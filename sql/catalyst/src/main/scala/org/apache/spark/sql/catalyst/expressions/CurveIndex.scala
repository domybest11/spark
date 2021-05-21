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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.hilbert.HilbertCurve
import org.apache.spark.sql.catalyst.expressions.zorder.Morton64

trait CurveIndex extends Serializable {
  /**
   * Get the curve value.
   * @param values dimension values.
   * @return long curve value.
   */
  def index(values: Long*): Long
}

object CurveIndex {
  def apply(size: Int, useHibert: Boolean): CurveIndex = {
    if (useHibert) new HibertCurveIndex(size) else new ZOrderCurveIndex(size)
  }
}

/**
 * A ZOrder curve index wrapper.
 * @param size dimension number.
 */
class ZOrderCurveIndex(size: Int) extends CurveIndex {
  private val interleavingIndex = new Morton64(size, 63/size)

  override def index(values: Long*): Long = interleavingIndex.pack(values: _*)
}

/**
 * A Hibert curve index wrapper.
 * @param size dimension number.
 */
class HibertCurveIndex(size: Int) extends CurveIndex {
  private val hibertIndex = HilbertCurve.small().bits(63/size).dimensions(size)

  override def index(values: Long*): Long = hibertIndex.index(values: _*)
}
