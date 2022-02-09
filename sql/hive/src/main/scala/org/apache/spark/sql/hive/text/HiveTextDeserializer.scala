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

package org.apache.spark.sql.hive.text

import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{BinaryObjectInspector, BooleanObjectInspector, ByteObjectInspector, DateObjectInspector, DoubleObjectInspector, FloatObjectInspector, HiveCharObjectInspector, HiveDecimalObjectInspector, HiveVarcharObjectInspector, IntObjectInspector, LongObjectInspector, ShortObjectInspector, TimestampObjectInspector}
import org.apache.hadoop.io._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.hive.HadoopTableReader.unwrapperFor
import org.apache.spark.sql.hive.HiveShim
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * A deserializer to deserialize Text to Spark rows.
 */
class HiveTextDeserializer(
    tableDeser: Deserializer,
    partDeser: Deserializer,
    mutableRow: InternalRow,
    requiredSchema: StructType) {

  private val soi = if (partDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
    partDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
  } else {
    ObjectInspectorConverters.getConvertedOI(
      partDeser.getObjectInspector,
      tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
  }

  private val (fieldRefs, fieldOrdinals) =
    requiredSchema.toAttributes.zipWithIndex.map { case (f, index) =>
      soi.getStructFieldRef(f.name) -> index
    }.toArray.unzip

  def deserialize(line: Text): InternalRow = {

    val unwrappers: Seq[(Any, InternalRow, Int) => Unit] = fieldRefs.map {
      _.getFieldObjectInspector match {
        case oi: BooleanObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
        case oi: ByteObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
        case oi: ShortObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
        case oi: IntObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
        case oi: LongObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
        case oi: FloatObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
        case oi: DoubleObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
        case oi: HiveVarcharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveCharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveDecimalObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, HiveShim.toCatalystDecimal(oi, value))
        case oi: TimestampObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(oi.getPrimitiveJavaObject(value)))
        case oi: DateObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setInt(ordinal, DateTimeUtils.fromJavaDate(oi.getPrimitiveJavaObject(value)))
        case oi: BinaryObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, oi.getPrimitiveJavaObject(value))
        case oi =>
          val unwrapper = unwrapperFor(oi)
          (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
      }
    }

    val converter = ObjectInspectorConverters.getConverter(partDeser.getObjectInspector, soi)

    val raw = converter.convert(partDeser.deserialize(line))
    var i = 0
    val length = fieldRefs.length
    while (i < length) {
      try {
        val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
        if (fieldValue == null) {
          mutableRow.setNullAt(fieldOrdinals(i))
        } else {
          unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
        }
        i += 1
      } catch {
        case ex: Throwable =>
          throw ex
      }
    }

    mutableRow: InternalRow
  }

}
