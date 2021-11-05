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

package org.apache.spark.sql.execution

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

import org.apache.spark.Partitioner
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.CurveIndex
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, AtomicType, StructType}
import org.apache.spark.util.CollectionsUtils
import org.apache.spark.util.random.SamplingUtils

/**
 * Copy from RangePartitioner with some modifications. Use {@CurveIndex curve index} to generate
 * curve value to order input elements.
 */
class CurveRangePartitioner(
    partitions: Int,
    rdd: RDD[_ <: Product2[InternalRow, Null]],
    orders: Seq[SortOrder],
    curveIndex: CurveIndex,
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20)
  extends Partitioner with Logging {

  // A constructor declared in order to maintain backward compatibility for Java, when we add the
  // 4th constructor parameter samplePointsPerPartitionHint. See SPARK-22160.
  // This is added to make sure from a bytecode point of view, there is still a 3-arg ctor.
  def this(
      partitions: Int,
      rdd: RDD[_ <: Product2[InternalRow, Null]],
      orders: Seq[SortOrder],
      curveIndex: CurveIndex,
      ascending: Boolean) = {
    this(partitions, rdd, orders, curveIndex, ascending, samplePointsPerPartitionHint = 20)
  }

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")
  require(samplePointsPerPartitionHint > 0,
    s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

  private implicit val ordering = implicitly[Ordering[Long]]

  private var dimensionBounds: Array[(SortOrder, Ordering[Any], Array[Any])] = Array.empty
  private var rangeBounds: Array[Long] = null
  // An array of upper bounds for the first (partitions - 1) partitions
  rangeBounds = {
    if (partitions <= 1) {
      Array.empty[Long]
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // Cast to double to avoid overflowing ints or longs
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = CurveRangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty[Long]
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(InternalRow, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }

        logDebug(s"Total sampled ${candidates.size} rows to do partitioning.")

        val candidatesAndWeightsWithIndex = orders.map(orderKey => {
          val cardinalityWithWeights = candidates.map {
            case (row, weight) =>
              (orderKey.child.eval(row), weight)
          }.filter(_._1 != null)
            .groupBy(_._1)
            .mapValues(_.map(_._2).sum)
            .toSeq
          logDebug(s"Sampled cardinality on order key ${orderKey.child.toString} =" +
            s" ${cardinalityWithWeights.size}")
          cardinalityWithWeights
        }).zipWithIndex

        def getOrdering(idx: Int): Ordering[Any] = {
          orders(idx).dataType match {
            case dt: AtomicType =>
              dt.ordering.asInstanceOf[Ordering[Any]]
            case a: ArrayType =>
              a.interpretedOrdering.asInstanceOf[Ordering[Any]]
            case s: StructType =>
              s.interpretedOrdering.asInstanceOf[Ordering[Any]]
            case other =>
              throw new IllegalArgumentException(s"Type $other does not" +
                s" support ordered operations")
          }
        }

        val sortedKeyAndWeightsWithIndex = candidatesAndWeightsWithIndex.sortBy(_._1.size)
        var totalOverheadPartitions = partitions * SQLConf.get.zorderEstimatedPartitionsFactor
        val shouldNotCompute = candidatesAndWeightsWithIndex
          .map(_._1.size)
          .filter(_ > 0)
          .foldLeft(totalOverheadPartitions)((rest, card) => rest / card) >= 1

        val computedDimensionBounds = for (i <- sortedKeyAndWeightsWithIndex.indices) yield {
          val aggCandidates = sortedKeyAndWeightsWithIndex(i)._1
          val orderIdx = sortedKeyAndWeightsWithIndex(i)._2
          implicit val ordering = getOrdering(orderIdx)

          val estimatedPartitions = if (!shouldNotCompute) {
            val avgDimensionSize = math.ceil(
              math.pow(totalOverheadPartitions, 1d / (orders.size - i))).toInt
            val estimatedPartitions = math.min(avgDimensionSize, aggCandidates.size)
            totalOverheadPartitions = math.ceil(
              totalOverheadPartitions.toDouble / estimatedPartitions).toInt
            estimatedPartitions
          } else {
            aggCandidates.size
          }

          logDebug(s"Estimated partitions on order key ${orders(orderIdx).child.toString} =" +
            s" $estimatedPartitions")

          val dimensionBoundary = CurveRangePartitioner.determineBounds(
            aggCandidates, estimatedPartitions)

          (orders(orderIdx), ordering, dimensionBoundary)
        }

        dimensionBounds = computedDimensionBounds.toArray

        val indexCandidates = candidates.map {
          case (row, weight) =>
            (mappingIndex(row), weight)
        }

        CurveRangePartitioner.determineBounds(indexCandidates,
          math.min(partitions, candidates.size))
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private val binarySearch: ((Array[Long], Long) => Int) = CollectionsUtils.makeBinarySearch[Long]

  def getPartition(key: Any): Int = {
    if (partitions == 1) {
      0
    } else {
      val k = mappingIndex(key.asInstanceOf[InternalRow])
      var partition = 0
      if (rangeBounds.length <= 128) {
        // If we have less than 128 partitions naive search
        while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
          partition += 1
        }
      } else {
        // Determine which binary search method to use only once.
        partition = binarySearch(rangeBounds, k)
        // binarySearch either returns the match location or -[insertion point]-1
        if (partition < 0) {
          partition = -partition-1
        }
        if (partition > rangeBounds.length) {
          partition = rangeBounds.length
        }
      }
      if (ascending) {
        partition
      } else {
        rangeBounds.length - partition
      }
    }
  }

  private def mappingIndex(row: InternalRow): Long = {
    val indexes = dimensionBounds.map { case (order, ordering, boundary) =>
      val value = order.child.eval(row)
      if (value == null) {
        0
      } else {
        var partition = 0
        while (partition < boundary.length &&
          ordering.gt(value, boundary(partition))) {
          partition += 1
        }
        partition.toLong
      }
    }
    curveIndex.index(indexes: _*)
  }

  override def equals(other: Any): Boolean = other match {
    case r: CurveRangePartitioner =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }
}

private[spark] object CurveRangePartitioner {

  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K : ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   *
   * @param candidates unordered candidates with weights
   * @param partitions number of partitions
   * @return selected bounds
   */
  def determineBounds[K : Ordering : ClassTag](
      candidates: Seq[(K, Float)],
      partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}
