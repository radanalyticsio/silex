/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2016 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package io.radanalytics.silex.rdd.multiplex

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Partition, TaskContext, 
                         Dependency, NarrowDependency, OneToOneDependency}

import org.apache.spark.storage.StorageLevel

/**
 * Enhance RDDs with methods for generating multiplexed RDDs
 * @tparam T the element type of the RDD
 * {{{
 * // enable multiplexing methods
 * import io.radanalytics.silex.rdd.multiplex.implicits._
 *
 * // A boolean predicate on data elements
 * val pred: Int => Boolean = ....
 *
 * // pos will contain data elements for which 'pred' was true.
 * // neg will contain elements for which 'pred' was false.
 * val (pos, neg) = data.flatMuxPartitions((data: Iterator[Int]) => {
 *   val pT = scala.collection.mutable.ArrayBuffer.empty[Int]
 *   val pF = scala.collection.mutable.ArrayBuffer.empty[Int]
 *   data.foreach { e => (if (pred(e)) pT else pF) += e }
 *   (pT, pF)
 * })
 * }}}
 */
class MuxRDDFunctions[T :ClassTag](self: RDD[T]) extends Serializable {
  import MuxRDDFunctions.defaultSL

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from the jth element
   * returned by applying (f) to each input partition.
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition into a sequence of (n) objects of type U
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The sequence of RDDs, as described above
   */
  def muxPartitions[U :ClassTag](
    n: Int, f: Iterator[T] => Seq[U],
    persist: StorageLevel = defaultSL):
      Seq[RDD[U]] =
    muxPartitionsWithIndex(n, (id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from the jth element
   * returned by applying (f) to each input partition and its id.
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition, along with that partition's (id) value,
   * into a sequence of (n) objects of type U
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The sequence of RDDs, as described above
   */
  def muxPartitionsWithIndex[U :ClassTag](
    n: Int, f: (Int, Iterator[T]) => Seq[U],
    persist: StorageLevel = defaultSL):
      Seq[RDD[U]] = {
    require(n >= 0, "expected sequence length must be >= 0")
    val mux = self.mapPartitionsWithIndex { case (id, itr) =>
      val r = f(id, itr)
      require(r.length == n, s"multiplexed sequence did not have expected length $n")
      Iterator.single(r)
    }.persist(persist)
    Vector.tabulate(n) { j => mux.mapPartitions { itr => Iterator.single(itr.next()(j)) } }
  }

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition
   * @param f Function maps data from a partition into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def mux2Partitions[U1 :ClassTag, U2 :ClassTag](
    f: Iterator[T] => (U1, U2),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2]) =
    mux2PartitionsWithIndex((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def mux2PartitionsWithIndex[U1 :ClassTag, U2 :ClassTag](
    f: (Int, Iterator[T]) => (U1, U2),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2]) = {
    val mux = self.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1))
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2))
    (mux1, mux2)
  }

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition
   * @param f Function maps data from a partition into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def mux3Partitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag](
    f: Iterator[T] => (U1, U2, U3),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3]) =
    mux3PartitionsWithIndex((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def mux3PartitionsWithIndex[U1 :ClassTag, U2 :ClassTag, U3: ClassTag](
    f: (Int, Iterator[T]) => (U1, U2, U3),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3]) = {
    val mux = self.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1))
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2))
    val mux3 = mux.mapPartitions(itr => Iterator.single(itr.next._3))
    (mux1, mux2, mux3)
  }

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition
   * @param f Function maps data from a partition into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def mux4Partitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag](
    f: Iterator[T] => (U1, U2, U3, U4),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    mux4PartitionsWithIndex((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def mux4PartitionsWithIndex[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag](
    f: (Int, Iterator[T]) => (U1, U2, U3, U4),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) = {
    val mux = self.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1))
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2))
    val mux3 = mux.mapPartitions(itr => Iterator.single(itr.next._3))
    val mux4 = mux.mapPartitions(itr => Iterator.single(itr.next._4))
    (mux1, mux2, mux3, mux4)
  }

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition
   * @param f Function maps data from a partition into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def mux5Partitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: Iterator[T] => (U1, U2, U3, U4, U5),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    mux5PartitionsWithIndex((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def mux5PartitionsWithIndex[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: (Int, Iterator[T]) => (U1, U2, U3, U4, U5),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) = {
    val mux = self.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    val mux1 = mux.mapPartitions(itr => Iterator.single(itr.next._1))
    val mux2 = mux.mapPartitions(itr => Iterator.single(itr.next._2))
    val mux3 = mux.mapPartitions(itr => Iterator.single(itr.next._3))
    val mux4 = mux.mapPartitions(itr => Iterator.single(itr.next._4))
    val mux5 = mux.mapPartitions(itr => Iterator.single(itr.next._5))
    (mux1, mux2, mux3, mux4, mux5)
  }

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from flattening the jth elements
   * returned by applying (f) to each input partition.
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition into a sequence of (n) sequences of type U
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The sequence of RDDs, as described above
   */
  def flatMuxPartitions[U :ClassTag](
    n: Int, f: Iterator[T] => Seq[TraversableOnce[U]],
    persist: StorageLevel = defaultSL):
      Seq[RDD[U]] =
    flatMuxPartitionsWithIndex(n, (id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from flattening the jth elements
   * returned by applying (f) to each input partition and its id
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition and its id into a sequence of (n)
   * sequences of type U
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The sequence of RDDs, as described above
   */
  def flatMuxPartitionsWithIndex[U :ClassTag](
    n: Int, f: (Int, Iterator[T]) => Seq[TraversableOnce[U]],
    persist: StorageLevel = defaultSL):
      Seq[RDD[U]] = {
    require(n >= 0, "expected sequence length must be >= 0")
    val mux = self.mapPartitionsWithIndex { case (id, itr) =>
      val r = f(id, itr)
      require(r.length == n, s"multiplexed sequence was not expected length $n")
      Iterator.single(r)
    }.persist(persist)
    Vector.tabulate(n) { j => mux.mapPartitions { itr => itr.next()(j).toIterator } }
  }

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition.
   * @param f Function maps data from a partition into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMux2Partitions[U1 :ClassTag, U2 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2]),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2]) =
    flatMux2PartitionsWithIndex((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMux2PartitionsWithIndex[U1 :ClassTag, U2 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2]),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2]) = {
    val mux = self.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    val mux1 = mux.mapPartitions(itr => itr.next._1.toIterator)
    val mux2 = mux.mapPartitions(itr => itr.next._2.toIterator)
    (mux1, mux2)
  }

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition.
   * @param f Function maps data from a partition into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMux3Partitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3]),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3]) =
    flatMux3PartitionsWithIndex((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMux3PartitionsWithIndex[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3]),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3]) = {
    val mux = self.mapPartitionsWithIndex { case(id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    val mux1 = mux.mapPartitions(itr => itr.next._1.toIterator)
    val mux2 = mux.mapPartitions(itr => itr.next._2.toIterator)
    val mux3 = mux.mapPartitions(itr => itr.next._3.toIterator)
    (mux1, mux2, mux3)
  }

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition.
   * @param f Function maps data from a partition into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMux4Partitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4]),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    flatMux4PartitionsWithIndex((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMux4PartitionsWithIndex[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4]),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) = {
    val mux = self.mapPartitionsWithIndex { case (id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    val mux1 = mux.mapPartitions(itr => itr.next._1.toIterator)
    val mux2 = mux.mapPartitions(itr => itr.next._2.toIterator)
    val mux3 = mux.mapPartitions(itr => itr.next._3.toIterator)
    val mux4 = mux.mapPartitions(itr => itr.next._4.toIterator)
    (mux1, mux2, mux3, mux4)
  }

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition.
   * @param f Function maps data from a partition into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMux5Partitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4], TraversableOnce[U5]),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    flatMux5PartitionsWithIndex((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMux5PartitionsWithIndex[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4], TraversableOnce[U5]),
    persist: StorageLevel = defaultSL):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) = {
    val mux = self.mapPartitionsWithIndex { case(id, itr) =>
      Iterator.single(f(id, itr))
    }.persist(persist)
    val mux1 = mux.mapPartitions(itr => itr.next._1.toIterator)
    val mux2 = mux.mapPartitions(itr => itr.next._2.toIterator)
    val mux3 = mux.mapPartitions(itr => itr.next._3.toIterator)
    val mux4 = mux.mapPartitions(itr => itr.next._4.toIterator)
    val mux5 = mux.mapPartitions(itr => itr.next._5.toIterator)
    (mux1, mux2, mux3, mux4, mux5)
  }
}

/** Definitions used by MuxRDDFunctions instances */
object MuxRDDFunctions {
  val defaultSL = StorageLevel.MEMORY_ONLY
}

/** Implicit conversions to enhance RDDs with multiplexing methods */
object implicits {
  import scala.language.implicitConversions
  implicit def toMuxRDDFunctions[T :ClassTag](rdd: RDD[T]): MuxRDDFunctions[T] =
    new MuxRDDFunctions(rdd)
}
