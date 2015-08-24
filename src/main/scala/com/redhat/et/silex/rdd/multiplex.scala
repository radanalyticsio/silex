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
 * limitations under the License.c
 */

package com.redhat.et.silex.rdd.multiplex

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging, Partition, TaskContext, 
                         Dependency, NarrowDependency, OneToOneDependency}

import org.apache.spark.storage.StorageLevel

/**
 * Enhance RDDs with methods for generating multiplexed RDDs
 * @tparam T the element type of the RDD
 * {{{
 * // enable multiplexing methods
 * import com.redhat.et.silex.rdd.multiplex.implicits._
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
class MuxRDDFunctions[T :ClassTag](self: RDD[T]) extends Logging with Serializable {
  import MuxRDDFunctions.defaultSL

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from the jth element
   * returned by applying (f) to each input partition.
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition into a sequence of (n) objects of type U
   * @return The sequence of RDDs, as described above
   */
  def muxPartitions[U :ClassTag](
    n: Int, f: Iterator[T] => Seq[U]):
      Seq[RDD[U]] =
    muxPartitions(n, (id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from the jth element
   * returned by applying (f) to each input partition and its id.
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition, along with that partition's (id) value,
   * into a sequence of (n) objects of type U
   * @return The sequence of RDDs, as described above
   */
  def muxPartitions[U :ClassTag](
    n: Int, f: (Int, Iterator[T]) => Seq[U]):
      Seq[RDD[U]] =
    muxPartitions(n, f, defaultSL)

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from the jth element
   * returned by applying (f) to each input partition.
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition into a sequence of (n) objects of type U
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The sequence of RDDs, as described above
   */
  def muxPartitions[U :ClassTag](
    n: Int, f: Iterator[T] => Seq[U],
    persist: StorageLevel):
      Seq[RDD[U]] =
    muxPartitions(n, (id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from the jth element
   * returned by applying (f) to each input partition and its id.
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition, along with that partition's (id) value,
   * into a sequence of (n) objects of type U
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The sequence of RDDs, as described above
   */
  def muxPartitions[U :ClassTag](
    n: Int, f: (Int, Iterator[T]) => Seq[U],
    persist: StorageLevel):
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
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: Iterator[T] => (U1, U2)):
      (RDD[U1], RDD[U2]) =
    muxPartitions((id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: (Int, Iterator[T]) => (U1, U2)):
      (RDD[U1], RDD[U2]) =
    muxPartitions(f, defaultSL)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition
   * @param f Function maps data from a partition into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: Iterator[T] => (U1, U2),
    persist: StorageLevel):
      (RDD[U1], RDD[U2]) =
    muxPartitions((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: (Int, Iterator[T]) => (U1, U2),
    persist: StorageLevel):
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
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag](
    f: Iterator[T] => (U1, U2, U3)):
      (RDD[U1], RDD[U2], RDD[U3]) =
    muxPartitions((id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag](
    f: (Int, Iterator[T]) => (U1, U2, U3)):
      (RDD[U1], RDD[U2], RDD[U3]) =
    muxPartitions(f, defaultSL)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition
   * @param f Function maps data from a partition into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag](
    f: Iterator[T] => (U1, U2, U3),
    persist: StorageLevel):
      (RDD[U1], RDD[U2], RDD[U3]) =
    muxPartitions((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag](
    f: (Int, Iterator[T]) => (U1, U2, U3),
    persist: StorageLevel):
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
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag](
    f: Iterator[T] => (U1, U2, U3, U4)):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    muxPartitions((id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag](
    f: (Int, Iterator[T]) => (U1, U2, U3, U4)):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    muxPartitions(f, defaultSL)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition
   * @param f Function maps data from a partition into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag](
    f: Iterator[T] => (U1, U2, U3, U4),
    persist: StorageLevel):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    muxPartitions((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag](
    f: (Int, Iterator[T]) => (U1, U2, U3, U4),
    persist: StorageLevel):
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
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: Iterator[T] => (U1, U2, U3, U4, U5)):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    muxPartitions((id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: (Int, Iterator[T]) => (U1, U2, U3, U4, U5)):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    muxPartitions(f, defaultSL)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition
   * @param f Function maps data from a partition into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: Iterator[T] => (U1, U2, U3, U4, U5),
    persist: StorageLevel):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    muxPartitions((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where jth component is obtained from the corresponding component
   * returned by applying (f) to each partition and its id
   * @param f Function maps data from a partition and its id into a tuple of objects
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def muxPartitions[U1 :ClassTag, U2 :ClassTag, U3: ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: (Int, Iterator[T]) => (U1, U2, U3, U4, U5),
    persist: StorageLevel):
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
   * @return The sequence of RDDs, as described above
   */
  def flatMuxPartitions[U :ClassTag](
    n: Int, f: Iterator[T] => Seq[TraversableOnce[U]]):
      Seq[RDD[U]] =
    flatMuxPartitions(n, (id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from flattening the jth elements
   * returned by applying (f) to each input partition and its id
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition and its id into a sequence of (n)
   * sequences of type U
   * @return The sequence of RDDs, as described above
   */
  def flatMuxPartitions[U :ClassTag](
    n: Int, f: (Int, Iterator[T]) => Seq[TraversableOnce[U]]):
      Seq[RDD[U]] =
    flatMuxPartitions(n, f, defaultSL)

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from flattening the jth elements
   * returned by applying (f) to each input partition.
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition into a sequence of (n) sequences of type U
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The sequence of RDDs, as described above
   */
  def flatMuxPartitions[U :ClassTag](
    n: Int, f: Iterator[T] => Seq[TraversableOnce[U]],
    persist: StorageLevel):
      Seq[RDD[U]] =
    flatMuxPartitions(n, (id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a sequence of (n) RDDs where the jth RDD is obtained from flattening the jth elements
   * returned by applying (f) to each input partition and its id
   * @param n The length of sequence returned from (f)
   * @param f Function maps data from a partition and its id into a sequence of (n)
   * sequences of type U
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The sequence of RDDs, as described above
   */
  def flatMuxPartitions[U :ClassTag](
    n: Int, f: (Int, Iterator[T]) => Seq[TraversableOnce[U]],
    persist: StorageLevel):
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
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2])):
      (RDD[U1], RDD[U2]) =
    flatMuxPartitions((id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2])):
      (RDD[U1], RDD[U2]) =
    flatMuxPartitions(f, defaultSL)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition.
   * @param f Function maps data from a partition into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2]),
    persist: StorageLevel):
      (RDD[U1], RDD[U2]) =
    flatMuxPartitions((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2]),
    persist: StorageLevel):
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
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3])):
      (RDD[U1], RDD[U2], RDD[U3]) =
    flatMuxPartitions((id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3])):
      (RDD[U1], RDD[U2], RDD[U3]) =
    flatMuxPartitions(f, defaultSL)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition.
   * @param f Function maps data from a partition into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3]),
    persist: StorageLevel):
      (RDD[U1], RDD[U2], RDD[U3]) =
    flatMuxPartitions((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3]),
    persist: StorageLevel):
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
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4])):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    flatMuxPartitions((id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4])):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    flatMuxPartitions(f, defaultSL)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition.
   * @param f Function maps data from a partition into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4]),
    persist: StorageLevel):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4]) =
    flatMuxPartitions((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4]),
    persist: StorageLevel):
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
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4], TraversableOnce[U5])):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    flatMuxPartitions((id: Int, itr: Iterator[T]) => f(itr), defaultSL)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4], TraversableOnce[U5])):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    flatMuxPartitions(f, defaultSL)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition.
   * @param f Function maps data from a partition into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: Iterator[T] => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4], TraversableOnce[U5]),
    persist: StorageLevel):
      (RDD[U1], RDD[U2], RDD[U3], RDD[U4], RDD[U5]) =
    flatMuxPartitions((id: Int, itr: Iterator[T]) => f(itr), persist)

  /** Obtain a tuple of RDDs where the jth component is obtained from flattening the corresponding
   * components returned by applying (f) to each input partition and its id.
   * @param f Function maps data from a partition and its id into a tuple of sequences
   * @param persist The storage level to apply to the intermediate result returned by (f)
   * @return The tuple of RDDs, as described above
   */
  def flatMuxPartitions[U1 :ClassTag, U2 :ClassTag, U3 :ClassTag, U4 :ClassTag, U5 :ClassTag](
    f: (Int, Iterator[T]) => (TraversableOnce[U1], TraversableOnce[U2], TraversableOnce[U3], TraversableOnce[U4], TraversableOnce[U5]),
    persist: StorageLevel):
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
