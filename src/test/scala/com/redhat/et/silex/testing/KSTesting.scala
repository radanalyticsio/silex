/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2015 Red Hat, Inc.
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
 * limitations under the License.
 */

package com.redhat.et.silex.testing

/**
  * There are no actual KS tests implemented for scala (that I can find) - and so what I
  * have done here is pre-compute "D" - the KS statistic - that corresponds to a "weak"
  * p-value for a particular sample size.  I can then test that my measured KS stats
  * are less than D.  Computing D-values is easy, and implemented below.
  *
  * I used the scipy 'kstwobign' distribution to pre-compute my D value:
  *
  * def ksdval(q=0.1, n=1000):
  *     en = np.sqrt(float(n) / 2.0)
  *     return stats.kstwobign.isf(float(q)) / (en + 0.12 + 0.11 / en)
  *
  * When comparing KS stats I take the median of a small number of independent test runs
  * to compensate for the issue that any sampled statistic will show "false positive" with
  * some probability.  Even when two distributions are the same, they will register as
  * different 10% of the time at a p-value of 0.1
  */
object KSTesting {

  // This D value is the precomputed KS statistic for p-value 0.1, sample size 1000:
  // to-do: support other predefined sample sizes and p-values?  maybe via lookup table?
  val sampleSize = 1000
  val D = 0.0544280747619

  // An Iterator model of a random variable.
  // An infinite, lazy, memory-less stream of values that can be sampled from
  // Note that I'm requiring Traversable[T] instead of TraversableOnce[T] for a reason:
  // A block expression that produces an iterator can be problematic if it is something like:
  // SamplingIterator { itr }
  // because 'itr' will only yield data the first time, and then show up as empty thereafter.
  class SamplingIterator[T](dataBlock: => Traversable[T]) extends Iterator[T] with Serializable {
    var data = Iterator.continually(()).flatMap { u => dataBlock }

    def hasNext = data.hasNext
    def next = data.next

    override def filter(f: T => Boolean) = new SamplingIterator(data.filter(f).toStream)
    override def map[U](f: T => U) = new SamplingIterator(data.map(f).toStream)
    override def flatMap[U](f: T => scala.collection.GenTraversableOnce[U]) =
      new SamplingIterator(data.flatMap(f).toStream)

    def sample(n: Int) = Vector.fill(n) { data.next }

    def fork = {
      val (dup1, dup2) = data.duplicate
      data = dup1
      new SamplingIterator(dup2.toStream)
    }
  }

  object SamplingIterator {
    def apply[T](data: => Traversable[T]) = new SamplingIterator(data)
    def continually[T](value: => T) = new SamplingIterator(Stream.continually(value))
    implicit class ToSamplingIterator[T](data: TraversableOnce[T]) {
      def toSamplingIterator = new SamplingIterator(data.toStream)
    }
  }

  // Return a Kolmogorov-Smirnov 'D' value for two data samples
  def KSD[N](data1: SamplingIterator[N], data2: SamplingIterator[N])
      (implicit ord: math.Ordering[N]) = {
    val (c1, c2) = cumulants(data1.sample(sampleSize), data2.sample(sampleSize))
    KSDStatistic(c1, c2)
  }

  // Returns the median KS 'D' statistic between two samples, over (m) sampling trials
  def medianKSD[N](data1: SamplingIterator[N], data2: SamplingIterator[N], m: Int = 5)
      (implicit ord: math.Ordering[N]) = {
    require(m > 0)
    val ksd = (Vector.fill(m) { KSD(data1, data2) }).sorted
    ksd(m/2)
  }

  // Computes the Kolmogorov-Smirnov 'D' statistic from two cumulative distributions.
  // Assumes cdf1 and cdf2 are aligned: i.e. they are the same length and cdf1(j) 
  // corresponds to cdf2(j) contain the cdf value at the same point for all (j).
  def KSDStatistic(cdf1: Seq[Double], cdf2: Seq[Double]) = {
    require(cdf1.length == cdf2.length)
    val n = cdf1.length
    require(n > 0)
    require(cdf1(n-1) == 1.0)
    require(cdf2(n-1) == 1.0)
    cdf1.iterator.zip(cdf2.iterator).map { x => Math.abs(x._1 - x._2) }.max
  }

  // Returns aligned cumulative distributions from two arrays of data
  // Type parameter 'N' can be any type with an Ordering
  private def cumulants[N](d1: Seq[N], d2: Seq[N])(implicit ord: math.Ordering[N]) = {
    require(math.min(d1.length, d2.length) > 0)
    val (m1, m2) = (hist(d1), hist(d2))
    val itr1 = m1.toVector.sortBy(_._1).iterator.buffered
    val itr2 = m2.toVector.sortBy(_._1).iterator.buffered
    val h1 = scala.collection.mutable.ArrayBuffer.empty[Int]
    val h2 = scala.collection.mutable.ArrayBuffer.empty[Int]
    // Construct aligned histograms via merging logic
    // Assumes a pair of buffered iterators, each in sorted order
    while (itr1.hasNext && itr2.hasNext) {
      val (x1, c1) = itr1.head
      val (x2, c2) = itr2.head
      if (ord.lt(x1, x2)) {
        h1 += c1
        h2 += 0
        itr1.next
      } else if (ord.lt(x2, x1)) {
        h1 += 0
        h2 += c2
        itr2.next
      } else {
        h1 += c1
        h2 += c2
        itr1.next
        itr2.next
      }
    }
    // At most one of these will have any data left after previous loop
    while (itr1.hasNext) {
      val (x1, c1) = itr1.next
      h1 += c1
      h2 += 0
    }
    while (itr2.hasNext) {
      val (x2, c2) = itr2.next
      h1 += 0
      h2 += c2
    }
    require(h1.sum == h2.sum)
    (cumulativeDist(h1), cumulativeDist(h2))
  }

  // Returns the cumulative distribution from a histogram
  private def cumulativeDist(hist: Seq[Int]) = {
    val n = hist.sum.toDouble
    require(n > 0.0)
    hist.iterator.scanLeft(0)(_ + _).drop(1).map(_.toDouble / n).toVector
  }

  private def hist[N](data: Seq[N]) = {
    val map = scala.collection.mutable.Map.empty[N, Int]
    data.foreach { x =>
      map += ((x, 1 + map.getOrElse(x, 0)))
    }
    map
  }
}
