/*
 * sink.scala
 * 
 * author:  William Benton <willb@redhat.com>
 *
 * Copyright (c) 2010-2016 Red Hat, Inc.
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

package io.radanalytics.silex.util



/**
  * On-line mean and variance estimates for a stream of fractional values. Uses
  * Chan's formulae. 
  *
  * Type S must have a <tt>scala.math.Fractional[S]</tt> witness in scope, a
  * <tt>NumericLimits[S]</tt> witness in scope, and a function converting from
  * Long to S in scope. Scala provides the former and the latter for its
  * fractional numeric types; the SampleSink companion object provides witnesses
  * for<tt>NumericLimits[Float]</tt> and <tt>NumericLimits[Double]</tt>.
  *
  */
sealed class SampleSink[S](private var _count: Long, private var _min: S, private var _max: S, private var _mean: S, private var _m2: S)(implicit num: Fractional[S], long2s: (Long => S), lim: NumericLimits[S]) extends java.io.Serializable {
  import num._
  import SampleSink._
  
  @inline def put(sample: S) = {
    val dev = sample - _mean
    _mean = _mean + (dev / long2s(count + 1))
    _m2 = _m2 + (dev * dev) * long2s(count / (count + 1))
    _count = count + 1
    _min = num.min(_min, sample)
    _max = num.max(_max, sample)
  }
  
  def ++(other: SampleSink[S]): SampleSink[S] = {
    val result = SampleSink.emptyOf[S] += this
    result += other
  }

  def +=(other: SampleSink[S]): SampleSink[S] = {
    if(other.count == 0L) {
      this
    } else if (this.count == 0L) {
      this._mean = other.mean
      this._count = other.count
      this._min = other.min
      this._max = other.max
      this._m2 = other._m2
      this
    } else {
      val dev = other.mean - mean
      val newCount = long2s(other.count + count)
      val newMean = (long2s(this.count) * this.mean + long2s(other.count) * other.mean) / newCount
      val newM2 = this._m2 + other._m2 + (dev * dev) * long2s(this.count) * long2s(other.count) / newCount
      this._count = newCount.toLong
      this._min = num.min(this.min, other.min)
      this._max = num.max(this.max, other.max)
      this._mean = newMean
      this._m2 = newM2

      this
    }
  }

  @inline def mean = _mean
  @inline def count = _count
  @inline def min = _min
  @inline def max = _max
  @inline def variance = _m2 / long2s(count)
  
  def stddev = math.sqrt(variance.toDouble)
  
  override def toString = s"SampleSink(count=$count, min=$min, max=$max, mean=$mean, variance=$variance)"
}

object SampleSink {  
  case object DoubleLimits extends NumericLimits[Double] {
    override val minimum = Double.MinValue
    override val maximum = Double.MaxValue
  }

  case object FloatLimits extends NumericLimits[Float] {
    override val minimum = Float.MinValue
    override val maximum = Float.MaxValue
  }
  
  implicit val doubleRange: NumericLimits[Double] = DoubleLimits
  implicit val floatRange: NumericLimits[Float] = FloatLimits
  
  def emptyOf[S](implicit num: Fractional[S], lim: NumericLimits[S], conv: (Long => S)): SampleSink[S] = new SampleSink[S](0, lim.maximum, lim.minimum, num.zero, num.zero)
  
  def empty = emptyOf[Double]
}


trait NumericLimits[T] {
  /** The largest positive value representable as a <tt>T</tt> */
  val minimum: T

  /** The negative value with the largest magnitude representable as a <tt>T</tt> */
  val maximum: T
} 
