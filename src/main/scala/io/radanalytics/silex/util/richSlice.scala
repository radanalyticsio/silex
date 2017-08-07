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

package io.radanalytics.silex.util

/**
  * @define rsExamples {{{
  * import io.radanalytics.silex.util.richslice._
  * RichSlice(1)             // selects element(1)
  * RichSlice(2, 3, 5 to 11) // selects elements 2, 3 and 5 through 11
  * RichSlice(9 to 0 by -1)  // selects first 10 elements in reverse order
  * RichSlice(20 to * by 2)  // selects elements 20 to end, by 2s
  * }}}
  *
  * @define rsStarExamples {{{
  * import io.radanalytics.silex.util.richslice._
  * data.richSlice(* to 5)   // selects elements 0 through 5
  * data.richSlice(5 to *)   // selects elmeents 5 up to end of data
  * data.richSlice(*)        // selects all data
  * data.richSlice(* by -1)  // selects data in reverse order
  * }}}
  */
object richslice {
  import scala.language.higherKinds
  import scala.collection.SeqLike
  import scala.collection.generic.CanBuildFrom
  import scala.language.implicitConversions
  import scala.collection.immutable.Range

  /** Internal mechanism to support use of '*' in rich slice expressions as a wildcard index */
  sealed trait Star {
    def to(j: Int): Range = starIdx to j
    def to(s: Star): Range = starIdx to starIdx
    def until(j: Int): Range = starIdx until j
    def until(s: Star): Range = starIdx until starIdx
    def by(step: Int): Range = starIdx until starIdx by step
  }

  /** Internal mechanism to support use of '*' in rich slice expressions as a wildcard index */
  object star extends Star
  private val starIdx = scala.Int.MinValue

  /** Internal mechanism to support use of '*' in rich slice expressions as a wildcard index */
  implicit class StarToRange(n: Int) {
    def to(s: Star): Range = n to starIdx
    def until(s: Star): Range = n until starIdx
  }

  /** When '*' is used in place of a numeric index, it matches all indices up to or after a 
    * bound.
    * $rsStarExamples
    */
  def * = star

  /** One slice in a rich slice expression list */
  sealed trait Slice
  private case class IndexS(idx: Int) extends Slice
  private case class RangeS(beg: Int, end: Int) extends Slice
  private case class RangeStepS(beg: Int, end: Int, step: Int) extends Slice {
    require(step != 0, "RichSlice step values cannot be zero")
  }

  /** Implicit conversions from rich slice expressions to Slice objects */
  object Slice {
    implicit def intToSlice(idx: Int): Slice = IndexS(idx)

    implicit def rangeToSlice(r: Range): Slice = {
      val d = if (!r.isInclusive || r.end == starIdx) 0 else if (r.step > 0) 1 else -1
      if (r.step == 1) RangeS(r.start, r.end + d) else RangeStepS(r.start, r.end + d, r.step)
    }

    implicit def starToSlice(s: Star): Slice = RangeS(starIdx, starIdx)
  }

  /** A Rich Slice object, containing a sequence of slice expressions 
    * $rsExamples
    */
  case class RichSlice(slices: List[Slice]) extends Serializable

  /** Factory methods for instantiating RichSlice objects 
    * $rsExamples
     */
  object RichSlice {
    /** Create a RichSlice object from zero or more individual slice expressions.  RichSlice objects
      * can be created and then given to the rich slice enhanced Seq methods.
      * $rsExamples
      */
    def apply(slices: Slice*): RichSlice = RichSlice(slices.toList)
  }

  /** Provides rich slice methods on Scala Seq types */
  implicit class RichSliceMethods[A, S[E] <: SeqLike[E, S[E]]](seq: S[A]) {
    private val N = seq.length

    /** Create an iterator that iterates over the sequence elements selcted by zero or more
      * slice expresions.
      * {{{
      * import io.radanalytics.silex.util.richslice._
      * data.richSliceIterator(1)             // iterate over data(1)
      * data.richSliceIterator(2, 3, 5 to 11) // over data(2), data(3) and data(5) through data(11)
      * data.richSliceIterator(9 to 0 by -1)  // over 1st 10 elements of data in reverse
      * data.richSliceIterator(20 to * by 2)  // over data(20) to end, by 2s
      * }}}
      */
    def richSliceIterator(slices: Slice*): Iterator[A] =
      richSliceIterator(RichSlice(slices.toList))

    /** Create an iterator that iterates over the sequence elements selected by the given
      * rich slice object
      * {{{
      * import io.radanalytics.silex.util.richslice._
      * val rs = RichSlice(2, 3, 5 to 11)
      * data.richSliceIterator(rs) // iterate over data(2), data(3) and data(5) through data(11)
      * }}}
      */
    def richSliceIterator(rs: RichSlice): Iterator[A] = {
      val itrList = rs.slices.map(resolveNegative).map {
        case IndexS(j) => Iterator.single(seq(j))
        case RangeS(beg, end) => {
          if (beg == starIdx) {
            if (end == starIdx) seq.iterator else seq.iterator.take(end)
          } else {
            if (end == starIdx) seq.iterator.drop(beg) else seq.iterator.slice(beg, end)
          }
        }
        case RangeStepS(beg, end, step) => {
          if (beg == starIdx) {
            if (end == starIdx) {
              if (step > 0) new IterU(seq, 0, N, step) else new IterD(seq, N-1, -1, step)
            } else {
              if (step > 0) new IterU(seq, 0, end, step) else new IterD(seq, N-1, end, step)
            }
          } else {
            if (end == starIdx) {
              if (step > 0) new IterU(seq, beg, N, step) else new IterD(seq, beg, -1, step)
            } else {
              if (step > 0) new IterU(seq, beg, end, step) else new IterD(seq, beg, end, step)
            }
          }
        }
      }
      if (itrList.length == 0) Iterator.empty
      else if (itrList.length == 1) itrList(0)
      else itrList.fold(Iterator.empty)(_ ++ _)
    }

    /** Create a new sub sequence from the elements selcted by zero or more
      * slice expresions.
      * {{{
      * import io.radanalytics.silex.util.richslice._
      * data.richSlice(1)             // select data(1)
      * data.richSlice(2, 3, 5 to 11) // select data(2), data(3) and data(5) through data(11)
      * data.richSlice(9 to 0 by -1)  // select 1st 10 elements of data in reverse
      * data.richSlice(20 to * by 2)  // select data(20) to end, by 2s
      * }}}
      */
    def richSlice(slices: Slice*)(implicit cbf: CanBuildFrom[Nothing, A, S[A]]): S[A] =
      richSliceIterator(RichSlice(slices.toList)).to[S]

    /** Create a new subsequence from the elmenents selected by a given rich slice object
      * {{{
      * import io.radanalytics.silex.util.richslice._
      * val rs = RichSlice(2, 3, 5 to 11)
      * data.richSlice(rs) // select data(2), data(3) and data(5) through data(11)
      * }}}
      */
    def richSlice(rs: RichSlice)(implicit cbf: CanBuildFrom[Nothing, A, S[A]]): S[A] =
      richSliceIterator(rs).to[S]

    /** Used internally by rich slice iterators */
    class IterU(data: S[A], beg: Int, end: Int, step: Int) extends Iterator[A] {
      require(step > 0)
      var j = if (beg < end) beg else end
      def hasNext = j < end
      def next = {
        val r = data(j)
        j += step
        r
      }
    }

    /** Used internally by rich slice iterators */
    class IterD(data: S[A], beg: Int, end: Int, step: Int) extends Iterator[A] {
      require(step < 0)
      var j = if (beg > end) beg else end
      def hasNext = j > end
      def next = {
        val r = data(j)
        j += step
        r
      }
    }

    private def resolveNegative(slice: Slice) = {
      def resolve(j: Int, step: Int = 1) = {
        if (j >= 0) j
        else if (j == starIdx) j
        else if (j == -1  &&  step < 0) j
        else N + j
      }
      slice match {
        case IndexS(j) => IndexS(resolve(j))
        case RangeS(b, e) => RangeS(resolve(b), resolve(e))
        case RangeStepS(b, e, s) => RangeStepS(resolve(b), resolve(e, s), s)
      }
    }
  }
}
