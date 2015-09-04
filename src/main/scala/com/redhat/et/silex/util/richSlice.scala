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
 * limitations under the License.c
 */

package com.redhat.et.silex.util

object richslice {
  import scala.language.implicitConversions
  import scala.collection.immutable.Range

  sealed trait Star {
    def to(j: Int): Range = starIdx to j
    def to(s: Star): Range = starIdx to starIdx
    def until(j: Int): Range = starIdx until j
    def until(s: Star): Range = starIdx until starIdx
    def by(step: Int): Range = starIdx until starIdx by step
  }
  object star extends Star
  def * = star
  private val starIdx = scala.Int.MinValue

  implicit class StarToRange(n: Int) {
    def to(s: Star): Range = n to starIdx
    def until(s: Star): Range = n until starIdx
  }

  sealed trait Slice
  private case class IndexS(idx: Int) extends Slice
  private case class RangeS(beg: Int, end: Int) extends Slice
  private case class RangeStepS(beg: Int, end: Int, step: Int) extends Slice {
    require(step != 0, "RichSlice step values cannot be zero")
  }

  object Slice {
    implicit def intToSlice(idx: Int): Slice = IndexS(idx)

    implicit def rangeToSlice(r: Range): Slice = {
      val d = if (!r.isInclusive || r.end == starIdx) 0 else if (r.step > 0) 1 else -1
      if (r.step == 1) RangeS(r.start, r.end + d) else RangeStepS(r.start, r.end + d, r.step)
    }

    implicit def starToSlice(s: Star): Slice = RangeS(starIdx, starIdx)
  }

  case class RichSlice(slices: List[Slice]) extends Serializable

  object RichSlice {
    def apply(slices: Slice*): RichSlice = RichSlice(slices.toList)
  }

  implicit class RichSliceMethods[A](self: Seq[A]) {
    private val N = self.length

    def richSliceIterator(slices: Slice*): Iterator[A] =
      richSliceIterator(RichSlice(slices.toList))

    def richSliceIterator(rs: RichSlice): Iterator[A] = {
      val itrList = rs.slices.map(resolveNegative).map {
        case IndexS(j) => Iterator.single(self(j))
        case RangeS(beg, end) => {
          if (beg == starIdx) {
            if (end == starIdx) self.iterator else self.iterator.take(end)
          } else {
            if (end == starIdx) self.iterator.drop(beg) else self.iterator.slice(beg, end)
          }
        }
        case RangeStepS(beg, end, step) => {
          if (beg == starIdx) {
            if (end == starIdx) {
              if (step > 0) new IterU(self, 0, N, step) else new IterD(self, N-1, -1, step)
            } else {
              if (step > 0) new IterU(self, 0, end, step) else new IterD(self, N-1, end, step)
            }
          } else {
            if (end == starIdx) {
              if (step > 0) new IterU(self, beg, N, step) else new IterD(self, beg, -1, step)
            } else {
              if (step > 0) new IterU(self, beg, end, step) else new IterD(self, beg, end, step)
            }
          }
        }
      }
      if (itrList.length == 0) Iterator.empty
      else if (itrList.length == 1) itrList(0)
      else itrList.fold(Iterator.empty)(_ ++ _)
    }

    def richSlice(slices: Slice*): Seq[A] = richSliceIterator(RichSlice(slices.toList)).toSeq
    def richSlice(rs: RichSlice): Seq[A] = richSliceIterator(rs).toSeq

    class IterU(data: Seq[A], beg: Int, end: Int, step: Int) extends Iterator[A] {
      require(step > 0)
      var j = if (beg < end) beg else end
      def hasNext = j < end
      def next = {
        val r = data(j)
        j += step
        r
      }
    }
    class IterD(data: Seq[A], beg: Int, end: Int, step: Int) extends Iterator[A] {
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
