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

  sealed abstract class RichSliceOp

  private case class IndexRS(idx: Int) extends RichSliceOp
  private case class RangeRS(beg: Int, end: Int) extends RichSliceOp
  private case class RangeStepRS(beg: Int, end: Int, step: Int) extends RichSliceOp {
    require(step != 0)
  }

  implicit def intToIndexRS(idx: Int): RichSliceOp = IndexRS(idx)

  implicit def rangeToRangeRS(r: Range): RichSliceOp = {
    val d = if (!r.isInclusive || r.end == star) 0 else if (r.step > 0) 1 else -1
    if (r.step == 1) RangeRS(r.start, r.end + d) else RangeStepRS(r.start, r.end + d, r.step)
  }

  abstract class RichSliceStar {
    def to(j: Int): Range = star to j
    def to(s: RichSliceStar): Range = star to star
    def until(j: Int): Range = star until j
    def until(s: RichSliceStar): Range = star until star
    def by(step: Int): Range = star until star by step
  }
  private val star = scala.Int.MinValue
  object starobj extends RichSliceStar
  def * = starobj

  implicit def starToRangeRS(s: RichSliceStar): RichSliceOp = RangeStepRS(star, star, 1)

  class RichSliceIntWrapper(n: Int) {
    def to(s: RichSliceStar): Range = n to star
    def until(s: RichSliceStar): Range = n until star
  }
  implicit def toRichSliceIntWrapper(n: Int) = new RichSliceIntWrapper(n)

  case class RichSliceSeq(ops: Seq[RichSliceOp]) extends Serializable

  object RichSlice {
    def apply(ops: RichSliceOp*) = RichSliceSeq(ops)
  }

  class RichSliceSeqWrapper[A](self: Seq[A]) {
    val N = self.length

    def richSliceIterator(ops: RichSliceOp*): Iterator[A] =
      richSliceIterator(RichSliceSeq(ops))

    def richSliceIterator(rs: RichSliceSeq): Iterator[A] = {
      val itrList = rs.ops.map(resolveNegative).map { op =>
        op match {
          case IndexRS(j) => Iterator.single(self(j))
          case RangeRS(beg, end) => {
            if (beg == star) {
              if (end == star) { self.iterator } else { self.iterator.take(end) }
            } else {
              if (end == star) { self.iterator.drop(beg) } else { self.iterator.slice(beg, end) }
            }
          }
          case RangeStepRS(beg, end, step) => {
            if (beg == star) {
              if (end == star) {
                if (step > 0) new IterU(self, 0, N, step) else new IterD(self, N-1, -1, step)
              } else {
                if (step > 0) new IterU(self, 0, end, step) else new IterD(self, N-1, end, step)
              }
            } else {
              if (end == star) {
                if (step > 0) new IterU(self, beg, N, step) else new IterD(self, beg, -1, step)
              } else {
                if (step > 0) new IterU(self, beg, end, step) else new IterD(self, beg, end, step)
              }
            }
          }
        }
      }
      if (itrList.length == 0) Iterator.empty
      else if (itrList.length == 1) itrList(0)
      else itrList.fold(Iterator.empty)(_ ++ _)
    }

    def richSlice(ops: RichSliceOp*): Seq[A] = richSlice(RichSliceSeq(ops))

    def richSlice(rs: RichSliceSeq): Seq[A] = {
      val r = scala.collection.mutable.ArrayBuffer.empty[A]
      rs.ops.map(resolveNegative).foreach { op =>
        op match {
          case IndexRS(j) => { r += self(j) }
          case RangeRS(beg, end) => {
            if (beg == star) {
              if (end == star) { r ++= self } else { r ++= self.take(end) }
            } else {
              if (end == star) { r ++= self.drop(beg) } else { r ++= self.slice(beg, end) }
            }
          }
          case RangeStepRS(beg, end, step) => {
            if (beg == star) {
              if (end == star) {
                if (step > 0) doStep(0, N, step, r) else doStep(N-1, -1, step, r)
              } else {
                if (step > 0) doStep(0, end, step, r) else doStep(N-1, end, step, r)
              }
            } else {
              if (end == star) {
                if (step > 0) doStep(beg, N, step, r) else doStep(beg, -1, step, r)
              } else {
                doStep(beg, end, step, r)
              }
            }
          }
        }
      }
      r
    }

    private def doStep(beg: Int, end: Int, step: Int, r: scala.collection.mutable.ArrayBuffer[A]) {
      require(step != 0)
      var j = beg
      if (step > 0) {
        if (beg < end) {
          while (j < end) {
            r += self(j)
            j += step
          }
        } 
      } else {
        if (beg > end) {
          while (j > end) {
            r += self(j)
            j += step
          }
        } 
      }
    }

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

    private def resolveNegative(op: RichSliceOp) = {
      def resolve(j: Int, step: Int = 1) = {
        if (j >= 0) j
        else if (j == star) j
        else if (j == -1  &&  step < 0) j
        else N + j
      }
      op match {
        case IndexRS(j) => IndexRS(resolve(j))
        case RangeRS(b, e) => RangeRS(resolve(b), resolve(e))
        case RangeStepRS(b, e, s) => RangeStepRS(resolve(b), resolve(e, s), s)
      }
    }
  }
  implicit def toRichSliceSeqWrapper[A](self: Seq[A]): RichSliceSeqWrapper[A] =
    new RichSliceSeqWrapper[A](self)
  implicit def arrayToRichSliceSeqWrapper[A](self: Array[A]): RichSliceSeqWrapper[A] =
    new RichSliceSeqWrapper[A](self.toSeq)
}
