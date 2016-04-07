/*
 * cramersv.scala
 * author:  RJ Nowling <rnowling@redhat.com>
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
 * limitations under the License.
 */

package com.redhat.et.silex.statistics

import scala.util.Random

/**
 * "[[https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V Cramers' V]] is a measure of
 * association between two nominal variables, giving a value between 0 and +1
 * (inclusive)." 
 */
object CramersV {
  private def cross[A, B](sa: TraversableOnce[A], sb: TraversableOnce[B]): Iterator[(A, B)] =
    for(
      a <- sa.toIterator;
      b <- sb.toIterator
    ) yield (a, b)

  /**
   * Calculate Cramer's V for a collection of values co-sampled from two
   * variables.
   *
   * @param values Sequence of 2-tuples containing co-sampled values
   * @return Cramer's V
   */
  def cramersV[T, U](values : Seq[(T, U)]) : Double = {
    val values1 = values.map { _._1 }
    val values2 = values.map { _._2 }

    cramersV(values1, values2)
  }

  /**
   * Calculate Cramer's V for two sets (`values1` and `values2`) of co-sampled values.
   *
   * @param values1 Values sampled from variable 1
   * @param values2 Values sampled from variable 2
   * @return Cramer's V
   */
  def cramersV[T, U](values1 : Seq[T], values2 : Seq[U]) : Double = {
    if (values1.size != values2.size) {
      throw new IllegalArgumentException("Value sequences must be the same length.")
    }

    val set1 = values1.toSet
    val set2 = values2.toSet

    if (set1.size == 1 && set2.size == 1) {
      1.0
    } else if (values1.size == 0 || set1.size == 1 || set2.size == 1) {
      0.0
    } else {
      val pairCounts = values1.zip(values2)
        .foldLeft(Map.empty[(T, U), Double]) {
        case (counts, value) =>
          counts + (value -> (counts.getOrElse(value, 0.0) + 1.0))
      }

      val counts1 = values1
        .foldLeft(Map.empty[T, Double]) {
        case (counts, value) =>
          counts + (value -> (counts.getOrElse(value, 0.0) + 1.0))
      }

      val counts2 = values2
        .foldLeft(Map.empty[U, Double]) {
        case (counts, value) =>
          counts + (value -> (counts.getOrElse(value, 0.0) + 1.0))
      }

      val nObs = values1.size.toDouble

      val chi2 = cross(set1, set2)
        .foldLeft(0.0) {
          case (runningSum, (value1, value2)) =>
            val nij = pairCounts.getOrElse((value1, value2), 0.0)
            val ni = counts1.getOrElse(value1, 0.0)
            val nj = counts2.getOrElse(value2, 0.0)

            val b = ni * nj / nObs
            val c = (nij - b) * (nij - b) / b

            runningSum + c
        }

      val minDim = math.min(set1.size - 1, set1.size - 1).toDouble
      
      val v = math.sqrt(chi2 / nObs / minDim)

      v
    }
  }

  /**
   * Perform a permutation test to get a p-value indicating the probability of getting
   * a lower assocation value.  Take the association level as the null hypothesis, reject
   * if the p-value is less than your desired threshold.
   *
   * @param values Values co-sampled from variables 1 and 2
   * @param rounds Number of permutations to generate
   * @param rng (Optional) Random number generator used to generate permutations
   * @return p-value giving the probability of getting a lower association value
   */
  def permutationTest[T, U](values : Seq[(T, U)], rounds : Int, rng : Random = new Random()) : Double = {
    val values1 = values.map { _._1 }
    val values2 = values.map { _._2 }

    permutationTest(values1, values2, rounds, rng)
  }

  /**
   * Perform a permutation test to get a p-value indicating the probability of getting
   * a lower assocation value.  Take the association level as the null hypothesis, reject
   * if the p-value is less than your desired threshold.
   *
   * @param values1 Values sampled from variable 1
   * @param values2 Values sampled from variable 2
   * @param rounds Number of permutations to generate
   * @param rng (Optional) Random number generator used to generate permutations
   * @return p-value giving the probability of getting a lower association value
   */
  def permutationTest[T, U](values1 : Seq[T], values2 : Seq[U], rounds : Int, rng : Random = new Random()) : Double = {

    val testV = cramersV(values1, values2)
    
    val worseCount = (1 to rounds).iterator.map {
      i =>
        val shuffled = rng.shuffle(values1)
        cramersV(shuffled, values2)
    }
    .filter {
      v =>
        v < testV
    }
    .size

    val pvalue = worseCount.toDouble / rounds.toDouble

    pvalue
  }
}
