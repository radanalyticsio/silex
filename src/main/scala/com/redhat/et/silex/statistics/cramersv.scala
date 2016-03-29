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
  /**
   * Calculate Cramer's V for two sets (`values1` and `values2`) of co-sampled values.
   */
  def apply[T, U](values1 : Seq[T], values2 : Seq[U]) : Double = {
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

      val combinations = set1.flatMap {
        v1 =>
          set2.map {
            v2 =>
              (v1, v2)
          }
      }
      .toSeq

      val chi2 = combinations.map {
        case (value1, value2) =>
          val nij = pairCounts.getOrElse((value1, value2), 0.0)
          val ni = counts1.getOrElse(value1, 0.0)
          val nj = counts2.getOrElse(value2, 0.0)

          val b = ni * nj / nObs
          val c = (nij - b) * (nij - b) / b

          c
      }
      .sum

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
   * The parameter `rounds` indicates how many permutations to generate. A random number
   * generator can be provided (if desired) via the `rng` parameter.
   */
  def permutationTest[T, U](values1 : Seq[T], values2 : Seq[U], rounds : Int, rng : Random = new Random()) : Double = {

    val testV = CramersV(values1, values2)
    
    val worseCount = (1 to rounds).map {
      i =>
        val shuffled = rng.shuffle(values1)
        CramersV(shuffled, values2)
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
