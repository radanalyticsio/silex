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

package com.redhat.et.silex.util.VectorID

import org.apache.spark.mllib.linalg.{Vector => MLVec, DenseVector => MLDenseVec, SparseVector => MLSparseVec}

class DenseVectorID[ID](val data: Array[Double], val id: ID) extends MLDenseVec(data)

object DenseVectorID {
  def apply[ID](data: Array[Double], id: ID) = new DenseVectorID[ID](data, id)
  def unapply[ID](v: DenseVectorID[ID]): Option[(Array[Double], ID)] =
    Some((v.data, v.id))
}

class SparseVectorID[ID](val sz: Int, val idx: Array[Int], val data: Array[Double], val id: ID)
    extends MLSparseVec(sz, idx, data)

object SparseVectorID {
  def apply[ID](sz: Int, idx: Array[Int], data: Array[Double], id: ID) =
    new SparseVectorID[ID](sz, idx, data, id)
  def unapply[ID](v: SparseVectorID[ID]): Option[(Int, Array[Int], Array[Double], ID)] =
    Some((v.sz, v.idx, v.data, v.id))
}
