/*
 * whitelist.scala
 * author:  William Benton <willb@redhat.com>
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

package com.redhat.et.silex.text;

import org.apache.spark.rdd.RDD

import scala.collection.immutable.BitSet
import scala.util.hashing.{MurmurHash3, ByteswapHashing, Hashing}

private[text] trait TableHashing extends Hashing[String] {
  val table: Array[Long]
  
  // XXX: this is not sensible for wide characters
  def hashLong(s: String): Long = s.foldLeft(0L)((acc, c) => acc ^ table(c.toByte + 128))
  def hash(s: String): Int = (hashLong(s) & -1).toInt
  def hash2(s: String): Int = ((hashLong(s) >>> 32) & -1).toInt
}

private [text] object TableHash1 extends TableHashing {
  val table = {
    // spring classics
    val rnd = new scala.util.Random(0x57616c6c6f6e6961L)
    (0 until 256).map { x => rnd.nextLong}.toArray
  }
}

private [text] object TableHash2 extends TableHashing {
  val table = {
    // sand dunes
    val rnd = new scala.util.Random(0x4b6f6b73696a6465L)
    (0 until 256).map { x => rnd.nextLong}.toArray
  }
}

private [text] object AWLHash {
  val bsh = new ByteswapHashing[String]()
  val mh = MurmurHash3
  val th1 = TableHash1
  val th2 = TableHash2
  
  val shift = 15
  val mask = (1 << shift) - 1
  
  def hashes(s: String): Set[Int] = {
    val first = bsh.hash(s)
    val second = mh.stringHash(s)
    val third = TableHash1.hashLong(s)
    val fourth = TableHash2.hashLong(s)
    
    Set[Int](first & mask, 
             (first >>> shift) & mask, 
             second & mask, 
             (second >>> shift) & mask,
             third.toInt & mask, 
             (third >>> shift).toInt & mask, 
             (third >>> shift * 2).toInt & mask, 
             (third >>> shift * 3).toInt & mask,
             fourth.toInt & mask, 
             (fourth >>> shift).toInt & mask, 
             (fourth >>> shift * 2).toInt & mask, 
             (fourth >>> shift * 3).toInt & mask)
  }
}

/**
 * An <tt>ApproximateWhitelist</tt> is a basic Bloom filter intended for holding natural-language
 * vocabularies.  It deals with String values natively and can be trained from a sequence or from 
 * an RDD of any element type <tt>T</tt>, as long as there is an implicit conversion in scope
 * from <tt>T</tt> to <tt>String</tt>.
 *
 */
case class ApproximateWhitelist(val filter: BitSet) {
  /** Creates a whitelist that accepts a superset of anything accepted by <tt>this</tt> and anything accepted by <tt>other</tt>. */
  def combine(other: => ApproximateWhitelist): ApproximateWhitelist = ApproximateWhitelist(filter | other.filter)
  
  /** Adds an element to the whitelist. */
  def add[A](s: A)(implicit f: A => String) = ApproximateWhitelist(filter ++ AWLHash.hashes(f(s)))
  
  /**
   * Returns true if <tt>s</tt> is possibly contained in the whitelist and false if it definitely is not.
   */
  def maybeContains[A](s: A)(implicit f: A => String): Boolean = {
    val hashes = AWLHash.hashes(f(s))
    (filter & hashes) == hashes
  }
}

object ApproximateWhitelist {
  /**
   * An empty approximate whitelist.
   */
  val zero: ApproximateWhitelist = ApproximateWhitelist(BitSet())
  
  /**
    * Trains an approximate whitelist on a sequence of values.  Each element in the sequence should correspond to one element in the whitelist (e.g., a word).  There must be a stable implicit conversion from the sequence's element type to <tt>String</ss>s.
    */
  def train[A](source: Seq[A])(implicit f: A => String): ApproximateWhitelist = {
    source.foldLeft(zero)((awl, elt) => awl.add(f(elt)))
  }
  
  /**
    * Trains an approximate whitelist on an RDD of values.  Each element in the RDD should correspond to one element in the whitelist (e.g., a word).  There must be a stable implicit conversion from the RDD's element type to <tt>String</ss>s.
    */
  def train[A](source: RDD[A])(implicit f: A => String): ApproximateWhitelist = {
    source.aggregate(zero)((awl, elt) => awl.add(f(elt)), (a1, a2) => a1.combine(a2))
  }
}