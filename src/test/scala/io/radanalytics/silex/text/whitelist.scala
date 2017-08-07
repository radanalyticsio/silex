/*
 * whitelist.scala
 * author:  William Benton <willb@redhat.com>
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

package io.radanalytics.silex.text

import io.radanalytics.silex.testing.PerTestSparkContext

import org.scalatest._

class ApproxWhitelistSpec extends FlatSpec with Matchers with PerTestSparkContext {
  val wordsURL = getClass.getResource("/74550-common.txt")
  
  it should "include all elements in the specified whitelist when training from an RDD" in {
    val rdd = context.textFile(wordsURL.toString)
    val Array(whitelist, other) = rdd.randomSplit(Array(0.2, 0.8), 0xdeadbeefl)
    val awl = ApproximateWhitelist.train(whitelist)
    assert(whitelist.filter(s => (awl maybeContains s)).count() == whitelist.count())
  }
  
  it should "include all elements in the specified whitelist when training from a sequence" in {
    val rdd = context.textFile(wordsURL.toString)
    val Array(whitelist, other) = rdd.randomSplit(Array(0.2, 0.8), 0xdeadbeefl)
    val awl = ApproximateWhitelist.train(whitelist.collect())
    assert(whitelist.filter(s => (awl maybeContains s)).count() == whitelist.count())
  }

  it should "reject at least some elements not in the specified whitelist when training from an RDD" in {
    val rdd = context.textFile(wordsURL.toString)
    val Array(whitelist, other) = rdd.randomSplit(Array(0.2, 0.8), 0xdeadbeefl)
    val awl = ApproximateWhitelist.train(whitelist)
    assert(other.filter(s => (awl maybeContains s)).count() < other.count())
  }
  
  it should "reject at least some elements not in the specified whitelist when training from a sequence" in {
    val rdd = context.textFile(wordsURL.toString)
    val Array(whitelist, other) = rdd.randomSplit(Array(0.2, 0.8), 0xdeadbeefl)
    val awl = ApproximateWhitelist.train(whitelist.collect())
    assert(other.filter(s => (awl maybeContains s)).count() < other.count())
  }
}
