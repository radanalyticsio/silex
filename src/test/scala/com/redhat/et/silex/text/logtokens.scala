/*
 * logtokens.scala
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

package io.radanalytics.silex.text;

import org.scalatest._

class LogTokensSpec extends FlatSpec with Matchers {
  it should "split a simple message into the appropriate sequence of tokens" in {
    assert(LogTokenizer.tokens("abc def ghi jkl").equals(Seq("abc", "def", "ghi", "jkl")))
  }
  
  it should "split a message with extra whitespace into the appropriate sequence of tokens" in {
    assert(LogTokenizer.tokens("abc   def     ghi        jkl").equals(Seq("abc", "def", "ghi", "jkl")))
  }
  
  it should "strip intertoken punctuation" in {
    assert(LogTokenizer.tokens("+abc def. =ghi@ .jkl").equals(Seq("abc", "def", "ghi", "jkl")))
  }
  
  it should "strip rejected intratoken punctuation" in {
    assert(LogTokenizer.tokens("a#bc de$f g()hi j!kl").equals(Seq("abc", "def", "ghi", "jkl")))
  }
  
  it should "retain acceptable intratoken punctuation" in {
    assert(LogTokenizer.tokens("a-bc /de/f g.hi j@kl").equals(Seq("a-bc", "/de/f", "g.hi", "j@kl")))
  }
  
  it should "retain acceptable intratoken punctuation while stripping rejected punctuation" in {
    assert(LogTokenizer.tokens("a-b#c /d$e/f =g.hi .j@k()l").equals(Seq("a-bc", "/de/f", "g.hi", "j@kl")))
  }
  
  it should "support postprocessing and postfiltering" in {
    assert(LogTokenizer.tokens("cba fed ihg lkj augh", {_.reverse}, {_.length == 3}).equals(Seq("abc", "def", "ghi", "jkl")))
  }
}