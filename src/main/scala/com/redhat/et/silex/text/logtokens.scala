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

package com.redhat.et.silex.text;

trait LogTokenizing {
  import scala.util.matching.Regex
  
  private [text] val spaces = new Regex("[\\s]+")
  private [text] val oneletter = new Regex(".*([A-Za-z_-]).*")
  private [text] val defaultRejectedIntratokenPunctuation = new Regex("[^A-Za-z0-9-_./:@]")
  private [text] val defaultLeadingPunctuation = new Regex("(\\s)[^\\sA-Za-z0-9-_/]+|()^[^\\sA-Za-z0-9-_/]+")
  private [text] val defaultTrailingPunctuation = new Regex("[^\\sA-Za-z0-9-_/]+(\\s)|()[^\\sA-Za-z0-9-_/]+$")
  
  /**
    * A regular expression describing punctuation to strip from the beginning of tokens; matches will be stripped by 
    * replacing them with their first match group.  Override this definition to customize tokenizer behavior.
    * Defaults to <pre>"(\\s)[^\\sA-Za-z0-9-_/]+|()^[^\\sA-Za-z0-9-_/]+"</pre>.
    */
  def leadingPunctuation: Regex = defaultLeadingPunctuation

  /**
    * A regular expression describing punctuation to strip from the end of tokens; matches will be stripped by 
    * replacing them with their first match group.  Override this definition to customize tokenizer behavior.
    * Defaults to <pre>"[^\\sA-Za-z0-9-_/]+(\\s)|()[^\\sA-Za-z0-9-_/]+$"</pre> if not overridden.
    */
  def trailingPunctuation: Regex = defaultTrailingPunctuation
  
  /**
    * A regular expression describing punctuation to strip from within tokens; matches will be stripped by 
    * replacing them with the empty string.  Override this definition to customize tokenizer behavior.
    * Defaults to <pre>"[^A-Za-z0-9-_./:@]"</pre> if not overridden.
    */
  def rejectedIntratokenPunctuation: Regex = defaultRejectedIntratokenPunctuation
    
  private [text] def replace(r: Regex, s: String) = { (orig:String) => r.replaceAllIn(orig, s) }

  private [text] val collapseWhitespace: String => String = replace(spaces, " ")
  private [text] val stripPunctuation: String => String = 
    replace(leadingPunctuation, "$1") compose 
    replace(trailingPunctuation, "$1") compose 
    replace(rejectedIntratokenPunctuation, "")
  
  /**
    * Splits a log message into a sequence of tokens, by 
    * <ol>
    *  <li>collapsing runs of whitespace into single spaces,</li>
    *  <li>stripping rejected intertoken punctuation,</li>
    *  <li>stripping rejected intratoken punctuation,</li>
    *  <li>splitting on whitespace,</li>
    *  <li>rejecting candidate tokens not containing at least one letter, and</li>
    *  <li>applying optional user-supplied transformation and filtering functions.</li>
    * </ol>
    * 
    * @param msg: the log message to split
    * @param post: an optional postprocessing function to apply to tokens after splitting; defaults to <pre>identity[String]</pre>
    * @param pred: an optional filtering function, applied after rejecting all tokens that don't contain at least one letter.  Should return <pre>true</pre> for tokens to keep and <pre>false</pre> for tokens to reject.
    * @return a sequence of tokens
    * @see <a href="http://chapeau.freevariable.com/2015/12/using-word2vec-on-log-messages.html">Using word2vec on log messages</a>
    */
  def tokens(msg: String, post: String=>String = identity[String], pred: String=>Boolean = { str => true}): Seq[String] = 
    collapseWhitespace(msg)
      .split(" ")
      .map(s => post(stripPunctuation(s)))
      .collect { case token @ oneletter(_) if pred(token) => token } 
}

object LogTokenizer extends LogTokenizing { }
