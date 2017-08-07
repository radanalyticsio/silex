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
 * Provides idiomatic optional parameter values on top of [[Option]], but which can be passed in as
 * raw values, instead of having to pass in Some(value)
 * {{{
 * import io.radanalytics.silex.util.OptionalArg
 *
 * def possiblyFilter(data: Seq[Int], filterMax: OptionalArg[Int] = None) = {
 *   // Treat an OptionalArg like an Option, for idiomatic handling of unset vals as None
 *   if (filterMax.isEmpty) data else data.filter(_ <= filterMax.get)
 * }
 *
 * val data = Seq(1, -1, 2)
 *
 * // default filterMax is None
 * possiblyFilter(data) // returns data unfiltered
 *
 * // provide argument as (-1), instead of having to specify Some(-1)
 * possiblyFilter(data, filterMax = -1) // returns Seq(-1)
 *
 * // Support full Option methods and implicit conversion to Option
 * import OptionalArg.fullOptionSupport
 * def possiblyFilterUsingMap(data: Seq[Int], filterMax: OptionalArg[Int] = None) = {
 *   // use the 'map' method on an OptionalArg:
 *   filterMax.map(t => data.filter(_ <= t)).getOrElse(data)
 * }
 * }}}
 *
 * @tparam A The underlying value type.
 * @param option The raw Option value.  May be implicitly constructed from raw value of type A
 */
class OptionalArg[A](val option: Option[A]) extends AnyVal {
  /** Determine if the optional argument is empty (contains None)
   * @return true if the option is None, false otherwise
   */
  def isEmpty = option.isEmpty

  /** Determine if the optional argument is defined (contains Some(value))
   * @return true if option contains Some(value), false otherwise
   */
  def isDefined = option.isDefined

  /** Get the value of the optional argument, assuming the argument is not empty.
   * @return the value of the optional argument, or throw an exception if it is None
   */
  def get = option.get

  /** Get the value of the optional argument if it is defined, otherwise return the given default.
   * @param d A block evaluating to a default value, if the the optional value is not defined
   * @return The value of the optional argument if one was defined, otherwise the value of d
   */
  def getOrElse[A1 >: A](d: => A1): A1 = option.getOrElse(d)
}

/** Factory methods and implicit conversions from raw values and Option to [[OptionalArg]] */
object OptionalArg {
  import scala.language.implicitConversions

  /** Obtain an empty OptionalArg of type A
   * @return An empty OptionalArg (containing value None)
   */
  def empty[A] = new OptionalArg[A](None)

  /** Obtain an optional argument from a raw value
   * @param v the raw value to use
   * @return An optional argument containing Some(v)
   */
  def apply[A](v: A): OptionalArg[A] = new OptionalArg(Some(v))

  /** Obtain an optional argument from an Option
   * @param o The Option value
   * @return An OptionalArg containing the Option
   */
  def apply[A](o: Option[A]): OptionalArg[A] = new OptionalArg(o)

  /** Implicit conversion of a raw value to an OptionalArg -- used to allow optional arguments 
   * to be passed as raw values
   * @param v The raw value
   * @return An OptionalArg containing Some(v)
   */
  implicit def toOptionalArgFromValue[A](v: A) = new OptionalArg(Some(v))

  /** Implicit conversion of an Option to an OptionalArg -- allows arguments to be passed as an 
   * Option
   * @param o The Option
   * @return An OptionalArg containing the Option
   */
  implicit def toOptionalArgFromOption[A](o: Option[A]) = new OptionalArg(o)

  /** Implicit conversion of an OptionArg to an Option -- allows all Option methods to be called
   * on an OptionalArg, and allows an OptionalArg to be used as an Option value
   * @param a The OptionalArg value
   * @return The underlying Option value
   */ 
  implicit def fullOptionSupport[A](a: OptionalArg[A]): Option[A] = a.option
}
