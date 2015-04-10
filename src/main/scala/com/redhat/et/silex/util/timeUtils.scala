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

import org.joda.time.{DateTime, DateTimeZone, Period, Days}
import scala.language.implicitConversions

/** A simple structure representing a calendar date in UTC.
  * 
  * This class is deliberately extremely simple and delegates out to {{joda-time}}
  * for its actual functionality; it exists solely to abstract away our choice of 
  * date and time library.  (In JDK 8, it would probably make sense to use the new
  * standard library date and time classes.)
  *
  * If you need to deal with multiple time zones or different calendars, 
  * you're probably best served by using something more sophisticated (although the
  * {{from}} method in the companion object to convert from a [[org.joda.time.DateTime]] 
  * will convert to UTC first).
  */
case class DateTimeUTC(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, millis: Int = 0) {
  /** Converts from {{DateTimeUTC}} to {{T}}, if an appropriate implicit conversion function is in scope */
  def as[T](implicit ev: (DateTimeUTC) => T): T = ev(this)
  
  lazy val asSecondsSinceEpoch = (as[DateTime].getMillis / 1000).asInstanceOf[Int]
  lazy val dayOfYear = as[DateTime].getDayOfYear
  def daysBetween(other: DateTimeUTC) = Days.daysBetween(this.as[DateTime], other.as[DateTime]).getDays
}

object DateTimeUTC {
  implicit def dtutc2joda(d: DateTimeUTC): DateTime = 
    new DateTime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.millis, DateTimeZone.UTC)
  
  implicit def joda2dtutc(d: DateTime): DateTimeUTC = {
    val utc = d.toDateTime(DateTimeZone.UTC)
    new DateTimeUTC(utc.getYear, utc.getMonthOfYear, utc.getDayOfMonth, 
                    utc.getHourOfDay, utc.getMinuteOfHour, utc.getSecondOfMinute, 
                    utc.getMillisOfSecond)
  }
  
  def fromSecondsSinceEpoch(epoch: Int) = from(new DateTime(epoch * 1000L))
  
  /** Converts from {{T}} to {{DateTimeUTC}}, if an appropriate implicit conversion function is in scope */
  def from[T](t: T)(implicit ev: (T) => DateTimeUTC): DateTimeUTC = ev(t)
}

object Amortizer {
  val ONE_DAY = new org.joda.time.Period().withDays(1)
  
  /**
   * Amortizes some quantity over the days between {{start}} and {{end}}.
   *  
   * Returns a [[Seq]] of pairs consisting of the [[DateTimeUTC]] to which 
   * the value should be ascribed and the amortized amount.  If {{start}} and
   * {{end}} are on the same day, return a [[Seq]] with {{start}} and the whole
   * amount.
   */
  def amortize(start: DateTimeUTC, end: DateTimeUTC, amt: Double): Seq[Pair[DateTimeUTC, Double]] = {
    val db = start.daysBetween(end)
    if (db > 0) {
      val perDay = amt / db
      val startDateTime = start.as[DateTime]
      (0 until db).map { offset => 
        (
          DateTimeUTC.from(startDateTime.plus(ONE_DAY.multipliedBy(offset))), 
          perDay
        )
      }
    } else {
      Seq(Pair(start, amt))
    }
  }
}

/** A function object to convert to and from times in some particular string format */
trait TimeLens {
  def apply(date: String): DateTimeUTC
  def apply(d: DateTimeUTC): String
}

/** 
  * A function object to convert to and from times in the AWS billing format.
  * 
  * These are UTC, in the form {{YYYY-MM-DD HH:MM:SS}}.  By converting to the [[DateTimeUTC]] 
  * format, you can manipulate individual components or convert to another format for further
  * processing.
  */
object AWSTimeLens extends TimeLens {
  import RegexImplicits._
  
  def apply(date: String) = date match {
    case r"(\d\d\d\d)$year-(\d\d)$month-(\d\d)$day (\d\d)$hour:(\d\d)$minute:(\d\d)$second" => 
      DateTimeUTC(year.toInt, month.toInt, day.toInt, hour.toInt, minute.toInt, second.toInt)
  }
  
  def apply(d: DateTimeUTC) = 
    "%04d-%02d-%02d %02d:%02d:%02d".format(d.year, d.month, d.day, d.hour, d.minute, d.second)
}

object TimeUtils {
  import org.joda.time.DateTime
  def timestampToYearAndDay(secondsSinceEpoch: Int): Pair[Int, Int] = {
    val dt = new DateTime(secondsSinceEpoch * 1000L)
    (dt.getYear, dt.getDayOfYear)
  }
}