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

/** A simple class to abstract away our choice of date and time library */
case class DateTimeUTC(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, millis: Int = 0) {
  def as[T](implicit ev: (DateTimeUTC) => T): T = ev(this)
  
  lazy val asSecondsFromEpoch = as[DateTime].getMillis / 1000
  lazy val dayOfYear = as[DateTime].getDayOfYear
  def daysBetween(other: DateTimeUTC) = Days.daysBetween(this.as[DateTime], other.as[DateTime]).getDays
}

object DateTimeUTC {
  implicit def dtutc2joda(d: DateTimeUTC): DateTime = 
    new DateTime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.millis, DateTimeZone.UTC)

  def from[T](t: T)(implicit ev: (T) => DateTimeUTC): DateTimeUTC = ev(t)
}

object Amortizer {
  val ONE_DAY = new org.joda.time.Period().withDays(1)
  
  def amortize(start: DateTimeUTC, end: DateTimeUTC, amt: Double) = {
    val db = start.daysBetween(end)
    if (db > 0) {
      val perDay = amt / db
      (0 until db).map { offset => (start.as[DateTime].plus(ONE_DAY.multipliedBy(offset)), perDay)}
    } else {
      Seq(Pair(start.as[DateTime], amt))
    }
  }
}

/** A function object to convert to and from times in some particular string format */
trait TimeConverter {
  def apply(date: String): DateTimeUTC
  def apply(d: DateTimeUTC): String = d.toString
}

/** A function object to convert to and from times in the AWS billing format */
object AWSTimeConverter extends TimeConverter {
  import RegexImplicits._
  
  def apply(date: String) = date match {
    case r"(\d\d\d\d)$year-(\d\d)$month-(\d\d)$day (\d\d)$hour:(\d\d)$minute:(\d\d)$second" => 
      DateTimeUTC(year.toInt, month.toInt, day.toInt, hour.toInt, minute.toInt, second.toInt)
  }
  
  override def apply(d: DateTimeUTC) = 
    "%04d-%02d-%02d %02d:%02d:%02d".format(d.year, d.month, d.day, d.hour, d.minute, d.second)
}

object TimeUtils {
  import org.joda.time.DateTime
  def timestampToYearAndDay(secondsSinceEpoch: Int): Pair[Int, Int] = {
    val dt = new DateTime(secondsSinceEpoch * 1000L)
    (dt.getYear, dt.getDayOfYear)
  }
}