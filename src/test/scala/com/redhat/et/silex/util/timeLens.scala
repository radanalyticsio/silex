/*
 * This file is part of the "silex" library of helpers for Apache Spark.
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

package com.redhat.et.silex.util

import org.scalatest._

// most of the heavy lifting in this class is delegated to Joda, so this is really a test
// of lens-specific functionality and a smoke for deeper functionality
class TimeUtilsSpec extends FlatSpec with Matchers {
  it should "roundtrip an AWS timestamp via the AWS lens" in {
    val awsTime = "2016-01-01 01:23:45"
    assert(awsTime == AWSTimeLens(AWSTimeLens(awsTime)))
  }

  it should "convert an AWS timestamp to a DateTimeUTC via the AWS lens" in {
    val awsTime = "2016-07-08 01:23:45"
    val timeStruct = AWSTimeLens(awsTime)
    assert(timeStruct.year == 2016)
    assert(timeStruct.month == 7)
    assert(timeStruct.day == 8)
    assert(timeStruct.hour == 1)
    assert(timeStruct.minute == 23)
    assert(timeStruct.second == 45)
    
    // 189 in non-leap years
    assert(timeStruct.dayOfYear == 190)
  }
  
  it should "convert an updated DateTimeUTC to an AWS timestamp" in {
    val awsTime = "2016-07-08 01:23:45"
    val updatedTimeStruct = AWSTimeLens(awsTime).copy(year=2000, month=1, day=2, hour=12, minute=34, second=56)
    assert(AWSTimeLens(updatedTimeStruct) == "2000-01-02 12:34:56")
  }
  
  it should "roundtrip between UNIX time and DateTimeUTC objects" in {
    val timeStruct = AWSTimeLens("2016-07-08 01:23:45")
    val roundtrip = DateTimeUTC.fromSecondsSinceEpoch(timeStruct.asSecondsSinceEpoch)
    
    assert(timeStruct == roundtrip)
  }
}
